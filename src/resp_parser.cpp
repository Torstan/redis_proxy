#include "redis_proxy/resp_parser.h"

#include <algorithm>
#include <new>
#include <utility>

namespace redis_proxy {

RespParser::RespParser() { setLimits(1024 * 1024, 1024, 8); }

void RespParser::setLimits(std::size_t max_bulk_bytes,
                           std::size_t max_array_elements,
                           std::size_t max_depth,
                           std::size_t max_frame_bytes) {
  limits_.max_bulk_bytes = max_bulk_bytes;
  limits_.max_array_elements = max_array_elements;
  limits_.max_depth = max_depth;
  max_frame_bytes_ = max_frame_bytes;
}

redis::RespResult RespParser::unpack(std::string_view input) {
  input = input.substr(0, max_frame_bytes_);
  // Every encoded RESP value occupies at least three bytes. A declaration
  // without its payload must not allocate an arena proportional to its count.
  const std::size_t max_slots = input.size() / 3 + 1;
  try {
    if (scratch_.empty()) scratch_.resize(std::min<std::size_t>(16, max_slots));
    for (;;) {
      const std::size_t slots = std::min(scratch_.size(), max_slots);
      auto result = redis::UnpackOne(input, scratch_.data(), slots, limits_);
      if (result.status == redis::RespStatus::kNoMemory) {
        if (slots < max_slots) {
          scratch_.resize(slots + std::min(slots, max_slots - slots));
          continue;
        }
        result.status = redis::RespStatus::kNeedMore;
      }
      if (result.status == redis::RespStatus::kNeedMore &&
          input.size() >= max_frame_bytes_) {
        result.status = redis::RespStatus::kError;
      }
      return result;
    }
  } catch (const std::bad_alloc&) {
    return {redis::RespStatus::kNoMemory, 0, nullptr, "scratch allocation failed"};
  }
}

ParseStatus RespParser::convert(redis::RespStatus status) const {
  switch (status) {
    case redis::RespStatus::kOk: return ParseStatus::kOk;
    case redis::RespStatus::kNeedMore: return ParseStatus::kNeedMore;
    case redis::RespStatus::kNoMemory: return ParseStatus::kNoMemory;
    case redis::RespStatus::kError: return ParseStatus::kError;
  }
  return ParseStatus::kError;
}

bool RespParser::extractCommandInfo(const redis::RespValue& value,
                                    RespFrameInfo* out) const {
  if (value.type != redis::RespType::kArray || value.element_count == 0 ||
      value.elements == nullptr) {
    return false;
  }
  for (std::size_t i = 0; i < value.element_count; ++i) {
    if (value.elements[i].type != redis::RespType::kBulkString) return false;
  }
  out->argc = value.element_count;
  const auto& command = value.elements[0].text;
  out->command_name.assign(command.data(), command.size());
  return true;
}

ParseStatus RespParser::peekFrame(IoBuffer& input, std::size_t offset,
                                  RespFrameInfo* out) {
  if (offset > input.readableBytes()) return ParseStatus::kError;
  auto result = unpack(input.readableView().substr(offset));
  if (result.status != redis::RespStatus::kOk) return convert(result.status);
  RespFrameInfo info;
  if (!extractCommandInfo(*result.value, &info)) return ParseStatus::kError;
  info.consumed = result.consumed;
  *out = std::move(info);
  return ParseStatus::kOk;
}

ParseStatus RespParser::nextReplyFrame(IoBuffer& input, BufferChain* out,
                                       std::size_t* consumed) {
  auto result = unpack(input.readableView());
  if (result.status != redis::RespStatus::kOk) return convert(result.status);
  *consumed = result.consumed;
  *out = input.slicePrefix(result.consumed);
  return ParseStatus::kOk;
}

}  // namespace redis_proxy
