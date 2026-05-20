#include "redis_proxy/resp_parser.h"

#include <utility>

namespace redis_proxy {

RespParser::RespParser() {
  limits_.max_bulk_bytes = 1024 * 1024;
  limits_.max_array_elements = 1024;
  limits_.max_depth = 8;
}

void RespParser::setLimits(std::size_t max_bulk_bytes,
                           std::size_t max_array_elements,
                           std::size_t max_depth) {
  limits_.max_bulk_bytes = max_bulk_bytes;
  limits_.max_array_elements = max_array_elements;
  limits_.max_depth = max_depth;
}

ParseStatus RespParser::convert(redis::RespStatus status) const {
  switch (status) {
    case redis::RespStatus::kOk:
      return ParseStatus::kOk;
    case redis::RespStatus::kNeedMore:
      return ParseStatus::kNeedMore;
    case redis::RespStatus::kNoMemory:
      return ParseStatus::kNoMemory;
    case redis::RespStatus::kError:
      return ParseStatus::kError;
  }
  return ParseStatus::kError;
}

bool RespParser::extractCommand(const redis::RespValue& value,
                                CommandView* out) const {
  if (value.type != redis::RespType::kArray || value.element_count == 0 ||
      value.elements == nullptr) {
    return false;
  }
  out->argc = value.element_count;
  out->args.clear();
  out->args.reserve(value.element_count);
  for (std::size_t i = 0; i < value.element_count; ++i) {
    const redis::RespValue& element = value.elements[i];
    if (element.type != redis::RespType::kBulkString) {
      return false;
    }
    out->args.push_back(element.text);
  }
  out->name.assign(out->args[0].data(), out->args[0].size());
  return true;
}

bool RespParser::extractCommandInfo(const redis::RespValue& value,
                                    RespFrameInfo* out) const {
  if (value.type != redis::RespType::kArray || value.element_count == 0 ||
      value.elements == nullptr) {
    return false;
  }
  for (std::size_t i = 0; i < value.element_count; ++i) {
    if (value.elements[i].type != redis::RespType::kBulkString) {
      return false;
    }
  }
  out->argc = value.element_count;
  const redis::RespValue& command = value.elements[0];
  out->command_name.assign(command.text.data(), command.text.size());
  return true;
}

ParseStatus RespParser::peekFrame(IoBuffer& input, std::size_t offset,
                                  RespFrameInfo* out) {
  const std::size_t available = input.readableBytes();
  if (offset > available) {
    return ParseStatus::kError;
  }
  if (offset == available) {
    return ParseStatus::kNeedMore;
  }
  if (!input.ensureContiguousPrefix(available)) {
    return ParseStatus::kNeedMore;
  }
  std::string_view view = input.contiguousPrefixForTest(available);
  view.remove_prefix(offset);

  redis::RespResult result =
      redis::UnpackOne(view, scratch_.data(), scratch_.size(), limits_);
  if (result.status != redis::RespStatus::kOk) {
    return convert(result.status);
  }

  RespFrameInfo info;
  if (!extractCommandInfo(*result.value, &info)) {
    return ParseStatus::kError;
  }
  info.consumed = result.consumed;
  *out = std::move(info);
  return ParseStatus::kOk;
}

ParseStatus RespParser::nextFrame(IoBuffer& input, RespFrame* out) {
  const std::size_t available = input.readableBytes();
  if (available == 0) {
    return ParseStatus::kNeedMore;
  }
  if (!input.ensureContiguousPrefix(available)) {
    return ParseStatus::kNeedMore;
  }
  const std::string_view view = input.contiguousPrefixForTest(available);
  redis::RespResult result =
      redis::UnpackOne(view, scratch_.data(), scratch_.size(), limits_);
  if (result.status != redis::RespStatus::kOk) {
    return convert(result.status);
  }
  CommandView command;
  if (!extractCommand(*result.value, &command)) {
    return ParseStatus::kError;
  }
  out->consumed = result.consumed;
  out->command = std::move(command);
  out->bytes = input.slicePrefix(result.consumed);
  return ParseStatus::kOk;
}

ParseStatus RespParser::nextReplyFrame(IoBuffer& input, BufferChain* out,
                                       std::size_t* consumed) {
  const std::size_t available = input.readableBytes();
  if (available == 0) {
    return ParseStatus::kNeedMore;
  }
  if (!input.ensureContiguousPrefix(available)) {
    return ParseStatus::kNeedMore;
  }
  const std::string_view view = input.contiguousPrefixForTest(available);
  redis::RespResult result =
      redis::UnpackOne(view, scratch_.data(), scratch_.size(), limits_);
  if (result.status != redis::RespStatus::kOk) {
    return convert(result.status);
  }
  *consumed = result.consumed;
  *out = input.slicePrefix(result.consumed);
  return ParseStatus::kOk;
}

}  // namespace redis_proxy
