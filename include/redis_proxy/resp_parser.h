#pragma once

#include <cstddef>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include "redis/resp.h"
#include "redis_proxy/buffer.h"

namespace redis_proxy {

enum class ParseStatus { kOk, kNeedMore, kError, kNoMemory };

struct RespFrameInfo {
  std::size_t consumed = 0;
  std::string command_name;
  std::size_t argc = 0;
};

class RespParser {
public:
  RespParser();

  void setLimits(std::size_t max_bulk_bytes, std::size_t max_array_elements,
                 std::size_t max_depth,
                 std::size_t max_frame_bytes =
                     std::numeric_limits<std::size_t>::max());
  ParseStatus peekFrame(IoBuffer& input, std::size_t offset,
                        RespFrameInfo* out);
  ParseStatus nextReplyFrame(IoBuffer& input, BufferChain* out,
                             std::size_t* consumed);

private:
  redis::RespLimits limits_;
  std::vector<redis::RespValue> scratch_;
  std::size_t max_frame_bytes_ = std::numeric_limits<std::size_t>::max();

  redis::RespResult unpack(std::string_view input);
  ParseStatus convert(redis::RespStatus status) const;
  bool extractCommandInfo(const redis::RespValue& value,
                          RespFrameInfo* out) const;
};

}  // namespace redis_proxy
