#pragma once

#include <array>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

#include "redis/resp.h"
#include "redis_proxy/buffer.h"

namespace redis_proxy {

enum class ParseStatus { kOk, kNeedMore, kError, kNoMemory };

struct CommandView {
  std::string name;
  std::vector<std::string_view> args;
  std::size_t argc = 0;
};

struct RespFrame {
  BufferChain bytes;
  CommandView command;
  std::size_t consumed = 0;
};

struct RespFrameInfo {
  std::size_t consumed = 0;
  std::string command_name;
  std::size_t argc = 0;
};

class RespParser {
public:
  RespParser();

  void setLimits(std::size_t max_bulk_bytes, std::size_t max_array_elements,
                 std::size_t max_depth);
  ParseStatus nextFrame(IoBuffer& input, RespFrame* out);
  ParseStatus peekFrame(IoBuffer& input, std::size_t offset,
                        RespFrameInfo* out);
  ParseStatus nextReplyFrame(IoBuffer& input, BufferChain* out,
                             std::size_t* consumed);

private:
  redis::RespLimits limits_;
  std::array<redis::RespValue, 2048> scratch_;

  ParseStatus convert(redis::RespStatus status) const;
  bool extractCommand(const redis::RespValue& value, CommandView* out) const;
  bool extractCommandInfo(const redis::RespValue& value,
                          RespFrameInfo* out) const;
};

}  // namespace redis_proxy
