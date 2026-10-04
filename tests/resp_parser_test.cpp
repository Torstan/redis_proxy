#include "redis_proxy/buffer.h"
#include "redis_proxy/resp_parser.h"
#include "test_common.h"

#include <iostream>
#include <new>
#include <string>

namespace {
bool track_allocations = false;
std::size_t allocated_bytes = 0;
}

// Observe the resource contract without exposing parser internals to tests.
void* operator new(std::size_t size) {
  if (track_allocations) allocated_bytes += size;
  if (void* p = std::malloc(size == 0 ? 1 : size)) return p;
  throw std::bad_alloc();
}

void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

int main() {
  redis_proxy::BlockPool pool(64);
  redis_proxy::IoBuffer input(&pool);
  input.append("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");

  redis_proxy::RespParser parser;
  redis_proxy::RespFrameInfo frame;
  redis_proxy::ParseStatus status = parser.peekFrame(input, 0, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(frame.consumed == 22);
  RP_REQUIRE(frame.argc == 2);
  RequireEqual(frame.command_name, "GET");
  RP_REQUIRE(input.slicePrefix(frame.consumed).toStringForTest() ==
             "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");

  input.append("*1\r\n$4\r\nPING");
  status = parser.peekFrame(input, 0, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kNeedMore);

  input.append("\r\n");
  status = parser.peekFrame(input, 0, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RequireEqual(frame.command_name, "PING");
  input.consume(frame.consumed);

  input.append("PING\r\n");
  status = parser.peekFrame(input, 0, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kError);

  redis_proxy::IoBuffer batch_input(&pool);
  const std::string ping = "*1\r\n$4\r\nPING\r\n";
  const std::string get = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  batch_input.append(ping + get);

  redis_proxy::RespFrameInfo first;
  status = parser.peekFrame(batch_input, 0, &first);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(first.consumed == ping.size());
  RequireEqual(first.command_name, "PING");
  RP_REQUIRE(first.argc == 1);

  redis_proxy::RespFrameInfo second;
  status = parser.peekFrame(batch_input, first.consumed, &second);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(second.consumed == get.size());
  RequireEqual(second.command_name, "GET");
  RP_REQUIRE(second.argc == 2);
  RP_REQUIRE(batch_input.readableBytes() == ping.size() + get.size());

  redis_proxy::BlockPool small_pool(16);
  redis_proxy::IoBuffer split_input(&small_pool);
  const std::string set_cmd =
      "*3\r\n$3\r\nSET\r\n$5\r\nsplit\r\n$5\r\nvalue\r\n";
  split_input.append(set_cmd + ping);
  redis_proxy::RespFrameInfo split_first;
  status = parser.peekFrame(split_input, 0, &split_first);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(split_first.consumed == set_cmd.size());
  RequireEqual(split_first.command_name, "SET");
  RP_REQUIRE(split_first.argc == 3);
  RP_REQUIRE(split_input.readableBytes() == set_cmd.size() + ping.size());

  redis_proxy::IoBuffer invalid_batch(&pool);
  invalid_batch.append(ping + "PING\r\n");
  redis_proxy::RespFrameInfo valid_prefix;
  status = parser.peekFrame(invalid_batch, 0, &valid_prefix);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  redis_proxy::RespFrameInfo invalid_second;
  status =
      parser.peekFrame(invalid_batch, valid_prefix.consumed, &invalid_second);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kError);
  RP_REQUIRE(invalid_batch.readableBytes() == ping.size() + 6);

  parser.setLimits(2 * 1024 * 1024, 10000, 8);
  redis_proxy::IoBuffer large_input(&pool);
  std::string large_request = "*4097\r\n$4\r\nMGET\r\n";
  for (int i = 0; i < 4096; ++i) large_request += "$1\r\nk\r\n";
  large_input.append(large_request);
  redis_proxy::RespFrameInfo large_info;
  RP_REQUIRE(parser.peekFrame(large_input, 0, &large_info) ==
             redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(large_info.argc == 4097);
  RP_REQUIRE(large_info.consumed == large_request.size());

  redis_proxy::IoBuffer nested(&pool);
  std::string nested_reply = "*2\r\n*1500\r\n";
  for (int i = 0; i < 1500; ++i) nested_reply += ":1\r\n";
  nested_reply += "*1500\r\n";
  for (int i = 0; i < 1500; ++i) nested_reply += "$1\r\nx\r\n";
  nested.append(nested_reply);
  redis_proxy::BufferChain reply;
  std::size_t reply_size = 0;
  RP_REQUIRE(parser.nextReplyFrame(nested, &reply, &reply_size) ==
             redis_proxy::ParseStatus::kOk);
  RequireEqual(reply.toStringForTest(), nested_reply);

  redis_proxy::RespParser fragmented_parser;
  fragmented_parser.setLimits(2 * 1024 * 1024, 1000000, 8);
  redis_proxy::IoBuffer fragmented(&pool);
  fragmented.append("*1000000\r\n");
  allocated_bytes = 0;
  track_allocations = true;
  const auto fragmented_status = fragmented_parser.peekFrame(fragmented, 0, &large_info);
  track_allocations = false;
  RP_REQUIRE(fragmented_status == redis_proxy::ParseStatus::kNeedMore);
  RP_REQUIRE(allocated_bytes < 4096);

  // A byte limit applies to each frame, not to the whole buffered pipeline.
  parser.setLimits(1024, 100, 8, ping.size());
  redis_proxy::IoBuffer limited(&pool);
  limited.append(ping + ping);
  RP_REQUIRE(parser.peekFrame(limited, 0, &first) == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(parser.peekFrame(limited, first.consumed, &second) ==
             redis_proxy::ParseStatus::kOk);
  limited.clear();
  limited.append(get);
  RP_REQUIRE(parser.peekFrame(limited, 0, &first) == redis_proxy::ParseStatus::kError);

  parser.setLimits(1024, 100, 2);
  limited.clear();
  limited.append("*1\r\n*1\r\n*0\r\n");
  RP_REQUIRE(parser.nextReplyFrame(limited, &reply, &reply_size) ==
             redis_proxy::ParseStatus::kError);

  std::cout << "resp_parser_test passed\n";
  return 0;
}
