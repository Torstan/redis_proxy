#include "redis_proxy/buffer.h"
#include "redis_proxy/resp_parser.h"
#include "test_common.h"

#include <iostream>
#include <string>

int main() {
  redis_proxy::BlockPool pool(64);
  redis_proxy::IoBuffer input(&pool);
  input.appendForTest("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");

  redis_proxy::RespParser parser;
  redis_proxy::RespFrame frame;
  redis_proxy::ParseStatus status = parser.nextFrame(input, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(frame.consumed == 22);
  RP_REQUIRE(frame.command.argc == 2);
  RequireEqual(frame.command.name, "GET");
  RP_REQUIRE(frame.command.args[1] == "key");
  RP_REQUIRE(frame.bytes.toStringForTest() ==
             "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");

  input.appendForTest("*1\r\n$4\r\nPING");
  status = parser.nextFrame(input, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kNeedMore);

  input.appendForTest("\r\n");
  status = parser.nextFrame(input, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RequireEqual(frame.command.name, "PING");

  input.appendForTest("PING\r\n");
  status = parser.nextFrame(input, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kError);

  redis_proxy::IoBuffer batch_input(&pool);
  const std::string ping = "*1\r\n$4\r\nPING\r\n";
  const std::string get = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  batch_input.appendForTest(ping + get);

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
  split_input.appendForTest(set_cmd + ping);
  redis_proxy::RespFrameInfo split_first;
  status = parser.peekFrame(split_input, 0, &split_first);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(split_first.consumed == set_cmd.size());
  RequireEqual(split_first.command_name, "SET");
  RP_REQUIRE(split_first.argc == 3);
  RP_REQUIRE(split_input.readableBytes() == set_cmd.size() + ping.size());

  redis_proxy::IoBuffer invalid_batch(&pool);
  invalid_batch.appendForTest(ping + "PING\r\n");
  redis_proxy::RespFrameInfo valid_prefix;
  status = parser.peekFrame(invalid_batch, 0, &valid_prefix);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  redis_proxy::RespFrameInfo invalid_second;
  status =
      parser.peekFrame(invalid_batch, valid_prefix.consumed, &invalid_second);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kError);
  RP_REQUIRE(invalid_batch.readableBytes() == ping.size() + 6);

  std::cout << "resp_parser_test passed\n";
  return 0;
}
