#include "redis_proxy/buffer.h"
#include "redis_proxy/resp_parser.h"
#include "test_common.h"

#include <iostream>

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

  std::cout << "resp_parser_test passed\n";
  return 0;
}
