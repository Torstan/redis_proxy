#include "redis_proxy/backend_channel.h"
#include "test_common.h"

#include <iostream>
#include <string>
#include <vector>

class FakeSink : public redis_proxy::ReplySink {
public:
  void onBackendReply(redis_proxy::BufferChain reply) override {
    replies.push_back(reply.toStringForTest());
  }
  void onBackendFailure(const redis_proxy::Status &status) override {
    failures.push_back(status.message());
  }
  std::vector<std::string> replies;
  std::vector<std::string> failures;
};

int main() {
  redis_proxy::BlockPool pool(64);
  redis_proxy::BackendChannel channel(
      0, redis_proxy::Endpoint("127.0.0.1", 6380), &pool);
  FakeSink a;
  FakeSink b;

  channel.submit(
      &a, redis_proxy::MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"), 1, 1);
  RP_REQUIRE(channel.writerSignalPendingForTest());
  channel.submit(
      &b, redis_proxy::MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"), 1, 2);
  RP_REQUIRE(channel.pendingBatchCountForTest() == 2);

  channel.dispatchReplyForTest(
      redis_proxy::MakeBufferChain(&pool, "+PONG\r\n"));
  channel.dispatchReplyForTest(
      redis_proxy::MakeBufferChain(&pool, "+PONG\r\n"));

  RP_REQUIRE(a.replies.size() == 1);
  RP_REQUIRE(b.replies.size() == 1);
  RP_REQUIRE(channel.pendingBatchCountForTest() == 0);
  RP_REQUIRE(!channel.healthSignalPendingForTest());

  channel.submit(
      &a, redis_proxy::MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"), 1, 10);
  channel.submit(
      &b, redis_proxy::MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"), 1, 11);
  channel.detachOwner(&a);
  RP_REQUIRE(channel.pendingBatchCountForTest() == 1);
  channel.dispatchReplyForTest(
      redis_proxy::MakeBufferChain(&pool, "+PONG\r\n"));
  RP_REQUIRE(b.replies.size() == 2);

  FakeSink c;
  channel.submit(
      &c, redis_proxy::MakeBufferChain(&pool, "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n"),
      2, 3);
  RP_REQUIRE(channel.pendingBatchCountForTest() == 1);
  channel.dispatchReplyForTest(
      redis_proxy::MakeBufferChain(&pool, "$1\r\n1\r\n"));
  RP_REQUIRE(channel.pendingBatchCountForTest() == 1);
  channel.dispatchReplyForTest(
      redis_proxy::MakeBufferChain(&pool, "$1\r\n2\r\n"));
  RP_REQUIRE(channel.pendingBatchCountForTest() == 0);

  std::cout << "backend_channel_test passed\n";
  return 0;
}
