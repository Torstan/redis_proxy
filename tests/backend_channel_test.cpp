#include "redis_proxy/backend_channel.h"
#include "runtime_test.h"

#include <iostream>
#include <csignal>
#include <string>
#include <vector>

class FakeSink : public redis_proxy::ReplySink {
public:
  void onBackendReply(redis_proxy::BufferChain reply) override {
    replies.push_back(reply.toStringForTest());
    if (on_reply) on_reply();
  }
  void onBackendFailure(const redis_proxy::Status&) override { ++failures; }
  std::vector<std::string> replies;
  std::function<void()> on_reply;
  int failures = 0;
};

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  RunCoroutines([] {
    using namespace redis_proxy;
    Config cfg;
    int listener = ListenForTest(&cfg.redis);
    BlockPool pool(32768);
    BackendChannel channel(cfg.redis, &pool);
    FakeSink a, b;
    const std::string ping = "*1\r\n$4\r\nPING\r\n";
    RP_REQUIRE(!channel.submit(&a, MakeBufferChain(&pool, ping), 1));
    channel.start(cfg);
    WaitUntil([&] { return channel.isHealthy(); });
    CoSocket peer(AcceptForTest(listener));
    IoBuffer input(&pool);

    RP_REQUIRE(channel.submit(&a, MakeBufferChain(&pool, ping), 1));
    RP_REQUIRE(channel.submit(&b, MakeBufferChain(&pool, ping), 1));
    channel.detachOwner(&a);
    RP_REQUIRE(channel.queuedCommandCount() == 1);
    RequireEqual(ReadBytes(&peer, &input, ping.size()), ping);
    RP_REQUIRE(peer.writeAll(MakeBufferChain(&pool, "+B\r\n"), 2000).ok());
    WaitUntil([&] { return b.replies.size() == 1; });
    RP_REQUIRE(a.replies.empty());

    // Started requests keep their reply slots after the client leaves.
    channel.submit(&a, MakeBufferChain(&pool, ping), 1);
    channel.submit(&b, MakeBufferChain(&pool, ping), 1);
    RequireEqual(ReadBytes(&peer, &input, ping.size() * 2), ping + ping);
    channel.detachOwner(&a);
    b.on_reply = [&] { channel.detachOwner(&b); };
    RP_REQUIRE(peer.writeAll(MakeBufferChain(&pool, ":1\r\n:2\r\n"), 2000).ok());
    WaitUntil([&] { return b.replies.size() == 2; });
    RequireEqual(b.replies.back(), ":2\r\n");
    RP_REQUIRE(channel.queuedCommandCount() == 0);
    b.on_reply = {};

    // Cancel an owner while a large batch is still being written. Its bytes
    // must finish before the next owner's request, and its reply is discarded.
    const std::string value(4 * 1024 * 1024, 'x');
    std::string large_request;
    redis::PackCommand({"SET", "k", value}, &large_request);
    channel.submit(&a, MakeBufferChain(&pool, ping + large_request), 2);
    channel.submit(&b, MakeBufferChain(&pool, ping), 1);
    RP_REQUIRE(peer.readSome(&input, 2000).ok());
    RP_REQUIRE(input.readableBytes() < large_request.size());
    RequireEqual(ReadBytes(&peer, &input, ping.size()), ping);
    RP_REQUIRE(peer.writeAll(MakeBufferChain(&pool, "+PONG\r\n"), 2000).ok());
    WaitUntil([&] { return a.replies.size() == 1; });
    channel.detachOwner(&a);
    RequireEqual(ReadBytes(&peer, &input, large_request.size() + ping.size()),
                 large_request + ping);
    RP_REQUIRE(peer.writeAll(MakeBufferChain(&pool, "+OK\r\n+PONG\r\n"), 2000).ok());
    WaitUntil([&] { return b.replies.size() == 3; });
    RP_REQUIRE(a.replies.size() == 1);

    // Reply limits follow the configured limits, including bulk > 1 MiB.
    channel.submit(&b, MakeBufferChain(&pool, ping), 1);
    ReadBytes(&peer, &input, ping.size());
    const std::string large_reply = "$1048577\r\n" + std::string(1048577, 'v') + "\r\n";
    RP_REQUIRE(peer.writeAll(MakeBufferChain(&pool, large_reply), 2000).ok());
    WaitUntil([&] { return b.replies.size() == 4; });
    RequireEqual(b.replies.back(), large_reply);

    // Two pending batches belonging to one owner fail exactly once.
    channel.submit(&a, MakeBufferChain(&pool, ping), 1);
    channel.submit(&a, MakeBufferChain(&pool, ping), 1);
    channel.submit(&b, MakeBufferChain(&pool, ping), 1);
    ReadBytes(&peer, &input, ping.size() * 3);
    RP_REQUIRE(peer.writeAll(MakeBufferChain(&pool, "$100\r\nold"), 2000).ok());
    peer.close();
    WaitUntil([&] { return a.failures != 0; });
    RP_REQUIRE(a.failures == 1);
    RP_REQUIRE(b.failures == 1);
    WaitUntil([&] { return channel.isHealthy(); });
    peer.reset(AcceptForTest(listener));
    channel.submit(&b, MakeBufferChain(&pool, ping), 1);
    RequireEqual(ReadBytes(&peer, &input, ping.size()), ping);
    RP_REQUIRE(peer.writeAll(MakeBufferChain(&pool, "+NEW\r\n"), 2000).ok());
    WaitUntil([&] { return b.replies.size() == 5; });
    RequireEqual(b.replies.back(), "+NEW\r\n");
    channel.submit(&a, MakeBufferChain(&pool, large_request), 1);
    channel.submit(&b, MakeBufferChain(&pool, ping), 1);
    RP_REQUIRE(peer.readSome(&input, 2000).ok());
    peer.close();
    WaitUntil([&] { return a.failures == 2 && b.failures == 2; });
    channel.stop();
    WaitUntil([&] { return channel.isStopped(); });
    close(listener);
  });
  std::cout << "backend_channel_test passed\n";
}
