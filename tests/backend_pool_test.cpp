#include "redis_proxy/backend_pool.h"
#include "runtime_test.h"
#include <iostream>

class PoolSink : public redis_proxy::ReplySink {
public:
  void onBackendReply(redis_proxy::BufferChain) override {}
  void onBackendFailure(const redis_proxy::Status&) override {}
};

int main() {
  RunCoroutines([] {
    using namespace redis_proxy;
    Config cfg;
    cfg.backend_conns_per_worker = 2;
    const int listener = ListenForTest(&cfg.redis);
    BlockPool pool(64);
    BackendPool backends(cfg, &pool);
    PoolSink a, b;
    const auto request = [&] { return MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"); };
    RP_REQUIRE(backends.submit(&a, nullptr, request(), 1) == nullptr);
    backends.start();
    WaitUntil([&] { return backends.channelForTest(0)->isHealthy() &&
                           backends.channelForTest(1)->isHealthy(); });
    CoSocket peer1(AcceptForTest(listener)), peer2(AcceptForTest(listener));
    auto* first = backends.submit(&a, nullptr, request(), 1);
    RP_REQUIRE(first != nullptr);
    RP_REQUIRE(backends.submit(&a, first, request(), 1) == first);
    auto* other = backends.submit(&b, nullptr, request(), 1);
    RP_REQUIRE(other != nullptr && other != first);
    backends.detachOwner(&a);
    RP_REQUIRE(first->queuedCommandCount() == 0);
    first->stop();
    RP_REQUIRE(backends.submit(&a, first, request(), 1) == nullptr);
    RP_REQUIRE(backends.submit(&a, nullptr, request(), 1) == other);
    backends.stop();
    WaitUntil([&] { return backends.isStopped(); });
    close(listener);
  });
  std::cout << "backend_pool_test passed\n";
}
