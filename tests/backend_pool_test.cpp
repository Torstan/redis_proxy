#include "redis_proxy/backend_pool.h"
#include "redis_proxy/backend_channel.h"
#include "test_common.h"

#include <iostream>

class PoolSink : public redis_proxy::ReplySink {
public:
  void onBackendReply(redis_proxy::BufferChain) override { ++replies; }
  void onBackendFailure(const redis_proxy::Status&) override { ++failures; }
  int replies = 0;
  int failures = 0;
};

int main() {
  redis_proxy::Config cfg;
  cfg.backend_conns_per_worker = 2;
  redis_proxy::BlockPool pool(64);
  redis_proxy::BackendPool backend_pool(0, cfg, &pool);
  PoolSink session;

  redis_proxy::BackendChannel* first =
      backend_pool.selectForSessionForTest(&session, false, nullptr);
  RP_REQUIRE(first != nullptr);
  redis_proxy::BackendChannel* sticky =
      backend_pool.selectForSessionForTest(&session, true, first);
  RP_REQUIRE(sticky == first);
  redis_proxy::BackendChannel* next =
      backend_pool.selectForSessionForTest(&session, false, nullptr);
  RP_REQUIRE(next != nullptr);

  std::cout << "backend_pool_test passed\n";
  return 0;
}
