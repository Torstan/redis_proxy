#pragma once

#include <memory>
#include <vector>

#include "redis_proxy/backend_channel.h"
#include "redis_proxy/config.h"

namespace redis_proxy {

class BackendPool {
public:
  BackendPool(int worker_id, const Config& config, BlockPool* pool);

  void start();
  BackendChannel* submit(ReplySink* owner, BackendChannel* current,
                         bool has_pending, BufferChain bytes,
                         uint32_t command_count, uint64_t sequence_base);
  BackendChannel* selectForSessionForTest(ReplySink* owner, bool has_pending,
                                          BackendChannel* current);
  BackendChannel* channelForTest(std::size_t index);

private:
  BackendChannel* select(ReplySink* owner, bool has_pending,
                         BackendChannel* current);

  int worker_id_;
  Config config_;
  BlockPool* pool_;
  std::vector<std::unique_ptr<BackendChannel>> channels_;
};

}  // namespace redis_proxy
