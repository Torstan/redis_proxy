#pragma once

#include <memory>
#include <vector>

#include "redis_proxy/backend_channel.h"
#include "redis_proxy/config.h"

namespace redis_proxy {

class BackendPool {
public:
  BackendPool(const Config& config, BlockPool* pool);

  void start();
  void stop();
  bool isStopped() const;
  void detachOwner(ReplySink* owner);
  BackendChannel* submit(ReplySink* owner, BackendChannel* current,
                         BufferChain bytes, uint32_t command_count);
  BackendChannel* channelForTest(std::size_t index);

private:
  BackendChannel* select(BackendChannel* current);

  Config config_;
  BlockPool* pool_;
  std::vector<std::unique_ptr<BackendChannel>> channels_;
};

}  // namespace redis_proxy
