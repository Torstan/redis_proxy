#include "redis_proxy/backend_pool.h"

#include <memory>
#include <utility>

namespace redis_proxy {

BackendPool::BackendPool(int worker_id, const Config& config, BlockPool* pool)
    : worker_id_(worker_id), config_(config), pool_(pool) {
  for (int i = 0; i < config_.backend_conns_per_worker; ++i) {
    channels_.push_back(
        std::make_unique<BackendChannel>(i, config_.redis, pool_));
  }
}

void BackendPool::start() {
  for (auto& channel : channels_) {
    channel->start(config_);
  }
}

BackendChannel* BackendPool::select(ReplySink*, bool has_pending,
                                    BackendChannel* current) {
  if (has_pending && current != nullptr) {
    return current;
  }
  BackendChannel* best = channels_.front().get();
  for (auto& channel : channels_) {
    if (channel->queuedCommandCount() < best->queuedCommandCount()) {
      best = channel.get();
    }
  }
  return best;
}

BackendChannel* BackendPool::submit(ReplySink* owner, BackendChannel* current,
                                    bool has_pending, BufferChain bytes,
                                    uint32_t command_count,
                                    uint64_t sequence_base) {
  BackendChannel* channel = select(owner, has_pending, current);
  channel->submit(owner, std::move(bytes), command_count, sequence_base);
  return channel;
}

BackendChannel* BackendPool::selectForSessionForTest(ReplySink* owner,
                                                     bool has_pending,
                                                     BackendChannel* current) {
  return select(owner, has_pending, current);
}

}  // namespace redis_proxy
