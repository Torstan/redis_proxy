#include "redis_proxy/backend_pool.h"

#include <memory>
#include <utility>

namespace redis_proxy {

BackendPool::BackendPool(const Config& config, BlockPool* pool)
    : config_(config), pool_(pool) {
  for (int i = 0; i < config_.backend_conns_per_worker; ++i) {
    channels_.push_back(
        std::make_unique<BackendChannel>(config_.redis, pool_));
  }
}

void BackendPool::start() {
  for (auto& channel : channels_) {
    channel->start(config_);
  }
}

void BackendPool::stop() {
  for (auto& channel : channels_) channel->stop();
}

bool BackendPool::isStopped() const {
  for (const auto& channel : channels_) if (!channel->isStopped()) return false;
  return true;
}

void BackendPool::detachOwner(ReplySink* owner) {
  for (auto& channel : channels_) channel->detachOwner(owner);
}

BackendChannel* BackendPool::select(BackendChannel* current) {
  if (current != nullptr) return current->isHealthy() ? current : nullptr;
  BackendChannel* best = nullptr;
  for (auto& channel : channels_) {
    if (channel->isHealthy() &&
        (!best || channel->queuedCommandCount() < best->queuedCommandCount())) {
      best = channel.get();
    }
  }
  return best;
}

BackendChannel* BackendPool::submit(ReplySink* owner, BackendChannel* current,
                                    BufferChain bytes, uint32_t command_count) {
  BackendChannel* channel = select(current);
  return channel && channel->submit(owner, std::move(bytes), command_count)
             ? channel : nullptr;
}

BackendChannel* BackendPool::channelForTest(std::size_t index) {
  if (index >= channels_.size()) {
    return nullptr;
  }
  return channels_[index].get();
}

}  // namespace redis_proxy
