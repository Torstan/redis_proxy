#include "redis_proxy/worker.h"

#include "co_routine.h"
#include "thread_worker.h"
#include "task.h"

#include <memory>
#include <poll.h>

namespace redis_proxy {

Worker::Worker(int id, const Config& config, CommandRules* rules)
    : id_(id),
      config_(config),
      rules_(rules),
      pool_(std::make_unique<BlockPool>(32 * 1024)),
      backend_pool_(std::make_unique<BackendPool>(config, pool_.get())) {}

Worker::~Worker() = default;

void Worker::start() { thread_ = std::thread([this]() { run(); }); }

void Worker::join() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

int Worker::id() const { return id_; }

void Worker::dispatchFd(int fd) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    pending_fds_.push_back(fd);
    has_pending_fds_.store(true, std::memory_order_release);
  }
  (void)fd_notifier_.notify();
}

void Worker::reapFds() {
  if (!has_pending_fds_.load(std::memory_order_acquire)) {
    return;
  }
  std::deque<int> fds;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    fds.swap(pending_fds_);
    has_pending_fds_.store(false, std::memory_order_release);
  }
  while (!fds.empty()) {
    int fd = fds.front();
    fds.pop_front();
    auto session = std::make_unique<ClientSession>(
        fd, config_, rules_, backend_pool_.get(),
        pool_.get());
    auto* ptr = session.get();
    sessions_.emplace(ptr, std::move(session));
    ptr->start([this](ClientSession* finished) {
      finished_sessions_.push_back(finished);
      (void)fd_notifier_.notify();
    });
  }
}

void Worker::reapSessions() {
  for (auto* session : finished_sessions_) sessions_.erase(session);
  finished_sessions_.clear();
}

void Worker::run() {
  co::co_enable_hook_sys();
  backend_pool_->start();
  co::schedule(co::make_task([this]() {
    co::co_enable_hook_sys();
    while (true) {
      reapFds();
      reapSessions();
      pollfd pfd{fd_notifier_.readFd(), POLLIN | POLLERR | POLLHUP, 0};
      const int ret = co::co_poll(&pfd, 1, -1);
      if (ret > 0) {
        (void)fd_notifier_.drain();
      }
    }
  }));
  co::ThreadWorker loop(id_);
  loop.run_loop();
}

}  // namespace redis_proxy
