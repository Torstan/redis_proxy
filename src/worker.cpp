#include "redis_proxy/worker.h"

#include "co_routine.h"
#include "thread_worker.h"

#include <memory>
#include <poll.h>

namespace redis_proxy {

Worker::Worker(int id, const Config& config, CommandRules* rules)
    : id_(id),
      config_(config),
      rules_(rules),
      pool_(std::make_unique<BlockPool>(32 * 1024)),
      backend_pool_(std::make_unique<BackendPool>(id, config, pool_.get())) {}

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
        next_session_id_++, fd, config_, rules_, backend_pool_.get(),
        pool_.get());
    session->start();
    sessions_.push_back(std::move(session));
  }
}

void Worker::run() {
  co::co_enable_hook_sys();
  backend_pool_->start();
  co::Coroutine* reaper = co::co_create([this]() {
    co::co_enable_hook_sys();
    while (true) {
      reapFds();
      pollfd pfd{fd_notifier_.readFd(), POLLIN | POLLERR | POLLHUP, 0};
      const int ret = co::co_poll(&pfd, 1, -1);
      if (ret > 0) {
        (void)fd_notifier_.drain();
      }
    }
  });
  co::co_resume(reaper);
  co::ThreadWorker loop(id_);
  loop.run_loop();
}

}  // namespace redis_proxy
