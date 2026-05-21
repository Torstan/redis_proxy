#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "redis_proxy/client_session.h"
#include "util/fd_notifier.h"

namespace redis_proxy {

class Worker {
public:
  Worker(int id, const Config& config, CommandRules* rules);
  ~Worker();

  void start();
  void join();
  void dispatchFd(int fd);
  int id() const;

private:
  int id_;
  Config config_;
  CommandRules* rules_;
  std::thread thread_;
  std::mutex mutex_;
  std::deque<int> pending_fds_;
  std::atomic<bool> has_pending_fds_{false};
  FdNotifier fd_notifier_;
  std::unique_ptr<BlockPool> pool_;
  std::unique_ptr<BackendPool> backend_pool_;
  std::vector<std::unique_ptr<ClientSession>> sessions_;
  int next_session_id_ = 1;

  void run();
  void reapFds();
};

}  // namespace redis_proxy
