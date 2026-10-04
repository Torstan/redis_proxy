#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <vector>

#include "conn_util/fd_notifier.h"
#include "redis_proxy/client_session.h"

namespace redis_proxy {

using FdNotifier = conn_util::FdNotifier;

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
  std::unordered_map<ClientSession*, std::unique_ptr<ClientSession>> sessions_;
  // Worker-thread-only completion queue; destruction happens off session stacks.
  std::vector<ClientSession*> finished_sessions_;

  void run();
  void reapFds();
  void reapSessions();
};

}  // namespace redis_proxy
