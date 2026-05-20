#include "redis_proxy/proxy_server.h"

#include "co_routine.h"
#include "thread_worker.h"

#include <csignal>
#include <iostream>
#include <poll.h>
#include <utility>
#include <unistd.h>

namespace redis_proxy {

ProxyServer::ProxyServer(Config config)
    : config_(std::move(config)), rules_(CommandRules::Default()) {}

int ProxyServer::run() {
  std::signal(SIGPIPE, SIG_IGN);
  listen_fd_ = CreateTcpListenSocket(config_.listen, 1024);
  if (listen_fd_ < 0) {
    std::cerr << "failed to listen on " << config_.listen.toString() << "\n";
    return 1;
  }
  for (int i = 0; i < config_.workers; ++i) {
    auto worker = std::make_unique<Worker>(i, config_, &rules_);
    worker->start();
    workers_.push_back(std::move(worker));
  }
  co::Coroutine* acceptor = co::co_create([this]() { acceptLoop(); });
  co::co_resume(acceptor);
  co::ThreadWorker loop(-1);
  loop.run_loop();
  return 0;
}

void ProxyServer::acceptLoop() {
  co::co_enable_hook_sys();
  std::size_t next_worker = 0;
  while (true) {
    sockaddr_in addr;
    socklen_t len = sizeof(addr);
    int fd = co::co_accept(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    if (fd < 0) {
      pollfd pfd{listen_fd_, POLLIN | POLLERR | POLLHUP, 0};
      co::co_poll(&pfd, 1, 1000);
      continue;
    }
    SetNonBlocking(fd);
    SetTcpNoDelay(fd);
    workers_[next_worker]->dispatchFd(fd);
    next_worker = (next_worker + 1) % workers_.size();
  }
}

}  // namespace redis_proxy
