#pragma once

#include <memory>
#include <vector>

#include "redis_proxy/command_rules.h"
#include "redis_proxy/config.h"
#include "redis_proxy/worker.h"

namespace redis_proxy {

class ProxyServer {
public:
  explicit ProxyServer(Config config);

  int run();

private:
  Config config_;
  CommandRules rules_;
  int listen_fd_ = -1;
  std::vector<std::unique_ptr<Worker>> workers_;

  void acceptLoop();
};

}  // namespace redis_proxy
