#include "redis_proxy/config.h"
#include "redis_proxy/proxy_server.h"

#include <iostream>

int main(int argc, char** argv) {
  redis_proxy::Config config;
  if (!redis_proxy::LoadConfigFromArgs(argc, argv, &config)) {
    std::cerr << "usage: redis_proxy [--config path] [--listen host:port] "
                 "[--redis host:port] [--workers n] [--backend-conns n]\n";
    return 2;
  }
  redis_proxy::ProxyServer server(config);
  return server.run();
}
