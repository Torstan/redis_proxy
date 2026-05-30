#include "redis_proxy/config.h"
#include "redis_proxy/endpoint.h"
#include "test_common.h"

#include <cstdio>
#include <fstream>
#include <iostream>

int main() {
  redis_proxy::Endpoint ep;
  RP_REQUIRE(redis_proxy::Endpoint::parse("127.0.0.1:6380", &ep));
  RequireEqual(ep.host(), "127.0.0.1");
  RP_REQUIRE(ep.port() == 6380);
  RP_REQUIRE(!redis_proxy::Endpoint::parse("127.0.0.1", &ep));
  RP_REQUIRE(!redis_proxy::Endpoint::parse("127.0.0.1:0", &ep));
  RP_REQUIRE(!redis_proxy::Endpoint::parse("127.0.0.1:70000", &ep));

  const char* path = "config_test.conf";
  {
    std::ofstream out(path);
    out << "listen = 0.0.0.0:6379\n";
    out << "redis = 127.0.0.1:6380\n";
    out << "workers = 2\n";
    out << "backend_conns_per_worker = 3\n";
    out << "max_request_bytes = 4096\n";
    out << "max_bulk_bytes = 2048\n";
    out << "max_array_elements = 32\n";
    out << "max_pipeline_commands_per_read = 16\n";
  }

  redis_proxy::Config cfg;
  RP_REQUIRE(redis_proxy::LoadConfigFile(path, &cfg));
  RP_REQUIRE(cfg.listen.port() == 6379);
  RP_REQUIRE(cfg.redis.port() == 6380);
  RP_REQUIRE(cfg.workers == 2);
  RP_REQUIRE(cfg.backend_conns_per_worker == 3);
  RP_REQUIRE(cfg.max_request_bytes == 4096);
  RP_REQUIRE(cfg.max_bulk_bytes == 2048);
  RP_REQUIRE(cfg.max_array_elements == 32);
  RP_REQUIRE(cfg.max_pipeline_commands_per_read == 16);

  const char* argv[] = {"redis_proxy", "--config", path, "--workers", "4",
                        "--backend-conns", "2", "--listen",
                        "127.0.0.1:6390"};
  redis_proxy::Config cli_cfg;
  RP_REQUIRE(
      redis_proxy::LoadConfigFromArgs(9, const_cast<char**>(argv), &cli_cfg));
  RP_REQUIRE(cli_cfg.workers == 4);
  RP_REQUIRE(cli_cfg.backend_conns_per_worker == 2);
  RP_REQUIRE(cli_cfg.listen.port() == 6390);

  std::remove(path);
  std::cout << "config_test passed\n";
  return 0;
}
