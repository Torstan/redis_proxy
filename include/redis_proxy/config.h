#pragma once

#include <cstddef>
#include <string>

#include "conn_util/endpoint.h"

namespace redis_proxy {

using Endpoint = conn_util::Endpoint;

struct Config {
  Endpoint listen = Endpoint("127.0.0.1", 6379);
  Endpoint redis = Endpoint("127.0.0.1", 6380);
  int workers = 1;
  int backend_conns_per_worker = 1;
  std::size_t max_request_bytes = 512* 1024 * 1024;
  std::size_t max_bulk_bytes = 512* 1024 * 1024;
  std::size_t max_array_elements = 1024 * 1024;
  std::size_t max_pipeline_commands_per_read = 1024 * 1024;
  int connect_timeout_ms = 5000;
  int read_timeout_ms = 30000;
  int write_timeout_ms = 30000;
};

bool LoadConfigFile(const std::string& path, Config* out);
bool LoadConfigFromArgs(int argc, char** argv, Config* out);
bool ValidateConfig(const Config& config, std::string* error);

}  // namespace redis_proxy
