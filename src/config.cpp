#include "redis_proxy/config.h"

#include <cstdlib>
#include <fstream>
#include <string>

namespace redis_proxy {
namespace {

std::string Trim(const std::string& s) {
  const std::size_t first = s.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) {
    return "";
  }
  const std::size_t last = s.find_last_not_of(" \t\r\n");
  return s.substr(first, last - first + 1);
}

bool ParseSize(const std::string& value, std::size_t* out) {
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
  if (*end != '\0') {
    return false;
  }
  *out = static_cast<std::size_t>(parsed);
  return true;
}

bool ParseInt(const std::string& value, int* out) {
  char* end = nullptr;
  const long parsed = std::strtol(value.c_str(), &end, 10);
  if (*end != '\0' || parsed < 0 || parsed > 1000000) {
    return false;
  }
  *out = static_cast<int>(parsed);
  return true;
}

bool Apply(Config* cfg, const std::string& key, const std::string& value) {
  if (key == "listen") return Endpoint::parse(value, &cfg->listen);
  if (key == "redis") return Endpoint::parse(value, &cfg->redis);
  if (key == "workers") return ParseInt(value, &cfg->workers);
  if (key == "backend_conns_per_worker") {
    return ParseInt(value, &cfg->backend_conns_per_worker);
  }
  if (key == "max_request_bytes") {
    return ParseSize(value, &cfg->max_request_bytes);
  }
  if (key == "max_bulk_bytes") return ParseSize(value, &cfg->max_bulk_bytes);
  if (key == "max_array_elements") {
    return ParseSize(value, &cfg->max_array_elements);
  }
  if (key == "max_pipeline_commands_per_read") {
    return ParseSize(value, &cfg->max_pipeline_commands_per_read);
  }
  if (key == "connect_timeout_ms") {
    return ParseInt(value, &cfg->connect_timeout_ms);
  }
  if (key == "read_timeout_ms") return ParseInt(value, &cfg->read_timeout_ms);
  if (key == "write_timeout_ms") return ParseInt(value, &cfg->write_timeout_ms);
  return false;
}

}  // namespace

bool LoadConfigFile(const std::string& path, Config* out) {
  std::ifstream in(path);
  if (!in) {
    return false;
  }
  std::string line;
  while (std::getline(in, line)) {
    std::string trimmed = Trim(line);
    if (trimmed.empty() || trimmed[0] == '#') {
      continue;
    }
    const std::size_t eq = trimmed.find('=');
    if (eq == std::string::npos) {
      return false;
    }
    if (!Apply(out, Trim(trimmed.substr(0, eq)), Trim(trimmed.substr(eq + 1)))) {
      return false;
    }
  }
  std::string error;
  return ValidateConfig(*out, &error);
}

bool LoadConfigFromArgs(int argc, char** argv, Config* out) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto need_value = [&](std::string* value) -> bool {
      if (i + 1 >= argc) {
        return false;
      }
      *value = argv[++i];
      return true;
    };
    std::string value;
    if (arg == "--config") {
      if (!need_value(&value) || !LoadConfigFile(value, &cfg)) return false;
    } else if (arg == "--listen") {
      if (!need_value(&value) || !Endpoint::parse(value, &cfg.listen)) {
        return false;
      }
    } else if (arg == "--redis") {
      if (!need_value(&value) || !Endpoint::parse(value, &cfg.redis)) {
        return false;
      }
    } else if (arg == "--workers") {
      if (!need_value(&value) || !ParseInt(value, &cfg.workers)) return false;
    } else if (arg == "--backend-conns") {
      if (!need_value(&value) ||
          !ParseInt(value, &cfg.backend_conns_per_worker)) {
        return false;
      }
    } else {
      return false;
    }
  }
  std::string error;
  if (!ValidateConfig(cfg, &error)) {
    return false;
  }
  *out = cfg;
  return true;
}

bool ValidateConfig(const Config& config, std::string* error) {
  if (config.workers <= 0) {
    *error = "workers must be positive";
    return false;
  }
  if (config.backend_conns_per_worker <= 0 ||
      config.backend_conns_per_worker > 4) {
    *error = "backend_conns_per_worker must be 1..4";
    return false;
  }
  if (config.max_request_bytes == 0 || config.max_bulk_bytes == 0 ||
      config.max_array_elements == 0 ||
      config.max_pipeline_commands_per_read == 0) {
    *error = "limits must be positive";
    return false;
  }
  return true;
}

}  // namespace redis_proxy
