#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <unordered_map>

#include "redis_proxy/status.h"

namespace redis_proxy {

struct CommandRule {
  bool allowed = false;
  uint16_t min_argc = 0;
  uint16_t max_argc = 0;
  bool read = false;
  bool write = false;
  bool dangerous = false;
};

class CommandRules {
public:
  static CommandRules Default();

  Status validate(std::string_view command, std::size_t argc) const;
  bool loadRuleLine(std::string_view line);
  void setRuleForTest(std::string_view command, const CommandRule& rule);

private:
  std::unordered_map<std::string, CommandRule> rules_;

  static std::string normalize(std::string_view command);
  void allow(std::string_view command, uint16_t min_argc, uint16_t max_argc,
             bool read, bool write);
  void deny(std::string_view command);
};

}  // namespace redis_proxy
