#include "redis_proxy/command_rules.h"

#include <algorithm>
#include <cctype>
#include <sstream>

namespace redis_proxy {

std::string CommandRules::normalize(std::string_view command) {
  std::string out(command);
  std::transform(out.begin(), out.end(), out.begin(), [](unsigned char c) {
    return static_cast<char>(std::toupper(c));
  });
  return out;
}

void CommandRules::allow(std::string_view command, uint16_t min_argc,
                         uint16_t max_argc, bool read, bool write) {
  rules_[normalize(command)] =
      CommandRule{true, min_argc, max_argc, read, write, false};
}

void CommandRules::deny(std::string_view command) {
  rules_[normalize(command)] = CommandRule{false, 0, 0, false, false, true};
}

CommandRules CommandRules::Default() {
  CommandRules rules;
  rules.allow("PING", 1, 2, true, false);
  rules.allow("GET", 2, 2, true, false);
  rules.allow("MGET", 2, 1024, true, false);
  rules.allow("SET", 3, 6, false, true);
  rules.allow("DEL", 2, 1024, false, true);
  rules.allow("EXISTS", 2, 1024, true, false);
  rules.allow("INCR", 2, 2, false, true);
  rules.allow("DECR", 2, 2, false, true);
  rules.allow("EXPIRE", 3, 4, false, true);
  rules.allow("TTL", 2, 2, true, false);
  const char* denied[] = {
      "AUTH",      "SELECT",      "CLIENT",    "HELLO",
      "MULTI",     "EXEC",        "DISCARD",   "WATCH",
      "UNWATCH",   "SUBSCRIBE",   "PSUBSCRIBE", "SSUBSCRIBE",
      "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE", "BLPOP",
      "BRPOP",     "BRPOPLPUSH",  "BZPOPMIN",  "BZPOPMAX",
      "BLMOVE",    "XREAD",       "XREADGROUP", "DEBUG",
      "MODULE",    "CONFIG",      "SHUTDOWN",  "SAVE",
      "BGSAVE",    "SCRIPT",      "EVAL",      "EVALSHA",
      "FUNCTION",  "ACL",         "MONITOR",   "SYNC",
      "PSYNC"};
  for (const char* command : denied) {
    rules.deny(command);
  }
  return rules;
}

Status CommandRules::validate(std::string_view command,
                              std::size_t argc) const {
  const auto it = rules_.find(normalize(command));
  if (it == rules_.end()) {
    return Status::ProtocolError("ERR proxy rejected unknown command");
  }
  const CommandRule& rule = it->second;
  if (!rule.allowed || rule.dangerous) {
    return Status::ProtocolError("ERR proxy rejected command");
  }
  if (argc < rule.min_argc || argc > rule.max_argc) {
    return Status::ProtocolError("ERR wrong number of arguments");
  }
  return Status::Ok();
}

bool CommandRules::loadRuleLine(std::string_view line) {
  std::istringstream in{std::string(line)};
  std::string command;
  int allowed = 0;
  int min_argc = 0;
  int max_argc = 0;
  int read = 0;
  int write = 0;
  int dangerous = 0;
  if (!(in >> command >> allowed >> min_argc >> max_argc >> read >> write >>
        dangerous)) {
    return false;
  }
  rules_[normalize(command)] =
      CommandRule{allowed != 0, static_cast<uint16_t>(min_argc),
                  static_cast<uint16_t>(max_argc), read != 0, write != 0,
                  dangerous != 0};
  return true;
}

void CommandRules::setRuleForTest(std::string_view command,
                                  const CommandRule& rule) {
  rules_[normalize(command)] = rule;
}

}  // namespace redis_proxy
