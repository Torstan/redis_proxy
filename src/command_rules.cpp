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

  // String commands
  rules.allow("PING", 1, 2, true, false);
  rules.allow("GET", 2, 2, true, false);
  rules.allow("MGET", 2, UINT16_MAX, true, false);
  rules.allow("SET", 3, UINT16_MAX, false, true);
  rules.allow("MSET", 3, UINT16_MAX, false, true);
  rules.allow("MSETNX", 3, UINT16_MAX, false, true);
  rules.allow("GETSET", 3, 3, false, true);
  rules.allow("SETNX", 3, 3, false, true);
  rules.allow("SETEX", 4, 4, false, true);
  rules.allow("PSETEX", 4, 4, false, true);
  rules.allow("GETEX", 2, UINT16_MAX, true, false);
  rules.allow("GETDEL", 2, 2, false, true);
  rules.allow("APPEND", 3, 3, false, true);
  rules.allow("STRLEN", 2, 2, true, false);
  rules.allow("GETRANGE", 4, 4, true, false);
  rules.allow("SETRANGE", 4, 4, false, true);
  rules.allow("INCR", 2, 2, false, true);
  rules.allow("DECR", 2, 2, false, true);
  rules.allow("INCRBY", 3, 3, false, true);
  rules.allow("DECRBY", 3, 3, false, true);
  rules.allow("INCRBYFLOAT", 3, 3, false, true);

  // Set commands
  rules.allow("SADD", 3, UINT16_MAX, false, true);
  rules.allow("SREM", 3, UINT16_MAX, false, true);
  rules.allow("SMEMBERS", 2, 2, true, false);
  rules.allow("SISMEMBER", 3, 3, true, false);
  rules.allow("SMISMEMBER", 3, UINT16_MAX, true, false);
  rules.allow("SCARD", 2, 2, true, false);
  rules.allow("SPOP", 2, 3, false, true);
  rules.allow("SRANDMEMBER", 2, 3, true, false);
  rules.allow("SMOVE", 4, 4, false, true);
  rules.allow("SINTER", 2, UINT16_MAX, true, false);
  rules.allow("SINTERSTORE", 3, UINT16_MAX, false, true);
  rules.allow("SUNION", 2, UINT16_MAX, true, false);
  rules.allow("SUNIONSTORE", 3, UINT16_MAX, false, true);
  rules.allow("SDIFF", 2, UINT16_MAX, true, false);
  rules.allow("SDIFFSTORE", 3, UINT16_MAX, false, true);

  // List commands
  rules.allow("LPUSH", 3, UINT16_MAX, false, true);
  rules.allow("RPUSH", 3, UINT16_MAX, false, true);
  rules.allow("LPUSHX", 3, UINT16_MAX, false, true);
  rules.allow("RPUSHX", 3, UINT16_MAX, false, true);
  rules.allow("LPOP", 2, 3, false, true);
  rules.allow("RPOP", 2, 3, false, true);
  rules.allow("LLEN", 2, 2, true, false);
  rules.allow("LRANGE", 4, 4, true, false);
  rules.allow("LINDEX", 3, 3, true, false);
  rules.allow("LSET", 4, 4, false, true);
  rules.allow("LINSERT", 5, 5, false, true);
  rules.allow("LREM", 4, 4, false, true);
  rules.allow("LTRIM", 4, 4, false, true);
  rules.allow("LPOS", 3, UINT16_MAX, true, false);
  rules.allow("LMOVE", 5, 5, false, true);
  rules.allow("RPOPLPUSH", 3, 3, false, true);

  // Hash commands
  rules.allow("HSET", 4, UINT16_MAX, false, true);
  rules.allow("HGET", 3, 3, true, false);
  rules.allow("HMSET", 4, UINT16_MAX, false, true);
  rules.allow("HMGET", 3, UINT16_MAX, true, false);
  rules.allow("HGETALL", 2, 2, true, false);
  rules.allow("HDEL", 3, UINT16_MAX, false, true);
  rules.allow("HEXISTS", 3, 3, true, false);
  rules.allow("HKEYS", 2, 2, true, false);
  rules.allow("HVALS", 2, 2, true, false);
  rules.allow("HLEN", 2, 2, true, false);
  rules.allow("HINCRBY", 4, 4, false, true);
  rules.allow("HINCRBYFLOAT", 4, 4, false, true);
  rules.allow("HSETNX", 4, 4, false, true);
  rules.allow("HSTRLEN", 3, 3, true, false);
  rules.allow("HRANDFIELD", 2, 3, true, false);

  // Sorted Set commands
  rules.allow("ZADD", 4, UINT16_MAX, false, true);
  rules.allow("ZREM", 3, UINT16_MAX, false, true);
  rules.allow("ZSCORE", 3, 3, true, false);
  rules.allow("ZMSCORE", 3, UINT16_MAX, true, false);
  rules.allow("ZINCRBY", 4, 4, false, true);
  rules.allow("ZCARD", 2, 2, true, false);
  rules.allow("ZCOUNT", 4, 4, true, false);
  rules.allow("ZLEXCOUNT", 4, 4, true, false);
  rules.allow("ZRANGE", 4, UINT16_MAX, true, false);
  rules.allow("ZREVRANGE", 4, UINT16_MAX, true, false);
  rules.allow("ZRANGEBYSCORE", 4, UINT16_MAX, true, false);
  rules.allow("ZREVRANGEBYSCORE", 4, UINT16_MAX, true, false);
  rules.allow("ZRANGEBYLEX", 4, UINT16_MAX, true, false);
  rules.allow("ZREVRANGEBYLEX", 4, UINT16_MAX, true, false);
  rules.allow("ZRANGESTORE", 5, UINT16_MAX, false, true);
  rules.allow("ZRANK", 3, 3, true, false);
  rules.allow("ZREVRANK", 3, 3, true, false);
  rules.allow("ZREMRANGEBYRANK", 4, 4, false, true);
  rules.allow("ZREMRANGEBYSCORE", 4, 4, false, true);
  rules.allow("ZREMRANGEBYLEX", 4, 4, false, true);
  rules.allow("ZPOPMIN", 2, 3, false, true);
  rules.allow("ZPOPMAX", 2, 3, false, true);
  rules.allow("ZINTER", 3, UINT16_MAX, true, false);
  rules.allow("ZINTERSTORE", 3, UINT16_MAX, false, true);
  rules.allow("ZUNION", 3, UINT16_MAX, true, false);
  rules.allow("ZUNIONSTORE", 3, UINT16_MAX, false, true);
  rules.allow("ZDIFF", 3, UINT16_MAX, true, false);
  rules.allow("ZDIFFSTORE", 3, UINT16_MAX, false, true);
  rules.allow("ZRANDMEMBER", 2, 3, true, false);

  // Generic commands (already have: PING, DEL, EXISTS, EXPIRE, TTL)
  rules.allow("EXPIREAT", 3, 4, false, true);
  rules.allow("PTTL", 2, 2, true, false);
  rules.allow("PERSIST", 2, 2, false, true);
  rules.allow("TYPE", 2, 2, true, false);
  rules.allow("RENAME", 3, 3, false, true);
  rules.allow("RENAMENX", 3, 3, false, true);
  rules.allow("UNLINK", 2, UINT16_MAX, false, true);
  rules.allow("TOUCH", 2, UINT16_MAX, false, true);

  rules.allow("DEL", 2, UINT16_MAX, false, true);
  rules.allow("EXISTS", 2, UINT16_MAX, true, false);
  rules.allow("EXPIRE", 3, 4, false, true);
  rules.allow("TTL", 2, 2, true, false);
  const char* denied[] = {
      // Auth/Session
      "AUTH",      "SELECT",      "CLIENT",    "HELLO",
      // Transaction
      "MULTI",     "EXEC",        "DISCARD",   "WATCH",
      "UNWATCH",
      // Pub/Sub
      "SUBSCRIBE",   "PSUBSCRIBE",   "SSUBSCRIBE",
      "UNSUBSCRIBE", "PUNSUBSCRIBE", "SUNSUBSCRIBE",
      "PUBLISH",     "PUBSUB",       "SPUBLISH",
      // Blocking operations
      "BLPOP",     "BRPOP",       "BLMOVE",    "BLMPOP",
      "BZPOPMIN",  "BZPOPMAX",    "BZMPOP",    "BRPOPLPUSH",
      "XREAD",     "XREADGROUP",
      // Admin/Dangerous
      "DEBUG",     "MODULE",      "CONFIG",    "SHUTDOWN",
      "SAVE",      "BGSAVE",      "BGREWRITEAOF",
      "FLUSHDB",   "FLUSHALL",    "KEYS",      "SCAN",
      // Script
      "SCRIPT",    "EVAL",        "EVALSHA",   "FUNCTION",
      // Cluster/Replication
      "ACL",       "MONITOR",     "SYNC",      "PSYNC"};
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
