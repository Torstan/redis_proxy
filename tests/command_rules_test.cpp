#include "redis_proxy/command_rules.h"
#include "test_common.h"

int main() {
  redis_proxy::CommandRules rules = redis_proxy::CommandRules::Default();

  RP_REQUIRE(rules.validate("GET", 2).ok());
  RP_REQUIRE(rules.validate("get", 2).ok());
  RP_REQUIRE(rules.validate("SET", 3).ok());
  RP_REQUIRE(!rules.validate("SET", 1).ok());
  RP_REQUIRE(!rules.validate("CONFIG", 2).ok());
  RP_REQUIRE(!rules.validate("AUTH", 2).ok());
  RP_REQUIRE(!rules.validate("SUBSCRIBE", 2).ok());
  RP_REQUIRE(!rules.validate("BLPOP", 3).ok());
  RP_REQUIRE(!rules.validate("UNKNOWN", 1).ok());

  // String commands
  RP_REQUIRE(rules.validate("MSET", 3).ok());
  RP_REQUIRE(rules.validate("GETSET", 3).ok());
  RP_REQUIRE(rules.validate("SETNX", 3).ok());
  RP_REQUIRE(rules.validate("SETEX", 4).ok());
  RP_REQUIRE(rules.validate("APPEND", 3).ok());
  RP_REQUIRE(rules.validate("STRLEN", 2).ok());
  RP_REQUIRE(rules.validate("GETRANGE", 4).ok());
  RP_REQUIRE(rules.validate("INCRBY", 3).ok());
  RP_REQUIRE(rules.validate("DECRBY", 3).ok());
  RP_REQUIRE(rules.validate("INCRBYFLOAT", 3).ok());
  RP_REQUIRE(rules.validate("MSETNX", 3).ok());
  RP_REQUIRE(rules.validate("PSETEX", 4).ok());
  RP_REQUIRE(rules.validate("GETEX", 2).ok());
  RP_REQUIRE(rules.validate("GETDEL", 2).ok());
  RP_REQUIRE(rules.validate("SETRANGE", 4).ok());
  RP_REQUIRE(!rules.validate("MSET", 2).ok());  // Too few args

  // Set commands
  RP_REQUIRE(rules.validate("SADD", 3).ok());
  RP_REQUIRE(rules.validate("SREM", 3).ok());
  RP_REQUIRE(rules.validate("SMEMBERS", 2).ok());
  RP_REQUIRE(rules.validate("SISMEMBER", 3).ok());
  RP_REQUIRE(rules.validate("SCARD", 2).ok());
  RP_REQUIRE(rules.validate("SINTER", 3).ok());
  RP_REQUIRE(rules.validate("SUNION", 3).ok());
  RP_REQUIRE(rules.validate("SDIFF", 3).ok());
  RP_REQUIRE(rules.validate("SMISMEMBER", 3).ok());
  RP_REQUIRE(rules.validate("SPOP", 2).ok());
  RP_REQUIRE(rules.validate("SRANDMEMBER", 2).ok());
  RP_REQUIRE(rules.validate("SMOVE", 4).ok());
  RP_REQUIRE(rules.validate("SINTERSTORE", 3).ok());
  RP_REQUIRE(rules.validate("SUNIONSTORE", 3).ok());
  RP_REQUIRE(rules.validate("SDIFFSTORE", 3).ok());
  RP_REQUIRE(!rules.validate("SADD", 2).ok());  // Too few args

  // List commands
  RP_REQUIRE(rules.validate("LPUSH", 3).ok());
  RP_REQUIRE(rules.validate("RPUSH", 3).ok());
  RP_REQUIRE(rules.validate("LPUSHX", 3).ok());
  RP_REQUIRE(rules.validate("RPUSHX", 3).ok());
  RP_REQUIRE(rules.validate("LPOP", 2).ok());
  RP_REQUIRE(rules.validate("RPOP", 2).ok());
  RP_REQUIRE(rules.validate("LLEN", 2).ok());
  RP_REQUIRE(rules.validate("LRANGE", 4).ok());
  RP_REQUIRE(rules.validate("LINDEX", 3).ok());
  RP_REQUIRE(rules.validate("LSET", 4).ok());
  RP_REQUIRE(rules.validate("LINSERT", 5).ok());
  RP_REQUIRE(rules.validate("LREM", 4).ok());
  RP_REQUIRE(rules.validate("LTRIM", 4).ok());
  RP_REQUIRE(rules.validate("LPOS", 3).ok());
  RP_REQUIRE(rules.validate("LMOVE", 5).ok());
  RP_REQUIRE(rules.validate("RPOPLPUSH", 3).ok());
  RP_REQUIRE(!rules.validate("BLPOP", 3).ok());  // Blocking - should be denied
  RP_REQUIRE(!rules.validate("BRPOP", 3).ok());  // Blocking - should be denied

  // Hash commands
  RP_REQUIRE(rules.validate("HSET", 4).ok());
  RP_REQUIRE(rules.validate("HGET", 3).ok());
  RP_REQUIRE(rules.validate("HMSET", 4).ok());
  RP_REQUIRE(rules.validate("HMGET", 3).ok());
  RP_REQUIRE(rules.validate("HGETALL", 2).ok());
  RP_REQUIRE(rules.validate("HDEL", 3).ok());
  RP_REQUIRE(rules.validate("HEXISTS", 3).ok());
  RP_REQUIRE(rules.validate("HKEYS", 2).ok());
  RP_REQUIRE(rules.validate("HVALS", 2).ok());
  RP_REQUIRE(rules.validate("HLEN", 2).ok());
  RP_REQUIRE(rules.validate("HINCRBY", 4).ok());
  RP_REQUIRE(rules.validate("HINCRBYFLOAT", 4).ok());
  RP_REQUIRE(rules.validate("HSETNX", 4).ok());
  RP_REQUIRE(rules.validate("HSTRLEN", 3).ok());
  RP_REQUIRE(rules.validate("HRANDFIELD", 2).ok());
  RP_REQUIRE(!rules.validate("HSET", 3).ok());  // Too few args

  // Sorted Set commands
  RP_REQUIRE(rules.validate("ZADD", 4).ok());
  RP_REQUIRE(rules.validate("ZREM", 3).ok());
  RP_REQUIRE(rules.validate("ZSCORE", 3).ok());
  RP_REQUIRE(rules.validate("ZMSCORE", 3).ok());
  RP_REQUIRE(rules.validate("ZINCRBY", 4).ok());
  RP_REQUIRE(rules.validate("ZCARD", 2).ok());
  RP_REQUIRE(rules.validate("ZCOUNT", 4).ok());
  RP_REQUIRE(rules.validate("ZLEXCOUNT", 4).ok());
  RP_REQUIRE(rules.validate("ZRANGE", 4).ok());
  RP_REQUIRE(rules.validate("ZREVRANGE", 4).ok());
  RP_REQUIRE(rules.validate("ZRANGEBYSCORE", 4).ok());
  RP_REQUIRE(rules.validate("ZREVRANGEBYSCORE", 4).ok());
  RP_REQUIRE(rules.validate("ZRANGEBYLEX", 4).ok());
  RP_REQUIRE(rules.validate("ZREVRANGEBYLEX", 4).ok());
  RP_REQUIRE(rules.validate("ZRANGESTORE", 5).ok());
  RP_REQUIRE(rules.validate("ZRANK", 3).ok());
  RP_REQUIRE(rules.validate("ZREVRANK", 3).ok());
  RP_REQUIRE(rules.validate("ZREMRANGEBYRANK", 4).ok());
  RP_REQUIRE(rules.validate("ZREMRANGEBYSCORE", 4).ok());
  RP_REQUIRE(rules.validate("ZREMRANGEBYLEX", 4).ok());
  RP_REQUIRE(rules.validate("ZPOPMIN", 2).ok());
  RP_REQUIRE(rules.validate("ZPOPMAX", 2).ok());
  RP_REQUIRE(rules.validate("ZINTER", 3).ok());
  RP_REQUIRE(rules.validate("ZINTERSTORE", 3).ok());
  RP_REQUIRE(rules.validate("ZUNION", 3).ok());
  RP_REQUIRE(rules.validate("ZUNIONSTORE", 3).ok());
  RP_REQUIRE(rules.validate("ZDIFF", 3).ok());
  RP_REQUIRE(rules.validate("ZDIFFSTORE", 3).ok());
  RP_REQUIRE(rules.validate("ZRANDMEMBER", 2).ok());
  RP_REQUIRE(!rules.validate("BZPOPMIN", 3).ok());  // Blocking - denied
  RP_REQUIRE(!rules.validate("BZPOPMAX", 3).ok());  // Blocking - denied

  redis_proxy::CommandRule custom;
  custom.allowed = true;
  custom.min_argc = 1;
  custom.max_argc = 1;
  custom.read = true;
  custom.write = false;
  custom.dangerous = false;
  rules.setRuleForTest("PING", custom);
  RP_REQUIRE(rules.validate("PING", 1).ok());
  RP_REQUIRE(!rules.validate("PING", 2).ok());

  std::cout << "command_rules_test passed\n";
  return 0;
}
