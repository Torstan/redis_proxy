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
