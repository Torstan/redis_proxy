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
