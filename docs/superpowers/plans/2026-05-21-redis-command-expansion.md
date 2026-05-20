# Redis Command Expansion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expand redis_proxy to support all String/Set/List/Hash/ZSet commands while blocking dangerous and blocking operations.

**Architecture:** Extend the whitelist in `CommandRules::Default()` to include all commands for the five core data structures, using UINT16_MAX for variable-argument commands. Expand the denied list to block all blocking operations and dangerous admin commands.

**Tech Stack:** C++17, CMake, CTest

---

## File Structure

**Modified Files:**
- `src/command_rules.cpp` - Add all new command rules
- `tests/command_rules_test.cpp` - Add comprehensive tests

**No new files needed** - all changes are additions to existing files.

---

### Task 1: Add String Commands

**Files:**
- Modify: `src/command_rules.cpp:27-38`
- Test: `tests/command_rules_test.cpp`

- [ ] **Step 1: Write failing tests for String commands**

Add to `tests/command_rules_test.cpp` after line 15:

```cpp
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
  RP_REQUIRE(!rules.validate("MSET", 2).ok());  // Too few args

```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test`
Expected: Tests fail with "ERR proxy rejected unknown command" for new String commands

- [ ] **Step 3: Add String commands to command_rules.cpp**

In `src/command_rules.cpp`, replace lines 28-38 with:

```cpp
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
  
  rules.allow("DEL", 2, UINT16_MAX, false, true);
  rules.allow("EXISTS", 2, UINT16_MAX, true, false);
  rules.allow("EXPIRE", 3, 4, false, true);
  rules.allow("TTL", 2, 2, true, false);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test`
Expected: All String command tests pass

- [ ] **Step 5: Commit String commands**

```bash
git add src/command_rules.cpp tests/command_rules_test.cpp
git commit -m "feat: add String command support to redis_proxy"
```

---

### Task 2: Add Set Commands

**Files:**
- Modify: `src/command_rules.cpp:~55`
- Test: `tests/command_rules_test.cpp`

- [ ] **Step 1: Write failing tests for Set commands**

Add to `tests/command_rules_test.cpp` after String tests:

```cpp
  // Set commands
  RP_REQUIRE(rules.validate("SADD", 3).ok());
  RP_REQUIRE(rules.validate("SREM", 3).ok());
  RP_REQUIRE(rules.validate("SMEMBERS", 2).ok());
  RP_REQUIRE(rules.validate("SISMEMBER", 3).ok());
  RP_REQUIRE(rules.validate("SCARD", 2).ok());
  RP_REQUIRE(rules.validate("SINTER", 3).ok());
  RP_REQUIRE(rules.validate("SUNION", 3).ok());
  RP_REQUIRE(rules.validate("SDIFF", 3).ok());
  RP_REQUIRE(!rules.validate("SADD", 2).ok());  // Too few args
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test`
Expected: Tests fail for Set commands

- [ ] **Step 3: Add Set commands to command_rules.cpp**

Add after String commands in `src/command_rules.cpp`:

```cpp
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test`
Expected: All Set command tests pass

- [ ] **Step 5: Commit Set commands**

```bash
git add src/command_rules.cpp tests/command_rules_test.cpp
git commit -m "feat: add Set command support to redis_proxy"
```

---

### Task 3: Add List Commands

**Files:**
- Modify: `src/command_rules.cpp:~70`
- Test: `tests/command_rules_test.cpp`

- [ ] **Step 1: Write failing tests for List commands**

Add to `tests/command_rules_test.cpp` after Set tests:

```cpp
  // List commands
  RP_REQUIRE(rules.validate("LPUSH", 3).ok());
  RP_REQUIRE(rules.validate("RPUSH", 3).ok());
  RP_REQUIRE(rules.validate("LPOP", 2).ok());
  RP_REQUIRE(rules.validate("RPOP", 2).ok());
  RP_REQUIRE(rules.validate("LLEN", 2).ok());
  RP_REQUIRE(rules.validate("LRANGE", 4).ok());
  RP_REQUIRE(rules.validate("LINDEX", 3).ok());
  RP_REQUIRE(!rules.validate("BLPOP", 3).ok());  // Blocking - should be denied
  RP_REQUIRE(!rules.validate("BRPOP", 3).ok());  // Blocking - should be denied
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test`
Expected: Tests fail for List commands (except blocking commands which should pass the denial test)

- [ ] **Step 3: Add List commands to command_rules.cpp**

Add after Set commands in `src/command_rules.cpp`:

```cpp
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test`
Expected: All List command tests pass

- [ ] **Step 5: Commit List commands**

```bash
git add src/command_rules.cpp tests/command_rules_test.cpp
git commit -m "feat: add List command support to redis_proxy"
```

---

### Task 4: Add Hash Commands

**Files:**
- Modify: `src/command_rules.cpp:~86`
- Test: `tests/command_rules_test.cpp`

- [ ] **Step 1: Write failing tests for Hash commands**

Add to `tests/command_rules_test.cpp` after List tests:

```cpp
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
  RP_REQUIRE(!rules.validate("HSET", 3).ok());  // Too few args
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test`
Expected: Tests fail for Hash commands

- [ ] **Step 3: Add Hash commands to command_rules.cpp**

Add after List commands in `src/command_rules.cpp`:

```cpp
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test`
Expected: All Hash command tests pass

- [ ] **Step 5: Commit Hash commands**

```bash
git add src/command_rules.cpp tests/command_rules_test.cpp
git commit -m "feat: add Hash command support to redis_proxy"
```

---

### Task 5: Add Sorted Set Commands

**Files:**
- Modify: `src/command_rules.cpp:~101`
- Test: `tests/command_rules_test.cpp`

- [ ] **Step 1: Write failing tests for Sorted Set commands**

Add to `tests/command_rules_test.cpp` after Hash tests:

```cpp
  // Sorted Set commands
  RP_REQUIRE(rules.validate("ZADD", 4).ok());
  RP_REQUIRE(rules.validate("ZREM", 3).ok());
  RP_REQUIRE(rules.validate("ZSCORE", 3).ok());
  RP_REQUIRE(rules.validate("ZINCRBY", 4).ok());
  RP_REQUIRE(rules.validate("ZCARD", 2).ok());
  RP_REQUIRE(rules.validate("ZCOUNT", 4).ok());
  RP_REQUIRE(rules.validate("ZRANGE", 4).ok());
  RP_REQUIRE(rules.validate("ZREVRANGE", 4).ok());
  RP_REQUIRE(rules.validate("ZRANK", 3).ok());
  RP_REQUIRE(rules.validate("ZPOPMIN", 2).ok());
  RP_REQUIRE(!rules.validate("BZPOPMIN", 3).ok());  // Blocking - denied
  RP_REQUIRE(!rules.validate("BZPOPMAX", 3).ok());  // Blocking - denied
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test`
Expected: Tests fail for Sorted Set commands

- [ ] **Step 3: Add Sorted Set commands to command_rules.cpp**

Add after Hash commands in `src/command_rules.cpp`:

```cpp
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test`
Expected: All Sorted Set command tests pass

- [ ] **Step 5: Commit Sorted Set commands**

```bash
git add src/command_rules.cpp tests/command_rules_test.cpp
git commit -m "feat: add Sorted Set command support to redis_proxy"
```

---

### Task 6: Add Generic Commands

**Files:**
- Modify: `src/command_rules.cpp:~129`
- Test: `tests/command_rules_test.cpp`

- [ ] **Step 1: Write failing tests for Generic commands**

Add to `tests/command_rules_test.cpp` after Sorted Set tests:

```cpp
  // Generic commands
  RP_REQUIRE(rules.validate("EXPIREAT", 3).ok());
  RP_REQUIRE(rules.validate("PTTL", 2).ok());
  RP_REQUIRE(rules.validate("PERSIST", 2).ok());
  RP_REQUIRE(rules.validate("TYPE", 2).ok());
  RP_REQUIRE(rules.validate("RENAME", 3).ok());
  RP_REQUIRE(rules.validate("RENAMENX", 3).ok());
  RP_REQUIRE(rules.validate("UNLINK", 2).ok());
  RP_REQUIRE(rules.validate("TOUCH", 2).ok());
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `make test`
Expected: Tests fail for Generic commands

- [ ] **Step 3: Add Generic commands to command_rules.cpp**

Add after Sorted Set commands in `src/command_rules.cpp`:

```cpp
  // Generic commands (already have: PING, DEL, EXISTS, EXPIRE, TTL)
  rules.allow("EXPIREAT", 3, 4, false, true);
  rules.allow("PTTL", 2, 2, true, false);
  rules.allow("PERSIST", 2, 2, false, true);
  rules.allow("TYPE", 2, 2, true, false);
  rules.allow("RENAME", 3, 3, false, true);
  rules.allow("RENAMENX", 3, 3, false, true);
  rules.allow("UNLINK", 2, UINT16_MAX, false, true);
  rules.allow("TOUCH", 2, UINT16_MAX, false, true);
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test`
Expected: All Generic command tests pass

- [ ] **Step 5: Commit Generic commands**

```bash
git add src/command_rules.cpp tests/command_rules_test.cpp
git commit -m "feat: add Generic command support to redis_proxy"
```

---

### Task 7: Expand Denied Commands List

**Files:**
- Modify: `src/command_rules.cpp:~140`
- Test: `tests/command_rules_test.cpp`

- [ ] **Step 1: Write tests for denied commands**

Add to `tests/command_rules_test.cpp` after Generic tests:

```cpp
  // Verify blocking commands are denied
  RP_REQUIRE(!rules.validate("BLPOP", 3).ok());
  RP_REQUIRE(!rules.validate("BRPOP", 3).ok());
  RP_REQUIRE(!rules.validate("BLMOVE", 6).ok());
  RP_REQUIRE(!rules.validate("BLMPOP", 4).ok());
  RP_REQUIRE(!rules.validate("BZPOPMIN", 3).ok());
  RP_REQUIRE(!rules.validate("BZPOPMAX", 3).ok());
  RP_REQUIRE(!rules.validate("BZMPOP", 4).ok());
  RP_REQUIRE(!rules.validate("BRPOPLPUSH", 4).ok());
  
  // Verify dangerous admin commands are denied
  RP_REQUIRE(!rules.validate("FLUSHDB", 1).ok());
  RP_REQUIRE(!rules.validate("FLUSHALL", 1).ok());
  RP_REQUIRE(!rules.validate("KEYS", 2).ok());
  RP_REQUIRE(!rules.validate("SCAN", 2).ok());
  RP_REQUIRE(!rules.validate("BGREWRITEAOF", 1).ok());
  
  // Verify pub/sub commands are denied
  RP_REQUIRE(!rules.validate("PUBLISH", 3).ok());
  RP_REQUIRE(!rules.validate("PUBSUB", 2).ok());
  RP_REQUIRE(!rules.validate("SSUBSCRIBE", 2).ok());
  RP_REQUIRE(!rules.validate("SUNSUBSCRIBE", 2).ok());
  RP_REQUIRE(!rules.validate("SPUBLISH", 3).ok());
```

- [ ] **Step 2: Run tests to verify existing denied commands work**

Run: `make test`
Expected: Existing denied command tests pass, new ones fail

- [ ] **Step 3: Expand denied commands array in command_rules.cpp**

Replace the `denied[]` array in `src/command_rules.cpp` (around line 39-49):

```cpp
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
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `make test`
Expected: All denied command tests pass

- [ ] **Step 5: Commit denied commands expansion**

```bash
git add src/command_rules.cpp tests/command_rules_test.cpp
git commit -m "feat: expand denied commands list for security"
```

---

### Task 8: Final Integration Test

**Files:**
- Test: `tests/command_rules_test.cpp`

- [ ] **Step 1: Add comprehensive integration test**

Add at the end of `tests/command_rules_test.cpp` before the final output:

```cpp
  // Comprehensive coverage test - sample from each category
  // String
  RP_REQUIRE(rules.validate("SETEX", 4).ok());
  RP_REQUIRE(rules.validate("GETRANGE", 4).ok());
  // Set
  RP_REQUIRE(rules.validate("SINTERSTORE", 3).ok());
  RP_REQUIRE(rules.validate("SUNIONSTORE", 3).ok());
  // List
  RP_REQUIRE(rules.validate("LINSERT", 5).ok());
  RP_REQUIRE(rules.validate("RPOPLPUSH", 3).ok());
  // Hash
  RP_REQUIRE(rules.validate("HINCRBYFLOAT", 4).ok());
  RP_REQUIRE(rules.validate("HRANDFIELD", 2).ok());
  // Sorted Set
  RP_REQUIRE(rules.validate("ZRANGEBYSCORE", 4).ok());
  RP_REQUIRE(rules.validate("ZINTERSTORE", 3).ok());
  // Generic
  RP_REQUIRE(rules.validate("RENAMENX", 3).ok());
  RP_REQUIRE(rules.validate("TOUCH", 3).ok());
```

- [ ] **Step 2: Run full test suite**

Run: `make test`
Expected: All tests pass

- [ ] **Step 3: Run integration tests if available**

Run: `cd build && ctest --output-on-failure -R integration`
Expected: Integration tests pass (or skip if not available)

- [ ] **Step 4: Verify no performance regression**

Run: `make bench` (if benchmark exists)
Expected: Performance similar to baseline

- [ ] **Step 5: Final commit**

```bash
git add tests/command_rules_test.cpp
git commit -m "test: add comprehensive integration tests for command expansion"
```

---

## Verification Checklist

After completing all tasks, verify:

- [ ] All String commands (20) are allowed with correct argument counts
- [ ] All Set commands (15) are allowed with correct argument counts
- [ ] All List commands (16) are allowed with correct argument counts
- [ ] All Hash commands (15) are allowed with correct argument counts
- [ ] All Sorted Set commands (29) are allowed with correct argument counts
- [ ] All Generic commands (13) are allowed with correct argument counts
- [ ] All blocking commands (8+) are denied
- [ ] All dangerous admin commands are denied
- [ ] All pub/sub commands are denied
- [ ] All transaction commands are denied
- [ ] All script commands are denied
- [ ] Variable-argument commands use UINT16_MAX
- [ ] All tests pass: `make test`
- [ ] Code is organized with clear comments by data structure
- [ ] Commit history is clean with descriptive messages

---

## Success Criteria

1. ✅ All 108+ commands properly categorized and configured
2. ✅ All blocking and dangerous commands explicitly denied
3. ✅ All tests pass without errors
4. ✅ No performance regression in command validation
5. ✅ Code is maintainable with clear organization
6. ✅ Backward compatibility maintained for existing commands

---

## Notes

- Each task is independent and can be executed sequentially
- Tests are written before implementation (TDD approach)
- Commits are frequent and atomic
- UINT16_MAX (65535) is used for variable-argument commands
- Command names are case-insensitive (normalized to uppercase)
- The `read` and `write` flags help with future monitoring/metrics
