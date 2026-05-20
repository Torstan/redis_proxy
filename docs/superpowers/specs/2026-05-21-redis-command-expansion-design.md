---
name: Redis Command Expansion Design
description: Expand redis_proxy to support all String/Set/List/Hash/ZSet commands while blocking dangerous and blocking operations
type: design
date: 2026-05-21
---

# Redis Command Expansion Design

## Overview

Expand the redis_proxy command whitelist to support all Redis commands for the five core data structures (String, Set, List, Hash, Sorted Set), while maintaining security by blocking dangerous administrative commands and blocking operations.

## Background

Currently, redis_proxy only allows a minimal set of Redis commands (GET, SET, MGET, DEL, EXISTS, INCR, DECR, EXPIRE, TTL, PING). This limits its usefulness as a general-purpose Redis proxy. The goal is to expand support to cover all common data structure operations while maintaining security and stability.

## Requirements

### Functional Requirements

1. Support all Redis commands for the following data structures:
   - String: GET, SET, MGET, MSET, GETSET, SETNX, SETEX, PSETEX, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, GETDEL, GETEX
   - Set: SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE, SINTER, SINTERSTORE, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SMISMEMBER
   - List: LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, LPOS, LMOVE, RPOPLPUSH, LPUSHX, RPUSHX
   - Hash: HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD
   - Sorted Set: ZADD, ZREM, ZSCORE, ZINCRBY, ZCARD, ZCOUNT, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANK, ZREVRANK, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZPOPMIN, ZPOPMAX, ZINTER, ZINTERSTORE, ZUNION, ZUNIONSTORE, ZDIFF, ZDIFFSTORE, ZMSCORE, ZRANDMEMBER, ZRANGESTORE

2. Support common generic commands:
   - PING, EXISTS, DEL, EXPIRE, EXPIREAT, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, UNLINK, TOUCH

3. Block all dangerous and blocking commands:
   - Blocking: BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, BRPOPLPUSH
   - Transaction: MULTI, EXEC, DISCARD, WATCH, UNWATCH
   - Pub/Sub: SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB, SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH
   - Admin: CONFIG, SHUTDOWN, SAVE, BGSAVE, DEBUG, MONITOR, SYNC, PSYNC, FLUSHDB, FLUSHALL, KEYS, SCAN
   - Script: EVAL, EVALSHA, SCRIPT, FUNCTION
   - Auth: AUTH, ACL, HELLO, SELECT, CLIENT
   - Module: MODULE

### Non-Functional Requirements

1. Maintain backward compatibility with existing allowed commands
2. No performance degradation in command validation
3. Clear error messages for rejected commands
4. Easy to maintain and audit the command list

## Design Decisions

### Decision 1: Whitelist vs Blacklist Approach

**Chosen: Explicit Whitelist (Hardcoded)**

Rationale:
- Security-first: Unknown commands are rejected by default
- Explicit control: Every allowed command is reviewed
- Audit-friendly: Easy to see what's permitted
- Consistent with existing codebase pattern

Alternatives considered:
- Blacklist approach: Rejected due to security concerns
- External config file: Rejected due to added complexity

### Decision 2: Variable Argument Limits

**Chosen: No artificial upper limit for variable-argument commands**

For commands that accept variable numbers of arguments (MGET, SADD, LPUSH, etc.), we use UINT16_MAX (65535) as the max_argc value to indicate "no practical limit" rather than imposing an artificial cap like 1024.

Rationale:
- Redis itself handles resource limits and protections
- Proxy should not second-guess legitimate use cases
- Simpler implementation without arbitrary thresholds
- Backend Redis will reject truly abusive requests

### Decision 3: Blocking Commands Policy

**Chosen: Block all blocking commands**

All commands that can block client connections (BLPOP, BRPOP, BZPOPMIN, etc.) are explicitly denied.

Rationale:
- Blocking commands tie up backend connections in the pool
- Reduces connection reuse efficiency
- Can cause timeout issues in proxy scenarios
- Non-blocking alternatives exist for most use cases

## Implementation Plan

### Changes to command_rules.cpp

Modify `CommandRules::Default()` function:

1. **Organize allow() calls by data structure:**
   - Group commands with comments: String, Set, List, Hash, Sorted Set, Generic
   - Use UINT16_MAX for variable-argument commands
   - Specify correct min_argc, max_argc, read, write flags

2. **Expand denied[] array:**
   - Add all blocking commands
   - Add dangerous admin commands (FLUSHDB, FLUSHALL, KEYS, SCAN)
   - Add pub/sub commands
   - Keep existing denied commands

3. **Maintain existing structure:**
   - No changes to CommandRule struct
   - No changes to validate() logic
   - No changes to normalize() logic

### Command Argument Specifications

For each command, we define:
- `min_argc`: Minimum arguments including command name
- `max_argc`: Maximum arguments (UINT16_MAX for variable args)
- `read`: true if command reads data
- `write`: true if command modifies data

Examples:
- `GET key` → (2, 2, true, false)
- `MGET key [key ...]` → (2, UINT16_MAX, true, false)
- `SADD key member [member ...]` → (3, UINT16_MAX, false, true)
- `HGETALL key` → (2, 2, true, false)

### Testing Strategy

1. **Unit tests in command_rules_test.cpp:**
   - Test sample commands from each data structure
   - Verify blocking commands are rejected
   - Verify dangerous commands are rejected
   - Test argument count validation

2. **Integration tests:**
   - Test actual command execution through proxy
   - Verify error messages for rejected commands

## Command Reference

### Allowed Commands by Category

**String (17 commands):**
GET, SET, MGET, MSET, GETSET, SETNX, SETEX, PSETEX, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, GETDEL, GETEX, MSETNX

**Set (14 commands):**
SADD, SREM, SMEMBERS, SISMEMBER, SCARD, SPOP, SRANDMEMBER, SMOVE, SINTER, SINTERSTORE, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SMISMEMBER

**List (16 commands):**
LPUSH, RPUSH, LPOP, RPOP, LLEN, LRANGE, LINDEX, LSET, LINSERT, LREM, LTRIM, LPOS, LMOVE, RPOPLPUSH, LPUSHX, RPUSHX

**Hash (15 commands):**
HSET, HGET, HMSET, HMGET, HGETALL, HDEL, HEXISTS, HKEYS, HVALS, HLEN, HINCRBY, HINCRBYFLOAT, HSETNX, HSTRLEN, HRANDFIELD

**Sorted Set (26 commands):**
ZADD, ZREM, ZSCORE, ZINCRBY, ZCARD, ZCOUNT, ZRANGE, ZREVRANGE, ZRANGEBYSCORE, ZREVRANGEBYSCORE, ZRANK, ZREVRANK, ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZPOPMIN, ZPOPMAX, ZINTER, ZINTERSTORE, ZUNION, ZUNIONSTORE, ZDIFF, ZDIFFSTORE, ZMSCORE, ZRANDMEMBER, ZRANGESTORE, ZLEXCOUNT, ZRANGEBYLEX, ZREVRANGEBYLEX, ZREMRANGEBYLEX

**Generic (12 commands):**
PING, EXISTS, DEL, EXPIRE, EXPIREAT, TTL, PTTL, PERSIST, TYPE, RENAME, RENAMENX, UNLINK, TOUCH

### Blocked Commands

**Blocking operations:**
BLPOP, BRPOP, BLMOVE, BLMPOP, BZPOPMIN, BZPOPMAX, BZMPOP, BRPOPLPUSH

**Transaction:**
MULTI, EXEC, DISCARD, WATCH, UNWATCH

**Pub/Sub:**
SUBSCRIBE, PSUBSCRIBE, UNSUBSCRIBE, PUNSUBSCRIBE, PUBLISH, PUBSUB, SSUBSCRIBE, SUNSUBSCRIBE, SPUBLISH

**Admin/Dangerous:**
CONFIG, SHUTDOWN, SAVE, BGSAVE, DEBUG, MONITOR, SYNC, PSYNC, FLUSHDB, FLUSHALL, KEYS, SCAN, BGREWRITEAOF

**Script:**
EVAL, EVALSHA, SCRIPT, FUNCTION

**Auth:**
AUTH, ACL, HELLO, SELECT, CLIENT

**Module:**
MODULE

## Success Criteria

1. All String/Set/List/Hash/ZSet commands are allowed and validated correctly
2. All blocking and dangerous commands are rejected with clear error messages
3. Existing tests pass
4. New tests cover representative commands from each category
5. No performance regression in command validation
6. Code is organized and maintainable with clear comments

## Future Considerations

1. **Stream support:** Redis Streams (XADD, XREAD, etc.) could be added in a future iteration
2. **Geospatial support:** GEO commands could be added if needed
3. **HyperLogLog support:** PFADD, PFCOUNT, PFMERGE could be added
4. **Bitmap support:** SETBIT, GETBIT, BITCOUNT, BITOP could be added
5. **Configuration flexibility:** Consider external config file if customization needs grow
