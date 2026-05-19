# Redis Proxy Design

Date: 2026-05-19

## Goal

Build a high-performance Redis protocol proxy in C++17. The proxy accepts client Redis RESP2 requests, validates protocol and command safety, forwards valid requests to a single `redis-server`, reads Redis responses, and returns responses to the originating clients.

The first version prioritizes a small, explicit design with predictable coroutine I/O behavior, low memory copying, and correct per-client pipeline ordering.

## Scope

In scope:

- Add git submodules:
  - `thirdparty/cpp_util` -> `https://github.com/Torstan/cpp_util`
  - `thirdparty/libco` -> `https://github.com/Torstan/libco`
  - `thirdparty/jemalloc` -> `git@github.com:jemalloc/jemalloc.git`
- Use C++17.
- Use libco as the coroutine framework.
- Use `cpp_util/redis` for Redis protocol pack/unpack support where applicable.
- Support one configured Redis backend process.
- Support one process with multiple worker threads.
- Each worker owns 1-4 Redis backend connections.
- Support RESP2 array/multibulk client requests.
- Parse backend RESP replies to frame boundaries, without business-level response interpretation.
- Guarantee request execution and response order within the same client connection.
- Allow natural interleaving between different client connections.
- Use configurable command safety rules initialized at process startup.
- Include performance tests that measure single-worker QPS.

Out of scope for the first version:

- Redis Cluster routing, `MOVED`, or `ASK`.
- RESP3 and `HELLO 3`.
- Inline Redis command syntax.
- Transaction, subscription, blocking, or connection-state commands.
- Slow-client backpressure.
- Slow-backend queue protection.
- Request replay after backend failure.
- Dynamic hot reload of command rules.
- Metrics service or admin API.

## Ordering Semantics

The proxy guarantees ordering only inside one client connection.

For a single client connection:

- Requests are parsed in socket receive order.
- Requests are submitted to the same backend connection while the client has outstanding responses.
- Requests are written to Redis in the same order they were accepted from that client.
- Redis responses are returned to the client in the same order.

For different client connections:

- The proxy does not guarantee ordering.
- Requests may interleave according to worker scheduling and backend channel availability.

This matches the selected first-version requirement: pipeline correctness is required per client connection, but global ordering across clients is not required.

## Architecture

The process is a single binary with one acceptor and multiple worker threads.

### Components

```text
ProxyServer
  Acceptor
  Worker[N]
    BackendPool
      BackendChannel[M]
    ClientSession...
    CommandRules
    RespParser
    IoBuffer / BufferChain
    CoSocket / CoWriter
```

### Responsibilities

- `ProxyServer`: Loads configuration, initializes command rules, configures allocator/linking, starts acceptor and workers.
- `Acceptor`: Owns the listen socket and accepts client fds. It dispatches accepted fds to workers by round-robin or least-client-count.
- `Worker`: Owns one libco scheduler context, client sessions, and backend pool. Worker-owned state is not shared across worker threads.
- `BackendPool`: Owns 1-4 backend channels for one worker. It selects a channel for a client session, honoring temporary per-session affinity while responses are pending.
- `BackendChannel`: Owns one Redis backend TCP connection, request write queue, response pending queue, backend reader coroutine, and backend writer coroutine.
- `ClientSession`: Owns one client TCP connection, request parser state, current backend affinity, pending response count, and client writer coroutine.
- `CommandRules`: Owns the startup-initialized command validation table and exposes a read-only validation API.
- `RespParser`: Scans RESP2 frame boundaries and extracts command metadata for validation.
- `IoBuffer`: Owns socket read buffers and produces reference-counted buffer slices.
- `CoSocket` / `CoWriter`: Wraps libco-compatible socket reads and writes so business logic uses coroutine I/O instead of raw blocking system calls.

## Worker Thread Model

Each worker thread owns all state for its sessions and backend channels. No backend queues, buffer blocks, sessions, or channels are shared between workers. This allows most state to avoid locks and atomic operations.

The acceptor transfers accepted client fds to workers. The exact transfer mechanism can be implemented with an existing libco pattern from `example_echosvr.cpp`, such as fd dispatch plus worker-local coroutine creation.

Each worker creates its backend pool during startup. Backend connections reconnect independently if Redis disconnects.

## Shared BackendChannel Design

Multiple client sessions may share one backend TCP connection. Correctness depends on keeping request write order and response dispatch order identical.

### Internal Data Structures

```cpp
class BackendChannel {
public:
    bool submit(ClientSession& owner, RequestBatch batch);
    size_t queuedCommandCount() const;
    bool isHealthy() const;

private:
    int redis_fd_;
    std::deque<RequestBatch> write_queue_;
    std::deque<PendingBatch> pending_queue_;
    IoBuffer redis_in_;
    CoSocket redis_socket_;
    bool reconnecting_;
    size_t queued_commands_;

    void writerLoop();
    void readerLoop();
    void failAllPending();
    void reconnectLoop();
};
```

```cpp
class RequestBatch {
public:
    ClientSession* owner() const;
    const BufferChain& encoded() const;
    uint32_t commandCount() const;
    uint64_t sequenceBase() const;

private:
    ClientSession* owner_;
    BufferChain encoded_;
    uint32_t command_count_;
    uint64_t sequence_base_;
};
```

```cpp
class PendingBatch {
public:
    ClientSession* owner() const;
    uint32_t remainingReplies() const;
    void consumeOneReply();

private:
    ClientSession* owner_;
    uint32_t remaining_replies_;
    uint64_t sequence_base_;
};
```

These are shown as conceptual interfaces. Implementation should keep mutable fields private and expose narrow methods.

### Submit Path

`ClientSession` does not directly mutate a backend queue. It calls `BackendPool::submit(session, batch)`.

`BackendPool` applies this policy:

- If the session has outstanding responses, reuse the same `BackendChannel`.
- If the session has no outstanding responses, choose the backend channel with the lowest lightweight load score, such as queued command count.
- After selecting a channel for a new outstanding sequence, record the channel on the session as temporary affinity.
- Clear the affinity when the session pending count reaches zero.

`BackendChannel::submit()` appends to both queues in the same logical order:

1. Append `RequestBatch` to `write_queue_`.
2. Append matching `PendingBatch` to `pending_queue_`.
3. Wake or schedule the writer coroutine.

The append order must match Redis write order. The reader relies on `pending_queue_.front()` to decide which client receives each Redis reply.

### Writer Loop

The writer coroutine serializes `write_queue_` to the Redis backend connection in FIFO order.

It uses `CoWriter::writeAll()` to write each `BufferChain` through libco-compatible coroutine I/O. The business layer does not call blocking `write`, `send`, or `writev` directly.

The low-level writer may use these strategies internally:

- Use libco-hooked `writev` if available and verified.
- Otherwise write one slice at a time with libco-hooked `write` or `send`.
- Optionally coalesce very small adjacent slices into a worker-local scratch buffer with a small fixed cap.

The queue item is removed only after all bytes in the batch are written or the backend fails.

### Reader Loop

The reader coroutine reads Redis replies into `redis_in_` and uses `RespParser` to find complete RESP reply frames.

For every complete reply frame:

1. Read `pending_queue_.front()`.
2. Transfer the reply `BufferChain` to that owner session.
3. Decrement `remaining_replies_`.
4. Pop `PendingBatch` when its reply count reaches zero.
5. Notify the owner session so it can clear backend affinity if its session-level pending count is now zero.

The parser only needs complete RESP frame boundaries. It does not interpret Redis error meaning or response business data.

### Client Disconnect

If a client disconnects:

- Unwritten queued batches owned by that session are removed from backend write queues.
- Already-written pending replies for that session remain represented in the backend pending queue.
- When those replies arrive from Redis, the backend reader consumes and discards them.

This preserves response queue alignment for other clients sharing the same backend connection.

## Memory Model

The design uses block-based buffers and reference-counted slices to avoid copying full Redis requests or replies.

### Buffer Types

```cpp
class BufferBlock {
private:
    char* data_;
    uint32_t capacity_;
    uint32_t begin_;
    uint32_t end_;
    uint32_t refcount_;
};

class BufferSlice {
private:
    BufferBlock* block_;
    uint32_t offset_;
    uint32_t length_;
};

class BufferChain {
public:
    size_t size() const;
    bool empty() const;

private:
    SmallVector<BufferSlice, 4> slices_;
    size_t total_len_;
};

class IoBuffer {
public:
    ReadResult readFrom(CoSocket& socket);
    ParseResult nextRespFrame(RespParser& parser);

private:
    BlockPool* pool_;
    std::deque<BufferBlock*> blocks_;
};
```

The exact small-vector implementation can come from an existing local utility if available, or be replaced by a small fixed inline array plus overflow vector if not.

### Reference Counting

Reference counting is required because parsed frame slices outlive the input buffer that produced them:

- Client request slices move from `ClientSession::input` to `BackendChannel::write_queue_`.
- Redis reply slices move from `BackendChannel::redis_in_` to `ClientSession::output`.

All buffer ownership stays inside one worker thread, so refcounts can be non-atomic integers. `BufferSlice` and `BufferChain` should use RAII constructors, move operations, and destructors to retain and release blocks. Business logic should not manually free blocks.

Blocks are returned to a worker-local pool when refcount reaches zero. This avoids cross-thread allocator contention and keeps buffer lifetime explicit.

### Copy Avoidance

The preferred request path is:

1. Read client bytes into `IoBuffer` blocks.
2. Locate complete RESP2 command frames.
3. Validate command metadata.
4. Create `BufferChain` slices referencing the original bytes.
5. Submit those slices to the backend channel.

The preferred response path is:

1. Read Redis bytes into backend `IoBuffer` blocks.
2. Locate complete RESP replies.
3. Create `BufferChain` slices referencing the original bytes.
4. Queue those slices to the target client session.

The proxy should not parse then repack full commands on the hot path. Temporary copying is acceptable only for small metadata extraction or frame headers that cross block boundaries.

## libco I/O Model

All network I/O must be coroutine-compatible.

`CoSocket` owns an fd and exposes coroutine methods such as:

```cpp
class CoSocket {
public:
    ssize_t readSome(IoBuffer& buffer, int timeout_ms);
    bool writeAll(const BufferChain& chain, int timeout_ms);
    void close();

private:
    int fd_;
};
```

Implementation details:

- Enable libco socket hooks in worker coroutine context where required.
- Use nonblocking fds.
- On `EAGAIN` or partial write, wait for readiness through libco-compatible polling/yielding.
- Hide short reads, short writes, and timeout handling inside `CoSocket`.
- Keep business code expressed as coroutine operations.

The design can still internally represent writes as slices suitable for `iovec`, but the final write operation must respect libco scheduling.

## Protocol Handling

Client request support is limited to RESP2 array/multibulk commands.

Accepted request form:

```text
*<argc>\r\n
$<len>\r\n
<arg>\r\n
...
```

Rejected request forms:

- Inline Redis commands.
- RESP3 negotiation, including `HELLO 3`.
- Malformed RESP.
- Over-limit bulk strings, arrays, or full request frames.

`RespParser` extracts:

- Complete frame byte range.
- Argument count.
- Command name.
- Optional minimal metadata needed by `CommandRules`.

The parser should use `cpp_util/redis` pack/unpack support where it fits the hot path. If the module requires contiguous memory for full parsing, the design should wrap it behind `RespParser` and limit copying to metadata extraction or validation. The forwarded bytes should remain the original request frame slices.

Redis backend replies are parsed only to complete RESP frame boundaries. The proxy forwards the original reply bytes without interpreting business meaning.

## Command Safety Rules

Command rules are configurable at startup. The process initializes a default C++ rule table, then applies config-file values, then command-line overrides if provided.

Rule fields:

```cpp
class CommandRule {
public:
    bool allowed() const;
    uint16_t minArgc() const;
    uint16_t maxArgc() const;
    bool isReadCommand() const;
    bool isWriteCommand() const;
    bool isDangerous() const;

private:
    bool allowed_;
    uint16_t min_argc_;
    uint16_t max_argc_;
    bool read_;
    bool write_;
    bool dangerous_;
};
```

Validation rules:

- Unknown commands are rejected by default.
- Dangerous commands are rejected by default.
- Connection-state commands are rejected by default.
- Blocking commands are rejected by default.
- Command name matching is case-insensitive.
- Runtime validation is a read-only lookup by normalized command name.

Default rejected command families include:

- Connection state: `AUTH`, `SELECT`, `CLIENT`, `HELLO`.
- Transactions and watch state: `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`.
- Pub/sub: `SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `SUNSUBSCRIBE`.
- Blocking or potentially blocking stream/list commands: `BLPOP`, `BRPOP`, `BRPOPLPUSH`, `BZPOPMIN`, `BZPOPMAX`, `BLMOVE`, `XREAD`, `XREADGROUP`.
- Dangerous/admin commands: `DEBUG`, `MODULE`, `CONFIG`, `SHUTDOWN`, `SAVE`, `BGSAVE`, `SCRIPT`, `EVAL`, `EVALSHA`, `FUNCTION`, `ACL`, `MONITOR`, `SYNC`, `PSYNC`.

The exact default table can be refined during implementation, but first-version behavior is deny-by-default for commands not explicitly allowed.

## Configuration

Configuration supports both a file and command-line overrides.

Example fields:

```text
listen = 0.0.0.0:6379
redis = 127.0.0.1:6380
workers = 4
backend_conns_per_worker = 2
max_request_bytes = 1048576
max_bulk_bytes = 1048576
max_array_elements = 1024
max_pipeline_commands_per_read = 256
connect_timeout_ms = 1000
read_timeout_ms = 30000
write_timeout_ms = 30000
```

Command-line values override config-file values. The process validates config before opening listen sockets.

## Error Handling

Protocol or validation failure:

- Return a RESP error when possible.
- Close the client connection for malformed protocol or size-limit violations.

Client disconnect:

- Remove not-yet-written batches owned by the client.
- Consume and discard already-written pending replies for the client.
- Keep backend channel response alignment intact.

Backend disconnect or timeout:

- Fail all queued and pending client sessions associated with the backend channel.
- Do not replay requests.
- Reconnect the backend channel.

Request replay is intentionally not supported because some write commands may already have executed in Redis.

Slow-client and slow-backend queue protection are intentionally not implemented in the first version. Protocol size limits still apply because they are correctness and memory-safety constraints rather than backpressure behavior.

## Object-Oriented Encapsulation

The code should favor class boundaries with private mutable state.

Guidelines:

- Do not expose backend queues directly.
- Do not expose raw block release APIs to business logic.
- Keep fd ownership inside `CoSocket` or owner classes.
- Keep command validation inside `CommandRules`.
- Keep pipeline ordering inside `BackendChannel`.
- Keep backend selection and temporary affinity inside `BackendPool`.
- Keep session state transitions inside `ClientSession`.

This keeps correctness properties local: callers submit requests and receive replies, but do not manipulate queue internals.

## Build Design

Use CMake and C++17.

Suggested layout:

```text
CMakeLists.txt
include/redis_proxy/
src/
tests/
bench/
thirdparty/
```

The main binary links:

- libco
- jemalloc
- cpp_util Redis protocol module

The build should keep third-party code isolated under `thirdparty/` and avoid modifying submodule sources unless unavoidable.

## Test Design

Unit tests:

- RESP2 frame parsing.
- Command rule lookup and rejection.
- BufferBlock refcount retain/release.
- BufferChain move/copy lifetime.
- BackendChannel write queue and pending queue ordering.
- Client session affinity while pending responses exist.

Integration tests:

- Start local `redis-server`.
- Start proxy.
- Send RESP2 pipeline from one client and assert response order.
- Verify Redis final values match command order.
- Run multiple clients concurrently and assert each client sees its own ordered responses.
- Disconnect a client after requests were written and verify backend channel stays aligned for other clients.
- Restart or kill Redis and verify affected clients fail without replay.

Performance tests:

- Provide a repeatable single-worker benchmark runner under `bench/`.
- Run the proxy with `workers = 1` and sweep `backend_conns_per_worker = 1, 2, 4`.
- Use a local Redis backend on loopback and record a direct-to-Redis baseline in the same run.
- Measure fixed command profiles:
  - `PING` for minimal protocol overhead.
  - `GET` on preloaded small keys.
  - `SET` with small values, such as 16 or 64 bytes.
- Sweep client concurrency and pipeline depth, for example:
  - clients: `1, 16, 64, 128`
  - pipeline depth: `1, 16, 64, 128`
- Use a warmup period before measurement, for example 5 seconds warmup and 30 seconds measurement.
- Report at least:
  - sustained QPS
  - direct Redis baseline QPS
  - proxy-to-direct ratio
  - average latency
  - p99 latency if the benchmark client can collect it cheaply
  - worker count, backend connection count, client count, pipeline depth, command profile
- Save results as text plus CSV or JSON so later runs can be compared.
- Support an optional `--min-qps` or environment threshold for performance gates. The default should report results without failing because absolute QPS depends on host CPU, Redis version, and OS settings.

The benchmark client should avoid Python on the hot path. Prefer a small C++ benchmark binary that uses the same RESP2 request encoding approach as the proxy tests, or use `redis-benchmark` as a secondary comparison tool. The C++ benchmark is preferred because it can explicitly verify per-client pipeline response order while measuring QPS.

Manual performance checks:

- Multi-worker scaling after single-worker behavior is understood.
- Request forwarding path allocation count.
- Copies per request and per response on the hot path.

## Acceptance Criteria

- The project builds with C++17 through CMake.
- Submodules are present at the specified paths.
- Proxy can accept RESP2 array requests from a Redis client.
- Proxy forwards allowed commands to one Redis backend and returns Redis replies.
- Same-client pipeline execution and response order are preserved.
- Different clients may interleave without global ordering guarantees.
- Disallowed commands are rejected by configurable startup command rules.
- Backend failure does not replay requests.
- Shared backend connection logic is encapsulated in `BackendChannel` and tested for pending queue correctness.
- A single-worker performance benchmark can report QPS for configured client counts, pipeline depths, and backend connection counts.
- The benchmark can optionally fail when a configured minimum QPS threshold is not met.
