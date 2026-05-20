# Redis Proxy Single Worker Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Raise `redis_proxy` single-worker pipelined `PING` throughput toward 100k requests/sec by replacing timeout polling on hot queues with event-driven wakeups and forwarding client pipelines as backend batches.

**Architecture:** Keep the existing one-acceptor, one-or-more worker, worker-local backend-pool design. Add small wakeup primitives: a same-thread coroutine signal backed by libco `CoCond` for hot in-worker queues, and a pipe-backed fd notifier for cross-thread worker fd handoff. Add non-consuming RESP frame metadata parsing so a client read can validate and submit multiple pipelined commands as one backend batch.

**Tech Stack:** C++17, CMake, libco (`co_routine`, `co_cond`, `co_poll`), POSIX sockets/pipes, `redis-benchmark`, CTest, local Redis backend on `127.0.0.1:8888`.

---

## Root Cause Summary

Measured symptom: `redis_proxy -> 127.0.0.1:8888`, `redis-benchmark ... PING`, single worker and one backend connection, about 434 requests/sec with one client and about 13k-15k requests/sec with many clients. Direct Redis is much faster under the same host.

Primary hot-path causes:

- `src/backend_channel.cpp:173-175` waits for backend write work with `co::co_poll(nullptr, 0, 1)`. A request submitted just after the writer sleeps waits up to 1ms before it is sent to Redis.
- `src/client_session.cpp:81-84` waits for client output with `co::co_poll(nullptr, 0, 1)`. A backend reply waits up to 1ms before it is written back to the client.
- `thirdparty/libco/co_routine.cpp:479-491` shows `co_poll(nullptr, 0, 1)` is timeout-only; there is no fd readiness event that can wake it early.
- `src/client_session.cpp:57-68` parses multiple pipelined frames from one read, but `submitFrame()` sends each command with `command_count = 1`, creating per-command backend queue entries and per-command wakeups.
- A `redis-benchmark -P 64 PING` workload therefore becomes roughly 64 backend `RequestBatch` writes per client read, even though `BackendChannel` already has `command_count` and `PendingBatch` support for multi-reply batches.

Reference pattern:

- `thirdparty/libco/example/example_echosvr.cpp:67-76` registers fd readiness into the current thread epoll context and `example_echosvr.cpp:142-144` yields until that readiness callback resumes the coroutine.
- For same-thread non-fd events, `thirdparty/libco/co_cond.cpp:14-22` activates a waiter through the libco active list instead of direct nested `co_resume`.

Benchmark rule:

- Use explicit `PING`: `redis-benchmark -h 127.0.0.1 -p <port> -n <n> -c <c> -P <p> -q PING`.
- Do not use `redis-benchmark -t ping`; that mode includes `PING_INLINE`, and this proxy intentionally accepts RESP arrays only.

## File Structure

Create:

- `include/redis_proxy/coroutine_signal.h`: same-thread single-consumer coroutine signal with a pending bit.
- `src/coroutine_signal.cpp`: `CoroutineSignal` implementation using `co::CoCond`.
- `tests/coroutine_signal_test.cpp`: no-lost-wakeup and active-list wake tests.
- `include/redis_proxy/fd_notifier.h`: pipe-backed notifier for cross-thread worker wakeups.
- `src/fd_notifier.cpp`: nonblocking pipe notify/drain implementation.
- `tests/fd_notifier_test.cpp`: poll/drain behavior test.
- `tests/client_session_test.cpp`: focused tests for client output wake and batch submit accounting.
- `bench/run_redis_benchmark_smoke.sh`: reproducible direct-vs-proxy `redis-benchmark` smoke against backend port `8888`.

Modify:

- `include/redis_proxy/backend_channel.h`: add writer and health `CoroutineSignal` members plus test accessors.
- `src/backend_channel.cpp`: notify backend writer on submit and notify backend reader when reconnect succeeds.
- `include/redis_proxy/client_session.h`: add output `CoroutineSignal`, batch submit method, and test accessors.
- `src/client_session.cpp`: wake writer on output/close and submit pipelined requests as one backend batch.
- `include/redis_proxy/resp_parser.h`: add `RespFrameInfo` and non-consuming `peekFrame()`.
- `src/resp_parser.cpp`: implement non-consuming metadata parse.
- `tests/resp_parser_test.cpp`: cover `peekFrame()` without consuming input.
- `include/redis_proxy/buffer.h`: add `BufferChain::appendChain()`.
- `src/buffer.cpp`: implement chain append for reply coalescing.
- `include/redis_proxy/co_socket.h`: keep public `writeAll()` signature.
- `src/co_socket.cpp`: use `writev` for multi-slice `BufferChain` writes.
- `tests/buffer_test.cpp`: cover chain append.
- `tests/co_socket_test.cpp`: cover multi-slice `writeAll()`.
- `include/redis_proxy/worker.h`: add `FdNotifier`.
- `src/worker.cpp`: block reaper on notifier fd instead of 1ms timer.
- `bench/run_single_worker_qps.sh`: call the new smoke script or document the explicit `redis-benchmark` mode.

## Task 1: Add Reproducible Redis-Benchmark Smoke

**Files:**

- Create: `bench/run_redis_benchmark_smoke.sh`
- Modify: `bench/run_single_worker_qps.sh`

- [ ] **Step 1: Write the failing smoke command**

Run:

```bash
REDIS_BACKEND_PORT=8888 PROXY_PORT=6391 REQUESTS=1000 CLIENTS=1 PIPELINE=1 bench/run_redis_benchmark_smoke.sh
```

Expected: fails with `No such file or directory` because the smoke script does not exist.

- [ ] **Step 2: Create the smoke script**

Create `bench/run_redis_benchmark_smoke.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

build_dir="${BUILD_DIR:-build}"
proxy_bin="${PROXY_BIN:-${build_dir}/redis_proxy}"
backend_host="${REDIS_BACKEND_HOST:-127.0.0.1}"
backend_port="${REDIS_BACKEND_PORT:-8888}"
proxy_host="${PROXY_HOST:-127.0.0.1}"
proxy_port="${PROXY_PORT:-6391}"
requests="${REQUESTS:-100000}"
clients="${CLIENTS:-50}"
pipeline="${PIPELINE:-16}"
backend_conns="${BACKEND_CONNS:-1}"
min_proxy_qps="${MIN_PROXY_QPS:-100000}"
min_direct_qps="${MIN_DIRECT_QPS:-125000}"
out_dir="${BENCH_OUT_DIR:-${build_dir}/bench-results}"

mkdir -p "${out_dir}"

if [[ ! -x "${proxy_bin}" ]]; then
  echo "missing redis_proxy binary: ${proxy_bin}" >&2
  exit 1
fi
if ! command -v redis-benchmark >/dev/null 2>&1; then
  echo "redis-benchmark not found" >&2
  exit 1
fi
if ! command -v redis-cli >/dev/null 2>&1; then
  echo "redis-cli not found" >&2
  exit 1
fi

redis-cli -h "${backend_host}" -p "${backend_port}" PING >/dev/null

"${proxy_bin}" \
  --listen "${proxy_host}:${proxy_port}" \
  --redis "${backend_host}:${backend_port}" \
  --workers 1 \
  --backend-conns "${backend_conns}" \
  >"${out_dir}/redis-benchmark-smoke-proxy.log" 2>&1 &
proxy_pid=$!
trap 'kill "${proxy_pid}" >/dev/null 2>&1 || true' EXIT
sleep 0.5

extract_qps() {
  awk -F'[:, ]+' '/requests per second/ { print $2; exit }'
}

direct_out="${out_dir}/direct-ping-c${clients}-p${pipeline}.txt"
proxy_out="${out_dir}/proxy-ping-c${clients}-p${pipeline}.txt"

redis-benchmark -h "${backend_host}" -p "${backend_port}" \
  -n "${requests}" -c "${clients}" -P "${pipeline}" -q PING \
  | tee "${direct_out}"

redis-benchmark -h "${proxy_host}" -p "${proxy_port}" \
  -n "${requests}" -c "${clients}" -P "${pipeline}" -q PING \
  | tee "${proxy_out}"

direct_qps="$(extract_qps <"${direct_out}")"
proxy_qps="$(extract_qps <"${proxy_out}")"

awk -v qps="${direct_qps}" -v min="${min_direct_qps}" 'BEGIN { exit(qps >= min ? 0 : 1) }' || {
  echo "direct Redis qps ${direct_qps} is below ${min_direct_qps}; environment cannot prove 100k proxy target" >&2
  exit 2
}

awk -v qps="${proxy_qps}" -v min="${min_proxy_qps}" 'BEGIN { exit(qps >= min ? 0 : 1) }' || {
  echo "proxy qps ${proxy_qps} is below ${min_proxy_qps}" >&2
  exit 3
}

echo "direct_qps=${direct_qps}"
echo "proxy_qps=${proxy_qps}"
```

- [ ] **Step 3: Make the script executable**

Run:

```bash
chmod +x bench/run_redis_benchmark_smoke.sh
```

Expected: command exits with status `0`.

- [ ] **Step 4: Wire the existing benchmark wrapper**

Append this block near the top of `bench/run_single_worker_qps.sh`, after variable initialization:

```bash
if [[ "${USE_REDIS_BENCHMARK_SMOKE:-0}" == "1" ]]; then
  BUILD_DIR="${build_dir}" \
  REDIS_BACKEND_PORT="${REDIS_PORT:-8888}" \
  PROXY_PORT="${proxy_port}" \
  BACKEND_CONNS="${BACKEND_CONNS:-1}" \
  bench/run_redis_benchmark_smoke.sh
  exit $?
fi
```

- [ ] **Step 5: Verify script syntax**

Run:

```bash
bash -n bench/run_redis_benchmark_smoke.sh bench/run_single_worker_qps.sh
```

Expected: exits with status `0`.

- [ ] **Step 6: Commit**

```bash
git add bench/run_redis_benchmark_smoke.sh bench/run_single_worker_qps.sh
git commit -m "bench: add redis-benchmark proxy smoke"
```

## Task 2: Add Same-Thread Coroutine Signal

**Files:**

- Create: `include/redis_proxy/coroutine_signal.h`
- Create: `src/coroutine_signal.cpp`
- Create: `tests/coroutine_signal_test.cpp`

- [ ] **Step 1: Write the failing test**

Create `tests/coroutine_signal_test.cpp`:

```cpp
#include "redis_proxy/coroutine_signal.h"
#include "test_common.h"

#include "co_routine.h"

#include <iostream>

int main() {
  redis_proxy::CoroutineSignal signal;
  int stage = 0;
  bool done = false;

  signal.notify();
  co::Coroutine* waiter = co::co_create([&]() {
    signal.wait();
    RP_REQUIRE(stage == 0);
    stage = 1;
    signal.wait();
    RP_REQUIRE(stage == 2);
    stage = 3;
    done = true;
  });

  co::co_resume(waiter);
  RP_REQUIRE(stage == 1);
  RP_REQUIRE(!done);

  co::Coroutine* notifier = co::co_create([&]() {
    stage = 2;
    signal.notify();
  });
  co::co_resume(notifier);

  co::co_eventloop(
      [](void* arg) -> int { return *static_cast<bool*>(arg) ? -1 : 0; },
      &done);

  RP_REQUIRE(stage == 3);
  co::co_free(waiter);
  co::co_free(notifier);

  std::cout << "coroutine_signal_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R coroutine_signal_test --output-on-failure
```

Expected: build fails because `redis_proxy/coroutine_signal.h` does not exist.

- [ ] **Step 3: Add the signal interface**

Create `include/redis_proxy/coroutine_signal.h`:

```cpp
#pragma once

#include "co_cond.h"

namespace redis_proxy {

class CoroutineSignal {
public:
  CoroutineSignal() = default;
  CoroutineSignal(const CoroutineSignal&) = delete;
  CoroutineSignal& operator=(const CoroutineSignal&) = delete;

  void notify();
  void wait();
  bool pendingForTest() const;

private:
  co::CoCond cond_;
  bool pending_ = false;
};

}  // namespace redis_proxy
```

- [ ] **Step 4: Add the signal implementation**

Create `src/coroutine_signal.cpp`:

```cpp
#include "redis_proxy/coroutine_signal.h"

namespace redis_proxy {

void CoroutineSignal::notify() {
  pending_ = true;
  cond_.Signal();
}

void CoroutineSignal::wait() {
  if (pending_) {
    pending_ = false;
    return;
  }
  cond_.Timedwait(0);
  pending_ = false;
}

bool CoroutineSignal::pendingForTest() const { return pending_; }

}  // namespace redis_proxy
```

- [ ] **Step 5: Run the new test**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R coroutine_signal_test --output-on-failure
```

Expected: `coroutine_signal_test` passes.

- [ ] **Step 6: Commit**

```bash
git add include/redis_proxy/coroutine_signal.h src/coroutine_signal.cpp tests/coroutine_signal_test.cpp
git commit -m "feat: add coroutine signal primitive"
```

## Task 3: Wake Backend Writer and Reader Without 1ms Queue Polling

**Files:**

- Modify: `include/redis_proxy/backend_channel.h`
- Modify: `src/backend_channel.cpp`
- Modify: `tests/backend_channel_test.cpp`

- [ ] **Step 1: Write the failing backend wake assertions**

In `tests/backend_channel_test.cpp`, after the first `submitForTest()` call, add:

```cpp
  RP_REQUIRE(channel.writerSignalPendingForTest());
```

After the first two `dispatchReplyForTest()` calls and the `pendingBatchCountForTest() == 0` assertion, add:

```cpp
  RP_REQUIRE(!channel.healthSignalPendingForTest());
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R backend_channel_test --output-on-failure
```

Expected: build fails because `writerSignalPendingForTest()` and `healthSignalPendingForTest()` do not exist.

- [ ] **Step 3: Add backend signals to the header**

In `include/redis_proxy/backend_channel.h`, add this include:

```cpp
#include "redis_proxy/coroutine_signal.h"
```

Add these public test accessors after `pendingBatchCountForTest()`:

```cpp
  bool writerSignalPendingForTest() const;
  bool healthSignalPendingForTest() const;
```

Add these private members after `std::deque<PendingBatch> pending_queue_;`:

```cpp
  CoroutineSignal writer_signal_;
  CoroutineSignal health_signal_;
```

- [ ] **Step 4: Notify backend writer and reader**

In `src/backend_channel.cpp`, replace `BackendChannel::submit()` with:

```cpp
bool BackendChannel::submit(ReplySink* owner, BufferChain bytes,
                            uint32_t command_count, uint64_t sequence_base) {
  write_queue_.emplace_back(owner, std::move(bytes), command_count,
                            sequence_base);
  pending_queue_.emplace_back(owner, command_count, sequence_base);
  queued_commands_ += command_count;
  writer_signal_.notify();
  return true;
}
```

Add these accessors after `pendingBatchCountForTest()`:

```cpp
bool BackendChannel::writerSignalPendingForTest() const {
  return writer_signal_.pendingForTest();
}

bool BackendChannel::healthSignalPendingForTest() const {
  return health_signal_.pendingForTest();
}
```

In `connectOnce()`, replace the success block:

```cpp
  healthy_ = true;
  return Status::Ok();
```

with:

```cpp
  healthy_ = true;
  health_signal_.notify();
  return Status::Ok();
```

In `writerLoop()`, replace:

```cpp
    if (write_queue_.empty()) {
      co::co_poll(nullptr, 0, 1);
      continue;
    }
```

with:

```cpp
    if (write_queue_.empty()) {
      writer_signal_.wait();
      continue;
    }
```

In `readerLoop()`, replace:

```cpp
    while (!healthy_) {
      co::co_poll(nullptr, 0, reconnect_delay_ms_);
    }
```

with:

```cpp
    while (!healthy_) {
      health_signal_.wait();
    }
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R "backend_channel_test|coroutine_signal_test" --output-on-failure
```

Expected: both tests pass.

- [ ] **Step 6: Verify the hot polling site is gone**

Run:

```bash
rg -n "co_poll\\(nullptr, 0, 1\\)" src/backend_channel.cpp
```

Expected: no matches.

- [ ] **Step 7: Commit**

```bash
git add include/redis_proxy/backend_channel.h src/backend_channel.cpp tests/backend_channel_test.cpp
git commit -m "perf: wake backend channel writer by signal"
```

## Task 4: Wake Client Writer on Replies, Errors, and Close

**Files:**

- Modify: `include/redis_proxy/client_session.h`
- Modify: `src/client_session.cpp`
- Create: `tests/client_session_test.cpp`

- [ ] **Step 1: Write the failing client output wake test**

Create `tests/client_session_test.cpp`:

```cpp
#include "redis_proxy/backend_pool.h"
#include "redis_proxy/client_session.h"
#include "redis_proxy/command_rules.h"
#include "test_common.h"

#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

int main() {
  int fds[2];
  RP_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

  redis_proxy::Config cfg;
  cfg.backend_conns_per_worker = 1;
  redis_proxy::BlockPool pool(64);
  redis_proxy::CommandRules rules = redis_proxy::CommandRules::Default();
  redis_proxy::BackendPool backend_pool(0, cfg, &pool);
  redis_proxy::ClientSession session(1, fds[0], cfg, &rules, &backend_pool,
                                     &pool);

  session.onBackendReply(redis_proxy::MakeBufferChain(&pool, "+PONG\r\n"));
  RP_REQUIRE(session.outputSignalPendingForTest());

  close(fds[1]);
  std::cout << "client_session_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R client_session_test --output-on-failure
```

Expected: build fails because `outputSignalPendingForTest()` does not exist.

- [ ] **Step 3: Add client output signal to the header**

In `include/redis_proxy/client_session.h`, add this include:

```cpp
#include "redis_proxy/coroutine_signal.h"
```

Add this public accessor after `pendingRepliesForTest()`:

```cpp
  bool outputSignalPendingForTest() const;
```

Add this private member after `std::deque<BufferChain> client_out_;`:

```cpp
  CoroutineSignal output_signal_;
```

- [ ] **Step 4: Wake the client writer**

In `src/client_session.cpp`, add this accessor after `pendingRepliesForTest()`:

```cpp
bool ClientSession::outputSignalPendingForTest() const {
  return output_signal_.pendingForTest();
}
```

In `readerLoop()`, after `closed_ = true;` in the read-error branch, add:

```cpp
      output_signal_.notify();
```

Replace the empty-output branch in `writerLoop()`:

```cpp
    if (client_out_.empty()) {
      co::co_poll(nullptr, 0, 1);
      continue;
    }
```

with:

```cpp
    if (client_out_.empty()) {
      output_signal_.wait();
      continue;
    }
```

In `onBackendReply()`, replace:

```cpp
  if (!closed_) {
    client_out_.push_back(std::move(reply));
  }
```

with:

```cpp
  if (!closed_) {
    client_out_.push_back(std::move(reply));
    output_signal_.notify();
  }
```

In `enqueueErrorAndClose()`, after `closed_ = true;`, add:

```cpp
  output_signal_.notify();
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R "client_session_test|integration_proxy_test" --output-on-failure
```

Expected: both tests pass.

- [ ] **Step 6: Verify client hot polling site is gone**

Run:

```bash
rg -n "co_poll\\(nullptr, 0, 1\\)" src/client_session.cpp
```

Expected: no matches.

- [ ] **Step 7: Commit**

```bash
git add include/redis_proxy/client_session.h src/client_session.cpp tests/client_session_test.cpp
git commit -m "perf: wake client writer by signal"
```

## Task 5: Parse and Submit Pipelined Requests as One Backend Batch

**Files:**

- Modify: `include/redis_proxy/resp_parser.h`
- Modify: `src/resp_parser.cpp`
- Modify: `tests/resp_parser_test.cpp`
- Modify: `include/redis_proxy/client_session.h`
- Modify: `src/client_session.cpp`
- Modify: `tests/client_session_test.cpp`
- Modify: `tests/integration_proxy_test.cpp`

- [ ] **Step 1: Write failing RESP peek test**

In `tests/resp_parser_test.cpp`, add this include:

```cpp
#include <string>
```

In `tests/resp_parser_test.cpp`, before the final success print, add:

```cpp
  redis_proxy::IoBuffer batch_input(&pool);
  const std::string ping = "*1\r\n$4\r\nPING\r\n";
  const std::string get = "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  batch_input.appendForTest(ping + get);

  redis_proxy::RespFrameInfo first;
  status = parser.peekFrame(batch_input, 0, &first);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(first.consumed == ping.size());
  RequireEqual(first.command_name, "PING");
  RP_REQUIRE(first.argc == 1);

  redis_proxy::RespFrameInfo second;
  status = parser.peekFrame(batch_input, first.consumed, &second);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(second.consumed == get.size());
  RequireEqual(second.command_name, "GET");
  RP_REQUIRE(second.argc == 2);
  RP_REQUIRE(batch_input.readableBytes() == ping.size() + get.size());

  redis_proxy::BlockPool small_pool(16);
  redis_proxy::IoBuffer split_input(&small_pool);
  const std::string set_cmd =
      "*3\r\n$3\r\nSET\r\n$5\r\nsplit\r\n$5\r\nvalue\r\n";
  split_input.appendForTest(set_cmd + ping);
  redis_proxy::RespFrameInfo split_first;
  status = parser.peekFrame(split_input, 0, &split_first);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(split_first.consumed == set_cmd.size());
  RequireEqual(split_first.command_name, "SET");
  RP_REQUIRE(split_first.argc == 3);
  RP_REQUIRE(split_input.readableBytes() == set_cmd.size() + ping.size());

  redis_proxy::IoBuffer invalid_batch(&pool);
  invalid_batch.appendForTest(ping + "PING\r\n");
  redis_proxy::RespFrameInfo valid_prefix;
  status = parser.peekFrame(invalid_batch, 0, &valid_prefix);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  redis_proxy::RespFrameInfo invalid_second;
  status = parser.peekFrame(invalid_batch, valid_prefix.consumed,
                            &invalid_second);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kError);
  RP_REQUIRE(invalid_batch.readableBytes() == ping.size() + 6);
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R resp_parser_test --output-on-failure
```

Expected: build fails because `RespFrameInfo` and `peekFrame()` do not exist.

- [ ] **Step 3: Add non-consuming parser API**

In `include/redis_proxy/resp_parser.h`, add after `RespFrame`:

```cpp
struct RespFrameInfo {
  std::size_t consumed = 0;
  std::string command_name;
  std::size_t argc = 0;
};
```

Add this public method after `nextFrame()`:

```cpp
  ParseStatus peekFrame(IoBuffer& input, std::size_t offset,
                        RespFrameInfo* out);
```

Add this private method after `extractCommand()`:

```cpp
  bool extractCommandInfo(const redis::RespValue& value,
                          RespFrameInfo* out) const;
```

- [ ] **Step 4: Implement non-consuming parser API**

In `src/resp_parser.cpp`, add this method after `extractCommand()`:

```cpp
bool RespParser::extractCommandInfo(const redis::RespValue& value,
                                    RespFrameInfo* out) const {
  if (value.type != redis::RespType::kArray || value.element_count == 0 ||
      value.elements == nullptr) {
    return false;
  }
  for (std::size_t i = 0; i < value.element_count; ++i) {
    if (value.elements[i].type != redis::RespType::kBulkString) {
      return false;
    }
  }
  out->argc = value.element_count;
  const redis::RespValue& command = value.elements[0];
  out->command_name.assign(command.text.data(), command.text.size());
  return true;
}
```

Add this method before `nextFrame()`:

```cpp
ParseStatus RespParser::peekFrame(IoBuffer& input, std::size_t offset,
                                  RespFrameInfo* out) {
  const std::size_t available = input.readableBytes();
  if (offset > available) {
    return ParseStatus::kError;
  }
  if (offset == available) {
    return ParseStatus::kNeedMore;
  }
  if (!input.ensureContiguousPrefix(available)) {
    return ParseStatus::kNeedMore;
  }
  std::string_view view = input.contiguousPrefixForTest(available);
  view.remove_prefix(offset);

  redis::RespResult result =
      redis::UnpackOne(view, scratch_.data(), scratch_.size(), limits_);
  if (result.status != redis::RespStatus::kOk) {
    return convert(result.status);
  }

  RespFrameInfo info;
  if (!extractCommandInfo(*result.value, &info)) {
    return ParseStatus::kError;
  }
  info.consumed = result.consumed;
  *out = std::move(info);
  return ParseStatus::kOk;
}
```

- [ ] **Step 5: Run parser test**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R resp_parser_test --output-on-failure
```

Expected: `resp_parser_test` passes.

- [ ] **Step 6: Write failing client batch accounting test**

In `include/redis_proxy/backend_pool.h`, add this public declaration after `selectForSessionForTest()`:

```cpp
  BackendChannel* channelForTest(std::size_t index);
```

In `src/backend_pool.cpp`, add:

```cpp
BackendChannel* BackendPool::channelForTest(std::size_t index) {
  if (index >= channels_.size()) {
    return nullptr;
  }
  return channels_[index].get();
}
```

In `include/redis_proxy/client_session.h`, add this public declaration after `outputSignalPendingForTest()`:

```cpp
  void submitBatchForTest(BufferChain bytes, uint32_t command_count);
```

In `tests/client_session_test.cpp`, after the output signal assertion, add:

```cpp
  const std::string two_pings =
      "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
  session.submitBatchForTest(redis_proxy::MakeBufferChain(&pool, two_pings), 2);
  RP_REQUIRE(session.pendingRepliesForTest() == 2);
  redis_proxy::BackendChannel* channel = backend_pool.channelForTest(0);
  RP_REQUIRE(channel != nullptr);
  RP_REQUIRE(channel->pendingBatchCountForTest() == 1);
  RP_REQUIRE(channel->queuedCommandCount() == 2);
```

Run:

```bash
cmake --build build -j && ctest --test-dir build -R client_session_test --output-on-failure
```

Expected: build fails because `submitBatchForTest()` has no implementation.

- [ ] **Step 7: Add batch submit implementation**

In `include/redis_proxy/client_session.h`, replace the private declaration:

```cpp
  void submitFrame(RespFrame frame);
```

with:

```cpp
  void submitBatch(BufferChain bytes, uint32_t command_count);
```

In `src/client_session.cpp`, replace `submitFrame()` with:

```cpp
void ClientSession::submitBatch(BufferChain bytes, uint32_t command_count) {
  if (command_count == 0) {
    return;
  }
  const bool has_pending = pending_replies_ > 0;
  const uint64_t sequence_base = next_sequence_;
  pending_replies_ += command_count;
  next_sequence_ += command_count;
  current_backend_ = backend_pool_->submit(
      this, current_backend_, has_pending, std::move(bytes), command_count,
      sequence_base);
}

void ClientSession::submitBatchForTest(BufferChain bytes,
                                       uint32_t command_count) {
  submitBatch(std::move(bytes), command_count);
}
```

- [ ] **Step 8: Batch valid frames in the reader loop**

In `src/client_session.cpp`, replace the inner parse loop in `readerLoop()`:

```cpp
    std::size_t parsed = 0;
    while (parsed < config_.max_pipeline_commands_per_read) {
      RespFrame frame;
      ParseStatus ps = parser_.nextFrame(client_in_, &frame);
      if (ps == ParseStatus::kNeedMore) {
        break;
      }
      if (ps != ParseStatus::kOk) {
        enqueueErrorAndClose("ERR proxy protocol error");
        break;
      }
      submitFrame(std::move(frame));
      ++parsed;
      if (closed_) {
        break;
      }
    }
```

with:

```cpp
    std::size_t parsed = 0;
    std::size_t consumed = 0;
    while (parsed < config_.max_pipeline_commands_per_read) {
      RespFrameInfo info;
      ParseStatus ps = parser_.peekFrame(client_in_, consumed, &info);
      if (ps == ParseStatus::kNeedMore) {
        break;
      }
      if (ps != ParseStatus::kOk) {
        enqueueErrorAndClose("ERR proxy protocol error");
        break;
      }
      Status valid = rules_->validate(info.command_name, info.argc);
      if (!valid.ok()) {
        if (parsed > 0) {
          submitBatch(client_in_.slicePrefix(consumed),
                      static_cast<uint32_t>(parsed));
        }
        enqueueErrorAndClose(valid.message());
        break;
      }
      consumed += info.consumed;
      ++parsed;
    }
    if (!closed_ && parsed > 0) {
      submitBatch(client_in_.slicePrefix(consumed),
                  static_cast<uint32_t>(parsed));
    }
```

- [ ] **Step 9: Add integration coverage for a real pipeline batch**

In `tests/integration_proxy_test.cpp`, add this include:

```cpp
#include <string_view>
```

Add this helper after `ReadUntil()`:

```cpp
std::size_t CountOccurrences(std::string_view haystack,
                             std::string_view needle) {
  std::size_t count = 0;
  std::size_t pos = 0;
  while ((pos = haystack.find(needle, pos)) != std::string_view::npos) {
    ++count;
    pos += needle.size();
  }
  return count;
}

std::string ReadUntilOccurrences(int fd, std::string_view needle,
                                 std::size_t expected_count) {
  char buf[4096];
  std::string out;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline &&
         CountOccurrences(out, needle) < expected_count) {
    pollfd pfd{fd, POLLIN | POLLERR | POLLHUP, 0};
    const int ready = poll(&pfd, 1, 100);
    if (ready <= 0) {
      continue;
    }
    ssize_t n = read(fd, buf, sizeof(buf));
    if (n <= 0) {
      break;
    }
    out.append(buf, static_cast<std::size_t>(n));
  }
  return out;
}
```

In `main()`, after the first request/reply assertion block and before closing `fd`, add:

```cpp
  if (ok) {
    std::string ping_req;
    for (int i = 0; i < 128; ++i) {
      redis::PackCommand({"PING"}, &ping_req);
    }
    ok = write(fd, ping_req.data(), ping_req.size()) ==
         static_cast<ssize_t>(ping_req.size());
    if (ok) {
      reply = ReadUntilOccurrences(fd, "+PONG\r\n", 128);
      ok = CountOccurrences(reply, "+PONG\r\n") == 128;
    }
  }
```

- [ ] **Step 10: Run focused tests**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R "resp_parser_test|client_session_test|backend_channel_test|integration_proxy_test" --output-on-failure
```

Expected: all selected tests pass.

- [ ] **Step 11: Commit**

```bash
git add include/redis_proxy/resp_parser.h src/resp_parser.cpp tests/resp_parser_test.cpp include/redis_proxy/client_session.h src/client_session.cpp tests/client_session_test.cpp include/redis_proxy/backend_pool.h src/backend_pool.cpp tests/integration_proxy_test.cpp
git commit -m "perf: batch pipelined client requests"
```

## Task 6: Coalesce Client Reply Writes and Use Writev for BufferChains

**Files:**

- Modify: `include/redis_proxy/buffer.h`
- Modify: `src/buffer.cpp`
- Modify: `tests/buffer_test.cpp`
- Modify: `src/client_session.cpp`
- Modify: `src/co_socket.cpp`
- Modify: `tests/co_socket_test.cpp`

- [ ] **Step 1: Write failing BufferChain append test**

In `tests/buffer_test.cpp`, before the final success print, add:

```cpp
  redis_proxy::BufferChain left =
      redis_proxy::MakeBufferChain(&pool, "+PONG\r\n");
  redis_proxy::BufferChain right =
      redis_proxy::MakeBufferChain(&pool, ":1\r\n");
  left.appendChain(std::move(right));
  RequireEqual(left.toStringForTest(), "+PONG\r\n:1\r\n");
  RP_REQUIRE(right.empty());
```

Run:

```bash
cmake --build build -j && ctest --test-dir build -R buffer_test --output-on-failure
```

Expected: build fails because `appendChain()` does not exist.

- [ ] **Step 2: Add BufferChain append**

In `include/redis_proxy/buffer.h`, add after `void append(BufferSlice slice);`:

```cpp
  void appendChain(BufferChain&& chain);
```

In `src/buffer.cpp`, add after `BufferChain::append()`:

```cpp
void BufferChain::appendChain(BufferChain&& chain) {
  for (auto& slice : chain.slices_) {
    append(std::move(slice));
  }
  chain.slices_.clear();
  chain.total_ = 0;
}
```

- [ ] **Step 3: Coalesce queued client replies**

In `src/client_session.cpp`, replace:

```cpp
    BufferChain reply = std::move(client_out_.front());
    client_out_.pop_front();
    Status st = socket_.writeAll(reply, config_.write_timeout_ms);
```

with:

```cpp
    BufferChain reply = std::move(client_out_.front());
    client_out_.pop_front();
    std::size_t coalesced = 1;
    while (coalesced < config_.max_pipeline_commands_per_read &&
           !client_out_.empty()) {
      reply.appendChain(std::move(client_out_.front()));
      client_out_.pop_front();
      ++coalesced;
    }
    Status st = socket_.writeAll(reply, config_.write_timeout_ms);
```

- [ ] **Step 4: Write failing multi-slice socket test**

In `tests/co_socket_test.cpp`, replace the single `hello` block setup with:

```cpp
  {
    redis_proxy::BufferBlock* block = pool.acquire();
    std::memcpy(block->writePtr(), "hello", 5);
    block->advanceEnd(5);
    output.append(
        redis_proxy::BufferSlice::retain(block, block->begin(), block->size()));
    block->release();
  }
  {
    redis_proxy::BufferBlock* block = pool.acquire();
    std::memcpy(block->writePtr(), " world", 6);
    block->advanceEnd(6);
    output.append(
        redis_proxy::BufferSlice::retain(block, block->begin(), block->size()));
    block->release();
  }
```

Replace the reader assertion:

```cpp
    RequireEqual(input.contiguousPrefixForTest(5), "hello");
```

with:

```cpp
    while (input.readableBytes() < 11) {
      RP_REQUIRE(sock.readSome(&input, 1000).ok());
    }
    RequireEqual(input.contiguousPrefixForTest(11), "hello world");
```

Run:

```bash
cmake --build build -j && ctest --test-dir build -R co_socket_test --output-on-failure
```

Expected: test passes with the existing write loop, establishing behavior before the `writev` optimization.

- [ ] **Step 5: Replace per-slice writes with writev**

In `src/co_socket.cpp`, add:

```cpp
#include <algorithm>
#include <sys/uio.h>
```

Replace `CoSocket::writeAll()` with:

```cpp
Status CoSocket::writeAll(const BufferChain& chain, int timeout_ms) {
  const auto& slices = chain.slices();
  std::size_t index = 0;
  std::size_t offset = 0;

  while (index < slices.size()) {
    iovec iov[64];
    int iovcnt = 0;
    for (std::size_t i = index; i < slices.size() && iovcnt < 64; ++i) {
      const BufferSlice& slice = slices[i];
      const std::size_t slice_offset = (i == index) ? offset : 0;
      if (slice.size() <= slice_offset) {
        continue;
      }
      iov[iovcnt].iov_base =
          const_cast<char*>(slice.data() + slice_offset);
      iov[iovcnt].iov_len = slice.size() - slice_offset;
      ++iovcnt;
    }

    if (iovcnt == 0) {
      return Status::Ok();
    }

    const ssize_t n = ::writev(fd_, iov, iovcnt);
    if (n > 0) {
      std::size_t written = static_cast<std::size_t>(n);
      while (written > 0 && index < slices.size()) {
        const std::size_t available = slices[index].size() - offset;
        if (written < available) {
          offset += written;
          written = 0;
        } else {
          written -= available;
          ++index;
          offset = 0;
        }
      }
      continue;
    }

    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      pollfd pfd{fd_, POLLOUT | POLLERR | POLLHUP, 0};
      const int ret = co::co_poll(&pfd, 1, timeout_ms);
      if (ret <= 0) {
        return Status::IoError("write timeout");
      }
      continue;
    }

    return Status::IoError(n == 0 ? "zero write" : std::strerror(errno));
  }

  return Status::Ok();
}
```

- [ ] **Step 6: Run focused tests**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R "buffer_test|co_socket_test|client_session_test|integration_proxy_test" --output-on-failure
```

Expected: all selected tests pass.

- [ ] **Step 7: Commit**

```bash
git add include/redis_proxy/buffer.h src/buffer.cpp tests/buffer_test.cpp src/client_session.cpp src/co_socket.cpp tests/co_socket_test.cpp
git commit -m "perf: coalesce client reply writes"
```

## Task 7: Replace Worker Fd Handoff Polling with Pipe Notifier

**Files:**

- Create: `include/redis_proxy/fd_notifier.h`
- Create: `src/fd_notifier.cpp`
- Create: `tests/fd_notifier_test.cpp`
- Modify: `include/redis_proxy/worker.h`
- Modify: `src/worker.cpp`

- [ ] **Step 1: Write failing fd notifier test**

Create `tests/fd_notifier_test.cpp`:

```cpp
#include "redis_proxy/fd_notifier.h"
#include "test_common.h"

#include <iostream>
#include <poll.h>

int main() {
  redis_proxy::FdNotifier notifier;
  RP_REQUIRE(notifier.valid());
  RP_REQUIRE(notifier.notify().ok());

  pollfd pfd{notifier.readFd(), POLLIN | POLLERR | POLLHUP, 0};
  RP_REQUIRE(poll(&pfd, 1, 1000) == 1);
  RP_REQUIRE((pfd.revents & POLLIN) != 0);
  RP_REQUIRE(notifier.drain().ok());

  pfd.revents = 0;
  RP_REQUIRE(poll(&pfd, 1, 0) == 0);

  std::cout << "fd_notifier_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R fd_notifier_test --output-on-failure
```

Expected: build fails because `redis_proxy/fd_notifier.h` does not exist.

- [ ] **Step 3: Add fd notifier interface**

Create `include/redis_proxy/fd_notifier.h`:

```cpp
#pragma once

#include "redis_proxy/status.h"

namespace redis_proxy {

class FdNotifier {
public:
  FdNotifier();
  ~FdNotifier();
  FdNotifier(const FdNotifier&) = delete;
  FdNotifier& operator=(const FdNotifier&) = delete;

  bool valid() const;
  int readFd() const;
  Status notify();
  Status drain();

private:
  int read_fd_ = -1;
  int write_fd_ = -1;
};

}  // namespace redis_proxy
```

- [ ] **Step 4: Add fd notifier implementation**

Create `src/fd_notifier.cpp`:

```cpp
#include "redis_proxy/fd_notifier.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace redis_proxy {

namespace {

bool SetNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return false;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

}  // namespace

FdNotifier::FdNotifier() {
  int fds[2] = {-1, -1};
  if (pipe(fds) != 0) {
    return;
  }
  read_fd_ = fds[0];
  write_fd_ = fds[1];
  if (!SetNonBlocking(read_fd_) || !SetNonBlocking(write_fd_)) {
    close(read_fd_);
    close(write_fd_);
    read_fd_ = -1;
    write_fd_ = -1;
  }
}

FdNotifier::~FdNotifier() {
  if (read_fd_ >= 0) {
    close(read_fd_);
  }
  if (write_fd_ >= 0) {
    close(write_fd_);
  }
}

bool FdNotifier::valid() const { return read_fd_ >= 0 && write_fd_ >= 0; }

int FdNotifier::readFd() const { return read_fd_; }

Status FdNotifier::notify() {
  if (!valid()) {
    return Status::IoError("fd notifier is not initialized");
  }
  const char byte = 'x';
  ssize_t n = write(write_fd_, &byte, 1);
  if (n == 1 || errno == EAGAIN || errno == EWOULDBLOCK) {
    return Status::Ok();
  }
  return Status::IoError(std::strerror(errno));
}

Status FdNotifier::drain() {
  if (!valid()) {
    return Status::IoError("fd notifier is not initialized");
  }
  char buf[256];
  for (;;) {
    ssize_t n = read(read_fd_, buf, sizeof(buf));
    if (n > 0) {
      continue;
    }
    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      return Status::Ok();
    }
    if (n == 0) {
      return Status::Closed("fd notifier closed");
    }
    return Status::IoError(std::strerror(errno));
  }
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Use fd notifier in Worker**

In `include/redis_proxy/worker.h`, add:

```cpp
#include "redis_proxy/fd_notifier.h"
```

Add this private member after `std::atomic<bool> has_pending_fds_{false};`:

```cpp
  FdNotifier fd_notifier_;
```

In `src/worker.cpp`, add:

```cpp
#include <poll.h>
```

In `dispatchFd()`, after `has_pending_fds_.store(true, std::memory_order_release);`, add:

```cpp
  (void)fd_notifier_.notify();
```

Replace the reaper coroutine body:

```cpp
    while (true) {
      reapFds();
      co::co_poll(nullptr, 0, 1);
    }
```

with:

```cpp
    while (true) {
      reapFds();
      pollfd pfd{fd_notifier_.readFd(), POLLIN | POLLERR | POLLHUP, 0};
      const int ret = co::co_poll(&pfd, 1, -1);
      if (ret > 0) {
        (void)fd_notifier_.drain();
      }
    }
```

- [ ] **Step 6: Run focused tests and polling grep**

Run:

```bash
cmake --build build -j && ctest --test-dir build -R "fd_notifier_test|integration_proxy_test" --output-on-failure
```

Expected: both tests pass.

Run:

```bash
rg -n "co_poll\\(nullptr, 0, 1\\)" src
```

Expected: no matches.

- [ ] **Step 7: Commit**

```bash
git add include/redis_proxy/fd_notifier.h src/fd_notifier.cpp tests/fd_notifier_test.cpp include/redis_proxy/worker.h src/worker.cpp
git commit -m "perf: wake worker fd reaper by pipe"
```

## Task 8: Verify Single Worker Performance Target

**Files:**

- Modify: `docs/superpowers/plans/2026-05-20-redis-proxy-performance.md`

- [ ] **Step 1: Run all correctness tests**

Run:

```bash
cmake --build build -j && ctest --test-dir build --output-on-failure
```

Expected: all tests pass.

- [ ] **Step 2: Verify local backend is Redis on 8888**

Run:

```bash
redis-cli -p 8888 PING
```

Expected:

```text
PONG
```

- [ ] **Step 3: Run one-client latency smoke**

Run:

```bash
REQUESTS=5000 CLIENTS=1 PIPELINE=1 BACKEND_CONNS=1 MIN_DIRECT_QPS=1 MIN_PROXY_QPS=1 bench/run_redis_benchmark_smoke.sh
```

Expected: script exits with status `0`, and proxy output is materially above the old about-434 requests/sec baseline.

- [ ] **Step 4: Run target throughput smoke**

Run:

```bash
REQUESTS=100000 CLIENTS=50 PIPELINE=16 BACKEND_CONNS=1 MIN_DIRECT_QPS=125000 MIN_PROXY_QPS=100000 bench/run_redis_benchmark_smoke.sh
```

Expected: script exits with status `0` and prints both:

```text
direct_qps=<number at least 125000>
proxy_qps=<number at least 100000>
```

- [ ] **Step 5: Record measured numbers in this plan**

Append this section to this file with the actual command output numbers:

```markdown
## Verification Results

- Correctness: `ctest --test-dir build --output-on-failure` passed on YYYY-MM-DD.
- Backend: `redis-cli -p 8888 PING` returned `PONG`.
- One-client smoke: proxy explicit `PING`, `-c 1 -P 1`, measured <qps> requests/sec.
- Target smoke: proxy explicit `PING`, `-c 50 -P 16`, measured <qps> requests/sec.
- Direct Redis target baseline: explicit `PING`, `-c 50 -P 16`, measured <qps> requests/sec.
```

- [ ] **Step 6: Commit verification results**

```bash
git add docs/superpowers/plans/2026-05-20-redis-proxy-performance.md
git commit -m "docs: record redis proxy performance verification"
```

## Execution Notes

- Commit after each task. Each task above is one complete small feature or verification step.
- Use subagents for disjoint tasks:
  - Task 2 and Task 7 can be implemented independently.
  - Task 5 parser changes can run in parallel with Task 6 socket write coalescing after Task 4 lands.
  - Benchmark verification must run after Tasks 2-7 are integrated.
- Clean up completed subagents immediately after their result is reviewed.
- Do not delete untracked build artifacts such as `build/`, `build-jemalloc/`, `dump.rdb`, or `nohup.out` unless the user explicitly asks.

## Self-Review

- Spec coverage: the plan addresses the measured low QPS, the libco event-driven wakeup pattern, the local backend on `127.0.0.1:8888`, explicit `redis-benchmark` testing, single-worker 100k QPS acceptance, subagent-friendly decomposition, and per-feature commits.
- Placeholder scan: the plan contains concrete code, commands, expected results, and no omitted implementation steps.
- Type consistency: `CoroutineSignal`, `FdNotifier`, `RespFrameInfo`, `peekFrame()`, `submitBatch()`, `appendChain()`, and all test accessors are introduced before use.
