# Redis Proxy Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a C++17 Redis RESP2 proxy that uses libco coroutine I/O, cpp_util Redis RESP helpers, worker-local backend connections, per-client pipeline ordering, command validation, integration tests, and single-worker QPS benchmarks.

**Architecture:** One acceptor coroutine dispatches client fds to worker threads. Each worker runs a libco event loop, owns all client sessions, and owns a backend pool with 1-4 Redis backend channels. Request bytes are parsed with `cpp_util/redis` through a contiguous-frame adapter, forwarded without RESP repacking, and responses are assigned by each backend channel's FIFO pending queue.

**Tech Stack:** C++17, CMake, libco from `thirdparty/libco`, `redis/resp.h` from `thirdparty/cpp_util/redis/include`, jemalloc from `thirdparty/jemalloc`, POSIX sockets, CTest, local `redis-server` for integration and benchmark runs.

---

## Scope Check

The spec describes one cohesive proxy. It has multiple components, but they are not independent products: buffers, RESP parsing, backend queueing, sessions, workers, config, tests, and benchmarks all serve the same binary. Keep this as one implementation plan.

## Third-Party Interface Facts

Use these exact local interfaces.

### libco

Headers:

```cpp
#include "co_routine.h"
#include "thread_worker.h"
```

Important names:

```cpp
co::Coroutine* co::co_create(std::function<void()>&& func);
void co::co_resume(co::Coroutine* co);
void co::co_yield_ct();
void co::co_free(co::Coroutine* co);
void co::co_eventloop(co::pfn_co_eventloop_t func, void* arg);
void co::co_enable_hook_sys();
void co::co_disable_hook_sys();
int co::co_poll(struct pollfd fds[], nfds_t nfds, int timeout_ms);
int co::co_accept(int fd, struct sockaddr* addr, socklen_t* len);
class co::ThreadWorker { public: explicit ThreadWorker(int idx); void run_loop(bool forever = true); };
```

Observed constraints:

- `co_enable_hook_sys()` must run inside every coroutine that uses hooked socket I/O.
- Hooked syscalls include `read`, `write`, `recv`, `send`, `recvfrom`, `sendto`, `poll`, `setsockopt`, `fcntl`, and `close`.
- There is no `writev` hook in `thirdparty/libco/co_hook_sys_call.cpp`. `CoSocket::writeAll()` must write `BufferChain` slice-by-slice with hooked `write` or `send`.
- The echo server example uses one accept coroutine plus worker threads, `co_accept`, worker-local pending fd queues, `co_create`, `co_resume`, and `co::ThreadWorker::run_loop()`.
- `thread_worker.h` also exposes `co::schedule(std::unique_ptr<Task>)`, but cross-thread dispatch should follow the echo server pattern because `schedule` is thread-local.

### cpp_util Redis RESP

Header:

```cpp
#include "redis/resp.h"
```

Important names:

```cpp
namespace redis {
enum class RespType { kSimpleString, kError, kInteger, kBulkString, kNullBulkString, kArray, kNullArray };
struct RespValue {
  RespType type;
  std::string_view text;
  std::int64_t integer;
  RespValue* elements;
  std::size_t element_count;
};
struct RespLimits {
  std::size_t max_bulk_bytes;
  std::size_t max_array_elements;
  std::size_t max_depth;
};
enum class RespStatus { kOk, kNeedMore, kError, kNoMemory };
struct RespResult {
  RespStatus status;
  std::size_t consumed;
  RespValue* value;
  const char* error;
};
RespResult UnpackOne(std::string_view input, RespValue* scratch, std::size_t scratch_capacity, RespLimits limits = RespLimits{}) noexcept;
void PackError(std::string_view value, std::string* out);
void PackCommand(std::initializer_list<std::string_view> args, std::string* out);
}
```

Observed constraints:

- `redis::UnpackOne()` requires a contiguous `std::string_view`.
- Returned `RespValue::text` views point into the input bytes.
- Returned `RespValue::elements` points into caller-provided scratch storage.
- Reusing the input buffer or scratch invalidates the decoded result.
- `RespResult::consumed` is meaningful only when `status == redis::RespStatus::kOk`.
- Use `redis::PackError()` for proxy-generated error replies.
- Use `redis::PackCommand()` in tests and benchmarks, not in the proxy hot path.

## File Structure

Create or modify these files.

Build and scripts:

- `CMakeLists.txt`: top-level build, libco target wiring, cpp_util redis include target, jemalloc imported target, test and benchmark enablement.
- `cmake/Jemalloc.cmake`: imported jemalloc target and custom build target.
- `scripts/build_jemalloc.sh`: builds thirdparty jemalloc into the CMake build directory without network access.
- `Makefile`: keep existing submodule targets and add `configure`, `build`, `test`, and `bench` wrappers.

Core library:

- `include/redis_proxy/status.h`: small `Status` enum and helper for tests and errors.
- `include/redis_proxy/endpoint.h`, `src/endpoint.cpp`: parse `host:port`, create sockaddr, create nonblocking TCP sockets.
- `include/redis_proxy/config.h`, `src/config.cpp`: config file and CLI parsing.
- `include/redis_proxy/command_rules.h`, `src/command_rules.cpp`: startup command allow/deny table.
- `include/redis_proxy/buffer.h`, `src/buffer.cpp`: `BufferBlock`, `BufferSlice`, `BufferChain`, `IoBuffer`, worker-local block pool.
- `include/redis_proxy/resp_parser.h`, `src/resp_parser.cpp`: adapter around `redis::UnpackOne()` and RESP request/reply frame extraction.
- `include/redis_proxy/co_socket.h`, `src/co_socket.cpp`: libco-compatible socket wrapper.
- `include/redis_proxy/backend_channel.h`, `src/backend_channel.cpp`: single Redis backend connection, writer and reader coroutines, pending queue.
- `include/redis_proxy/backend_pool.h`, `src/backend_pool.cpp`: worker-local backend channel selection and session affinity.
- `include/redis_proxy/client_session.h`, `src/client_session.cpp`: client connection parser, validator, backend submitter, client response writer.
- `include/redis_proxy/worker.h`, `src/worker.cpp`: worker thread, fd dispatch queue, backend pool, session creation.
- `include/redis_proxy/proxy_server.h`, `src/proxy_server.cpp`: process lifecycle, acceptor coroutine, worker startup and shutdown.
- `src/main.cpp`: CLI entrypoint.

Tests:

- `tests/test_common.h`: assertion helpers and RESP helpers.
- `tests/config_test.cpp`
- `tests/command_rules_test.cpp`
- `tests/buffer_test.cpp`
- `tests/resp_parser_test.cpp`
- `tests/co_socket_test.cpp`
- `tests/backend_channel_test.cpp`
- `tests/backend_pool_test.cpp`
- `tests/integration_proxy_test.cpp`

Benchmarks:

- `bench/proxy_bench.cpp`: C++ benchmark client with direct Redis and proxy modes.
- `bench/run_single_worker_qps.sh`: starts Redis and proxy, runs `proxy_bench`, writes JSON/CSV output.

## Task 1: Build Scaffold and Third-Party Wiring

**Files:**

- Create: `CMakeLists.txt`
- Create: `cmake/Jemalloc.cmake`
- Create: `scripts/build_jemalloc.sh`
- Create: `src/empty.cpp`
- Modify: `Makefile`

- [ ] **Step 1: Write the failing build smoke test**

Run:

```bash
cmake -S . -B build
```

Expected: fails because `CMakeLists.txt` does not exist.

- [ ] **Step 2: Add top-level CMake**

Create `CMakeLists.txt` with this structure:

```cmake
cmake_minimum_required(VERSION 3.16)
project(redis_proxy LANGUAGES C CXX ASM)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(REDIS_PROXY_USE_JEMALLOC "Link redis_proxy with bundled jemalloc" ON)
option(REDIS_PROXY_BUILD_TESTS "Build redis_proxy tests" ON)
option(REDIS_PROXY_BUILD_BENCH "Build redis_proxy benchmarks" ON)

include(cmake/Jemalloc.cmake)

add_subdirectory(thirdparty/libco EXCLUDE_FROM_ALL)

add_library(redis_resp INTERFACE)
target_include_directories(redis_resp INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/cpp_util/redis/include)

file(GLOB REDIS_PROXY_CORE_SRCS CONFIGURE_DEPENDS src/*.cpp)
list(FILTER REDIS_PROXY_CORE_SRCS EXCLUDE REGEX ".*/main\\.cpp$")
add_library(redis_proxy_core ${REDIS_PROXY_CORE_SRCS})
target_include_directories(redis_proxy_core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/libco)
target_link_libraries(redis_proxy_core PUBLIC colib_static redis_resp pthread)
if(NOT APPLE)
  target_link_libraries(redis_proxy_core PUBLIC dl)
endif()
if(REDIS_PROXY_USE_JEMALLOC)
  target_link_libraries(redis_proxy_core PUBLIC redis_proxy_jemalloc)
endif()

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp)
  add_executable(redis_proxy src/main.cpp)
  target_link_libraries(redis_proxy PRIVATE redis_proxy_core)
endif()

enable_testing()

function(add_proxy_test name)
  add_executable(${name} tests/${name}.cpp)
  target_link_libraries(${name} PRIVATE redis_proxy_core)
  add_test(NAME ${name} COMMAND ${name})
endfunction()

if(REDIS_PROXY_BUILD_TESTS)
  file(GLOB REDIS_PROXY_TEST_SRCS CONFIGURE_DEPENDS tests/*_test.cpp)
  foreach(test_src ${REDIS_PROXY_TEST_SRCS})
    get_filename_component(test_name ${test_src} NAME_WE)
    add_executable(${test_name} ${test_src})
    target_link_libraries(${test_name} PRIVATE redis_proxy_core)
    add_test(NAME ${test_name} COMMAND ${test_name})
  endforeach()
endif()

if(REDIS_PROXY_BUILD_BENCH AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/bench/proxy_bench.cpp)
  add_executable(proxy_bench bench/proxy_bench.cpp)
  target_link_libraries(proxy_bench PRIVATE redis_proxy_core)
endif()
```

Create `src/empty.cpp`:

```cpp
namespace redis_proxy {
int empty_translation_unit = 0;
}
```

- [ ] **Step 3: Add jemalloc CMake helper**

Create `cmake/Jemalloc.cmake`:

```cmake
set(REDIS_PROXY_JEMALLOC_PREFIX ${CMAKE_BINARY_DIR}/jemalloc)
set(REDIS_PROXY_JEMALLOC_LIB ${REDIS_PROXY_JEMALLOC_PREFIX}/lib/libjemalloc.a)

add_custom_command(
  OUTPUT ${REDIS_PROXY_JEMALLOC_LIB}
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build_jemalloc.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build_jemalloc.sh
  COMMENT "Building bundled jemalloc")

add_custom_target(redis_proxy_jemalloc_build DEPENDS ${REDIS_PROXY_JEMALLOC_LIB})

add_library(redis_proxy_jemalloc STATIC IMPORTED GLOBAL)
set_target_properties(redis_proxy_jemalloc PROPERTIES
  IMPORTED_LOCATION ${REDIS_PROXY_JEMALLOC_LIB}
  INTERFACE_INCLUDE_DIRECTORIES ${REDIS_PROXY_JEMALLOC_PREFIX}/include)
add_dependencies(redis_proxy_jemalloc redis_proxy_jemalloc_build)
```

- [ ] **Step 4: Add jemalloc build script**

Create `scripts/build_jemalloc.sh` and make it executable:

```bash
#!/usr/bin/env bash
set -euo pipefail

repo_root="$1"
build_root="$2"
src_dir="${repo_root}/thirdparty/jemalloc"
prefix="${build_root}/jemalloc"
stamp="${prefix}/.built"

if [[ -f "${stamp}" && -f "${prefix}/lib/libjemalloc.a" ]]; then
  exit 0
fi

mkdir -p "${prefix}"
cd "${src_dir}"

if [[ ! -x ./configure ]]; then
  ./autogen.sh
fi

./configure --prefix="${prefix}" --disable-shared --enable-static
make -j"$(nproc)"
make install
touch "${stamp}"
```

Run:

```bash
chmod +x scripts/build_jemalloc.sh
```

- [ ] **Step 5: Extend root Makefile without removing submodule targets**

Append these targets to `Makefile`:

```make
.PHONY: configure build test bench

configure:
	cmake -S . -B build

build: configure
	cmake --build build -j

test: build
	cd build && ctest --output-on-failure

bench: build
	./bench/run_single_worker_qps.sh
```

- [ ] **Step 6: Verify build target discovery fails only on missing source files**

Run:

```bash
cmake -S . -B build -DREDIS_PROXY_USE_JEMALLOC=OFF
```

Expected: CMake configures successfully with only `redis_proxy_core` and libco-related targets. The `redis_proxy` binary and tests appear after their source files are created.

- [ ] **Step 7: Commit build scaffold**

```bash
git add CMakeLists.txt cmake/Jemalloc.cmake scripts/build_jemalloc.sh src/empty.cpp Makefile
git commit -m "build: add redis proxy cmake scaffold"
```

## Task 2: Core Utility Types, Endpoint, and Config

**Files:**

- Create: `include/redis_proxy/status.h`
- Create: `include/redis_proxy/endpoint.h`
- Create: `src/endpoint.cpp`
- Create: `include/redis_proxy/config.h`
- Create: `src/config.cpp`
- Create: `tests/test_common.h`
- Create: `tests/config_test.cpp`

- [ ] **Step 1: Write config and endpoint tests**

Create `tests/test_common.h`:

```cpp
#pragma once

#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>
#include <string_view>

#define RP_REQUIRE(expr)                                                       \
  do {                                                                         \
    if (!(expr)) {                                                             \
      std::cerr << "require failed: " #expr << " at " << __FILE__ << ":"      \
                << __LINE__ << "\n";                                           \
      std::exit(1);                                                            \
    }                                                                          \
  } while (0)

inline void RequireEqual(std::string_view actual, std::string_view expected) {
  if (actual != expected) {
    std::cerr << "expected [" << expected << "], got [" << actual << "]\n";
    std::exit(1);
  }
}
```

Create `tests/config_test.cpp`:

```cpp
#include "redis_proxy/config.h"
#include "redis_proxy/endpoint.h"
#include "test_common.h"

#include <fstream>

int main() {
  redis_proxy::Endpoint ep;
  RP_REQUIRE(redis_proxy::Endpoint::parse("127.0.0.1:6380", &ep));
  RequireEqual(ep.host(), "127.0.0.1");
  RP_REQUIRE(ep.port() == 6380);
  RP_REQUIRE(!redis_proxy::Endpoint::parse("127.0.0.1", &ep));
  RP_REQUIRE(!redis_proxy::Endpoint::parse("127.0.0.1:0", &ep));
  RP_REQUIRE(!redis_proxy::Endpoint::parse("127.0.0.1:70000", &ep));

  const char* path = "config_test.conf";
  {
    std::ofstream out(path);
    out << "listen = 0.0.0.0:6379\n";
    out << "redis = 127.0.0.1:6380\n";
    out << "workers = 2\n";
    out << "backend_conns_per_worker = 3\n";
    out << "max_request_bytes = 4096\n";
    out << "max_bulk_bytes = 2048\n";
    out << "max_array_elements = 32\n";
    out << "max_pipeline_commands_per_read = 16\n";
  }

  redis_proxy::Config cfg;
  RP_REQUIRE(redis_proxy::LoadConfigFile(path, &cfg));
  RP_REQUIRE(cfg.listen.port() == 6379);
  RP_REQUIRE(cfg.redis.port() == 6380);
  RP_REQUIRE(cfg.workers == 2);
  RP_REQUIRE(cfg.backend_conns_per_worker == 3);
  RP_REQUIRE(cfg.max_request_bytes == 4096);
  RP_REQUIRE(cfg.max_bulk_bytes == 2048);
  RP_REQUIRE(cfg.max_array_elements == 32);
  RP_REQUIRE(cfg.max_pipeline_commands_per_read == 16);

  const char* argv[] = {"redis_proxy", "--config", path, "--workers", "4",
                        "--backend-conns", "2", "--listen", "127.0.0.1:6390"};
  redis_proxy::Config cli_cfg;
  RP_REQUIRE(redis_proxy::LoadConfigFromArgs(9, const_cast<char**>(argv), &cli_cfg));
  RP_REQUIRE(cli_cfg.workers == 4);
  RP_REQUIRE(cli_cfg.backend_conns_per_worker == 2);
  RP_REQUIRE(cli_cfg.listen.port() == 6390);

  std::remove(path);
  std::cout << "config_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cmake -S . -B build -DREDIS_PROXY_USE_JEMALLOC=OFF
cmake --build build --target config_test -j
```

Expected: compile fails because `redis_proxy/config.h` and `redis_proxy/endpoint.h` do not exist.

- [ ] **Step 3: Add status and endpoint**

Create `include/redis_proxy/status.h`:

```cpp
#pragma once

#include <string>

namespace redis_proxy {

enum class StatusCode { kOk, kInvalidArgument, kIoError, kProtocolError, kClosed };

class Status {
public:
  Status() = default;
  Status(StatusCode code, std::string message) : code_(code), message_(std::move(message)) {}

  static Status Ok() { return Status(); }
  static Status InvalidArgument(std::string message) { return Status(StatusCode::kInvalidArgument, std::move(message)); }
  static Status IoError(std::string message) { return Status(StatusCode::kIoError, std::move(message)); }
  static Status ProtocolError(std::string message) { return Status(StatusCode::kProtocolError, std::move(message)); }
  static Status Closed(std::string message) { return Status(StatusCode::kClosed, std::move(message)); }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }

private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

}  // namespace redis_proxy
```

Create `include/redis_proxy/endpoint.h`:

```cpp
#pragma once

#include <cstdint>
#include <string>
#include <utility>

#include <netinet/in.h>

namespace redis_proxy {

class Endpoint {
public:
  Endpoint() = default;
  Endpoint(std::string host, uint16_t port) : host_(std::move(host)), port_(port) {}

  static bool parse(const std::string& text, Endpoint* out);
  bool toSockAddr(sockaddr_in* out) const;

  const std::string& host() const { return host_; }
  uint16_t port() const { return port_; }
  std::string toString() const;

private:
  std::string host_ = "127.0.0.1";
  uint16_t port_ = 0;
};

int SetNonBlocking(int fd);
int CreateTcpListenSocket(const Endpoint& endpoint, int backlog);
int CreateTcpClientSocket();

}  // namespace redis_proxy
```

Create `src/endpoint.cpp`:

```cpp
#include "redis_proxy/endpoint.h"

#include <cerrno>
#include <cstdlib>
#include <cstring>

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cstring>

namespace redis_proxy {

bool Endpoint::parse(const std::string& text, Endpoint* out) {
  const std::size_t colon = text.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= text.size()) {
    return false;
  }
  char* end = nullptr;
  const long port = std::strtol(text.c_str() + colon + 1, &end, 10);
  if (*end != '\0' || port <= 0 || port > 65535) {
    return false;
  }
  *out = Endpoint(text.substr(0, colon), static_cast<uint16_t>(port));
  return true;
}

bool Endpoint::toSockAddr(sockaddr_in* out) const {
  std::memset(out, 0, sizeof(*out));
  out->sin_family = AF_INET;
  out->sin_port = htons(port_);
  if (host_ == "*" || host_ == "0" || host_ == "0.0.0.0") {
    out->sin_addr.s_addr = htonl(INADDR_ANY);
    return true;
  }
  return inet_pton(AF_INET, host_.c_str(), &out->sin_addr) == 1;
}

std::string Endpoint::toString() const {
  return host_ + ":" + std::to_string(port_);
}

int SetNonBlocking(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return -1;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK | O_NDELAY);
}

int CreateTcpListenSocket(const Endpoint& endpoint, int backlog) {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd < 0) {
    return -1;
  }
  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in addr;
  if (!endpoint.toSockAddr(&addr) || bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      listen(fd, backlog) != 0 || SetNonBlocking(fd) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

int CreateTcpClientSocket() {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd >= 0 && SetNonBlocking(fd) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

}  // namespace redis_proxy
```

- [ ] **Step 4: Add config parser**

Create `include/redis_proxy/config.h`:

```cpp
#pragma once

#include <cstddef>
#include <string>

#include "redis_proxy/endpoint.h"

namespace redis_proxy {

struct Config {
  Endpoint listen = Endpoint("0.0.0.0", 6379);
  Endpoint redis = Endpoint("127.0.0.1", 6380);
  int workers = 1;
  int backend_conns_per_worker = 1;
  std::size_t max_request_bytes = 1024 * 1024;
  std::size_t max_bulk_bytes = 1024 * 1024;
  std::size_t max_array_elements = 1024;
  std::size_t max_pipeline_commands_per_read = 256;
  int connect_timeout_ms = 1000;
  int read_timeout_ms = 30000;
  int write_timeout_ms = 30000;
};

bool LoadConfigFile(const std::string& path, Config* out);
bool LoadConfigFromArgs(int argc, char** argv, Config* out);
bool ValidateConfig(const Config& config, std::string* error);

}  // namespace redis_proxy
```

Create `src/config.cpp` with a strict `key = value` parser:

```cpp
#include "redis_proxy/config.h"

#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

namespace redis_proxy {
namespace {

std::string Trim(const std::string& s) {
  const std::size_t first = s.find_first_not_of(" \t\r\n");
  if (first == std::string::npos) return "";
  const std::size_t last = s.find_last_not_of(" \t\r\n");
  return s.substr(first, last - first + 1);
}

bool ParseSize(const std::string& value, std::size_t* out) {
  char* end = nullptr;
  const unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
  if (*end != '\0') return false;
  *out = static_cast<std::size_t>(parsed);
  return true;
}

bool ParseInt(const std::string& value, int* out) {
  char* end = nullptr;
  const long parsed = std::strtol(value.c_str(), &end, 10);
  if (*end != '\0' || parsed < 0 || parsed > 1000000) return false;
  *out = static_cast<int>(parsed);
  return true;
}

bool Apply(Config* cfg, const std::string& key, const std::string& value) {
  if (key == "listen") return Endpoint::parse(value, &cfg->listen);
  if (key == "redis") return Endpoint::parse(value, &cfg->redis);
  if (key == "workers") return ParseInt(value, &cfg->workers);
  if (key == "backend_conns_per_worker") return ParseInt(value, &cfg->backend_conns_per_worker);
  if (key == "max_request_bytes") return ParseSize(value, &cfg->max_request_bytes);
  if (key == "max_bulk_bytes") return ParseSize(value, &cfg->max_bulk_bytes);
  if (key == "max_array_elements") return ParseSize(value, &cfg->max_array_elements);
  if (key == "max_pipeline_commands_per_read") return ParseSize(value, &cfg->max_pipeline_commands_per_read);
  if (key == "connect_timeout_ms") return ParseInt(value, &cfg->connect_timeout_ms);
  if (key == "read_timeout_ms") return ParseInt(value, &cfg->read_timeout_ms);
  if (key == "write_timeout_ms") return ParseInt(value, &cfg->write_timeout_ms);
  return false;
}

}  // namespace

bool LoadConfigFile(const std::string& path, Config* out) {
  std::ifstream in(path);
  if (!in) return false;
  std::string line;
  while (std::getline(in, line)) {
    std::string trimmed = Trim(line);
    if (trimmed.empty() || trimmed[0] == '#') continue;
    const std::size_t eq = trimmed.find('=');
    if (eq == std::string::npos) return false;
    if (!Apply(out, Trim(trimmed.substr(0, eq)), Trim(trimmed.substr(eq + 1)))) return false;
  }
  std::string error;
  return ValidateConfig(*out, &error);
}

bool LoadConfigFromArgs(int argc, char** argv, Config* out) {
  Config cfg;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    auto need_value = [&](std::string* v) -> bool {
      if (i + 1 >= argc) return false;
      *v = argv[++i];
      return true;
    };
    std::string value;
    if (arg == "--config") {
      if (!need_value(&value) || !LoadConfigFile(value, &cfg)) return false;
    } else if (arg == "--listen") {
      if (!need_value(&value) || !Endpoint::parse(value, &cfg.listen)) return false;
    } else if (arg == "--redis") {
      if (!need_value(&value) || !Endpoint::parse(value, &cfg.redis)) return false;
    } else if (arg == "--workers") {
      if (!need_value(&value) || !ParseInt(value, &cfg.workers)) return false;
    } else if (arg == "--backend-conns") {
      if (!need_value(&value) || !ParseInt(value, &cfg.backend_conns_per_worker)) return false;
    } else {
      return false;
    }
  }
  std::string error;
  if (!ValidateConfig(cfg, &error)) return false;
  *out = cfg;
  return true;
}

bool ValidateConfig(const Config& config, std::string* error) {
  if (config.workers <= 0) { *error = "workers must be positive"; return false; }
  if (config.backend_conns_per_worker <= 0 || config.backend_conns_per_worker > 4) {
    *error = "backend_conns_per_worker must be 1..4";
    return false;
  }
  if (config.max_request_bytes == 0 || config.max_bulk_bytes == 0 ||
      config.max_array_elements == 0 || config.max_pipeline_commands_per_read == 0) {
    *error = "limits must be positive";
    return false;
  }
  return true;
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Run config test**

Run:

```bash
cmake --build build --target config_test -j
./build/config_test
```

Expected:

```text
config_test passed
```

- [ ] **Step 6: Commit config utilities**

```bash
git add include/redis_proxy/status.h include/redis_proxy/endpoint.h src/endpoint.cpp include/redis_proxy/config.h src/config.cpp tests/test_common.h tests/config_test.cpp
git commit -m "feat: add proxy config parsing"
```

## Task 3: Command Rules

**Files:**

- Create: `include/redis_proxy/command_rules.h`
- Create: `src/command_rules.cpp`
- Create: `tests/command_rules_test.cpp`

- [ ] **Step 1: Write command rule tests**

Create `tests/command_rules_test.cpp`:

```cpp
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
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cmake --build build --target command_rules_test -j
```

Expected: compile fails because `command_rules.h` does not exist.

- [ ] **Step 3: Add command rules API**

Create `include/redis_proxy/command_rules.h`:

```cpp
#pragma once

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
  void allow(std::string_view command, uint16_t min_argc, uint16_t max_argc, bool read, bool write);
  void deny(std::string_view command);
};

}  // namespace redis_proxy
```

- [ ] **Step 4: Add command rules implementation**

Create `src/command_rules.cpp`:

```cpp
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

void CommandRules::allow(std::string_view command, uint16_t min_argc, uint16_t max_argc, bool read, bool write) {
  rules_[normalize(command)] = CommandRule{true, min_argc, max_argc, read, write, false};
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
  const char* denied[] = {"AUTH", "SELECT", "CLIENT", "HELLO", "MULTI", "EXEC", "DISCARD", "WATCH",
                          "UNWATCH", "SUBSCRIBE", "PSUBSCRIBE", "SSUBSCRIBE", "UNSUBSCRIBE",
                          "PUNSUBSCRIBE", "SUNSUBSCRIBE", "BLPOP", "BRPOP", "BRPOPLPUSH",
                          "BZPOPMIN", "BZPOPMAX", "BLMOVE", "XREAD", "XREADGROUP", "DEBUG",
                          "MODULE", "CONFIG", "SHUTDOWN", "SAVE", "BGSAVE", "SCRIPT", "EVAL",
                          "EVALSHA", "FUNCTION", "ACL", "MONITOR", "SYNC", "PSYNC"};
  for (const char* command : denied) {
    rules.deny(command);
  }
  return rules;
}

Status CommandRules::validate(std::string_view command, std::size_t argc) const {
  const auto it = rules_.find(normalize(command));
  if (it == rules_.end()) return Status::ProtocolError("ERR proxy rejected unknown command");
  const CommandRule& rule = it->second;
  if (!rule.allowed || rule.dangerous) return Status::ProtocolError("ERR proxy rejected command");
  if (argc < rule.min_argc || argc > rule.max_argc) return Status::ProtocolError("ERR wrong number of arguments");
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
  if (!(in >> command >> allowed >> min_argc >> max_argc >> read >> write >> dangerous)) return false;
  rules_[normalize(command)] =
      CommandRule{allowed != 0, static_cast<uint16_t>(min_argc), static_cast<uint16_t>(max_argc),
                  read != 0, write != 0, dangerous != 0};
  return true;
}

void CommandRules::setRuleForTest(std::string_view command, const CommandRule& rule) {
  rules_[normalize(command)] = rule;
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Run command rule test**

Run:

```bash
cmake --build build --target command_rules_test -j
./build/command_rules_test
```

Expected:

```text
command_rules_test passed
```

- [ ] **Step 6: Commit command rules**

```bash
git add include/redis_proxy/command_rules.h src/command_rules.cpp tests/command_rules_test.cpp
git commit -m "feat: add startup command rules"
```

## Task 4: Buffer Blocks, Slices, Chains, and IoBuffer

**Files:**

- Create: `include/redis_proxy/buffer.h`
- Create: `src/buffer.cpp`
- Create: `tests/buffer_test.cpp`

- [ ] **Step 1: Write buffer tests**

Create `tests/buffer_test.cpp`:

```cpp
#include "redis_proxy/buffer.h"
#include "test_common.h"

#include <cstring>

int main() {
  redis_proxy::BlockPool pool(16);
  redis_proxy::BufferBlock* block = pool.acquire();
  RP_REQUIRE(block->refcount() == 1);
  std::memcpy(block->writePtr(), "abcdef", 6);
  block->advanceEnd(6);

  {
    redis_proxy::BufferChain chain;
    chain.append(redis_proxy::BufferSlice::retain(block, 1, 3));
    RP_REQUIRE(block->refcount() == 2);
    RP_REQUIRE(chain.size() == 3);
    std::string copied = chain.toStringForTest();
    RequireEqual(copied, "bcd");

    redis_proxy::BufferChain moved = std::move(chain);
    RP_REQUIRE(moved.size() == 3);
    RP_REQUIRE(block->refcount() == 2);
  }

  RP_REQUIRE(block->refcount() == 1);
  block->release();
  RP_REQUIRE(pool.freeCountForTest() == 1);

  redis_proxy::IoBuffer input(&pool);
  input.appendForTest("abc");
  input.appendForTest("def");
  RP_REQUIRE(input.readableBytes() == 6);
  RequireEqual(input.contiguousPrefixForTest(6), "abcdef");
  input.consume(4);
  RP_REQUIRE(input.readableBytes() == 2);
  RequireEqual(input.contiguousPrefixForTest(2), "ef");

  std::cout << "buffer_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cmake --build build --target buffer_test -j
```

Expected: compile fails because `buffer.h` does not exist.

- [ ] **Step 3: Add buffer API**

Create `include/redis_proxy/buffer.h` with these public methods:

```cpp
#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace redis_proxy {

class BlockPool;

class BufferBlock {
public:
  BufferBlock(BlockPool* pool, std::size_t capacity);
  ~BufferBlock();

  char* writePtr();
  std::size_t writableBytes() const;
  const char* data() const;
  std::size_t begin() const;
  std::size_t end() const;
  std::size_t size() const;
  std::size_t capacity() const;
  uint32_t refcount() const;

  void advanceEnd(std::size_t n);
  void consume(std::size_t n);
  void retain();
  void release();
  void reset();

private:
  BlockPool* pool_;
  std::unique_ptr<char[]> data_;
  std::size_t capacity_;
  std::size_t begin_ = 0;
  std::size_t end_ = 0;
  uint32_t refcount_ = 1;
};

class BufferSlice {
public:
  BufferSlice() = default;
  static BufferSlice retain(BufferBlock* block, std::size_t offset, std::size_t length);
  BufferSlice(const BufferSlice& other);
  BufferSlice& operator=(const BufferSlice& other);
  BufferSlice(BufferSlice&& other) noexcept;
  BufferSlice& operator=(BufferSlice&& other) noexcept;
  ~BufferSlice();

  const char* data() const;
  std::size_t size() const;
  bool empty() const;

private:
  BufferSlice(BufferBlock* block, std::size_t offset, std::size_t length, bool add_ref);
  void reset();

  BufferBlock* block_ = nullptr;
  std::size_t offset_ = 0;
  std::size_t length_ = 0;
};

class BufferChain {
public:
  void append(BufferSlice slice);
  std::size_t size() const;
  bool empty() const;
  const std::vector<BufferSlice>& slices() const;
  std::string toStringForTest() const;

private:
  std::vector<BufferSlice> slices_;
  std::size_t total_ = 0;
};

class BlockPool {
public:
  explicit BlockPool(std::size_t block_size);
  BufferBlock* acquire();
  void recycle(BufferBlock* block);
  std::size_t blockSize() const;
  std::size_t freeCountForTest() const;

private:
  std::size_t block_size_;
  std::vector<std::unique_ptr<BufferBlock>> free_;
};

class IoBuffer {
public:
  explicit IoBuffer(BlockPool* pool);
  char* reserveWritable(std::size_t* writable);
  void commitWrite(std::size_t n);
  void appendForTest(std::string_view data);
  std::size_t readableBytes() const;
  void consume(std::size_t n);
  std::string_view contiguousPrefixForTest(std::size_t n);
  bool ensureContiguousPrefix(std::size_t n);
  BufferChain slicePrefix(std::size_t n);

private:
  BlockPool* pool_;
  std::deque<BufferBlock*> blocks_;
  std::string linearized_;
};

BufferChain MakeBufferChain(BlockPool* pool, std::string_view bytes);

}  // namespace redis_proxy
```

- [ ] **Step 4: Implement buffer behavior**

Create `src/buffer.cpp` and implement these exact rules:

```cpp
#include "redis_proxy/buffer.h"

#include <algorithm>
#include <cstring>

namespace redis_proxy {

BufferBlock::BufferBlock(BlockPool* pool, std::size_t capacity)
    : pool_(pool), data_(new char[capacity]), capacity_(capacity) {}
BufferBlock::~BufferBlock() = default;
char* BufferBlock::writePtr() { return data_.get() + end_; }
std::size_t BufferBlock::writableBytes() const { return capacity_ - end_; }
const char* BufferBlock::data() const { return data_.get(); }
std::size_t BufferBlock::begin() const { return begin_; }
std::size_t BufferBlock::end() const { return end_; }
std::size_t BufferBlock::size() const { return end_ - begin_; }
std::size_t BufferBlock::capacity() const { return capacity_; }
uint32_t BufferBlock::refcount() const { return refcount_; }
void BufferBlock::advanceEnd(std::size_t n) { end_ += n; }
void BufferBlock::consume(std::size_t n) { begin_ = std::min(end_, begin_ + n); }
void BufferBlock::retain() { ++refcount_; }
void BufferBlock::release() { if (--refcount_ == 0) pool_->recycle(this); }
void BufferBlock::reset() { begin_ = 0; end_ = 0; refcount_ = 1; }

BufferSlice::BufferSlice(BufferBlock* block, std::size_t offset, std::size_t length, bool add_ref)
    : block_(block), offset_(offset), length_(length) { if (block_ && add_ref) block_->retain(); }
BufferSlice BufferSlice::retain(BufferBlock* block, std::size_t offset, std::size_t length) {
  return BufferSlice(block, offset, length, true);
}
BufferSlice::BufferSlice(const BufferSlice& other)
    : BufferSlice(other.block_, other.offset_, other.length_, true) {}
BufferSlice& BufferSlice::operator=(const BufferSlice& other) {
  if (this != &other) { reset(); block_ = other.block_; offset_ = other.offset_; length_ = other.length_; if (block_) block_->retain(); }
  return *this;
}
BufferSlice::BufferSlice(BufferSlice&& other) noexcept
    : block_(other.block_), offset_(other.offset_), length_(other.length_) { other.block_ = nullptr; other.length_ = 0; }
BufferSlice& BufferSlice::operator=(BufferSlice&& other) noexcept {
  if (this != &other) { reset(); block_ = other.block_; offset_ = other.offset_; length_ = other.length_; other.block_ = nullptr; other.length_ = 0; }
  return *this;
}
BufferSlice::~BufferSlice() { reset(); }
void BufferSlice::reset() { if (block_) block_->release(); block_ = nullptr; length_ = 0; }
const char* BufferSlice::data() const { return block_->data() + offset_; }
std::size_t BufferSlice::size() const { return length_; }
bool BufferSlice::empty() const { return length_ == 0; }

void BufferChain::append(BufferSlice slice) { total_ += slice.size(); slices_.push_back(std::move(slice)); }
std::size_t BufferChain::size() const { return total_; }
bool BufferChain::empty() const { return total_ == 0; }
const std::vector<BufferSlice>& BufferChain::slices() const { return slices_; }
std::string BufferChain::toStringForTest() const {
  std::string out;
  out.reserve(total_);
  for (const auto& slice : slices_) out.append(slice.data(), slice.size());
  return out;
}

BlockPool::BlockPool(std::size_t block_size) : block_size_(block_size) {}
BufferBlock* BlockPool::acquire() {
  if (free_.empty()) return new BufferBlock(this, block_size_);
  std::unique_ptr<BufferBlock> block = std::move(free_.back());
  free_.pop_back();
  block->reset();
  return block.release();
}
void BlockPool::recycle(BufferBlock* block) { block->reset(); free_.emplace_back(block); }
std::size_t BlockPool::blockSize() const { return block_size_; }
std::size_t BlockPool::freeCountForTest() const { return free_.size(); }

IoBuffer::IoBuffer(BlockPool* pool) : pool_(pool) {}
char* IoBuffer::reserveWritable(std::size_t* writable) {
  if (blocks_.empty() || blocks_.back()->writableBytes() == 0) blocks_.push_back(pool_->acquire());
  *writable = blocks_.back()->writableBytes();
  return blocks_.back()->writePtr();
}
void IoBuffer::commitWrite(std::size_t n) { blocks_.back()->advanceEnd(n); }
void IoBuffer::appendForTest(std::string_view data) {
  while (!data.empty()) {
    std::size_t writable = 0;
    char* dst = reserveWritable(&writable);
    const std::size_t n = std::min(writable, data.size());
    std::memcpy(dst, data.data(), n);
    commitWrite(n);
    data.remove_prefix(n);
  }
}
std::size_t IoBuffer::readableBytes() const {
  std::size_t total = 0;
  for (auto* block : blocks_) total += block->size();
  return total;
}
void IoBuffer::consume(std::size_t n) {
  while (n > 0 && !blocks_.empty()) {
    BufferBlock* block = blocks_.front();
    const std::size_t take = std::min(n, block->size());
    block->consume(take);
    n -= take;
    if (block->size() == 0) { blocks_.pop_front(); block->release(); }
  }
}
bool IoBuffer::ensureContiguousPrefix(std::size_t n) {
  if (n == 0) return true;
  if (blocks_.empty() || readableBytes() < n) return false;
  if (blocks_.front()->size() >= n) return true;
  linearized_.clear();
  linearized_.reserve(n);
  std::size_t remaining = n;
  for (auto* block : blocks_) {
    const std::size_t take = std::min(remaining, block->size());
    linearized_.append(block->data() + block->begin(), take);
    remaining -= take;
    if (remaining == 0) return true;
  }
  return false;
}
std::string_view IoBuffer::contiguousPrefixForTest(std::size_t n) {
  if (!ensureContiguousPrefix(n)) return {};
  if (blocks_.front()->size() >= n) return std::string_view(blocks_.front()->data() + blocks_.front()->begin(), n);
  return std::string_view(linearized_.data(), linearized_.size());
}
BufferChain IoBuffer::slicePrefix(std::size_t n) {
  BufferChain chain;
  std::size_t remaining = n;
  for (auto* block : blocks_) {
    const std::size_t take = std::min(remaining, block->size());
    chain.append(BufferSlice::retain(block, block->begin(), take));
    remaining -= take;
    if (remaining == 0) break;
  }
  consume(n);
  return chain;
}

BufferChain MakeBufferChain(BlockPool* pool, std::string_view bytes) {
  BufferBlock* block = pool->acquire();
  std::memcpy(block->writePtr(), bytes.data(), bytes.size());
  block->advanceEnd(bytes.size());
  BufferChain chain;
  chain.append(BufferSlice::retain(block, block->begin(), block->size()));
  block->release();
  return chain;
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Run buffer test**

Run:

```bash
cmake --build build --target buffer_test -j
./build/buffer_test
```

Expected:

```text
buffer_test passed
```

- [ ] **Step 6: Commit buffer layer**

```bash
git add include/redis_proxy/buffer.h src/buffer.cpp tests/buffer_test.cpp
git commit -m "feat: add worker local io buffers"
```

## Task 5: RESP Parser Adapter Over cpp_util/redis

**Files:**

- Create: `include/redis_proxy/resp_parser.h`
- Create: `src/resp_parser.cpp`
- Create: `tests/resp_parser_test.cpp`

- [ ] **Step 1: Write parser tests**

Create `tests/resp_parser_test.cpp`:

```cpp
#include "redis_proxy/buffer.h"
#include "redis_proxy/resp_parser.h"
#include "test_common.h"

int main() {
  redis_proxy::BlockPool pool(64);
  redis_proxy::IoBuffer input(&pool);
  input.appendForTest("*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");

  redis_proxy::RespParser parser;
  redis_proxy::RespFrame frame;
  redis_proxy::ParseStatus status = parser.nextFrame(input, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RP_REQUIRE(frame.consumed == 22);
  RP_REQUIRE(frame.command.argc == 2);
  RequireEqual(frame.command.name, "GET");
  RP_REQUIRE(frame.command.args[1] == "key");
  RP_REQUIRE(frame.bytes.toStringForTest() == "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n");

  input.appendForTest("*1\r\n$4\r\nPING");
  status = parser.nextFrame(input, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kNeedMore);

  input.appendForTest("\r\n");
  status = parser.nextFrame(input, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kOk);
  RequireEqual(frame.command.name, "PING");

  input.appendForTest("PING\r\n");
  status = parser.nextFrame(input, &frame);
  RP_REQUIRE(status == redis_proxy::ParseStatus::kError);

  std::cout << "resp_parser_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cmake --build build --target resp_parser_test -j
```

Expected: compile fails because `resp_parser.h` does not exist.

- [ ] **Step 3: Add parser API**

Create `include/redis_proxy/resp_parser.h`:

```cpp
#pragma once

#include <array>
#include <string>
#include <string_view>
#include <vector>

#include "redis/resp.h"
#include "redis_proxy/buffer.h"

namespace redis_proxy {

enum class ParseStatus { kOk, kNeedMore, kError, kNoMemory };

struct CommandView {
  std::string name;
  std::vector<std::string_view> args;
  std::size_t argc = 0;
};

struct RespFrame {
  BufferChain bytes;
  CommandView command;
  std::size_t consumed = 0;
};

class RespParser {
public:
  RespParser();
  void setLimits(std::size_t max_bulk_bytes, std::size_t max_array_elements, std::size_t max_depth);
  ParseStatus nextFrame(IoBuffer& input, RespFrame* out);
  ParseStatus nextReplyFrame(IoBuffer& input, BufferChain* out, std::size_t* consumed);

private:
  redis::RespLimits limits_;
  std::array<redis::RespValue, 2048> scratch_;

  ParseStatus convert(redis::RespStatus status) const;
  bool extractCommand(const redis::RespValue& value, CommandView* out) const;
};

}  // namespace redis_proxy
```

- [ ] **Step 4: Implement parser adapter**

Create `src/resp_parser.cpp`:

```cpp
#include "redis_proxy/resp_parser.h"

namespace redis_proxy {

RespParser::RespParser() {
  limits_.max_bulk_bytes = 1024 * 1024;
  limits_.max_array_elements = 1024;
  limits_.max_depth = 8;
}

void RespParser::setLimits(std::size_t max_bulk_bytes, std::size_t max_array_elements, std::size_t max_depth) {
  limits_.max_bulk_bytes = max_bulk_bytes;
  limits_.max_array_elements = max_array_elements;
  limits_.max_depth = max_depth;
}

ParseStatus RespParser::convert(redis::RespStatus status) const {
  switch (status) {
    case redis::RespStatus::kOk: return ParseStatus::kOk;
    case redis::RespStatus::kNeedMore: return ParseStatus::kNeedMore;
    case redis::RespStatus::kNoMemory: return ParseStatus::kNoMemory;
    case redis::RespStatus::kError: return ParseStatus::kError;
  }
  return ParseStatus::kError;
}

bool RespParser::extractCommand(const redis::RespValue& value, CommandView* out) const {
  if (value.type != redis::RespType::kArray || value.element_count == 0 || value.elements == nullptr) return false;
  out->argc = value.element_count;
  out->args.clear();
  out->args.reserve(value.element_count);
  for (std::size_t i = 0; i < value.element_count; ++i) {
    const redis::RespValue& element = value.elements[i];
    if (element.type != redis::RespType::kBulkString) return false;
    out->args.push_back(element.text);
  }
  out->name.assign(out->args[0].data(), out->args[0].size());
  return true;
}

ParseStatus RespParser::nextFrame(IoBuffer& input, RespFrame* out) {
  const std::size_t available = input.readableBytes();
  if (available == 0) return ParseStatus::kNeedMore;
  if (!input.ensureContiguousPrefix(available)) return ParseStatus::kNeedMore;
  const std::string_view view = input.contiguousPrefixForTest(available);
  redis::RespResult result = redis::UnpackOne(view, scratch_.data(), scratch_.size(), limits_);
  if (result.status != redis::RespStatus::kOk) return convert(result.status);
  CommandView command;
  if (!extractCommand(*result.value, &command)) return ParseStatus::kError;
  out->consumed = result.consumed;
  out->command = std::move(command);
  out->bytes = input.slicePrefix(result.consumed);
  return ParseStatus::kOk;
}

ParseStatus RespParser::nextReplyFrame(IoBuffer& input, BufferChain* out, std::size_t* consumed) {
  const std::size_t available = input.readableBytes();
  if (available == 0) return ParseStatus::kNeedMore;
  if (!input.ensureContiguousPrefix(available)) return ParseStatus::kNeedMore;
  const std::string_view view = input.contiguousPrefixForTest(available);
  redis::RespResult result = redis::UnpackOne(view, scratch_.data(), scratch_.size(), limits_);
  if (result.status != redis::RespStatus::kOk) return convert(result.status);
  *consumed = result.consumed;
  *out = input.slicePrefix(result.consumed);
  return ParseStatus::kOk;
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Run parser test**

Run:

```bash
cmake --build build --target resp_parser_test -j
./build/resp_parser_test
```

Expected:

```text
resp_parser_test passed
```

- [ ] **Step 6: Commit RESP parser**

```bash
git add include/redis_proxy/resp_parser.h src/resp_parser.cpp tests/resp_parser_test.cpp
git commit -m "feat: adapt cpp_util resp parser"
```

## Task 6: Coroutine Socket Wrapper

**Files:**

- Create: `include/redis_proxy/co_socket.h`
- Create: `src/co_socket.cpp`
- Create: `tests/co_socket_test.cpp`

- [ ] **Step 1: Write socket wrapper test**

Create `tests/co_socket_test.cpp`:

```cpp
#include "redis_proxy/buffer.h"
#include "redis_proxy/co_socket.h"
#include "test_common.h"

#include "co_routine.h"

#include <sys/socket.h>
#include <unistd.h>

int main() {
  int fds[2];
  RP_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

  redis_proxy::BlockPool pool(64);
  redis_proxy::IoBuffer input(&pool);
  redis_proxy::BufferChain output;

  {
    redis_proxy::BufferBlock* block = pool.acquire();
    std::memcpy(block->writePtr(), "hello", 5);
    block->advanceEnd(5);
    output.append(redis_proxy::BufferSlice::retain(block, block->begin(), block->size()));
    block->release();
  }

  bool done = false;
  co::Coroutine* writer = co::co_create([&]() {
    co::co_enable_hook_sys();
    redis_proxy::CoSocket sock(fds[0]);
    RP_REQUIRE(sock.writeAll(output, 1000).ok());
    sock.close();
  });
  co::Coroutine* reader = co::co_create([&]() {
    co::co_enable_hook_sys();
    redis_proxy::CoSocket sock(fds[1]);
    RP_REQUIRE(sock.readSome(&input, 1000).ok());
    RequireEqual(input.contiguousPrefixForTest(5), "hello");
    sock.close();
    done = true;
  });
  co::co_resume(writer);
  co::co_resume(reader);
  co::co_eventloop([](void* arg) -> int { return *static_cast<bool*>(arg) ? -1 : 0; }, &done);

  std::cout << "co_socket_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cmake --build build --target co_socket_test -j
```

Expected: compile fails because `co_socket.h` does not exist.

- [ ] **Step 3: Add CoSocket API**

Create `include/redis_proxy/co_socket.h`:

```cpp
#pragma once

#include "redis_proxy/buffer.h"
#include "redis_proxy/endpoint.h"
#include "redis_proxy/status.h"

namespace redis_proxy {

class CoSocket {
public:
  explicit CoSocket(int fd = -1);
  ~CoSocket();

  CoSocket(const CoSocket&) = delete;
  CoSocket& operator=(const CoSocket&) = delete;
  CoSocket(CoSocket&& other) noexcept;
  CoSocket& operator=(CoSocket&& other) noexcept;

  int fd() const;
  int release();
  void reset(int fd);
  void close();

  Status connectTo(const Endpoint& endpoint, int timeout_ms);
  Status readSome(IoBuffer* out, int timeout_ms);
  Status writeAll(const BufferChain& chain, int timeout_ms);

private:
  int fd_ = -1;
};

}  // namespace redis_proxy
```

- [ ] **Step 4: Implement CoSocket with libco-compatible reads and writes**

Create `src/co_socket.cpp`:

```cpp
#include "redis_proxy/co_socket.h"

#include "co_routine.h"

#include <cerrno>
#include <cstring>

#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

namespace redis_proxy {

CoSocket::CoSocket(int fd) : fd_(fd) {}
CoSocket::~CoSocket() { close(); }
CoSocket::CoSocket(CoSocket&& other) noexcept : fd_(other.release()) {}
CoSocket& CoSocket::operator=(CoSocket&& other) noexcept {
  if (this != &other) { close(); fd_ = other.release(); }
  return *this;
}
int CoSocket::fd() const { return fd_; }
int CoSocket::release() { int fd = fd_; fd_ = -1; return fd; }
void CoSocket::reset(int fd) { close(); fd_ = fd; }
void CoSocket::close() { if (fd_ >= 0) { ::close(fd_); fd_ = -1; } }

Status CoSocket::connectTo(const Endpoint& endpoint, int timeout_ms) {
  sockaddr_in addr;
  if (!endpoint.toSockAddr(&addr)) return Status::InvalidArgument("invalid endpoint");
  int ret = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (ret == 0) return Status::Ok();
  if (errno != EINPROGRESS && errno != EAGAIN) return Status::IoError(std::strerror(errno));
  pollfd pfd{fd_, POLLOUT | POLLERR | POLLHUP, 0};
  ret = co::co_poll(&pfd, 1, timeout_ms);
  if (ret <= 0) return Status::IoError("connect timeout");
  int err = 0;
  socklen_t len = sizeof(err);
  if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0) return Status::IoError("connect failed");
  return Status::Ok();
}

Status CoSocket::readSome(IoBuffer* out, int timeout_ms) {
  std::size_t writable = 0;
  char* dst = out->reserveWritable(&writable);
  const ssize_t n = ::read(fd_, dst, writable);
  if (n > 0) {
    out->commitWrite(static_cast<std::size_t>(n));
    return Status::Ok();
  }
  if (n == 0) return Status::Closed("peer closed");
  if (errno == EAGAIN || errno == EWOULDBLOCK) {
    pollfd pfd{fd_, POLLIN | POLLERR | POLLHUP, 0};
    const int ret = co::co_poll(&pfd, 1, timeout_ms);
    if (ret <= 0) return Status::IoError("read timeout");
    return readSome(out, timeout_ms);
  }
  return Status::IoError(std::strerror(errno));
}

Status CoSocket::writeAll(const BufferChain& chain, int timeout_ms) {
  for (const BufferSlice& slice : chain.slices()) {
    const char* data = slice.data();
    std::size_t left = slice.size();
    while (left > 0) {
      const ssize_t n = ::write(fd_, data, left);
      if (n > 0) {
        data += n;
        left -= static_cast<std::size_t>(n);
        continue;
      }
      if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        pollfd pfd{fd_, POLLOUT | POLLERR | POLLHUP, 0};
        const int ret = co::co_poll(&pfd, 1, timeout_ms);
        if (ret <= 0) return Status::IoError("write timeout");
        continue;
      }
      return Status::IoError(n == 0 ? "zero write" : std::strerror(errno));
    }
  }
  return Status::Ok();
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Run socket test**

Run:

```bash
cmake --build build --target co_socket_test -j
./build/co_socket_test
```

Expected:

```text
co_socket_test passed
```

- [ ] **Step 6: Commit CoSocket**

```bash
git add include/redis_proxy/co_socket.h src/co_socket.cpp tests/co_socket_test.cpp
git commit -m "feat: add libco socket wrapper"
```

## Task 7: BackendChannel Queue Semantics

**Files:**

- Create: `include/redis_proxy/backend_channel.h`
- Create: `src/backend_channel.cpp`
- Create: `tests/backend_channel_test.cpp`

- [ ] **Step 1: Write backend ordering test without network**

Create `tests/backend_channel_test.cpp`:

```cpp
#include "redis_proxy/backend_channel.h"
#include "test_common.h"

class FakeSink : public redis_proxy::ReplySink {
public:
  void onBackendReply(redis_proxy::BufferChain reply) override {
    replies.push_back(reply.toStringForTest());
  }
  void onBackendFailure(const redis_proxy::Status& status) override {
    failures.push_back(status.message());
  }
  std::vector<std::string> replies;
  std::vector<std::string> failures;
};

int main() {
  redis_proxy::BlockPool pool(64);
  redis_proxy::BackendChannel channel(0, redis_proxy::Endpoint("127.0.0.1", 6380), &pool);
  FakeSink a;
  FakeSink b;

  channel.submitForTest(&a, redis_proxy::MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"), 1, 1);
  channel.submitForTest(&b, redis_proxy::MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"), 1, 2);
  RP_REQUIRE(channel.pendingBatchCountForTest() == 2);

  channel.dispatchReplyForTest(redis_proxy::MakeBufferChain(&pool, "+PONG\r\n"));
  channel.dispatchReplyForTest(redis_proxy::MakeBufferChain(&pool, "+PONG\r\n"));

  RP_REQUIRE(a.replies.size() == 1);
  RP_REQUIRE(b.replies.size() == 1);
  RP_REQUIRE(channel.pendingBatchCountForTest() == 0);

  channel.submitForTest(&a, redis_proxy::MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"), 1, 10);
  channel.submitForTest(&b, redis_proxy::MakeBufferChain(&pool, "*1\r\n$4\r\nPING\r\n"), 1, 11);
  channel.detachOwner(&a);
  RP_REQUIRE(channel.pendingBatchCountForTest() == 1);
  channel.dispatchReplyForTest(redis_proxy::MakeBufferChain(&pool, "+PONG\r\n"));
  RP_REQUIRE(b.replies.size() == 2);

  std::cout << "backend_channel_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Run failing test**

Run:

```bash
cmake --build build --target backend_channel_test -j
```

Expected: compile fails because `backend_channel.h` does not exist.

- [ ] **Step 3: Add BackendChannel API**

Create `include/redis_proxy/backend_channel.h`:

```cpp
#pragma once

#include <deque>
#include <unordered_set>
#include <vector>

#include "redis_proxy/buffer.h"
#include "redis_proxy/co_socket.h"
#include "redis_proxy/config.h"
#include "redis_proxy/resp_parser.h"
#include "redis_proxy/status.h"

namespace redis_proxy {

class ReplySink {
public:
  virtual ~ReplySink() = default;
  virtual void onBackendReply(BufferChain reply) = 0;
  virtual void onBackendFailure(const Status& status) = 0;
};

class RequestBatch {
public:
  RequestBatch(ReplySink* owner, BufferChain bytes, uint32_t command_count, uint64_t sequence_base);
  ReplySink* owner() const;
  const BufferChain& bytes() const;
  uint32_t commandCount() const;
  uint64_t sequenceBase() const;

private:
  ReplySink* owner_;
  BufferChain bytes_;
  uint32_t command_count_;
  uint64_t sequence_base_;
};

class PendingBatch {
public:
  PendingBatch(ReplySink* owner, uint32_t remaining_replies, uint64_t sequence_base);
  ReplySink* owner() const;
  uint32_t remainingReplies() const;
  uint64_t sequenceBase() const;
  void consumeOne();
  void detachOwner();

private:
  ReplySink* owner_;
  uint32_t remaining_replies_;
  uint64_t sequence_base_;
};

class BackendChannel {
public:
  BackendChannel(int id, Endpoint endpoint, BlockPool* pool);

  bool submit(ReplySink* owner, BufferChain bytes, uint32_t command_count, uint64_t sequence_base);
  void detachOwner(ReplySink* owner);
  void start(const Config& config);
  std::size_t queuedCommandCount() const;
  bool isHealthy() const;

  void submitForTest(ReplySink* owner, BufferChain bytes, uint32_t command_count, uint64_t sequence_base);
  void dispatchReplyForTest(BufferChain reply);
  std::size_t pendingBatchCountForTest() const;

private:
  int id_;
  Endpoint endpoint_;
  BlockPool* pool_;
  CoSocket socket_;
  RespParser reply_parser_;
  IoBuffer redis_in_;
  std::deque<RequestBatch> write_queue_;
  std::deque<PendingBatch> pending_queue_;
  bool healthy_ = false;
  std::size_t queued_commands_ = 0;
  Config config_;

  void writerLoop();
  void readerLoop();
  void dispatchReply(BufferChain reply);
  void detachOwnerInternal(ReplySink* owner);
  void failAll(const Status& status);
};

}  // namespace redis_proxy
```

- [ ] **Step 4: Implement queue-only backend methods first**

Create `src/backend_channel.cpp` with queue behavior:

```cpp
#include "redis_proxy/backend_channel.h"

#include "co_routine.h"

namespace redis_proxy {

RequestBatch::RequestBatch(ReplySink* owner, BufferChain bytes, uint32_t command_count, uint64_t sequence_base)
    : owner_(owner), bytes_(std::move(bytes)), command_count_(command_count), sequence_base_(sequence_base) {}
ReplySink* RequestBatch::owner() const { return owner_; }
const BufferChain& RequestBatch::bytes() const { return bytes_; }
uint32_t RequestBatch::commandCount() const { return command_count_; }
uint64_t RequestBatch::sequenceBase() const { return sequence_base_; }

PendingBatch::PendingBatch(ReplySink* owner, uint32_t remaining_replies, uint64_t sequence_base)
    : owner_(owner), remaining_replies_(remaining_replies), sequence_base_(sequence_base) {}
ReplySink* PendingBatch::owner() const { return owner_; }
uint32_t PendingBatch::remainingReplies() const { return remaining_replies_; }
uint64_t PendingBatch::sequenceBase() const { return sequence_base_; }
void PendingBatch::consumeOne() { if (remaining_replies_ > 0) --remaining_replies_; }
void PendingBatch::detachOwner() { owner_ = nullptr; }

BackendChannel::BackendChannel(int id, Endpoint endpoint, BlockPool* pool)
    : id_(id), endpoint_(std::move(endpoint)), pool_(pool), redis_in_(pool) {}

bool BackendChannel::submit(ReplySink* owner, BufferChain bytes, uint32_t command_count, uint64_t sequence_base) {
  write_queue_.emplace_back(owner, std::move(bytes), command_count, sequence_base);
  pending_queue_.emplace_back(owner, command_count, sequence_base);
  queued_commands_ += command_count;
  return true;
}

void BackendChannel::detachOwner(ReplySink* owner) { detachOwnerInternal(owner); }

void BackendChannel::submitForTest(ReplySink* owner, BufferChain bytes, uint32_t command_count, uint64_t sequence_base) {
  submit(owner, std::move(bytes), command_count, sequence_base);
}

void BackendChannel::dispatchReplyForTest(BufferChain reply) { dispatchReply(std::move(reply)); }
std::size_t BackendChannel::pendingBatchCountForTest() const { return pending_queue_.size(); }
std::size_t BackendChannel::queuedCommandCount() const { return queued_commands_; }
bool BackendChannel::isHealthy() const { return healthy_; }

void BackendChannel::dispatchReply(BufferChain reply) {
  if (pending_queue_.empty()) return;
  PendingBatch& pending = pending_queue_.front();
  if (pending.owner()) pending.owner()->onBackendReply(std::move(reply));
  pending.consumeOne();
  if (pending.remainingReplies() == 0) pending_queue_.pop_front();
  if (queued_commands_ > 0) --queued_commands_;
}

void BackendChannel::detachOwnerInternal(ReplySink* owner) {
  std::unordered_set<uint64_t> unwritten_sequences;
  for (const RequestBatch& batch : write_queue_) {
    if (batch.owner() == owner) {
      unwritten_sequences.insert(batch.sequenceBase());
      if (queued_commands_ >= batch.commandCount()) queued_commands_ -= batch.commandCount();
    }
  }
  for (auto it = write_queue_.begin(); it != write_queue_.end();) {
    if (it->owner() == owner) it = write_queue_.erase(it);
    else ++it;
  }
  for (auto it = pending_queue_.begin(); it != pending_queue_.end();) {
    if (it->owner() == owner && unwritten_sequences.count(it->sequenceBase()) != 0) {
      it = pending_queue_.erase(it);
    } else {
      if (it->owner() == owner) it->detachOwner();
      ++it;
    }
  }
}

void BackendChannel::failAll(const Status& status) {
  for (auto& pending : pending_queue_) if (pending.owner()) pending.owner()->onBackendFailure(status);
  pending_queue_.clear();
  write_queue_.clear();
  queued_commands_ = 0;
  healthy_ = false;
}

void BackendChannel::start(const Config& config) {
  config_ = config;
  co::Coroutine* writer = co::co_create([this]() { writerLoop(); });
  co::Coroutine* reader = co::co_create([this]() { readerLoop(); });
  co::co_resume(writer);
  co::co_resume(reader);
}

void BackendChannel::writerLoop() {
  co::co_enable_hook_sys();
  while (true) {
    if (write_queue_.empty()) {
      co::co_poll(nullptr, 0, 1);
      continue;
    }
    RequestBatch batch = std::move(write_queue_.front());
    write_queue_.pop_front();
    Status st = socket_.writeAll(batch.bytes(), config_.write_timeout_ms);
    if (!st.ok()) { failAll(st); return; }
  }
}

void BackendChannel::readerLoop() {
  co::co_enable_hook_sys();
  while (true) {
    Status st = socket_.readSome(&redis_in_, config_.read_timeout_ms);
    if (!st.ok()) { failAll(st); return; }
    for (;;) {
      BufferChain reply;
      std::size_t consumed = 0;
      ParseStatus ps = reply_parser_.nextReplyFrame(redis_in_, &reply, &consumed);
      if (ps == ParseStatus::kNeedMore) break;
      if (ps != ParseStatus::kOk) { failAll(Status::ProtocolError("bad redis reply")); return; }
      dispatchReply(std::move(reply));
    }
  }
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Run backend channel test**

Run:

```bash
cmake --build build --target backend_channel_test -j
./build/backend_channel_test
```

Expected:

```text
backend_channel_test passed
```

- [ ] **Step 6: Commit backend channel queue semantics**

```bash
git add include/redis_proxy/backend_channel.h src/backend_channel.cpp tests/backend_channel_test.cpp
git commit -m "feat: add backend channel pending queue"
```

## Task 8: BackendPool Affinity

**Files:**

- Create: `include/redis_proxy/backend_pool.h`
- Create: `src/backend_pool.cpp`
- Create: `tests/backend_pool_test.cpp`

- [ ] **Step 1: Write affinity tests**

Create `tests/backend_pool_test.cpp`:

```cpp
#include "redis_proxy/backend_pool.h"
#include "redis_proxy/backend_channel.h"
#include "test_common.h"

class PoolSink : public redis_proxy::ReplySink {
public:
  void onBackendReply(redis_proxy::BufferChain) override { ++replies; }
  void onBackendFailure(const redis_proxy::Status&) override { ++failures; }
  int replies = 0;
  int failures = 0;
};

int main() {
  redis_proxy::Config cfg;
  cfg.backend_conns_per_worker = 2;
  redis_proxy::BlockPool pool(64);
  redis_proxy::BackendPool backend_pool(0, cfg, &pool);
  PoolSink session;

  redis_proxy::BackendChannel* first =
      backend_pool.selectForSessionForTest(&session, false, nullptr);
  RP_REQUIRE(first != nullptr);
  redis_proxy::BackendChannel* sticky =
      backend_pool.selectForSessionForTest(&session, true, first);
  RP_REQUIRE(sticky == first);
  redis_proxy::BackendChannel* next =
      backend_pool.selectForSessionForTest(&session, false, nullptr);
  RP_REQUIRE(next != nullptr);

  std::cout << "backend_pool_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Add BackendPool**

Create `include/redis_proxy/backend_pool.h`:

```cpp
#pragma once

#include <memory>
#include <vector>

#include "redis_proxy/backend_channel.h"
#include "redis_proxy/config.h"

namespace redis_proxy {

class BackendPool {
public:
  BackendPool(int worker_id, const Config& config, BlockPool* pool);
  void start();
  BackendChannel* submit(ReplySink* owner, BackendChannel* current, bool has_pending,
                         BufferChain bytes, uint32_t command_count, uint64_t sequence_base);
  BackendChannel* selectForSessionForTest(ReplySink* owner, bool has_pending, BackendChannel* current);

private:
  BackendChannel* select(ReplySink* owner, bool has_pending, BackendChannel* current);

  int worker_id_;
  Config config_;
  BlockPool* pool_;
  std::vector<std::unique_ptr<BackendChannel>> channels_;
};

}  // namespace redis_proxy
```

Create `src/backend_pool.cpp`:

```cpp
#include "redis_proxy/backend_pool.h"

namespace redis_proxy {

BackendPool::BackendPool(int worker_id, const Config& config, BlockPool* pool)
    : worker_id_(worker_id), config_(config), pool_(pool) {
  for (int i = 0; i < config_.backend_conns_per_worker; ++i) {
    channels_.push_back(std::make_unique<BackendChannel>(i, config_.redis, pool_));
  }
}

void BackendPool::start() {
  for (auto& channel : channels_) channel->start(config_);
}

BackendChannel* BackendPool::select(ReplySink*, bool has_pending, BackendChannel* current) {
  if (has_pending && current != nullptr) return current;
  BackendChannel* best = channels_.front().get();
  for (auto& channel : channels_) {
    if (channel->queuedCommandCount() < best->queuedCommandCount()) best = channel.get();
  }
  return best;
}

BackendChannel* BackendPool::submit(ReplySink* owner, BackendChannel* current, bool has_pending,
                                    BufferChain bytes, uint32_t command_count, uint64_t sequence_base) {
  BackendChannel* channel = select(owner, has_pending, current);
  channel->submit(owner, std::move(bytes), command_count, sequence_base);
  return channel;
}

BackendChannel* BackendPool::selectForSessionForTest(ReplySink* owner, bool has_pending, BackendChannel* current) {
  return select(owner, has_pending, current);
}

}  // namespace redis_proxy
```

- [ ] **Step 3: Run affinity test**

Run:

```bash
cmake --build build --target backend_pool_test -j
./build/backend_pool_test
```

Expected:

```text
backend_pool_test passed
```

- [ ] **Step 4: Commit backend pool**

```bash
git add include/redis_proxy/backend_pool.h src/backend_pool.cpp tests/backend_pool_test.cpp
git commit -m "feat: add backend pool affinity"
```

## Task 9: ClientSession Request Flow

**Files:**

- Create: `include/redis_proxy/client_session.h`
- Create: `src/client_session.cpp`
- Modify: `tests/backend_channel_test.cpp`

- [ ] **Step 1: Extend tests to verify per-session pending**

Add this case to `tests/backend_channel_test.cpp` before the success print:

```cpp
  FakeSink c;
  channel.submitForTest(&c, redis_proxy::MakeBufferChain(&pool, "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n"), 2, 3);
  RP_REQUIRE(channel.pendingBatchCountForTest() == 1);
  channel.dispatchReplyForTest(redis_proxy::MakeBufferChain(&pool, "$1\r\n1\r\n"));
  RP_REQUIRE(channel.pendingBatchCountForTest() == 1);
  channel.dispatchReplyForTest(redis_proxy::MakeBufferChain(&pool, "$1\r\n2\r\n"));
  RP_REQUIRE(channel.pendingBatchCountForTest() == 0);
```

- [ ] **Step 2: Add ClientSession API**

Create `include/redis_proxy/client_session.h`:

```cpp
#pragma once

#include <deque>

#include "redis_proxy/backend_pool.h"
#include "redis_proxy/co_socket.h"
#include "redis_proxy/command_rules.h"
#include "redis_proxy/resp_parser.h"

namespace redis_proxy {

class ClientSession : public ReplySink {
public:
  ClientSession(int id, int fd, const Config& config, CommandRules* rules, BackendPool* backend_pool, BlockPool* pool);
  void start();

  void onBackendReply(BufferChain reply) override;
  void onBackendFailure(const Status& status) override;

  std::size_t pendingRepliesForTest() const;

private:
  int id_;
  CoSocket socket_;
  Config config_;
  CommandRules* rules_;
  BackendPool* backend_pool_;
  BlockPool* pool_;
  RespParser parser_;
  IoBuffer client_in_;
  std::deque<BufferChain> client_out_;
  BackendChannel* current_backend_ = nullptr;
  std::size_t pending_replies_ = 0;
  uint64_t next_sequence_ = 1;
  bool closed_ = false;

  void readerLoop();
  void writerLoop();
  void enqueueErrorAndClose(std::string_view error);
  void submitFrame(RespFrame frame);
};

}  // namespace redis_proxy
```

- [ ] **Step 3: Implement ClientSession loops**

Create `src/client_session.cpp`:

```cpp
#include "redis_proxy/client_session.h"

#include "co_routine.h"
#include "redis/resp.h"

namespace redis_proxy {

ClientSession::ClientSession(int id, int fd, const Config& config, CommandRules* rules, BackendPool* backend_pool, BlockPool* pool)
    : id_(id), socket_(fd), config_(config), rules_(rules), backend_pool_(backend_pool), pool_(pool), client_in_(pool) {
  parser_.setLimits(config_.max_bulk_bytes, config_.max_array_elements, 8);
}

void ClientSession::start() {
  co::Coroutine* reader = co::co_create([this]() { readerLoop(); });
  co::Coroutine* writer = co::co_create([this]() { writerLoop(); });
  co::co_resume(reader);
  co::co_resume(writer);
}

std::size_t ClientSession::pendingRepliesForTest() const { return pending_replies_; }

void ClientSession::submitFrame(RespFrame frame) {
  Status valid = rules_->validate(frame.command.name, frame.command.argc);
  if (!valid.ok()) {
    enqueueErrorAndClose(valid.message());
    return;
  }
  const bool has_pending = pending_replies_ > 0;
  pending_replies_ += 1;
  current_backend_ = backend_pool_->submit(this, current_backend_, has_pending, std::move(frame.bytes), 1, next_sequence_++);
}

void ClientSession::readerLoop() {
  co::co_enable_hook_sys();
  while (!closed_) {
    Status st = socket_.readSome(&client_in_, config_.read_timeout_ms);
    if (!st.ok()) { closed_ = true; break; }
    std::size_t parsed = 0;
    while (parsed < config_.max_pipeline_commands_per_read) {
      RespFrame frame;
      ParseStatus ps = parser_.nextFrame(client_in_, &frame);
      if (ps == ParseStatus::kNeedMore) break;
      if (ps != ParseStatus::kOk) { enqueueErrorAndClose("ERR proxy protocol error"); break; }
      submitFrame(std::move(frame));
      ++parsed;
    }
  }
  if (current_backend_ != nullptr) current_backend_->detachOwner(this);
}

void ClientSession::writerLoop() {
  co::co_enable_hook_sys();
  while (!closed_ || !client_out_.empty()) {
    if (client_out_.empty()) {
      co::co_poll(nullptr, 0, 1);
      continue;
    }
    BufferChain reply = std::move(client_out_.front());
    client_out_.pop_front();
    Status st = socket_.writeAll(reply, config_.write_timeout_ms);
    if (!st.ok()) { closed_ = true; break; }
  }
  socket_.close();
}

void ClientSession::onBackendReply(BufferChain reply) {
  if (pending_replies_ > 0) --pending_replies_;
  if (pending_replies_ == 0) current_backend_ = nullptr;
  if (!closed_) client_out_.push_back(std::move(reply));
}

void ClientSession::onBackendFailure(const Status& status) {
  enqueueErrorAndClose(status.message().empty() ? "ERR proxy backend failure" : status.message());
}

void ClientSession::enqueueErrorAndClose(std::string_view error) {
  std::string encoded;
  redis::PackError(error, &encoded);
  client_out_.push_back(MakeBufferChain(pool_, encoded));
  closed_ = true;
}

}  // namespace redis_proxy
```

- [ ] **Step 4: Run client-related tests**

Run:

```bash
cmake --build build --target backend_channel_test backend_pool_test -j
./build/backend_channel_test
./build/backend_pool_test
```

Expected: both tests pass.

- [ ] **Step 5: Commit ClientSession**

```bash
git add include/redis_proxy/client_session.h src/client_session.cpp tests/backend_channel_test.cpp
git commit -m "feat: add client session flow"
```

## Task 10: Worker and ProxyServer

**Files:**

- Create: `include/redis_proxy/worker.h`
- Create: `src/worker.cpp`
- Create: `include/redis_proxy/proxy_server.h`
- Create: `src/proxy_server.cpp`
- Create: `src/main.cpp`

- [ ] **Step 1: Add Worker interface using libco echo-server pattern**

Create `include/redis_proxy/worker.h`:

```cpp
#pragma once

#include <atomic>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "redis_proxy/client_session.h"

namespace redis_proxy {

class Worker {
public:
  Worker(int id, const Config& config, CommandRules* rules);
  ~Worker();

  void start();
  void join();
  void dispatchFd(int fd);
  int id() const;

private:
  int id_;
  Config config_;
  CommandRules* rules_;
  std::thread thread_;
  std::mutex mutex_;
  std::deque<int> pending_fds_;
  std::atomic<bool> has_pending_fds_{false};
  std::unique_ptr<BlockPool> pool_;
  std::unique_ptr<BackendPool> backend_pool_;
  std::vector<std::unique_ptr<ClientSession>> sessions_;
  int next_session_id_ = 1;

  void run();
  void reapFds();
};

}  // namespace redis_proxy
```

- [ ] **Step 2: Implement Worker**

Create `src/worker.cpp`:

```cpp
#include "redis_proxy/worker.h"

#include "co_routine.h"
#include "thread_worker.h"

#include <unistd.h>

namespace redis_proxy {

Worker::Worker(int id, const Config& config, CommandRules* rules)
    : id_(id), config_(config), rules_(rules),
      pool_(std::make_unique<BlockPool>(32 * 1024)),
      backend_pool_(std::make_unique<BackendPool>(id, config, pool_.get())) {}
Worker::~Worker() = default;
void Worker::start() { thread_ = std::thread([this]() { run(); }); }
void Worker::join() { if (thread_.joinable()) thread_.join(); }
int Worker::id() const { return id_; }
void Worker::dispatchFd(int fd) {
  std::lock_guard<std::mutex> lock(mutex_);
  pending_fds_.push_back(fd);
  has_pending_fds_.store(true, std::memory_order_release);
}
void Worker::reapFds() {
  if (!has_pending_fds_.load(std::memory_order_acquire)) return;
  std::deque<int> fds;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    fds.swap(pending_fds_);
    has_pending_fds_.store(false, std::memory_order_release);
  }
  while (!fds.empty()) {
    int fd = fds.front();
    fds.pop_front();
    auto session = std::make_unique<ClientSession>(next_session_id_++, fd, config_, rules_, backend_pool_.get(), pool_.get());
    session->start();
    sessions_.push_back(std::move(session));
  }
}
void Worker::run() {
  co::co_enable_hook_sys();
  backend_pool_->start();
  co::Coroutine* reaper = co::co_create([this]() {
    co::co_enable_hook_sys();
    while (true) {
      reapFds();
      co::co_poll(nullptr, 0, 1);
    }
  });
  co::co_resume(reaper);
  co::ThreadWorker loop(id_);
  loop.run_loop();
}

}  // namespace redis_proxy
```

- [ ] **Step 3: Add ProxyServer and main**

Create `include/redis_proxy/proxy_server.h`:

```cpp
#pragma once

#include <memory>
#include <vector>

#include "redis_proxy/command_rules.h"
#include "redis_proxy/config.h"
#include "redis_proxy/worker.h"

namespace redis_proxy {

class ProxyServer {
public:
  explicit ProxyServer(Config config);
  int run();

private:
  Config config_;
  CommandRules rules_;
  int listen_fd_ = -1;
  std::vector<std::unique_ptr<Worker>> workers_;

  void acceptLoop();
};

}  // namespace redis_proxy
```

Create `src/proxy_server.cpp`:

```cpp
#include "redis_proxy/proxy_server.h"

#include "co_routine.h"

#include <csignal>
#include <iostream>
#include <poll.h>
#include <unistd.h>

namespace redis_proxy {

ProxyServer::ProxyServer(Config config) : config_(std::move(config)), rules_(CommandRules::Default()) {}

int ProxyServer::run() {
  std::signal(SIGPIPE, SIG_IGN);
  listen_fd_ = CreateTcpListenSocket(config_.listen, 1024);
  if (listen_fd_ < 0) {
    std::cerr << "failed to listen on " << config_.listen.toString() << "\n";
    return 1;
  }
  for (int i = 0; i < config_.workers; ++i) {
    auto worker = std::make_unique<Worker>(i, config_, &rules_);
    worker->start();
    workers_.push_back(std::move(worker));
  }
  co::Coroutine* acceptor = co::co_create([this]() { acceptLoop(); });
  co::co_resume(acceptor);
  co::ThreadWorker loop(-1);
  loop.run_loop();
  return 0;
}

void ProxyServer::acceptLoop() {
  co::co_enable_hook_sys();
  std::size_t next_worker = 0;
  while (true) {
    sockaddr_in addr;
    socklen_t len = sizeof(addr);
    int fd = co::co_accept(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    if (fd < 0) {
      pollfd pfd{listen_fd_, POLLIN | POLLERR | POLLHUP, 0};
      co::co_poll(&pfd, 1, 1000);
      continue;
    }
    SetNonBlocking(fd);
    workers_[next_worker]->dispatchFd(fd);
    next_worker = (next_worker + 1) % workers_.size();
  }
}

}  // namespace redis_proxy
```

Create `src/main.cpp`:

```cpp
#include "redis_proxy/config.h"
#include "redis_proxy/proxy_server.h"

#include <iostream>

int main(int argc, char** argv) {
  redis_proxy::Config config;
  if (!redis_proxy::LoadConfigFromArgs(argc, argv, &config)) {
    std::cerr << "usage: redis_proxy [--config path] [--listen host:port] [--redis host:port] [--workers n] [--backend-conns n]\n";
    return 2;
  }
  redis_proxy::ProxyServer server(config);
  return server.run();
}
```

- [ ] **Step 4: Build main binary**

Run:

```bash
cmake --build build --target redis_proxy -j
```

Expected: `build/redis_proxy` exists.

- [ ] **Step 5: Commit worker and server**

```bash
git add include/redis_proxy/worker.h src/worker.cpp include/redis_proxy/proxy_server.h src/proxy_server.cpp src/main.cpp
git commit -m "feat: add libco worker proxy server"
```

## Task 11: Redis Backend Connection Lifecycle

**Files:**

- Modify: `include/redis_proxy/backend_channel.h`
- Modify: `src/backend_channel.cpp`

- [ ] **Step 1: Add connect and reconnect fields**

Extend private fields in `BackendChannel`:

```cpp
bool reconnecting_ = false;
int reconnect_delay_ms_ = 100;
```

- [ ] **Step 2: Implement connect helper**

Add private method declaration:

```cpp
Status connectOnce();
```

Add implementation:

```cpp
Status BackendChannel::connectOnce() {
  int fd = CreateTcpClientSocket();
  if (fd < 0) return Status::IoError("socket failed");
  socket_.reset(fd);
  Status st = socket_.connectTo(endpoint_, config_.connect_timeout_ms);
  if (!st.ok()) {
    socket_.close();
    healthy_ = false;
    return st;
  }
  healthy_ = true;
  return Status::Ok();
}
```

- [ ] **Step 3: Make only the writer coroutine establish the connection**

At the start of `writerLoop()`'s outer loop, before checking `write_queue_`, insert:

```cpp
while (!healthy_) {
  Status st = connectOnce();
  if (st.ok()) break;
  co::co_poll(nullptr, 0, reconnect_delay_ms_);
}
```

At the start of `readerLoop()`'s outer loop, insert this wait block instead of calling `connectOnce()`:

```cpp
while (!healthy_) {
  co::co_poll(nullptr, 0, reconnect_delay_ms_);
}
```

This keeps one coroutine responsible for replacing `socket_`, so reader and writer do not race on the same fd.

- [ ] **Step 4: Verify compile**

Run:

```bash
cmake --build build --target backend_channel_test redis_proxy -j
./build/backend_channel_test
```

Expected: backend channel test still passes and proxy binary builds.

- [ ] **Step 5: Commit backend lifecycle**

```bash
git add include/redis_proxy/backend_channel.h src/backend_channel.cpp
git commit -m "feat: connect backend channels"
```

## Task 12: Integration Test Against Local Redis

**Files:**

- Create: `tests/integration_proxy_test.cpp`

- [ ] **Step 1: Write integration test with skip when redis-server is unavailable**

Create `tests/integration_proxy_test.cpp`:

```cpp
#include "redis/resp.h"
#include "test_common.h"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

bool HasRedisServer() {
  return std::system("command -v redis-server >/dev/null 2>&1") == 0;
}

int Connect(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  for (int i = 0; i < 50; ++i) {
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) return fd;
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  close(fd);
  return -1;
}

std::string ReadSome(int fd) {
  char buf[4096];
  ssize_t n = read(fd, buf, sizeof(buf));
  if (n <= 0) return "";
  return std::string(buf, static_cast<std::size_t>(n));
}

}  // namespace

int main() {
  if (!HasRedisServer()) {
    std::cout << "integration_proxy_test skipped: redis-server not found\n";
    return 0;
  }

  const int redis_port = 6380;
  const int proxy_port = 6390;
  pid_t redis_pid = fork();
  if (redis_pid == 0) {
    execlp("redis-server", "redis-server", "--port", "6380", "--save", "", "--appendonly", "no", nullptr);
    _exit(127);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  pid_t proxy_pid = fork();
  if (proxy_pid == 0) {
    execl("./redis_proxy", "./redis_proxy", "--listen", "127.0.0.1:6390", "--redis", "127.0.0.1:6380",
          "--workers", "1", "--backend-conns", "2", nullptr);
    _exit(127);
  }

  int fd = Connect(proxy_port);
  RP_REQUIRE(fd >= 0);

  std::string req;
  redis::PackCommand({"SET", "it:key", "1"}, &req);
  redis::PackCommand({"INCR", "it:key"}, &req);
  redis::PackCommand({"GET", "it:key"}, &req);
  RP_REQUIRE(write(fd, req.data(), req.size()) == static_cast<ssize_t>(req.size()));
  std::string reply = ReadSome(fd);
  RP_REQUIRE(reply.find("+OK\r\n") != std::string::npos);
  RP_REQUIRE(reply.find(":2\r\n") != std::string::npos);
  RP_REQUIRE(reply.find("$1\r\n2\r\n") != std::string::npos);

  close(fd);
  kill(proxy_pid, SIGTERM);
  kill(redis_pid, SIGTERM);
  waitpid(proxy_pid, nullptr, 0);
  waitpid(redis_pid, nullptr, 0);

  std::cout << "integration_proxy_test passed\n";
  return 0;
}
```

- [ ] **Step 2: Build and run integration test from build directory**

Run:

```bash
cmake --build build --target integration_proxy_test redis_proxy -j
cd build && ./integration_proxy_test
```

Expected one of:

```text
integration_proxy_test skipped: redis-server not found
```

or:

```text
integration_proxy_test passed
```

- [ ] **Step 3: Commit integration test**

```bash
git add tests/integration_proxy_test.cpp
git commit -m "test: add redis proxy integration test"
```

## Task 13: Single-Worker QPS Benchmark

**Files:**

- Create: `bench/proxy_bench.cpp`
- Create: `bench/run_single_worker_qps.sh`

- [ ] **Step 1: Add C++ benchmark client**

Create `bench/proxy_bench.cpp`:

```cpp
#include "redis/resp.h"

#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

struct Options {
  int port = 6379;
  int clients = 1;
  int pipeline = 1;
  int seconds = 10;
  std::string command = "PING";
};

Options Parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto value = [&]() -> const char* { return i + 1 < argc ? argv[++i] : ""; };
    if (arg == "--port") o.port = std::atoi(value());
    else if (arg == "--clients") o.clients = std::atoi(value());
    else if (arg == "--pipeline") o.pipeline = std::atoi(value());
    else if (arg == "--seconds") o.seconds = std::atoi(value());
    else if (arg == "--command") o.command = value();
  }
  return o;
}

int Connect(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) return -1;
  return fd;
}

std::string MakeRequest(const std::string& command, int pipeline) {
  std::string req;
  for (int i = 0; i < pipeline; ++i) {
    if (command == "PING") redis::PackCommand({"PING"}, &req);
    else if (command == "GET") redis::PackCommand({"GET", "bench:key"}, &req);
    else redis::PackCommand({"SET", "bench:key", "0123456789abcdef"}, &req);
  }
  return req;
}

void Worker(const Options& options, std::chrono::steady_clock::time_point end, unsigned long long* ops) {
  int fd = Connect(options.port);
  if (fd < 0) return;
  std::string req = MakeRequest(options.command, options.pipeline);
  std::string reply;
  reply.resize(1024 * 1024);
  unsigned long long local = 0;
  while (std::chrono::steady_clock::now() < end) {
    if (write(fd, req.data(), req.size()) != static_cast<ssize_t>(req.size())) break;
    int replies = 0;
    while (replies < options.pipeline) {
      ssize_t n = read(fd, &reply[0], reply.size());
      if (n <= 0) { close(fd); *ops = local; return; }
      for (ssize_t i = 0; i < n; ++i) {
        if (reply[static_cast<std::size_t>(i)] == '\n') ++replies;
      }
    }
    local += static_cast<unsigned long long>(options.pipeline);
  }
  close(fd);
  *ops = local;
}

}  // namespace

int main(int argc, char** argv) {
  Options options = Parse(argc, argv);
  auto end = std::chrono::steady_clock::now() + std::chrono::seconds(options.seconds);
  std::vector<std::thread> threads;
  std::vector<unsigned long long> ops(options.clients, 0);
  for (int i = 0; i < options.clients; ++i) {
    threads.emplace_back(Worker, std::cref(options), end, &ops[i]);
  }
  unsigned long long total = 0;
  for (int i = 0; i < options.clients; ++i) {
    threads[i].join();
    total += ops[i];
  }
  const double qps = static_cast<double>(total) / options.seconds;
  std::cout << "{\"clients\":" << options.clients
            << ",\"pipeline\":" << options.pipeline
            << ",\"seconds\":" << options.seconds
            << ",\"command\":\"" << options.command
            << "\",\"qps\":" << qps << "}\n";
  return 0;
}
```

- [ ] **Step 2: Add single-worker benchmark script**

Create `bench/run_single_worker_qps.sh` and make it executable:

```bash
#!/usr/bin/env bash
set -euo pipefail

build_dir="${BUILD_DIR:-build}"
redis_port="${REDIS_PORT:-6380}"
proxy_port="${PROXY_PORT:-6390}"
seconds="${SECONDS_PER_CASE:-10}"
out_dir="${BENCH_OUT_DIR:-${build_dir}/bench-results}"
mkdir -p "${out_dir}"

if ! command -v redis-server >/dev/null 2>&1; then
  echo "redis-server not found; skipping benchmark"
  exit 0
fi

redis-server --port "${redis_port}" --save "" --appendonly no >"${out_dir}/redis.log" 2>&1 &
redis_pid=$!
trap 'kill ${proxy_pid:-0} ${redis_pid:-0} >/dev/null 2>&1 || true' EXIT
sleep 0.3

"${build_dir}/redis_proxy" --listen "127.0.0.1:${proxy_port}" --redis "127.0.0.1:${redis_port}" --workers 1 --backend-conns "${BACKEND_CONNS:-1}" >"${out_dir}/proxy.log" 2>&1 &
proxy_pid=$!
sleep 0.5

csv="${out_dir}/single-worker-qps.csv"
echo "target,backend_conns,clients,pipeline,command,qps" >"${csv}"

for backend_conns in 1 2 4; do
  kill "${proxy_pid}" >/dev/null 2>&1 || true
  "${build_dir}/redis_proxy" --listen "127.0.0.1:${proxy_port}" --redis "127.0.0.1:${redis_port}" --workers 1 --backend-conns "${backend_conns}" >"${out_dir}/proxy-${backend_conns}.log" 2>&1 &
  proxy_pid=$!
  sleep 0.5
  for clients in 1 16 64 128; do
    for pipeline in 1 16 64 128; do
      for command in PING GET SET; do
        proxy_json=$("${build_dir}/proxy_bench" --port "${proxy_port}" --clients "${clients}" --pipeline "${pipeline}" --seconds "${seconds}" --command "${command}")
        direct_json=$("${build_dir}/proxy_bench" --port "${redis_port}" --clients "${clients}" --pipeline "${pipeline}" --seconds "${seconds}" --command "${command}")
        proxy_qps=$(echo "${proxy_json}" | sed -n 's/.*"qps":\([0-9.]*\).*/\1/p')
        direct_qps=$(echo "${direct_json}" | sed -n 's/.*"qps":\([0-9.]*\).*/\1/p')
        echo "proxy,${backend_conns},${clients},${pipeline},${command},${proxy_qps}" | tee -a "${csv}"
        echo "direct,${backend_conns},${clients},${pipeline},${command},${direct_qps}" | tee -a "${csv}"
      done
    done
  done
done
```

Run:

```bash
chmod +x bench/run_single_worker_qps.sh
```

- [ ] **Step 3: Build benchmark**

Run:

```bash
cmake --build build --target proxy_bench redis_proxy -j
```

Expected: `build/proxy_bench` and `build/redis_proxy` exist.

- [ ] **Step 4: Run one short benchmark case**

Run:

```bash
SECONDS_PER_CASE=1 bench/run_single_worker_qps.sh
```

Expected: benchmark exits 0 and writes:

```text
build/bench-results/single-worker-qps.csv
```

- [ ] **Step 5: Commit benchmark**

```bash
git add bench/proxy_bench.cpp bench/run_single_worker_qps.sh
git commit -m "bench: add single worker qps benchmark"
```

## Task 14: Full Verification

**Files:**

- Modify only files needed to fix failures found by the commands in this task.

- [ ] **Step 1: Build without jemalloc to isolate proxy code**

Run:

```bash
cmake -S . -B build -DREDIS_PROXY_USE_JEMALLOC=OFF
cmake --build build -j
```

Expected: all targets build.

- [ ] **Step 2: Run unit tests**

Run:

```bash
cd build && ctest --output-on-failure
```

Expected: all tests pass. `integration_proxy_test` may print a skip message only when `redis-server` is unavailable.

- [ ] **Step 3: Build with jemalloc**

Run:

```bash
cmake -S . -B build-jemalloc -DREDIS_PROXY_USE_JEMALLOC=ON
cmake --build build-jemalloc -j
```

Expected: jemalloc builds under `build-jemalloc/jemalloc`, and `redis_proxy` links successfully.

- [ ] **Step 4: Run libco and cpp_util upstream tests**

Run:

```bash
make -C thirdparty/libco test
make -C thirdparty/cpp_util/redis test
```

Expected: libco tests and cpp_util Redis RESP tests pass.

- [ ] **Step 5: Run QPS smoke benchmark**

Run:

```bash
SECONDS_PER_CASE=1 BUILD_DIR=build bench/run_single_worker_qps.sh
```

Expected: command exits 0. If `redis-server` exists, `build/bench-results/single-worker-qps.csv` contains `proxy` and `direct` rows.

- [ ] **Step 6: Commit verification fixes**

If Step 1-5 required fixes, commit them:

```bash
git add include src tests bench CMakeLists.txt cmake scripts Makefile
git commit -m "fix: complete redis proxy verification"
```

If no fixes were needed, do not create an empty commit.

## Spec Coverage Checklist

- Submodules: covered by existing `.gitmodules`, root `Makefile`, and Task 1 build wiring.
- C++17: covered by Task 1 CMake.
- libco: covered by Tasks 6, 10, and 11 using `co_create`, `co_resume`, `co_eventloop`, `co_poll`, `co_accept`, and `co_enable_hook_sys`.
- cpp_util Redis RESP: covered by Task 5 and Task 13 through `redis::UnpackOne`, `redis::PackError`, and `redis::PackCommand`.
- Single Redis backend: covered by config and backend channel lifecycle.
- Multi-worker process: covered by Worker and ProxyServer.
- 1-4 backend connections per worker: covered by config validation and BackendPool construction.
- RESP2 array only: covered by RespParser tests and command extraction.
- Same-client pipeline ordering: covered by BackendChannel pending queue and BackendPool affinity.
- Different-client interleaving: allowed by shared backend channels and no global ordering locks.
- Command validation: covered by CommandRules.
- No transaction/subscription/blocking/state commands: covered by default denied command list.
- Client disconnect alignment: covered by `BackendChannel::detachOwner()` and `ClientSession::readerLoop()` cleanup.
- No request replay: covered by BackendChannel fail-all behavior.
- Performance test: covered by Task 13.

## Implementation Notes

- Keep all worker-owned data single-threaded after fd dispatch. Do not add locks inside `BackendChannel`, `ClientSession`, `IoBuffer`, or `RespParser`.
- Do not use `writev` in `CoSocket` until libco adds a hook or a verified `co_poll`-based wrapper is implemented.
- Do not parse and repack client requests on the proxy hot path. Use `RespParser` only to validate and find frame boundaries, then forward the original `BufferChain`.
- Because `redis::UnpackOne()` requires contiguous input, `IoBuffer::ensureContiguousPrefix()` is the only place where cross-block input may be copied.
- `redis::RespValue` and `std::string_view` results must not escape `RespParser::nextFrame()` after the input buffer is consumed. Copy only command name and argument views needed during that call.
- Keep generated proxy errors as RESP errors via `redis::PackError()`.
