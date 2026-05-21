# Util Directory Refactoring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move reusable utility components from `src/` to `util/` directory with flat structure for better code reusability.

**Architecture:** Extract socket utility functions from endpoint.cpp into dedicated socket_utils module. Move endpoint and fd_notifier to util/ with headers co-located. Update all include paths and build system.

**Tech Stack:** C++17, CMake, existing test framework

---

## File Structure Overview

**New files to create:**
- `util/socket_utils.h` - Socket utility function declarations
- `util/socket_utils.cpp` - Socket utility function implementations
- `tests/endpoint_test.cpp` - Unit tests for Endpoint class
- `tests/socket_utils_test.cpp` - Unit tests for socket utilities
- `tests/fd_notifier_test.cpp` - Unit tests for FdNotifier

**Files to move:**
- `src/endpoint.cpp` → `util/endpoint.cpp`
- `include/redis_proxy/endpoint.h` → `util/endpoint.h`
- `src/fd_notifier.cpp` → `util/fd_notifier.cpp`
- `include/redis_proxy/fd_notifier.h` → `util/fd_notifier.h`

**Files to modify:**
- `CMakeLists.txt` - Add util library target
- `src/proxy_server.cpp` - Update includes
- `src/backend_channel.cpp` - Update includes
- `src/co_socket.cpp` - Update includes
- `src/worker.cpp` - Update includes
- `include/redis_proxy/worker.h` - Update includes
- `include/redis_proxy/co_socket.h` - Update includes

---

### Task 1: Create socket_utils module with tests

**Files:**
- Create: `util/socket_utils.h`
- Create: `util/socket_utils.cpp`
- Create: `tests/socket_utils_test.cpp`

- [ ] **Step 1: Write failing test for socket utilities**

Create `tests/socket_utils_test.cpp`:

```cpp
#include "util/socket_utils.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdexcept>

namespace redis_proxy {

void test_set_non_blocking() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket creation failed");
  }
  
  int result = SetNonBlocking(fd);
  if (result != 0) {
    close(fd);
    throw std::runtime_error("SetNonBlocking returned error");
  }
  
  int flags = fcntl(fd, F_GETFL, 0);
  close(fd);
  
  if ((flags & O_NONBLOCK) == 0) {
    throw std::runtime_error("Socket is not non-blocking");
  }
}

void test_set_tcp_no_delay() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket creation failed");
  }
  
  int result = SetTcpNoDelay(fd);
  close(fd);
  
  if (result != 0) {
    throw std::runtime_error("SetTcpNoDelay failed");
  }
}

}  // namespace redis_proxy

int main() {
  try {
    redis_proxy::test_set_non_blocking();
    redis_proxy::test_set_tcp_no_delay();
  } catch (const std::exception& e) {
    return 1;
  }
  return 0;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && cmake .. && make socket_utils_test`

Expected: Build fails with "util/socket_utils.h: No such file"

- [ ] **Step 3: Create socket_utils.h header**

Create `util/socket_utils.h`:

```cpp
#pragma once

namespace redis_proxy {

class Endpoint;

// Configure socket as non-blocking
int SetNonBlocking(int fd);

// Disable Nagle's algorithm (TCP_NODELAY)
int SetTcpNoDelay(int fd);

// Create a TCP listening socket bound to endpoint
int CreateTcpListenSocket(const Endpoint& endpoint, int backlog);

// Create a TCP client socket (non-blocking, TCP_NODELAY enabled)
int CreateTcpClientSocket();

}  // namespace redis_proxy
```

- [ ] **Step 4: Create socket_utils.cpp implementation**

Create `util/socket_utils.cpp`:

```cpp
#include "util/socket_utils.h"
#include "util/endpoint.h"

#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace redis_proxy {

int SetNonBlocking(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return -1;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK | O_NDELAY);
}

int SetTcpNoDelay(int fd) {
  int flag = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
}

int CreateTcpListenSocket(const Endpoint& endpoint, int backlog) {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd < 0) {
    return -1;
  }
  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in addr;
  if (!endpoint.toSockAddr(&addr) ||
      bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      listen(fd, backlog) != 0 || SetNonBlocking(fd) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

int CreateTcpClientSocket() {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd >= 0 && (SetNonBlocking(fd) != 0 || SetTcpNoDelay(fd) != 0)) {
    close(fd);
    return -1;
  }
  return fd;
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Update CMakeLists.txt to add util library**

Modify `CMakeLists.txt` after line 13 (after libco subdirectory):

```cmake
# Add util library
file(GLOB REDIS_PROXY_UTIL_SRCS CONFIGURE_DEPENDS util/*.cpp)
add_library(redis_proxy_util ${REDIS_PROXY_UTIL_SRCS})
target_include_directories(redis_proxy_util PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR})
```

And modify line 43 to link util library:

```cmake
target_link_libraries(redis_proxy_core PUBLIC redis_proxy_util colib_static redis_resp pthread)
```

- [ ] **Step 6: Update CMakeLists.txt to add socket_utils_test**

Add after line 65 in CMakeLists.txt:

```cmake
add_executable(socket_utils_test tests/socket_utils_test.cpp)
target_link_libraries(socket_utils_test PRIVATE redis_proxy_util)
add_test(NAME socket_utils_test COMMAND socket_utils_test)
```

- [ ] **Step 7: Build and run test to verify it passes**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && cmake .. && make socket_utils_test && ./socket_utils_test`

Expected: Test compiles but fails because Endpoint is not yet in util/

- [ ] **Step 8: Commit socket_utils module**

```bash
git add util/socket_utils.h util/socket_utils.cpp tests/socket_utils_test.cpp CMakeLists.txt
git commit -m "feat: add socket_utils module with utility functions"
```

---

### Task 2: Move endpoint module to util

**Files:**
- Move: `src/endpoint.cpp` → `util/endpoint.cpp`
- Move: `include/redis_proxy/endpoint.h` → `util/endpoint.h`
- Modify: `util/endpoint.cpp` (remove socket functions, add socket_utils include)
- Create: `tests/endpoint_test.cpp`

- [ ] **Step 1: Write failing test for Endpoint**

Create `tests/endpoint_test.cpp`:

```cpp
#include "util/endpoint.h"

#include <stdexcept>
#include <string>

namespace redis_proxy {

void test_endpoint_parse_valid() {
  Endpoint ep;
  if (!Endpoint::parse("127.0.0.1:6379", &ep)) {
    throw std::runtime_error("Failed to parse valid endpoint");
  }
  if (ep.host() != "127.0.0.1" || ep.port() != 6379) {
    throw std::runtime_error("Parsed endpoint has wrong values");
  }
}

void test_endpoint_parse_invalid() {
  Endpoint ep;
  if (Endpoint::parse("invalid", &ep)) {
    throw std::runtime_error("Should fail to parse invalid endpoint");
  }
  if (Endpoint::parse(":6379", &ep)) {
    throw std::runtime_error("Should fail with missing host");
  }
  if (Endpoint::parse("host:", &ep)) {
    throw std::runtime_error("Should fail with missing port");
  }
}

void test_endpoint_to_string() {
  Endpoint ep("192.168.1.1", 8080);
  if (ep.toString() != "192.168.1.1:8080") {
    throw std::runtime_error("toString() returned wrong value");
  }
}

}  // namespace redis_proxy

int main() {
  try {
    redis_proxy::test_endpoint_parse_valid();
    redis_proxy::test_endpoint_parse_invalid();
    redis_proxy::test_endpoint_to_string();
  } catch (const std::exception& e) {
    return 1;
  }
  return 0;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && cmake .. && make endpoint_test`

Expected: Build fails with "util/endpoint.h: No such file"

- [ ] **Step 3: Move endpoint.h to util/**

```bash
mv /mnt/d/ginobili/code/redis_proxy/include/redis_proxy/endpoint.h /mnt/d/ginobili/code/redis_proxy/util/endpoint.h
```

- [ ] **Step 4: Update endpoint.h to remove socket function declarations**

Edit `util/endpoint.h` to remove lines 29-32 (socket function declarations):

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
  Endpoint(std::string host, uint16_t port)
      : host_(std::move(host)), port_(port) {}

  static bool parse(const std::string& text, Endpoint* out);
  bool toSockAddr(sockaddr_in* out) const;

  const std::string& host() const { return host_; }
  uint16_t port() const { return port_; }
  std::string toString() const;

private:
  std::string host_ = "127.0.0.1";
  uint16_t port_ = 0;
};

}  // namespace redis_proxy
```

- [ ] **Step 5: Move endpoint.cpp to util/ and update it**

```bash
mv /mnt/d/ginobili/code/redis_proxy/src/endpoint.cpp /mnt/d/ginobili/code/redis_proxy/util/endpoint.cpp
```

- [ ] **Step 6: Update endpoint.cpp to remove socket functions and fix include**

Edit `util/endpoint.cpp` - change line 1 and remove lines 43-82:

```cpp
#include "util/endpoint.h"

#include <cstdlib>
#include <cstring>

#include <arpa/inet.h>

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

}  // namespace redis_proxy
```

- [ ] **Step 7: Add endpoint_test to CMakeLists.txt**

Add after socket_utils_test in CMakeLists.txt:

```cmake
add_executable(endpoint_test tests/endpoint_test.cpp)
target_link_libraries(endpoint_test PRIVATE redis_proxy_util)
add_test(NAME endpoint_test COMMAND endpoint_test)
```

- [ ] **Step 8: Build and run tests**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && cmake .. && make endpoint_test socket_utils_test && ./endpoint_test && ./socket_utils_test`

Expected: Both tests pass

- [ ] **Step 9: Commit endpoint module move**

```bash
git add util/endpoint.h util/endpoint.cpp tests/endpoint_test.cpp CMakeLists.txt
git rm include/redis_proxy/endpoint.h src/endpoint.cpp
git commit -m "refactor: move endpoint module to util directory"
```

---

### Task 3: Move fd_notifier module to util

**Files:**
- Move: `src/fd_notifier.cpp` → `util/fd_notifier.cpp`
- Move: `include/redis_proxy/fd_notifier.h` → `util/fd_notifier.h`
- Create: `tests/fd_notifier_test.cpp`

- [ ] **Step 1: Write failing test for FdNotifier**

Create `tests/fd_notifier_test.cpp`:

```cpp
#include "util/fd_notifier.h"

#include <poll.h>
#include <stdexcept>

namespace redis_proxy {

void test_fd_notifier_creation() {
  FdNotifier notifier;
  if (!notifier.valid()) {
    throw std::runtime_error("FdNotifier should be valid after construction");
  }
  if (notifier.readFd() < 0) {
    throw std::runtime_error("readFd should be valid");
  }
}

void test_fd_notifier_notify_drain() {
  FdNotifier notifier;
  if (!notifier.valid()) {
    throw std::runtime_error("FdNotifier not valid");
  }
  
  Status status = notifier.notify();
  if (!status.ok()) {
    throw std::runtime_error("notify failed");
  }
  
  // Check that readFd is readable
  pollfd pfd;
  pfd.fd = notifier.readFd();
  pfd.events = POLLIN;
  int result = poll(&pfd, 1, 100);
  if (result <= 0 || !(pfd.revents & POLLIN)) {
    throw std::runtime_error("readFd should be readable after notify");
  }
  
  status = notifier.drain();
  if (!status.ok()) {
    throw std::runtime_error("drain failed");
  }
  
  // Check that readFd is no longer readable
  result = poll(&pfd, 1, 100);
  if (result > 0 && (pfd.revents & POLLIN)) {
    throw std::runtime_error("readFd should not be readable after drain");
  }
}

}  // namespace redis_proxy

int main() {
  try {
    redis_proxy::test_fd_notifier_creation();
    redis_proxy::test_fd_notifier_notify_drain();
  } catch (const std::exception& e) {
    return 1;
  }
  return 0;
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && cmake .. && make fd_notifier_test`

Expected: Build fails with "util/fd_notifier.h: No such file"

- [ ] **Step 3: Move fd_notifier files to util/**

```bash
mv /mnt/d/ginobili/code/redis_proxy/include/redis_proxy/fd_notifier.h /mnt/d/ginobili/code/redis_proxy/util/fd_notifier.h && mv /mnt/d/ginobili/code/redis_proxy/src/fd_notifier.cpp /mnt/d/ginobili/code/redis_proxy/util/fd_notifier.cpp
```

- [ ] **Step 4: Update fd_notifier.h and fd_notifier.cpp includes**

Edit `util/fd_notifier.h` - change line 1:

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

Edit `util/fd_notifier.cpp` - change line 1:

```cpp
#include "util/fd_notifier.h"

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
  if (n == 1) {
    return Status::Ok();
  }
  if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
    return Status::Ok();
  }
  if (n == 0) {
    return Status::IoError("zero write");
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
    if (n == 0) {
      return Status::Closed("fd notifier closed");
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return Status::Ok();
    }
    return Status::IoError(std::strerror(errno));
  }
}

}  // namespace redis_proxy
```

- [ ] **Step 5: Add fd_notifier_test to CMakeLists.txt**

Add after endpoint_test in CMakeLists.txt:

```cmake
add_executable(fd_notifier_test tests/fd_notifier_test.cpp)
target_link_libraries(fd_notifier_test PRIVATE redis_proxy_util redis_proxy_core)
add_test(NAME fd_notifier_test COMMAND fd_notifier_test)
```

- [ ] **Step 6: Build and run test**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && cmake .. && make fd_notifier_test && ./fd_notifier_test`

Expected: Test passes

- [ ] **Step 7: Commit fd_notifier module move**

```bash
git add util/fd_notifier.h util/fd_notifier.cpp tests/fd_notifier_test.cpp CMakeLists.txt
git rm include/redis_proxy/fd_notifier.h src/fd_notifier.cpp
git commit -m "refactor: move fd_notifier module to util directory"
```

---

### Task 4: Update include paths in source files

**Files:**
- Modify: `include/redis_proxy/worker.h`
- Modify: `include/redis_proxy/co_socket.h`
- Modify: `src/proxy_server.cpp`
- Modify: `src/backend_channel.cpp`
- Modify: `src/co_socket.cpp`
- Modify: `src/worker.cpp`

- [ ] **Step 1: Update worker.h include**

Edit `include/redis_proxy/worker.h` line 11:

Change:
```cpp
#include "redis_proxy/fd_notifier.h"
```

To:
```cpp
#include "util/fd_notifier.h"
```

- [ ] **Step 2: Update co_socket.h include**

Edit `include/redis_proxy/co_socket.h` - find the endpoint.h include and change:

```cpp
#include "util/endpoint.h"
```

- [ ] **Step 3: Update proxy_server.cpp includes**

Edit `src/proxy_server.cpp` - add after line 1:

```cpp
#include "redis_proxy/proxy_server.h"

#include "util/socket_utils.h"

#include "co_routine.h"
```

- [ ] **Step 4: Update backend_channel.cpp includes**

Edit `src/backend_channel.cpp` - add after line 1:

```cpp
#include "redis_proxy/backend_channel.h"

#include "util/socket_utils.h"

#include "co_routine.h"
```

- [ ] **Step 5: Update co_socket.cpp include**

Edit `src/co_socket.cpp` line 1:

Change:
```cpp
#include "redis_proxy/co_socket.h"
```

To:
```cpp
#include "redis_proxy/co_socket.h"

#include "util/endpoint.h"
```

- [ ] **Step 6: Build to verify all includes are correct**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && cmake .. && make -j4`

Expected: Build succeeds without errors

- [ ] **Step 7: Run all tests to verify nothing broke**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && ctest --output-on-failure`

Expected: All tests pass

- [ ] **Step 8: Commit include path updates**

```bash
git add include/redis_proxy/worker.h include/redis_proxy/co_socket.h src/proxy_server.cpp src/backend_channel.cpp src/co_socket.cpp src/worker.cpp
git commit -m "refactor: update include paths to use util directory"
```

---

### Task 5: Verify and finalize

**Files:**
- All modified files

- [ ] **Step 1: Verify no old include paths remain**

Run: `grep -r "redis_proxy/endpoint.h\|redis_proxy/fd_notifier.h" /mnt/d/ginobili/code/redis_proxy/src /mnt/d/ginobili/code/redis_proxy/include --include="*.cpp" --include="*.h"`

Expected: No matches found

- [ ] **Step 2: Verify old header files are removed**

Run: `ls /mnt/d/ginobili/code/redis_proxy/include/redis_proxy/endpoint.h /mnt/d/ginobili/code/redis_proxy/include/redis_proxy/fd_notifier.h 2>&1`

Expected: "No such file or directory" for both

- [ ] **Step 3: Verify util directory structure**

Run: `ls -la /mnt/d/ginobili/code/redis_proxy/util/`

Expected output:
```
endpoint.h
endpoint.cpp
socket_utils.h
socket_utils.cpp
fd_notifier.h
fd_notifier.cpp
```

- [ ] **Step 4: Run full test suite**

Run: `cd /mnt/d/ginobili/code/redis_proxy/build && cmake .. && make -j4 && ctest --output-on-failure`

Expected: All tests pass including new util tests

- [ ] **Step 5: Run integration tests if available**

Run: `cd /mnt/d/ginobili/code/redis_proxy && make test`

Expected: All integration tests pass

- [ ] **Step 6: Verify build artifacts**

Run: `ls /mnt/d/ginobili/code/redis_proxy/build/libredis_proxy_util.a`

Expected: Util library exists

- [ ] **Step 7: Final commit if any cleanup needed**

```bash
git status
# If there are any remaining changes, review and commit them
```

---

## Success Criteria

✅ All files moved to util/ directory
✅ Socket utility functions extracted to dedicated module
✅ All include paths updated
✅ Build system updated with util library
✅ All existing tests pass
✅ New unit tests created for util components
✅ No references to old include paths remain
✅ Code is more modular and reusable

## Notes

- The util library is now independent and can be easily copied to other projects
- Socket utilities are separated from Endpoint class for better modularity
- FdNotifier's internal SetNonBlocking remains separate from the public API
- All changes are backward compatible at the binary level (no ABI changes)
