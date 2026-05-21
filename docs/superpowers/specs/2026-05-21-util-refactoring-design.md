# Util Directory Refactoring Design

**Date:** 2026-05-21  
**Status:** Approved

## Overview

Reorganize the codebase by moving reusable utility components from `src/` to `util/` directory to improve code reusability across projects. This refactoring adopts a flat directory structure for simplicity.

## Goals

1. Separate generic utility code from business logic
2. Make utility components easier to reuse in other projects
3. Improve code organization and maintainability
4. Consolidate socket-related functions into a dedicated module

## File Organization

### Files to Move to `util/`

The following files will be moved from `src/` and `include/redis_proxy/` to `util/`:

1. **endpoint.h/cpp** - Network endpoint abstraction
   - `Endpoint` class for host:port representation
   - Parsing and sockaddr conversion utilities

2. **socket_utils.h/cpp** - Socket utility functions (new file, extracted from endpoint.cpp)
   - `SetNonBlocking(int fd)` - Configure socket as non-blocking
   - `SetTcpNoDelay(int fd)` - Disable Nagle's algorithm
   - `CreateTcpListenSocket(const Endpoint&, int backlog)` - Create listening socket
   - `CreateTcpClientSocket()` - Create client socket

3. **fd_notifier.h/cpp** - File descriptor notification mechanism
   - `FdNotifier` class for cross-thread/coroutine signaling via pipe

### Files to Keep in `src/`

The following files remain in `src/` as they are tightly coupled with business logic:

- **coroutine_signal.h/cpp** - Coroutine synchronization primitive (depends on libco)
- **buffer.h/cpp** - Buffer management (used extensively in protocol handling)
- All other business logic files (backend_channel, client_session, proxy_server, etc.)

## Directory Structure

### Before
```
src/
├── endpoint.cpp
├── fd_notifier.cpp
├── coroutine_signal.cpp
├── buffer.cpp
└── ... (other files)

include/redis_proxy/
├── endpoint.h
├── fd_notifier.h
├── coroutine_signal.h
├── buffer.h
└── ... (other headers)
```

### After
```
util/
├── endpoint.h
├── endpoint.cpp
├── socket_utils.h
├── socket_utils.cpp
├── fd_notifier.h
└── fd_notifier.cpp

src/
├── coroutine_signal.cpp
├── buffer.cpp
└── ... (other business logic files)

include/redis_proxy/
├── coroutine_signal.h
├── buffer.h
└── ... (other business logic headers)
```

## Code Extraction Details

### socket_utils Module

Extract the following functions from `src/endpoint.cpp` into new `util/socket_utils.cpp`:

```cpp
namespace redis_proxy {

int SetNonBlocking(int fd);
int SetTcpNoDelay(int fd);
int CreateTcpListenSocket(const Endpoint& endpoint, int backlog);
int CreateTcpClientSocket();

}  // namespace redis_proxy
```

**Rationale:** These are pure socket utility functions that:
- Have no dependencies on business logic
- Can be reused across different projects
- Logically belong together as socket configuration/creation utilities

### endpoint Module

After extraction, `util/endpoint.cpp` will contain only:
- `Endpoint` class implementation
- `Endpoint::parse()` - Parse "host:port" strings
- `Endpoint::toSockAddr()` - Convert to sockaddr_in
- `Endpoint::toString()` - Convert to string representation

### fd_notifier Module

Move `src/fd_notifier.cpp` to `util/fd_notifier.cpp` as-is:
- Keep the anonymous namespace `SetNonBlocking` function (internal use only)
- This is independent from the `SetNonBlocking` in socket_utils

**Note:** The two `SetNonBlocking` functions serve different purposes:
- `socket_utils::SetNonBlocking` - Public API for socket configuration
- `fd_notifier::(anonymous)::SetNonBlocking` - Internal helper for pipe setup

## Header File Changes

### Include Path Updates

All files that currently include:
```cpp
#include "redis_proxy/endpoint.h"
#include "redis_proxy/fd_notifier.h"
```

Will be updated to:
```cpp
#include "util/endpoint.h"
#include "util/socket_utils.h"  // If using socket utilities
#include "util/fd_notifier.h"
```

### New Header: util/socket_utils.h

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

## Build System Changes

### CMakeLists.txt Updates

1. Add util directory to the build:
```cmake
# Add util library
file(GLOB REDIS_PROXY_UTIL_SRCS CONFIGURE_DEPENDS util/*.cpp)
add_library(redis_proxy_util ${REDIS_PROXY_UTIL_SRCS})
target_include_directories(redis_proxy_util PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR})
```

2. Link util library to core:
```cmake
target_link_libraries(redis_proxy_core PUBLIC redis_proxy_util colib_static redis_resp pthread)
```

3. Update source file collection to exclude moved files:
```cmake
file(GLOB REDIS_PROXY_CORE_SRCS CONFIGURE_DEPENDS src/*.cpp)
list(FILTER REDIS_PROXY_CORE_SRCS EXCLUDE REGEX ".*/main\\.cpp$")
# endpoint.cpp and fd_notifier.cpp are now in util/
```

## Testing Strategy

### Unit Tests

Create dedicated unit tests for util components in `tests/`:

1. **endpoint_test.cpp** - Test endpoint parsing and conversion
   - Valid/invalid host:port parsing
   - sockaddr_in conversion
   - Edge cases (IPv4 formats, port ranges)

2. **socket_utils_test.cpp** - Test socket utility functions
   - Socket creation and configuration
   - Non-blocking mode verification
   - TCP_NODELAY verification

3. **fd_notifier_test.cpp** - Test notification mechanism
   - notify/drain operations
   - Non-blocking behavior
   - Error handling

### Integration Tests

Existing integration tests should continue to work with minimal changes:
- Update include paths
- Verify socket creation still works correctly
- Ensure fd_notifier works in multi-coroutine scenarios

**Important:** Test code should preferentially call util interfaces directly rather than going through higher-level abstractions. This ensures:
- Util components are properly tested in isolation
- Test coverage for reusable components
- Validation that util APIs are sufficient for common use cases

## Migration Steps

1. **Create util directory structure**
   - Create `util/` directory
   - Set up CMakeLists.txt for util library

2. **Extract socket_utils**
   - Create `util/socket_utils.h` and `util/socket_utils.cpp`
   - Move socket functions from `src/endpoint.cpp`
   - Update `util/endpoint.cpp` to include `socket_utils.h`

3. **Move endpoint module**
   - Move `src/endpoint.cpp` → `util/endpoint.cpp`
   - Move `include/redis_proxy/endpoint.h` → `util/endpoint.h`
   - Update namespace and includes

4. **Move fd_notifier module**
   - Move `src/fd_notifier.cpp` → `util/fd_notifier.cpp`
   - Move `include/redis_proxy/fd_notifier.h` → `util/fd_notifier.h`
   - Keep anonymous namespace SetNonBlocking as-is

5. **Update all include statements**
   - Find all files including moved headers
   - Update to new util/ paths
   - Add socket_utils.h where needed

6. **Update build system**
   - Modify CMakeLists.txt to build util library
   - Link util library to core
   - Verify build succeeds

7. **Run tests**
   - Build and run all existing tests
   - Create new unit tests for util components
   - Verify integration tests pass

8. **Clean up**
   - Remove old header files from include/redis_proxy/
   - Verify no references to old paths remain

## Impact Analysis

### Files Requiring Include Path Updates

Based on current usage, the following files will need include path updates:

- `src/proxy_server.cpp` - Uses CreateTcpListenSocket, SetNonBlocking, SetTcpNoDelay
- `src/backend_channel.cpp` - Uses CreateTcpClientSocket
- `src/co_socket.cpp` - Uses Endpoint
- `src/worker.cpp` - May use FdNotifier
- Test files using these components

### Backward Compatibility

This is a breaking change for:
- Any external code including `redis_proxy/endpoint.h` or `redis_proxy/fd_notifier.h`
- Build scripts expecting these files in src/

**Mitigation:** This is an internal refactoring. No external API changes.

## Success Criteria

1. All files successfully moved to util/
2. Build system updated and compiles without errors
3. All existing tests pass
4. New unit tests created for util components
5. No references to old include paths remain
6. Code is more modular and reusable

## Future Considerations

- Consider moving other generic utilities as they are identified
- May add more socket utilities (e.g., SetReuseAddr, SetKeepAlive)
- Could extract more networking primitives if needed
- Util directory could become a separate library in the future
