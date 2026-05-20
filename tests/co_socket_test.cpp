#include "redis_proxy/buffer.h"
#include "redis_proxy/co_socket.h"
#include "test_common.h"

#include "co_routine.h"

#include <cstring>
#include <iostream>
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
    while (input.readableBytes() < 11) {
      RP_REQUIRE(sock.readSome(&input, 1000).ok());
    }
    RequireEqual(input.contiguousPrefixForTest(11), "hello world");
    sock.close();
    done = true;
  });
  co::co_resume(writer);
  co::co_resume(reader);
  co::co_eventloop(
      [](void* arg) -> int { return *static_cast<bool*>(arg) ? -1 : 0; },
      &done);

  std::cout << "co_socket_test passed\n";
  return 0;
}
