#include "redis_proxy/buffer.h"
#include "test_common.h"

#include <cstring>
#include <iostream>

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
