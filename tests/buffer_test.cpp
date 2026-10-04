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
  input.append("abc");
  input.append("def");
  RP_REQUIRE(input.readableBytes() == 6);
  RequireEqual(input.readableView(), "abcdef");
  input.consume(4);
  RP_REQUIRE(input.readableBytes() == 2);
  RequireEqual(input.readableView(), "ef");

  redis_proxy::BufferChain left =
      redis_proxy::MakeBufferChain(&pool, "+PONG\r\n");
  redis_proxy::BufferChain right = redis_proxy::MakeBufferChain(&pool, ":1\r\n");
  left.appendChain(std::move(right));
  RequireEqual(left.toStringForTest(), "+PONG\r\n:1\r\n");
  RP_REQUIRE(right.empty());

  {
    redis_proxy::BlockPool recycled(16);
    redis_proxy::BufferChain retained;
    {
      redis_proxy::IoBuffer abandoned(&recycled);
      abandoned.append(std::string(40, 'x'));
      retained = abandoned.slicePrefix(8);
    }
    RP_REQUIRE(recycled.freeCountForTest() == 2);
    RequireEqual(retained.toStringForTest(), std::string(8, 'x'));
    retained = {};
    RP_REQUIRE(recycled.freeCountForTest() == 3);

    auto large = redis_proxy::MakeBufferChain(&recycled, std::string(100, 'y'));
    RequireEqual(large.toStringForTest(), std::string(100, 'y'));
    auto moved = std::move(large);
    RP_REQUIRE(large.empty());
    RP_REQUIRE(moved.size() == 100);
    RP_REQUIRE(redis_proxy::MakeBufferChain(&recycled, "").empty());
  }

  redis_proxy::IoBuffer cached(&pool);
  cached.append("abcdefghijklmnopqrstuvwxyz0123456789");
  RequireEqual(cached.readableView(), "abcdefghijklmnopqrstuvwxyz0123456789");
  cached.consume(5);
  RequireEqual(cached.readableView(), "fghijklmnopqrstuvwxyz0123456789");
  auto prefix = cached.slicePrefix(15);
  RequireEqual(prefix.toStringForTest(), "fghijklmnopqrst");
  cached.append("ABC");
  RequireEqual(cached.readableView(), "uvwxyz0123456789ABC");
  cached.clear();
  RequireEqual(prefix.toStringForTest(), "fghijklmnopqrst");
  RP_REQUIRE(cached.readableView().empty());

  std::cout << "buffer_test passed\n";
  return 0;
}
