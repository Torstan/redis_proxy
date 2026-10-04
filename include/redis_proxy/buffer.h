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
  static BufferSlice retain(BufferBlock* block, std::size_t offset,
                            std::size_t length);
  BufferSlice(const BufferSlice& other);
  BufferSlice& operator=(const BufferSlice& other);
  BufferSlice(BufferSlice&& other) noexcept;
  BufferSlice& operator=(BufferSlice&& other) noexcept;
  ~BufferSlice();

  const char* data() const;
  std::size_t size() const;
  bool empty() const;

private:
  BufferSlice(BufferBlock* block, std::size_t offset, std::size_t length,
              bool add_ref);
  void reset();

  BufferBlock* block_ = nullptr;
  std::size_t offset_ = 0;
  std::size_t length_ = 0;
};

class BufferChain {
public:
  BufferChain() = default;
  BufferChain(const BufferChain&) = default;
  BufferChain& operator=(const BufferChain&) = default;
  BufferChain(BufferChain&& other) noexcept;
  BufferChain& operator=(BufferChain&& other) noexcept;
  void append(BufferSlice slice);
  void appendChain(BufferChain&& chain);
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
  ~IoBuffer();
  IoBuffer(const IoBuffer&) = delete;
  IoBuffer& operator=(const IoBuffer&) = delete;

  char* reserveWritable(std::size_t* writable);
  void commitWrite(std::size_t n);
  void append(std::string_view data);
  std::size_t readableBytes() const;
  void clear();
  void consume(std::size_t n);
  // Valid until the next mutation. Repeated reads/consumes reuse linearization.
  std::string_view readableView();
  BufferChain slicePrefix(std::size_t n);

private:
  BlockPool* pool_;
  std::deque<BufferBlock*> blocks_;
  std::size_t readable_bytes_ = 0;
  std::string linearized_;
  std::size_t linearized_offset_ = 0;
};

BufferChain MakeBufferChain(BlockPool* pool, std::string_view bytes);

}  // namespace redis_proxy
