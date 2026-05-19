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

void BufferBlock::consume(std::size_t n) {
  begin_ = std::min(end_, begin_ + n);
}

void BufferBlock::retain() { ++refcount_; }

void BufferBlock::release() {
  if (--refcount_ == 0) {
    pool_->recycle(this);
  }
}

void BufferBlock::reset() {
  begin_ = 0;
  end_ = 0;
  refcount_ = 1;
}

BufferSlice::BufferSlice(BufferBlock* block, std::size_t offset,
                         std::size_t length, bool add_ref)
    : block_(block), offset_(offset), length_(length) {
  if (block_ && add_ref) {
    block_->retain();
  }
}

BufferSlice BufferSlice::retain(BufferBlock* block, std::size_t offset,
                                std::size_t length) {
  return BufferSlice(block, offset, length, true);
}

BufferSlice::BufferSlice(const BufferSlice& other)
    : BufferSlice(other.block_, other.offset_, other.length_, true) {}

BufferSlice& BufferSlice::operator=(const BufferSlice& other) {
  if (this != &other) {
    reset();
    block_ = other.block_;
    offset_ = other.offset_;
    length_ = other.length_;
    if (block_) {
      block_->retain();
    }
  }
  return *this;
}

BufferSlice::BufferSlice(BufferSlice&& other) noexcept
    : block_(other.block_), offset_(other.offset_), length_(other.length_) {
  other.block_ = nullptr;
  other.length_ = 0;
}

BufferSlice& BufferSlice::operator=(BufferSlice&& other) noexcept {
  if (this != &other) {
    reset();
    block_ = other.block_;
    offset_ = other.offset_;
    length_ = other.length_;
    other.block_ = nullptr;
    other.length_ = 0;
  }
  return *this;
}

BufferSlice::~BufferSlice() { reset(); }

void BufferSlice::reset() {
  if (block_) {
    block_->release();
  }
  block_ = nullptr;
  length_ = 0;
}

const char* BufferSlice::data() const { return block_->data() + offset_; }

std::size_t BufferSlice::size() const { return length_; }

bool BufferSlice::empty() const { return length_ == 0; }

void BufferChain::append(BufferSlice slice) {
  total_ += slice.size();
  slices_.push_back(std::move(slice));
}

std::size_t BufferChain::size() const { return total_; }

bool BufferChain::empty() const { return total_ == 0; }

const std::vector<BufferSlice>& BufferChain::slices() const { return slices_; }

std::string BufferChain::toStringForTest() const {
  std::string out;
  out.reserve(total_);
  for (const auto& slice : slices_) {
    out.append(slice.data(), slice.size());
  }
  return out;
}

BlockPool::BlockPool(std::size_t block_size) : block_size_(block_size) {}

BufferBlock* BlockPool::acquire() {
  if (free_.empty()) {
    return new BufferBlock(this, block_size_);
  }
  std::unique_ptr<BufferBlock> block = std::move(free_.back());
  free_.pop_back();
  block->reset();
  return block.release();
}

void BlockPool::recycle(BufferBlock* block) {
  block->reset();
  free_.emplace_back(block);
}

std::size_t BlockPool::blockSize() const { return block_size_; }

std::size_t BlockPool::freeCountForTest() const { return free_.size(); }

IoBuffer::IoBuffer(BlockPool* pool) : pool_(pool) {}

char* IoBuffer::reserveWritable(std::size_t* writable) {
  if (blocks_.empty() || blocks_.back()->writableBytes() == 0) {
    blocks_.push_back(pool_->acquire());
  }
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
  for (auto* block : blocks_) {
    total += block->size();
  }
  return total;
}

void IoBuffer::consume(std::size_t n) {
  while (n > 0 && !blocks_.empty()) {
    BufferBlock* block = blocks_.front();
    const std::size_t take = std::min(n, block->size());
    block->consume(take);
    n -= take;
    if (block->size() == 0) {
      blocks_.pop_front();
      block->release();
    }
  }
}

bool IoBuffer::ensureContiguousPrefix(std::size_t n) {
  if (n == 0) {
    return true;
  }
  if (blocks_.empty() || readableBytes() < n) {
    return false;
  }
  if (blocks_.front()->size() >= n) {
    return true;
  }
  linearized_.clear();
  linearized_.reserve(n);
  std::size_t remaining = n;
  for (auto* block : blocks_) {
    const std::size_t take = std::min(remaining, block->size());
    linearized_.append(block->data() + block->begin(), take);
    remaining -= take;
    if (remaining == 0) {
      return true;
    }
  }
  return false;
}

std::string_view IoBuffer::contiguousPrefixForTest(std::size_t n) {
  if (!ensureContiguousPrefix(n)) {
    return {};
  }
  if (n == 0) {
    return {};
  }
  if (blocks_.front()->size() >= n) {
    return std::string_view(blocks_.front()->data() + blocks_.front()->begin(),
                            n);
  }
  return std::string_view(linearized_.data(), linearized_.size());
}

BufferChain IoBuffer::slicePrefix(std::size_t n) {
  BufferChain chain;
  std::size_t remaining = n;
  for (auto* block : blocks_) {
    const std::size_t take = std::min(remaining, block->size());
    chain.append(BufferSlice::retain(block, block->begin(), take));
    remaining -= take;
    if (remaining == 0) {
      break;
    }
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
