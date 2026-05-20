#include "redis_proxy/backend_channel.h"

#include "co_routine.h"

#include <utility>

namespace redis_proxy {

RequestBatch::RequestBatch(ReplySink* owner, BufferChain bytes,
                           uint32_t command_count, uint64_t sequence_base)
    : owner_(owner),
      bytes_(std::move(bytes)),
      command_count_(command_count),
      sequence_base_(sequence_base) {}

ReplySink* RequestBatch::owner() const { return owner_; }

const BufferChain& RequestBatch::bytes() const { return bytes_; }

uint32_t RequestBatch::commandCount() const { return command_count_; }

uint64_t RequestBatch::sequenceBase() const { return sequence_base_; }

PendingBatch::PendingBatch(ReplySink* owner, uint32_t remaining_replies,
                           uint64_t sequence_base)
    : owner_(owner),
      remaining_replies_(remaining_replies),
      sequence_base_(sequence_base) {}

ReplySink* PendingBatch::owner() const { return owner_; }

uint32_t PendingBatch::remainingReplies() const { return remaining_replies_; }

uint64_t PendingBatch::sequenceBase() const { return sequence_base_; }

void PendingBatch::consumeOne() {
  if (remaining_replies_ > 0) {
    --remaining_replies_;
  }
}

void PendingBatch::detachOwner() { owner_ = nullptr; }

BackendChannel::BackendChannel(int id, Endpoint endpoint, BlockPool* pool)
    : id_(id), endpoint_(std::move(endpoint)), pool_(pool), redis_in_(pool) {}

bool BackendChannel::submit(ReplySink* owner, BufferChain bytes,
                            uint32_t command_count, uint64_t sequence_base) {
  write_queue_.emplace_back(owner, std::move(bytes), command_count,
                            sequence_base);
  pending_queue_.emplace_back(owner, command_count, sequence_base);
  queued_commands_ += command_count;
  writer_signal_.notify();
  return true;
}

void BackendChannel::detachOwner(ReplySink* owner) { detachOwnerInternal(owner); }

void BackendChannel::dispatchReplyForTest(BufferChain reply) {
  dispatchReply(std::move(reply));
}

std::size_t BackendChannel::pendingBatchCountForTest() const {
  return pending_queue_.size();
}

bool BackendChannel::writerSignalPendingForTest() const {
  return writer_signal_.pendingForTest();
}

bool BackendChannel::healthSignalPendingForTest() const {
  return health_signal_.pendingForTest();
}

std::size_t BackendChannel::queuedCommandCount() const {
  return queued_commands_;
}

bool BackendChannel::isHealthy() const { return healthy_; }

Status BackendChannel::connectOnce() {
  int fd = CreateTcpClientSocket();
  if (fd < 0) {
    return Status::IoError("socket failed");
  }
  socket_.reset(fd);
  Status st = socket_.connectTo(endpoint_, config_.connect_timeout_ms);
  if (!st.ok()) {
    socket_.close();
    healthy_ = false;
    return st;
  }
  healthy_ = true;
  health_signal_.notify();
  return Status::Ok();
}

void BackendChannel::dispatchReply(BufferChain reply) {
  if (pending_queue_.empty()) {
    return;
  }
  PendingBatch& pending = pending_queue_.front();
  if (pending.owner()) {
    pending.owner()->onBackendReply(std::move(reply));
  }
  pending.consumeOne();
  if (pending.remainingReplies() == 0) {
    pending_queue_.pop_front();
  }
  if (queued_commands_ > 0) {
    --queued_commands_;
  }
}

void BackendChannel::detachOwnerInternal(ReplySink* owner) {
  std::unordered_set<uint64_t> unwritten_sequences;
  for (const RequestBatch& batch : write_queue_) {
    if (batch.owner() == owner) {
      unwritten_sequences.insert(batch.sequenceBase());
      if (queued_commands_ >= batch.commandCount()) {
        queued_commands_ -= batch.commandCount();
      }
    }
  }
  for (auto it = write_queue_.begin(); it != write_queue_.end();) {
    if (it->owner() == owner) {
      it = write_queue_.erase(it);
    } else {
      ++it;
    }
  }
  for (auto it = pending_queue_.begin(); it != pending_queue_.end();) {
    if (it->owner() == owner &&
        unwritten_sequences.count(it->sequenceBase()) != 0) {
      it = pending_queue_.erase(it);
    } else {
      if (it->owner() == owner) {
        it->detachOwner();
      }
      ++it;
    }
  }
}

void BackendChannel::failAll(const Status& status) {
  for (auto& pending : pending_queue_) {
    if (pending.owner()) {
      pending.owner()->onBackendFailure(status);
    }
  }
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
    while (!healthy_) {
      reconnecting_ = true;
      Status st = connectOnce();
      reconnecting_ = false;
      if (st.ok()) {
        break;
      }
      co::co_poll(nullptr, 0, reconnect_delay_ms_);
    }
    if (write_queue_.empty()) {
      writer_signal_.wait();
      continue;
    }
    RequestBatch batch = std::move(write_queue_.front());
    write_queue_.pop_front();
    Status st = socket_.writeAll(batch.bytes(), config_.write_timeout_ms);
    if (!st.ok()) {
      failAll(st);
      return;
    }
  }
}

void BackendChannel::readerLoop() {
  co::co_enable_hook_sys();
  while (true) {
    while (!healthy_) {
      health_signal_.wait();
    }
    Status st = socket_.readSome(&redis_in_, config_.read_timeout_ms);
    if (!st.ok()) {
      failAll(st);
      return;
    }
    for (;;) {
      BufferChain reply;
      std::size_t consumed = 0;
      ParseStatus ps = reply_parser_.nextReplyFrame(redis_in_, &reply, &consumed);
      if (ps == ParseStatus::kNeedMore) {
        break;
      }
      if (ps != ParseStatus::kOk) {
        failAll(Status::ProtocolError("bad redis reply"));
        return;
      }
      dispatchReply(std::move(reply));
    }
  }
}

}  // namespace redis_proxy
