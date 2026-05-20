#include "redis_proxy/client_session.h"

#include "co_routine.h"
#include "redis/resp.h"

#include <string>
#include <utility>

namespace redis_proxy {

ClientSession::ClientSession(int id, int fd, const Config& config,
                             CommandRules* rules, BackendPool* backend_pool,
                             BlockPool* pool)
    : id_(id),
      socket_(fd),
      config_(config),
      rules_(rules),
      backend_pool_(backend_pool),
      pool_(pool),
      client_in_(pool) {
  parser_.setLimits(config_.max_bulk_bytes, config_.max_array_elements, 8);
}

void ClientSession::start() {
  co::Coroutine* reader = co::co_create([this]() { readerLoop(); });
  co::Coroutine* writer = co::co_create([this]() { writerLoop(); });
  co::co_resume(reader);
  co::co_resume(writer);
}

std::size_t ClientSession::pendingRepliesForTest() const {
  return pending_replies_;
}

bool ClientSession::outputSignalPendingForTest() const {
  return output_signal_.pendingForTest();
}

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

void ClientSession::readerLoop() {
  co::co_enable_hook_sys();
  while (!closed_) {
    Status st = socket_.readSome(&client_in_, config_.read_timeout_ms);
    if (!st.ok()) {
      closed_ = true;
      output_signal_.notify();
      break;
    }
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
  }
  if (current_backend_ != nullptr) {
    current_backend_->detachOwner(this);
  }
}

void ClientSession::writerLoop() {
  co::co_enable_hook_sys();
  while (!closed_ || !client_out_.empty()) {
    if (client_out_.empty()) {
      output_signal_.wait();
      continue;
    }
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
    if (!st.ok()) {
      closed_ = true;
      break;
    }
  }
  socket_.close();
}

void ClientSession::onBackendReply(BufferChain reply) {
  if (pending_replies_ > 0) {
    --pending_replies_;
  }
  if (pending_replies_ == 0) {
    current_backend_ = nullptr;
  }
  if (!closed_) {
    client_out_.push_back(std::move(reply));
    output_signal_.notify();
  }
}

void ClientSession::onBackendFailure(const Status& status) {
  enqueueErrorAndClose(status.message().empty() ? "ERR proxy backend failure"
                                                : status.message());
}

void ClientSession::enqueueErrorAndClose(std::string_view error) {
  std::string encoded;
  redis::PackError(error, &encoded);
  client_out_.push_back(MakeBufferChain(pool_, encoded));
  closed_ = true;
  output_signal_.notify();
}

}  // namespace redis_proxy
