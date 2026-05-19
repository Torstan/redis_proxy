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

void ClientSession::submitFrame(RespFrame frame) {
  Status valid = rules_->validate(frame.command.name, frame.command.argc);
  if (!valid.ok()) {
    enqueueErrorAndClose(valid.message());
    return;
  }
  const bool has_pending = pending_replies_ > 0;
  pending_replies_ += 1;
  current_backend_ = backend_pool_->submit(
      this, current_backend_, has_pending, std::move(frame.bytes), 1,
      next_sequence_++);
}

void ClientSession::readerLoop() {
  co::co_enable_hook_sys();
  while (!closed_) {
    Status st = socket_.readSome(&client_in_, config_.read_timeout_ms);
    if (!st.ok()) {
      closed_ = true;
      break;
    }
    std::size_t parsed = 0;
    while (parsed < config_.max_pipeline_commands_per_read) {
      RespFrame frame;
      ParseStatus ps = parser_.nextFrame(client_in_, &frame);
      if (ps == ParseStatus::kNeedMore) {
        break;
      }
      if (ps != ParseStatus::kOk) {
        enqueueErrorAndClose("ERR proxy protocol error");
        break;
      }
      submitFrame(std::move(frame));
      ++parsed;
      if (closed_) {
        break;
      }
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
      co::co_poll(nullptr, 0, 1);
      continue;
    }
    BufferChain reply = std::move(client_out_.front());
    client_out_.pop_front();
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
}

}  // namespace redis_proxy
