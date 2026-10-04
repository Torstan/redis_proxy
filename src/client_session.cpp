#include "redis_proxy/client_session.h"

#include "co_routine.h"
#include "redis/resp.h"
#include "task.h"

#include <utility>

namespace redis_proxy {

ClientSession::ClientSession(int fd, const Config& config,
                             CommandRules* rules, BackendPool* backend_pool,
                             BlockPool* pool)
    : socket_(fd), config_(config), rules_(rules), backend_pool_(backend_pool),
      pool_(pool), client_in_(pool) {
  parser_.setLimits(config.max_bulk_bytes, config.max_array_elements, 8,
                    config.max_request_bytes);
}

void ClientSession::start(std::function<void(ClientSession*)> on_finished) {
  on_finished_ = std::move(on_finished);
  active_loops_ = 2;
  co::schedule(co::make_task([this] { readerLoop(); finishLoop(); }));
  co::schedule(co::make_task([this] { writerLoop(); finishLoop(); }));
}

void ClientSession::finishLoop() {
  if (--active_loops_ != 0) return;
  backend_pool_->detachOwner(this);
  socket_.close();
  on_finished_(this);
}

bool ClientSession::submitBatch(BufferChain bytes, uint32_t command_count) {
  if (command_count == 0) return true;
  auto* channel = backend_pool_->submit(this, current_backend_, std::move(bytes),
                                        command_count);
  if (channel == nullptr) {
    abort();
    return false;
  }
  current_backend_ = channel;
  pending_replies_ += command_count;
  return true;
}

void ClientSession::readerLoop() {
  co::co_enable_hook_sys();
  while (state_ == State::kOpen) {
    std::size_t parsed = 0;
    std::size_t consumed = 0;
    std::string error;
    ParseStatus ps = ParseStatus::kNeedMore;
    while (parsed < config_.max_pipeline_commands_per_read) {
      RespFrameInfo info;
      ps = parser_.peekFrame(client_in_, consumed, &info);
      if (ps == ParseStatus::kNeedMore) break;
      if (ps != ParseStatus::kOk) {
        error = ps == ParseStatus::kNoMemory ? "ERR proxy out of memory"
                                            : "ERR proxy protocol error";
        break;
      }
      Status valid = rules_->validate(info.command_name, info.argc);
      if (!valid.ok()) {
        error = valid.message();
        break;
      }
      consumed += info.consumed;
      ++parsed;
    }
    if (parsed > 0 &&
        !submitBatch(client_in_.slicePrefix(consumed), static_cast<uint32_t>(parsed))) {
      break;
    }
    if (!error.empty()) {
      drain(error);
      break;
    }
    if (ps == ParseStatus::kOk && client_in_.readableBytes() != 0) {
      // The parsing budget is a scheduling boundary, not a demand for new I/O.
      co::co_poll(nullptr, 0, 1);
      continue;
    }
    Status st = socket_.readSome(&client_in_, config_.read_timeout_ms);
    if (state_ != State::kOpen) break;
    if (!st.ok()) {
      if (st.code() == StatusCode::kClosed) {
        drain(client_in_.readableBytes() == 0 ? "" : "ERR proxy protocol error");
      } else {
        abort();
      }
      break;
    }
  }
}

void ClientSession::writerLoop() {
  co::co_enable_hook_sys();
  while (state_ != State::kAborted) {
    if (client_out_.empty()) {
      if (state_ == State::kDraining && pending_replies_ == 0) {
        if (final_error_.empty()) break;
        std::string encoded;
        redis::PackError(final_error_, &encoded);
        client_out_.push_back(MakeBufferChain(pool_, encoded));
        final_error_.clear();
      } else {
        output_signal_.wait();
        continue;
      }
    }
    BufferChain reply = std::move(client_out_.front());
    client_out_.pop_front();
    // Output batching is independent of the input parsing budget.
    for (std::size_t count = 1; count < 64 && !client_out_.empty(); ++count) {
      reply.appendChain(std::move(client_out_.front()));
      client_out_.pop_front();
    }
    if (!socket_.writeAll(reply, config_.write_timeout_ms).ok()) {
      abort();
      break;
    }
  }
}

void ClientSession::onBackendReply(BufferChain reply) {
  --pending_replies_;
  if (pending_replies_ == 0) current_backend_ = nullptr;
  client_out_.push_back(std::move(reply));
  output_signal_.notify();
}

void ClientSession::onBackendFailure(const Status&) { abort(); }

void ClientSession::drain(std::string_view error) {
  if (state_ != State::kOpen) return;
  state_ = State::kDraining;
  final_error_.assign(error);
  socket_.shutdownRead();
  output_signal_.notify();
}

void ClientSession::abort() {
  if (state_ == State::kAborted) return;
  state_ = State::kAborted;
  backend_pool_->detachOwner(this);
  current_backend_ = nullptr;
  pending_replies_ = 0;
  client_out_.clear();
  socket_.shutdown();
  output_signal_.notify();
}

}  // namespace redis_proxy
