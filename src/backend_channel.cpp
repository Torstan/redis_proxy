#include "redis_proxy/backend_channel.h"

#include "co_routine.h"
#include "conn_util/socket_utils.h"
#include "task.h"

#include <unordered_set>
#include <utility>

namespace redis_proxy {

BackendChannel::BackendChannel(Endpoint endpoint, BlockPool* pool)
    : endpoint_(std::move(endpoint)), redis_in_(pool) {}

bool BackendChannel::submit(ReplySink* owner, BufferChain bytes,
                            uint32_t command_count) {
  if (!isHealthy() || command_count == 0) return false;
  write_queue_.push_back({owner, std::move(bytes), command_count});
  queued_commands_ += command_count;
  writer_signal_.notify();
  return true;
}

void BackendChannel::detachOwner(ReplySink* owner) {
  for (auto it = write_queue_.begin(); it != write_queue_.end();) {
    if (it->owner == owner) {
      queued_commands_ -= it->command_count;
      it = write_queue_.erase(it);
    } else {
      ++it;
    }
  }
  // Started batches still occupy slots in Redis's ordered reply stream.
  for (auto& pending : pending_queue_) {
    if (pending.owner == owner) pending.owner = nullptr;
  }
}

std::size_t BackendChannel::queuedCommandCount() const { return queued_commands_; }
bool BackendChannel::isHealthy() const { return state_ == State::kReady; }
bool BackendChannel::isStopped() const { return state_ == State::kStopped; }

Status BackendChannel::connectOnce() {
  int fd = conn_util::CreateTcpClientSocket();
  if (fd < 0) return Status::IoError("socket failed");
  socket_.reset(fd);
  return socket_.connectTo(endpoint_, config_.connect_timeout_ms);
}

void BackendChannel::dispatchReply(BufferChain reply) {
  if (pending_queue_.empty()) {
    failConnection(Status::ProtocolError("unexpected redis reply"));
    return;
  }
  auto& pending = pending_queue_.front();
  ReplySink* owner = pending.owner;
  if (--pending.remaining_replies == 0) pending_queue_.pop_front();
  --queued_commands_;
  // The callback may detach its owner; no queue references survive it.
  if (owner) owner->onBackendReply(std::move(reply));
}

void BackendChannel::failConnection(const Status& status) {
  if (state_ != State::kReady) return;
  state_ = State::kClosing;
  socket_.shutdown();
  writer_signal_.notify();
  std::unordered_set<ReplySink*> owners;
  for (const auto& batch : write_queue_) if (batch.owner) owners.insert(batch.owner);
  for (const auto& batch : pending_queue_) if (batch.owner) owners.insert(batch.owner);
  write_queue_.clear();
  pending_queue_.clear();
  queued_commands_ = 0;
  for (auto* owner : owners) owner->onBackendFailure(status);
}

void BackendChannel::start(const Config& config) {
  if (!isStopped()) return;
  config_ = config;
  reply_parser_.setLimits(config.max_bulk_bytes, config.max_array_elements, 8);
  stop_requested_ = false;
  state_ = State::kConnecting;
  co::schedule(co::make_task([this] { run(); }));
}

void BackendChannel::stop() {
  stop_requested_ = true;
  failConnection(Status::Closed("backend stopped"));
  socket_.shutdown();
  writer_signal_.notify();
}

void BackendChannel::run() {
  co::co_enable_hook_sys();
  while (!stop_requested_) {
    state_ = State::kConnecting;
    Status st = connectOnce();
    if (st.ok() && !stop_requested_) {
      state_ = State::kReady;
      reader_done_ = false;
      co::schedule(co::make_task([this] {
        readerLoop();
        reader_done_ = true;
        writer_signal_.notify();
      }));
      writerLoop();
      while (!reader_done_) writer_signal_.wait();
    }
    // Shutdown wakes I/O; close/reuse only after both directions have exited.
    socket_.close();
    redis_in_.clear();
    if (!stop_requested_) co::co_poll(nullptr, 0, 100);
  }
  state_ = State::kStopped;
}

void BackendChannel::writerLoop() {
  while (isHealthy()) {
    if (write_queue_.empty()) {
      writer_signal_.wait();
      continue;
    }
    auto batch = std::move(write_queue_.front());
    write_queue_.pop_front();
    // Replies can arrive while writeAll is suspended on a partial write.
    pending_queue_.push_back({batch.owner, batch.command_count});
    Status st = socket_.writeAll(batch.bytes, config_.write_timeout_ms);
    if (!st.ok()) failConnection(st);
  }
}

void BackendChannel::readerLoop() {
  co::co_enable_hook_sys();
  while (isHealthy()) {
    Status st = socket_.readSome(&redis_in_, config_.read_timeout_ms);
    if (!isHealthy()) break;
    if (!st.ok()) {
      failConnection(st);
      break;
    }
    while (isHealthy()) {
      BufferChain reply;
      std::size_t consumed = 0;
      auto ps = reply_parser_.nextReplyFrame(redis_in_, &reply, &consumed);
      if (ps == ParseStatus::kNeedMore) break;
      if (ps != ParseStatus::kOk) {
        failConnection(Status::ProtocolError("bad redis reply"));
        break;
      }
      dispatchReply(std::move(reply));
    }
  }
}

}  // namespace redis_proxy
