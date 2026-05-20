#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <unordered_set>

#include "redis_proxy/buffer.h"
#include "redis_proxy/co_socket.h"
#include "redis_proxy/config.h"
#include "redis_proxy/coroutine_signal.h"
#include "redis_proxy/resp_parser.h"
#include "redis_proxy/status.h"

namespace redis_proxy {

class ReplySink {
public:
  virtual ~ReplySink() = default;
  virtual void onBackendReply(BufferChain reply) = 0;
  virtual void onBackendFailure(const Status& status) = 0;
};

class RequestBatch {
public:
  RequestBatch(ReplySink* owner, BufferChain bytes, uint32_t command_count,
               uint64_t sequence_base);

  ReplySink* owner() const;
  const BufferChain& bytes() const;
  uint32_t commandCount() const;
  uint64_t sequenceBase() const;

private:
  ReplySink* owner_;
  BufferChain bytes_;
  uint32_t command_count_;
  uint64_t sequence_base_;
};

class PendingBatch {
public:
  PendingBatch(ReplySink* owner, uint32_t remaining_replies,
               uint64_t sequence_base);

  ReplySink* owner() const;
  uint32_t remainingReplies() const;
  uint64_t sequenceBase() const;
  void consumeOne();
  void detachOwner();

private:
  ReplySink* owner_;
  uint32_t remaining_replies_;
  uint64_t sequence_base_;
};

class BackendChannel {
public:
  BackendChannel(int id, Endpoint endpoint, BlockPool* pool);

  bool submit(ReplySink* owner, BufferChain bytes, uint32_t command_count,
              uint64_t sequence_base);
  void detachOwner(ReplySink* owner);
  void start(const Config& config);
  std::size_t queuedCommandCount() const;
  bool isHealthy() const;

  void dispatchReplyForTest(BufferChain reply);
  std::size_t pendingBatchCountForTest() const;
  bool writerSignalPendingForTest() const;
  bool healthSignalPendingForTest() const;

private:
  int id_;
  Endpoint endpoint_;
  BlockPool* pool_;
  CoSocket socket_;
  RespParser reply_parser_;
  IoBuffer redis_in_;
  std::deque<RequestBatch> write_queue_;
  std::deque<PendingBatch> pending_queue_;
  CoroutineSignal writer_signal_;
  CoroutineSignal health_signal_;
  bool healthy_ = false;
  bool reconnecting_ = false;
  int reconnect_delay_ms_ = 100;
  std::size_t queued_commands_ = 0;
  Config config_;

  Status connectOnce();
  void writerLoop();
  void readerLoop();
  void dispatchReply(BufferChain reply);
  void detachOwnerInternal(ReplySink* owner);
  void failAll(const Status& status);
};

}  // namespace redis_proxy
