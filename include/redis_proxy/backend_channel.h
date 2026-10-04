#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>

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

// All operations and callbacks run on the owning worker thread. The channel
// must outlive its tasks: stop(), then wait for isStopped() before destruction.
class BackendChannel {
public:
  BackendChannel(Endpoint endpoint, BlockPool* pool);

  // Does not yield or call the owner. Rejected bytes are discarded.
  bool submit(ReplySink* owner, BufferChain bytes, uint32_t command_count);
  void detachOwner(ReplySink* owner);
  void start(const Config& config);
  void stop();
  bool isStopped() const;
  std::size_t queuedCommandCount() const;
  bool isHealthy() const;

private:
  struct RequestBatch {
    ReplySink* owner;
    BufferChain bytes;
    uint32_t command_count;
  };
  struct PendingBatch {
    ReplySink* owner;
    uint32_t remaining_replies;
  };
  enum class State { kStopped, kConnecting, kReady, kClosing };

  Endpoint endpoint_;
  CoSocket socket_;
  RespParser reply_parser_;
  IoBuffer redis_in_;
  std::deque<RequestBatch> write_queue_;
  std::deque<PendingBatch> pending_queue_;
  CoroutineSignal writer_signal_;
  State state_ = State::kStopped;
  bool stop_requested_ = false;
  bool reader_done_ = true;
  std::size_t queued_commands_ = 0;
  Config config_;

  Status connectOnce();
  void run();
  void writerLoop();
  void readerLoop();
  void dispatchReply(BufferChain reply);
  void failConnection(const Status& status);
};

}  // namespace redis_proxy
