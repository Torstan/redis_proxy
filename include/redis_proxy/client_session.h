#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <string>
#include <string_view>

#include "redis_proxy/backend_pool.h"
#include "redis_proxy/co_socket.h"
#include "redis_proxy/command_rules.h"
#include "redis_proxy/coroutine_signal.h"
#include "redis_proxy/resp_parser.h"

namespace redis_proxy {

class ClientSession : public ReplySink {
public:
  ClientSession(int fd, const Config& config, CommandRules* rules,
                BackendPool* backend_pool, BlockPool* pool);

  // Completion runs on the worker after both I/O loops exit. The callback
  // must defer destruction until control returns to the scheduler.
  void start(std::function<void(ClientSession*)> on_finished);

  void onBackendReply(BufferChain reply) override;
  void onBackendFailure(const Status& status) override;

private:
  enum class State { kOpen, kDraining, kAborted };
  CoSocket socket_;
  Config config_;
  CommandRules* rules_;
  BackendPool* backend_pool_;
  BlockPool* pool_;
  RespParser parser_;
  IoBuffer client_in_;
  std::deque<BufferChain> client_out_;
  CoroutineSignal output_signal_;
  BackendChannel* current_backend_ = nullptr;
  std::size_t pending_replies_ = 0;
  State state_ = State::kOpen;
  std::string final_error_;
  int active_loops_ = 0;
  std::function<void(ClientSession*)> on_finished_;

  void readerLoop();
  void writerLoop();
  void drain(std::string_view error = {});
  void abort();
  void finishLoop();
  bool submitBatch(BufferChain bytes, uint32_t command_count);
};

}  // namespace redis_proxy
