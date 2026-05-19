#pragma once

#include <cstddef>
#include <cstdint>
#include <deque>
#include <string_view>

#include "redis_proxy/backend_pool.h"
#include "redis_proxy/co_socket.h"
#include "redis_proxy/command_rules.h"
#include "redis_proxy/resp_parser.h"

namespace redis_proxy {

class ClientSession : public ReplySink {
public:
  ClientSession(int id, int fd, const Config& config, CommandRules* rules,
                BackendPool* backend_pool, BlockPool* pool);

  void start();

  void onBackendReply(BufferChain reply) override;
  void onBackendFailure(const Status& status) override;

  std::size_t pendingRepliesForTest() const;

private:
  int id_;
  CoSocket socket_;
  Config config_;
  CommandRules* rules_;
  BackendPool* backend_pool_;
  BlockPool* pool_;
  RespParser parser_;
  IoBuffer client_in_;
  std::deque<BufferChain> client_out_;
  BackendChannel* current_backend_ = nullptr;
  std::size_t pending_replies_ = 0;
  uint64_t next_sequence_ = 1;
  bool closed_ = false;

  void readerLoop();
  void writerLoop();
  void enqueueErrorAndClose(std::string_view error);
  void submitFrame(RespFrame frame);
};

}  // namespace redis_proxy
