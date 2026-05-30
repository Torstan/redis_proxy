#pragma once

#include "conn_util/endpoint.h"
#include "redis_proxy/buffer.h"
#include "redis_proxy/status.h"

namespace redis_proxy {

using Endpoint = conn_util::Endpoint;

class CoSocket {
public:
  explicit CoSocket(int fd = -1);
  ~CoSocket();

  CoSocket(const CoSocket&) = delete;
  CoSocket& operator=(const CoSocket&) = delete;
  CoSocket(CoSocket&& other) noexcept;
  CoSocket& operator=(CoSocket&& other) noexcept;

  int fd() const;
  int release();
  void reset(int fd);
  void close();

  Status connectTo(const Endpoint& endpoint, int timeout_ms);
  Status readSome(IoBuffer* out, int timeout_ms);
  Status writeAll(const BufferChain& chain, int timeout_ms);

private:
  int fd_ = -1;
};

}  // namespace redis_proxy
