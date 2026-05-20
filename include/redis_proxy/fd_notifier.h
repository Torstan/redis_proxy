#pragma once

#include "redis_proxy/status.h"

namespace redis_proxy {

class FdNotifier {
public:
  FdNotifier();
  ~FdNotifier();
  FdNotifier(const FdNotifier&) = delete;
  FdNotifier& operator=(const FdNotifier&) = delete;

  bool valid() const;
  int readFd() const;
  Status notify();
  Status drain();

private:
  int read_fd_ = -1;
  int write_fd_ = -1;
};

}  // namespace redis_proxy
