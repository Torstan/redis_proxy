#include "redis_proxy/fd_notifier.h"

#include <poll.h>
#include <stdexcept>

namespace redis_proxy {

void test_fd_notifier_creation() {
  FdNotifier notifier;
  if (!notifier.valid()) {
    throw std::runtime_error("FdNotifier should be valid after construction");
  }
  if (notifier.readFd() < 0) {
    throw std::runtime_error("readFd should be valid");
  }
}

void test_fd_notifier_notify_drain() {
  FdNotifier notifier;
  if (!notifier.valid()) {
    throw std::runtime_error("FdNotifier not valid");
  }

  Status status = notifier.notify();
  if (!status.ok()) {
    throw std::runtime_error("notify failed");
  }

  // Check that readFd is readable
  pollfd pfd;
  pfd.fd = notifier.readFd();
  pfd.events = POLLIN;
  int result = poll(&pfd, 1, 100);
  if (result <= 0 || !(pfd.revents & POLLIN)) {
    throw std::runtime_error("readFd should be readable after notify");
  }

  status = notifier.drain();
  if (!status.ok()) {
    throw std::runtime_error("drain failed");
  }

  // Check that readFd is no longer readable
  result = poll(&pfd, 1, 100);
  if (result > 0 && (pfd.revents & POLLIN)) {
    throw std::runtime_error("readFd should not be readable after drain");
  }
}

}  // namespace redis_proxy

int main() {
  try {
    redis_proxy::test_fd_notifier_creation();
    redis_proxy::test_fd_notifier_notify_drain();
  } catch (const std::exception& e) {
    return 1;
  }
  return 0;
}
