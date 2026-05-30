#include "conn_util/fd_notifier.h"
#include "conn_util/status.h"

#include <poll.h>
#include <stdexcept>

namespace {

void test_fd_notifier_creation() {
  conn_util::FdNotifier notifier;
  if (!notifier.valid()) {
    throw std::runtime_error("FdNotifier should be valid after construction");
  }
  if (notifier.readFd() < 0) {
    throw std::runtime_error("readFd should be valid");
  }
}

void test_fd_notifier_notify_drain() {
  conn_util::FdNotifier notifier;
  if (!notifier.valid()) {
    throw std::runtime_error("FdNotifier not valid");
  }

  conn_util::Status status = notifier.notify();
  if (!status.ok()) {
    throw std::runtime_error("notify failed");
  }

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

  result = poll(&pfd, 1, 100);
  if (result > 0 && (pfd.revents & POLLIN)) {
    throw std::runtime_error("readFd should not be readable after drain");
  }
}

}  // namespace

int main() {
  try {
    test_fd_notifier_creation();
    test_fd_notifier_notify_drain();
  } catch (const std::exception& e) {
    return 1;
  }
  return 0;
}
