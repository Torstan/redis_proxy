#include "redis_proxy/fd_notifier.h"
#include "test_common.h"

#include <poll.h>

#include <iostream>

int main() {
  redis_proxy::FdNotifier notifier;
  RP_REQUIRE(notifier.valid());
  RP_REQUIRE(notifier.notify().ok());

  pollfd pfd{notifier.readFd(), POLLIN | POLLERR | POLLHUP, 0};
  RP_REQUIRE(poll(&pfd, 1, 1000) == 1);
  RP_REQUIRE((pfd.revents & POLLIN) != 0);
  RP_REQUIRE(notifier.drain().ok());

  pfd.revents = 0;
  RP_REQUIRE(poll(&pfd, 1, 0) == 0);

  std::cout << "fd_notifier_test passed\n";
  return 0;
}
