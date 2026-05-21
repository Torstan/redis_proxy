#include "util/socket_utils.h"

#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdexcept>

namespace redis_proxy {

void test_set_non_blocking() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket creation failed");
  }

  int result = SetNonBlocking(fd);
  if (result != 0) {
    close(fd);
    throw std::runtime_error("SetNonBlocking returned error");
  }

  int flags = fcntl(fd, F_GETFL, 0);
  close(fd);

  if ((flags & O_NONBLOCK) == 0) {
    throw std::runtime_error("Socket is not non-blocking");
  }
}

void test_set_tcp_no_delay() {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    throw std::runtime_error("socket creation failed");
  }

  int result = SetTcpNoDelay(fd);
  close(fd);

  if (result != 0) {
    throw std::runtime_error("SetTcpNoDelay failed");
  }
}

}  // namespace redis_proxy

int main() {
  try {
    redis_proxy::test_set_non_blocking();
    redis_proxy::test_set_tcp_no_delay();
  } catch (const std::exception& e) {
    return 1;
  }
  return 0;
}
