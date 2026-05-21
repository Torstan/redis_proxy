#include "util/socket_utils.h"
#include "util/endpoint.h"

#include <fcntl.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace redis_proxy {

int SetNonBlocking(int fd) {
  const int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return -1;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK | O_NDELAY);
}

int SetTcpNoDelay(int fd) {
  int flag = 1;
  return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));
}

int CreateTcpListenSocket(const Endpoint& endpoint, int backlog) {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd < 0) {
    return -1;
  }
  int reuse = 1;
  setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  sockaddr_in addr;
  if (!endpoint.toSockAddr(&addr) ||
      bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
      listen(fd, backlog) != 0 || SetNonBlocking(fd) != 0) {
    close(fd);
    return -1;
  }
  return fd;
}

int CreateTcpClientSocket() {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd >= 0 && (SetNonBlocking(fd) != 0 || SetTcpNoDelay(fd) != 0)) {
    close(fd);
    return -1;
  }
  return fd;
}

}  // namespace redis_proxy
