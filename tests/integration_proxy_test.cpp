#include "redis/resp.h"
#include "test_common.h"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {

bool HasRedisServer() {
  return std::system("command -v redis-server >/dev/null 2>&1") == 0;
}

int Connect(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  for (int i = 0; i < 50; ++i) {
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
      return fd;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  close(fd);
  return -1;
}

std::string ReadUntil(int fd, const std::vector<std::string>& expected) {
  char buf[4096];
  std::string out;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline) {
    bool complete = true;
    for (const std::string& needle : expected) {
      if (out.find(needle) == std::string::npos) {
        complete = false;
        break;
      }
    }
    if (complete) {
      break;
    }
    pollfd pfd{fd, POLLIN | POLLERR | POLLHUP, 0};
    const int ready = poll(&pfd, 1, 100);
    if (ready <= 0) {
      continue;
    }
    ssize_t n = read(fd, buf, sizeof(buf));
    if (n <= 0) {
      break;
    }
    out.append(buf, static_cast<std::size_t>(n));
  }
  return out;
}

}  // namespace

int main() {
  if (!HasRedisServer()) {
    std::cout << "integration_proxy_test skipped: redis-server not found\n";
    return 0;
  }

  const int redis_port = 6380;
  const int proxy_port = 6390;
  pid_t redis_pid = fork();
  if (redis_pid == 0) {
    execlp("redis-server", "redis-server", "--port", "6380", "--save", "",
           "--appendonly", "no", nullptr);
    _exit(127);
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  pid_t proxy_pid = fork();
  if (proxy_pid == 0) {
    execl("./redis_proxy", "./redis_proxy", "--listen", "127.0.0.1:6390",
          "--redis", "127.0.0.1:6380", "--workers", "1", "--backend-conns",
          "2", nullptr);
    _exit(127);
  }

  int fd = Connect(proxy_port);
  bool ok = fd >= 0;

  std::string req;
  redis::PackCommand({"SET", "it:key", "1"}, &req);
  redis::PackCommand({"INCR", "it:key"}, &req);
  redis::PackCommand({"GET", "it:key"}, &req);
  std::string reply;
  if (ok) {
    ok = write(fd, req.data(), req.size()) == static_cast<ssize_t>(req.size());
  }
  if (ok) {
    reply = ReadUntil(fd, {"+OK\r\n", ":2\r\n", "$1\r\n2\r\n"});
    ok = reply.find("+OK\r\n") != std::string::npos &&
         reply.find(":2\r\n") != std::string::npos &&
         reply.find("$1\r\n2\r\n") != std::string::npos;
  }

  if (fd >= 0) {
    close(fd);
  }
  kill(proxy_pid, SIGTERM);
  kill(redis_pid, SIGTERM);
  waitpid(proxy_pid, nullptr, 0);
  waitpid(redis_pid, nullptr, 0);

  if (!ok) {
    std::cerr << "unexpected integration reply: [" << reply << "]\n";
  }
  RP_REQUIRE(ok);

  std::cout << "integration_proxy_test passed\n";
  return 0;
}
