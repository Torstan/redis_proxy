#include "redis/resp.h"
#include "test_common.h"

#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <string_view>
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

int ReservePort(int* port) {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  RP_REQUIRE(fd >= 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  RP_REQUIRE(bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0);
  socklen_t len = sizeof(addr);
  RP_REQUIRE(getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
  *port = ntohs(addr.sin_port);
  return fd;
}

int Connect(int port) {
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);
  for (int i = 0; i < 50; ++i) {
    int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (fd < 0) {
      return -1;
    }
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0) {
      return fd;
    }
    close(fd);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
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

std::size_t CountOccurrences(std::string_view haystack,
                             std::string_view needle) {
  std::size_t count = 0;
  std::size_t pos = 0;
  while ((pos = haystack.find(needle, pos)) != std::string_view::npos) {
    ++count;
    pos += needle.size();
  }
  return count;
}

std::string ReadUntilOccurrences(int fd, std::string_view needle,
                                 std::size_t expected_count) {
  char buf[4096];
  std::string out;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(3);
  while (std::chrono::steady_clock::now() < deadline &&
         CountOccurrences(out, needle) < expected_count) {
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

  int redis_port = 0;
  int proxy_port = 0;
  int redis_reservation = ReservePort(&redis_port);
  int proxy_reservation = ReservePort(&proxy_port);
  const std::string redis_port_arg = std::to_string(redis_port);
  const std::string redis_endpoint = "127.0.0.1:" + redis_port_arg;
  const std::string proxy_endpoint = "127.0.0.1:" + std::to_string(proxy_port);
  close(redis_reservation);
  pid_t redis_pid = fork();
  RP_REQUIRE(redis_pid >= 0);
  if (redis_pid == 0) {
    close(proxy_reservation);
    execlp("redis-server", "redis-server", "--bind", "127.0.0.1", "--port",
           redis_port_arg.c_str(), "--save", "", "--appendonly", "no", nullptr);
    _exit(127);
  }
  int redis_fd = Connect(redis_port);
  if (redis_fd < 0) {
    kill(redis_pid, SIGTERM);
    waitpid(redis_pid, nullptr, 0);
    close(proxy_reservation);
    RP_REQUIRE(redis_fd >= 0);
  }
  close(redis_fd);

  char config_path[] = "integration_proxy_test.XXXXXX";
  int config_fd = mkstemp(config_path);
  RP_REQUIRE(config_fd >= 0);
  const std::string config = "max_pipeline_commands_per_read=1\n";
  RP_REQUIRE(write(config_fd, config.data(), config.size()) ==
             static_cast<ssize_t>(config.size()));
  close(config_fd);

  close(proxy_reservation);
  pid_t proxy_pid = fork();
  if (proxy_pid < 0) {
    kill(redis_pid, SIGTERM);
    waitpid(redis_pid, nullptr, 0);
    RP_REQUIRE(proxy_pid >= 0);
  }
  if (proxy_pid == 0) {
    execl("./redis_proxy", "./redis_proxy", "--listen", proxy_endpoint.c_str(),
          "--redis", redis_endpoint.c_str(), "--workers", "1",
          "--backend-conns", "2", "--config", config_path, nullptr);
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
  if (ok) {
    std::string ping_req;
    for (int i = 0; i < 128; ++i) {
      redis::PackCommand({"PING"}, &ping_req);
    }
    ok = write(fd, ping_req.data(), ping_req.size()) ==
         static_cast<ssize_t>(ping_req.size());
    if (ok) {
      reply = ReadUntilOccurrences(fd, "+PONG\r\n", 128);
      ok = CountOccurrences(reply, "+PONG\r\n") == 128;
    }
  }

  if (fd >= 0) {
    close(fd);
  }
  kill(proxy_pid, SIGTERM);
  kill(redis_pid, SIGTERM);
  waitpid(proxy_pid, nullptr, 0);
  waitpid(redis_pid, nullptr, 0);
  unlink(config_path);

  if (!ok) {
    std::cerr << "unexpected integration reply: [" << reply << "]\n";
  }
  RP_REQUIRE(ok);

  std::cout << "integration_proxy_test passed\n";
  return 0;
}
