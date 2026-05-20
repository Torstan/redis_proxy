#include "redis/resp.h"

#include <chrono>
#include <array>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <poll.h>
#include <cerrno>

namespace {

struct Options {
  int port = 6379;
  int clients = 1;
  int pipeline = 1;
  int seconds = 10;
  std::string command = "PING";
};

Options Parse(int argc, char** argv) {
  Options o;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    auto value = [&]() -> const char* { return i + 1 < argc ? argv[++i] : ""; };
    if (arg == "--port") o.port = std::atoi(value());
    else if (arg == "--clients") o.clients = std::atoi(value());
    else if (arg == "--pipeline") o.pipeline = std::atoi(value());
    else if (arg == "--seconds") o.seconds = std::atoi(value());
    else if (arg == "--command") o.command = value();
  }
  return o;
}

int Connect(int port) {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd < 0) {
    std::cerr << "socket() failed: " << strerror(errno) << "\n";
    return -1;
  }

  // Set non-blocking mode for connect timeout
  int flags = fcntl(fd, F_GETFL, 0);
  fcntl(fd, F_SETFL, flags | O_NONBLOCK);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

  int ret = connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (ret < 0 && errno != EINPROGRESS) {
    std::cerr << "connect() failed immediately: " << strerror(errno) << "\n";
    close(fd);
    return -1;
  }

  // Wait for connection with timeout (1 second)
  if (errno == EINPROGRESS) {
    pollfd pfd{};
    pfd.fd = fd;
    pfd.events = POLLOUT;
    ret = poll(&pfd, 1, 1000);  // 1 second timeout
    if (ret <= 0) {
      std::cerr << "connect() timeout or poll error\n";
      close(fd);
      return -1;
    }

    // Check if connection succeeded
    int error = 0;
    socklen_t len = sizeof(error);
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
      std::cerr << "connect() failed: " << strerror(error ? error : errno) << "\n";
      close(fd);
      return -1;
    }
  }

  // Restore blocking mode
  fcntl(fd, F_SETFL, flags);
  return fd;
}

int CountReplies(std::string* pending, const char* data, std::size_t size) {
  pending->append(data, size);
  std::array<redis::RespValue, 256> scratch{};
  redis::RespLimits limits;
  int replies = 0;
  for (;;) {
    redis::RespResult result =
        redis::UnpackOne(*pending, scratch.data(), scratch.size(), limits);
    if (result.status == redis::RespStatus::kNeedMore) {
      break;
    }
    if (result.status != redis::RespStatus::kOk) {
      pending->clear();
      break;
    }
    pending->erase(0, result.consumed);
    ++replies;
  }
  return replies;
}

std::string MakeRequest(const std::string& command, int pipeline) {
  std::string req;
  for (int i = 0; i < pipeline; ++i) {
    if (command == "PING") redis::PackCommand({"PING"}, &req);
    else if (command == "GET") redis::PackCommand({"GET", "bench:key"}, &req);
    else redis::PackCommand({"SET", "bench:key", "0123456789abcdef"}, &req);
  }
  return req;
}

void Worker(const Options& options, std::chrono::steady_clock::time_point end, unsigned long long* ops) {
  int fd = Connect(options.port);
  if (fd < 0) return;
  std::string req = MakeRequest(options.command, options.pipeline);
  std::string reply;
  reply.resize(1024 * 1024);
  std::string pending;
  unsigned long long local = 0;
  while (std::chrono::steady_clock::now() < end) {
    if (write(fd, req.data(), req.size()) != static_cast<ssize_t>(req.size())) break;
    int replies = 0;
    while (replies < options.pipeline) {
      ssize_t n = read(fd, &reply[0], reply.size());
      if (n <= 0) { close(fd); *ops = local; return; }
      replies += CountReplies(&pending, reply.data(), static_cast<std::size_t>(n));
    }
    local += static_cast<unsigned long long>(options.pipeline);
  }
  close(fd);
  *ops = local;
}

}  // namespace

int main(int argc, char** argv) {
  Options options = Parse(argc, argv);
  auto end = std::chrono::steady_clock::now() + std::chrono::seconds(options.seconds);
  std::vector<std::thread> threads;
  std::vector<unsigned long long> ops(options.clients, 0);
  for (int i = 0; i < options.clients; ++i) {
    threads.emplace_back(Worker, std::cref(options), end, &ops[i]);
  }
  unsigned long long total = 0;
  for (int i = 0; i < options.clients; ++i) {
    threads[i].join();
    total += ops[i];
  }
  const double qps = static_cast<double>(total) / options.seconds;
  std::cout << "{\"clients\":" << options.clients
            << ",\"pipeline\":" << options.pipeline
            << ",\"seconds\":" << options.seconds
            << ",\"command\":\"" << options.command
            << "\",\"qps\":" << qps << "}\n";
  return 0;
}
