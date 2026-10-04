#pragma once

#include "test_common.h"
#include "co_routine.h"
#include "task.h"
#include "thread_worker.h"
#include "conn_util/socket_utils.h"
#include "redis_proxy/co_socket.h"

#include <chrono>
#include <functional>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

inline void WaitUntil(const std::function<bool()>& ready) {
  const auto end = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!ready()) {
    RP_REQUIRE(std::chrono::steady_clock::now() < end);
    co::co_poll(nullptr, 0, 1);
  }
}

inline void RunCoroutines(const std::function<void()>& test) {
  struct Context {
    bool done = false;
    std::chrono::steady_clock::time_point end =
        std::chrono::steady_clock::now() + std::chrono::seconds(15);
  } context;
  co::schedule(co::make_task([&]() {
    co::co_enable_hook_sys();
    test();
    context.done = true;
  }));
  co::ThreadWorker loop(0);
  loop.run_loop(false);
  if (!context.done) {
    co::co_eventloop([](void* arg) {
      auto& ctx = *static_cast<Context*>(arg);
      co::ThreadWorker(0).run_loop(false);
      RP_REQUIRE(std::chrono::steady_clock::now() < ctx.end);
      return ctx.done ? -1 : 0;
    }, &context);
  }
  loop.run_loop(false);
}

inline int ListenForTest(redis_proxy::Endpoint* endpoint) {
  const int fd = conn_util::CreateTcpListenSocket(
      redis_proxy::Endpoint("127.0.0.1", 0), 8);
  RP_REQUIRE(fd >= 0);
  sockaddr_in addr{};
  socklen_t len = sizeof(addr);
  RP_REQUIRE(getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len) == 0);
  *endpoint = redis_proxy::Endpoint("127.0.0.1", ntohs(addr.sin_port));
  return fd;
}

inline int AcceptForTest(int listener) {
  const int fd = co::co_accept(listener, nullptr, nullptr);
  RP_REQUIRE(fd >= 0);
  RP_REQUIRE(conn_util::SetNonBlocking(fd) == 0);
  conn_util::SetTcpNoDelay(fd);
  return fd;
}

inline std::string ReadBytes(redis_proxy::CoSocket* socket,
                              redis_proxy::IoBuffer* input, std::size_t size) {
  while (input->readableBytes() < size) {
    RP_REQUIRE(socket->readSome(input, 2000).ok());
  }
  return input->slicePrefix(size).toStringForTest();
}
