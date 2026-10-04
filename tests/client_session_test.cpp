#include "redis_proxy/client_session.h"
#include "runtime_test.h"

#include <csignal>
#include <iostream>
#include <memory>

int main() {
  std::signal(SIGPIPE, SIG_IGN);
  RunCoroutines([] {
    using namespace redis_proxy;
    Config cfg;
    cfg.max_pipeline_commands_per_read = 1;
    const std::string ping = "*1\r\n$4\r\nPING\r\n";
    cfg.max_request_bytes = ping.size();
    int listener = ListenForTest(&cfg.redis);
    BlockPool pool(64);
    CommandRules rules = CommandRules::Default();
    BackendPool backends(cfg, &pool);
    backends.start();
    WaitUntil([&] { return backends.channelForTest(0)->isHealthy(); });
    CoSocket backend(AcceptForTest(listener));
    IoBuffer backend_input(&pool);

    auto exercise = [&](bool invalid_suffix, bool half_close) {
      int fds[2];
      RP_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
      conn_util::SetNonBlocking(fds[0]);
      conn_util::SetNonBlocking(fds[1]);
      bool finished = false;
      auto session = std::make_unique<ClientSession>(fds[0], cfg, &rules,
                                                      &backends, &pool);
      session->start([&](ClientSession*) { finished = true; });
      CoSocket client(fds[1]);
      IoBuffer client_input(&pool);
      const std::string request = invalid_suffix ? ping + "*1\r\n$4\r\nAUTH\r\n"
                                                : ping + ping + ping;
      RP_REQUIRE(client.writeAll(MakeBufferChain(&pool, request), 2000).ok());
      if (half_close) ::shutdown(client.fd(), SHUT_WR);
      const auto expected = invalid_suffix ? ping : ping + ping + ping;
      RequireEqual(ReadBytes(&backend, &backend_input, expected.size()), expected);
      const std::string replies = invalid_suffix ? "+PONG\r\n" :
                                                   "+PONG\r\n+PONG\r\n+PONG\r\n";
      RP_REQUIRE(backend.writeAll(MakeBufferChain(&pool, replies), 2000).ok());
      std::string expected_reply = replies;
      if (invalid_suffix) expected_reply += "-ERR proxy rejected command\r\n";
      RequireEqual(ReadBytes(&client, &client_input, expected_reply.size()), expected_reply);
      client.close();
      WaitUntil([&] { return finished; });
      session.reset();
    };
    exercise(false, false);
    exercise(true, false);
    exercise(false, true);
    for (int i = 0; i < 32; ++i) exercise(false, true);

    // A rejected command must leave the connection usable, with replies in
    // request order even when valid commands are pipelined around it.
    int recovery_fds[2];
    RP_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, recovery_fds) == 0);
    conn_util::SetNonBlocking(recovery_fds[0]);
    conn_util::SetNonBlocking(recovery_fds[1]);
    Config recovery_config = cfg;
    recovery_config.max_request_bytes = 128;
    bool recovery_finished = false;
    auto recovery = std::make_unique<ClientSession>(
        recovery_fds[0], recovery_config, &rules, &backends, &pool);
    recovery->start([&](ClientSession*) { recovery_finished = true; });
    CoSocket recovery_client(recovery_fds[1]);
    IoBuffer recovery_input(&pool);
    const std::string rejected =
        "*2\r\n$10\r\nSMISMEMBER\r\n$1\r\ns\r\n";
    RP_REQUIRE(recovery_client.writeAll(
        MakeBufferChain(&pool, ping + rejected + ping), 2000).ok());
    RequireEqual(ReadBytes(&backend, &backend_input, ping.size()), ping);
    RP_REQUIRE(backend.writeAll(MakeBufferChain(&pool, "+PONG\r\n"), 2000).ok());
    RequireEqual(ReadBytes(&backend, &backend_input, ping.size()), ping);
    RP_REQUIRE(backend.writeAll(MakeBufferChain(&pool, "+PONG\r\n"), 2000).ok());
    const std::string expected_replies =
        "+PONG\r\n-ERR wrong number of arguments\r\n+PONG\r\n";
    RequireEqual(ReadBytes(&recovery_client, &recovery_input,
                           expected_replies.size()), expected_replies);
    recovery_client.close();
    WaitUntil([&] { return recovery_finished; });
    recovery.reset();

    // An oversized single request is rejected without sending it downstream.
    int fds[2];
    RP_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);
    conn_util::SetNonBlocking(fds[0]);
    conn_util::SetNonBlocking(fds[1]);
    bool finished = false;
    ClientSession limited(fds[0], cfg, &rules, &backends, &pool);
    limited.start([&](ClientSession*) { finished = true; });
    CoSocket client(fds[1]);
    IoBuffer input(&pool);
    RP_REQUIRE(client.writeAll(MakeBufferChain(&pool,
        "*2\r\n$3\r\nGET\r\n$1\r\nx\r\n"), 2000).ok());
    const std::string error = "-ERR proxy protocol error\r\n";
    RequireEqual(ReadBytes(&client, &input, error.size()), error);
    WaitUntil([&] { return finished; });
    RP_REQUIRE(backends.channelForTest(0)->queuedCommandCount() == 0);

    // Closing a client during a blocked output write must wake its reader too.
    int slow_fds[2];
    RP_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, slow_fds) == 0);
    conn_util::SetNonBlocking(slow_fds[0]);
    conn_util::SetNonBlocking(slow_fds[1]);
    int small_buffer = 1024;
    setsockopt(slow_fds[0], SOL_SOCKET, SO_SNDBUF, &small_buffer, sizeof(small_buffer));
    bool slow_finished = false;
    ClientSession slow(slow_fds[0], cfg, &rules, &backends, &pool);
    slow.start([&](ClientSession*) { slow_finished = true; });
    CoSocket slow_client(slow_fds[1]);
    RP_REQUIRE(slow_client.writeAll(MakeBufferChain(&pool, ping), 2000).ok());
    ReadBytes(&backend, &backend_input, ping.size());
    const std::string large_reply = "$1048576\r\n" + std::string(1048576, 'x') + "\r\n";
    RP_REQUIRE(backend.writeAll(MakeBufferChain(&pool, large_reply), 2000).ok());
    WaitUntil([&] { return backends.channelForTest(0)->queuedCommandCount() == 0; });
    slow_client.close();
    WaitUntil([&] { return slow_finished; });

    int failed_fds[2];
    RP_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, failed_fds) == 0);
    conn_util::SetNonBlocking(failed_fds[0]);
    conn_util::SetNonBlocking(failed_fds[1]);
    bool failed_finished = false;
    ClientSession failed(failed_fds[0], cfg, &rules, &backends, &pool);
    failed.start([&](ClientSession*) { failed_finished = true; });
    CoSocket failed_client(failed_fds[1]);
    RP_REQUIRE(failed_client.writeAll(MakeBufferChain(&pool, ping), 2000).ok());
    ReadBytes(&backend, &backend_input, ping.size());
    backend.close();
    WaitUntil([&] { return failed_finished; });
    RP_REQUIRE(!failed_client.readSome(&input, 2000).ok());
    backends.stop();
    WaitUntil([&] { return backends.isStopped(); });
    close(listener);
  });
  std::cout << "client_session_test passed\n";
}
