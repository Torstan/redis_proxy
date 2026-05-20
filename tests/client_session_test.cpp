#include "redis_proxy/backend_pool.h"
#include "redis_proxy/client_session.h"
#include "redis_proxy/command_rules.h"
#include "test_common.h"

#include <iostream>
#include <sys/socket.h>
#include <unistd.h>

int main() {
  int fds[2];
  RP_REQUIRE(socketpair(AF_UNIX, SOCK_STREAM, 0, fds) == 0);

  redis_proxy::Config cfg;
  cfg.backend_conns_per_worker = 1;
  redis_proxy::BlockPool pool(64);
  redis_proxy::CommandRules rules = redis_proxy::CommandRules::Default();
  redis_proxy::BackendPool backend_pool(0, cfg, &pool);
  redis_proxy::ClientSession session(1, fds[0], cfg, &rules, &backend_pool,
                                     &pool);

  session.onBackendReply(redis_proxy::MakeBufferChain(&pool, "+PONG\r\n"));
  RP_REQUIRE(session.outputSignalPendingForTest());

  close(fds[1]);
  std::cout << "client_session_test passed\n";
  return 0;
}
