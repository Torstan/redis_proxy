#include "redis_proxy/coroutine_signal.h"
#include "test_common.h"

#include "co_routine.h"

#include <iostream>

int main() {
  redis_proxy::CoroutineSignal signal;
  int stage = 0;
  bool done = false;

  signal.notify();
  co::Coroutine* waiter = co::co_create([&]() {
    signal.wait();
    RP_REQUIRE(stage == 0);
    stage = 1;
    signal.wait();
    RP_REQUIRE(stage == 2);
    stage = 3;
    done = true;
  });

  co::co_resume(waiter);
  RP_REQUIRE(stage == 1);
  RP_REQUIRE(!done);

  co::Coroutine* notifier = co::co_create([&]() {
    stage = 2;
    signal.notify();
  });
  co::co_resume(notifier);

  co::co_eventloop(
      [](void* arg) -> int { return *static_cast<bool*>(arg) ? -1 : 0; },
      &done);

  RP_REQUIRE(stage == 3);
  co::co_free(waiter);
  co::co_free(notifier);

  std::cout << "coroutine_signal_test passed\n";
  return 0;
}
