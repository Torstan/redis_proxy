#include "redis_proxy/coroutine_signal.h"

namespace redis_proxy {

void CoroutineSignal::notify() {
  pending_ = true;
  cond_.Signal();
}

void CoroutineSignal::wait() {
  if (pending_) {
    pending_ = false;
    return;
  }

  cond_.Timedwait(0);
  pending_ = false;
}

bool CoroutineSignal::pendingForTest() const { return pending_; }

}  // namespace redis_proxy
