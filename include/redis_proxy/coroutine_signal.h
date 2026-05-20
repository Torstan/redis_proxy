#pragma once

#include "co_cond.h"

namespace redis_proxy {

class CoroutineSignal {
public:
  CoroutineSignal() = default;
  CoroutineSignal(const CoroutineSignal&) = delete;
  CoroutineSignal& operator=(const CoroutineSignal&) = delete;

  void notify();
  void wait();
  bool pendingForTest() const;

private:
  co::CoCond cond_;
  bool pending_ = false;
};

}  // namespace redis_proxy
