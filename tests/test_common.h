#pragma once

#include <cstdlib>
#include <iostream>
#include <string>
#include <string_view>
#include <utility>

#define RP_REQUIRE(expr)                                                       \
  do {                                                                         \
    if (!(expr)) {                                                             \
      std::cerr << "require failed: " #expr << " at " << __FILE__ << ":"     \
                << __LINE__ << "\n";                                          \
      std::exit(1);                                                            \
    }                                                                          \
  } while (0)

inline void RequireEqual(std::string_view actual, std::string_view expected) {
  if (actual != expected) {
    std::cerr << "expected [" << expected << "], got [" << actual << "]\n";
    std::exit(1);
  }
}
