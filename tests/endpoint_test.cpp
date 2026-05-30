#include "conn_util/endpoint.h"

#include <stdexcept>
#include <string>

namespace {

void test_endpoint_parse_valid() {
  conn_util::Endpoint ep;
  if (!conn_util::Endpoint::parse("127.0.0.1:6379", &ep)) {
    throw std::runtime_error("Failed to parse valid endpoint");
  }
  if (ep.host() != "127.0.0.1" || ep.port() != 6379) {
    throw std::runtime_error("Parsed endpoint has wrong values");
  }
}

void test_endpoint_parse_invalid() {
  conn_util::Endpoint ep;
  if (conn_util::Endpoint::parse("invalid", &ep)) {
    throw std::runtime_error("Should fail to parse invalid endpoint");
  }
  if (conn_util::Endpoint::parse(":6379", &ep)) {
    throw std::runtime_error("Should fail with missing host");
  }
  if (conn_util::Endpoint::parse("host:", &ep)) {
    throw std::runtime_error("Should fail with missing port");
  }
}

void test_endpoint_to_string() {
  conn_util::Endpoint ep("192.168.1.1", 8080);
  if (ep.toString() != "192.168.1.1:8080") {
    throw std::runtime_error("toString() returned wrong value");
  }
}

}  // namespace

int main() {
  try {
    test_endpoint_parse_valid();
    test_endpoint_parse_invalid();
    test_endpoint_to_string();
  } catch (const std::exception& e) {
    return 1;
  }
  return 0;
}
