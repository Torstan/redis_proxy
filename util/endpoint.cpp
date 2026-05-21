#include "util/endpoint.h"

#include <cstdlib>
#include <cstring>

#include <arpa/inet.h>

namespace redis_proxy {

bool Endpoint::parse(const std::string& text, Endpoint* out) {
  const std::size_t colon = text.rfind(':');
  if (colon == std::string::npos || colon == 0 || colon + 1 >= text.size()) {
    return false;
  }
  char* end = nullptr;
  const long port = std::strtol(text.c_str() + colon + 1, &end, 10);
  if (*end != '\0' || port <= 0 || port > 65535) {
    return false;
  }
  *out = Endpoint(text.substr(0, colon), static_cast<uint16_t>(port));
  return true;
}

bool Endpoint::toSockAddr(sockaddr_in* out) const {
  std::memset(out, 0, sizeof(*out));
  out->sin_family = AF_INET;
  out->sin_port = htons(port_);
  if (host_ == "*" || host_ == "0" || host_ == "0.0.0.0") {
    out->sin_addr.s_addr = htonl(INADDR_ANY);
    return true;
  }
  return inet_pton(AF_INET, host_.c_str(), &out->sin_addr) == 1;
}

std::string Endpoint::toString() const {
  return host_ + ":" + std::to_string(port_);
}

}  // namespace redis_proxy
