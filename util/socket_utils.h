#pragma once

namespace redis_proxy {

class Endpoint;

// Configure socket as non-blocking
int SetNonBlocking(int fd);

// Disable Nagle's algorithm (TCP_NODELAY)
int SetTcpNoDelay(int fd);

// Create a TCP listening socket bound to endpoint
int CreateTcpListenSocket(const Endpoint& endpoint, int backlog);

// Create a TCP client socket (non-blocking, TCP_NODELAY enabled)
int CreateTcpClientSocket();

}  // namespace redis_proxy
