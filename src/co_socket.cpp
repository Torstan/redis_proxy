#include "redis_proxy/co_socket.h"

#include "co_routine.h"

#include <cerrno>
#include <cstring>

#include <poll.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

namespace redis_proxy {

CoSocket::CoSocket(int fd) : fd_(fd) {}

CoSocket::~CoSocket() { close(); }

CoSocket::CoSocket(CoSocket&& other) noexcept : fd_(other.release()) {}

CoSocket& CoSocket::operator=(CoSocket&& other) noexcept {
  if (this != &other) {
    close();
    fd_ = other.release();
  }
  return *this;
}

int CoSocket::fd() const { return fd_; }

int CoSocket::release() {
  int fd = fd_;
  fd_ = -1;
  return fd;
}

void CoSocket::reset(int fd) {
  close();
  fd_ = fd;
}

void CoSocket::close() {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

Status CoSocket::connectTo(const Endpoint& endpoint, int timeout_ms) {
  sockaddr_in addr;
  if (!endpoint.toSockAddr(&addr)) {
    return Status::InvalidArgument("invalid endpoint");
  }
  int ret = ::connect(fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
  if (ret == 0) {
    return Status::Ok();
  }
  if (errno != EINPROGRESS && errno != EAGAIN) {
    return Status::IoError(std::strerror(errno));
  }
  pollfd pfd{fd_, POLLOUT | POLLERR | POLLHUP, 0};
  ret = co::co_poll(&pfd, 1, timeout_ms);
  if (ret <= 0) {
    return Status::IoError("connect timeout");
  }
  int err = 0;
  socklen_t len = sizeof(err);
  if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len) != 0 || err != 0) {
    return Status::IoError("connect failed");
  }
  return Status::Ok();
}

Status CoSocket::readSome(IoBuffer* out, int timeout_ms) {
  std::size_t writable = 0;
  char* dst = out->reserveWritable(&writable);
  const ssize_t n = ::read(fd_, dst, writable);
  if (n > 0) {
    out->commitWrite(static_cast<std::size_t>(n));
    return Status::Ok();
  }
  if (n == 0) {
    return Status::Closed("peer closed");
  }
  if (errno == EAGAIN || errno == EWOULDBLOCK) {
    pollfd pfd{fd_, POLLIN | POLLERR | POLLHUP, 0};
    const int ret = co::co_poll(&pfd, 1, timeout_ms);
    if (ret <= 0) {
      return Status::IoError("read timeout");
    }
    return readSome(out, timeout_ms);
  }
  return Status::IoError(std::strerror(errno));
}

Status CoSocket::writeAll(const BufferChain& chain, int timeout_ms) {
  const auto& slices = chain.slices();
  std::size_t index = 0;
  std::size_t offset = 0;

  while (index < slices.size()) {
    iovec iov[64];
    int iovcnt = 0;
    for (std::size_t i = index; i < slices.size() && iovcnt < 64; ++i) {
      const BufferSlice& slice = slices[i];
      const std::size_t slice_offset = (i == index) ? offset : 0;
      if (slice.size() <= slice_offset) {
        continue;
      }
      iov[iovcnt].iov_base = const_cast<char*>(slice.data() + slice_offset);
      iov[iovcnt].iov_len = slice.size() - slice_offset;
      ++iovcnt;
    }

    if (iovcnt == 0) {
      return Status::Ok();
    }

    const ssize_t n = ::writev(fd_, iov, iovcnt);
    if (n > 0) {
      std::size_t written = static_cast<std::size_t>(n);
      while (written > 0 && index < slices.size()) {
        const std::size_t available = slices[index].size() - offset;
        if (written < available) {
          offset += written;
          written = 0;
        } else {
          written -= available;
          ++index;
          offset = 0;
        }
      }
      continue;
    }

    if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
      pollfd pfd{fd_, POLLOUT | POLLERR | POLLHUP, 0};
      const int ret = co::co_poll(&pfd, 1, timeout_ms);
      if (ret <= 0) {
        return Status::IoError("write timeout");
      }
      continue;
    }

    return Status::IoError(n == 0 ? "zero write" : std::strerror(errno));
  }

  return Status::Ok();
}

}  // namespace redis_proxy
