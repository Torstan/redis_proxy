#include "redis_proxy/fd_notifier.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

namespace redis_proxy {

namespace {

bool SetNonBlocking(int fd) {
  int flags = fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return false;
  }
  return fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

}  // namespace

FdNotifier::FdNotifier() {
  int fds[2] = {-1, -1};
  if (pipe(fds) != 0) {
    return;
  }

  read_fd_ = fds[0];
  write_fd_ = fds[1];
  if (!SetNonBlocking(read_fd_) || !SetNonBlocking(write_fd_)) {
    close(read_fd_);
    close(write_fd_);
    read_fd_ = -1;
    write_fd_ = -1;
  }
}

FdNotifier::~FdNotifier() {
  if (read_fd_ >= 0) {
    close(read_fd_);
  }
  if (write_fd_ >= 0) {
    close(write_fd_);
  }
}

bool FdNotifier::valid() const { return read_fd_ >= 0 && write_fd_ >= 0; }

int FdNotifier::readFd() const { return read_fd_; }

Status FdNotifier::notify() {
  if (!valid()) {
    return Status::IoError("fd notifier is not initialized");
  }

  const char byte = 'x';
  ssize_t n = write(write_fd_, &byte, 1);
  if (n == 1) {
    return Status::Ok();
  }
  if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
    return Status::Ok();
  }
  if (n == 0) {
    return Status::IoError("zero write");
  }
  return Status::IoError(std::strerror(errno));
}

Status FdNotifier::drain() {
  if (!valid()) {
    return Status::IoError("fd notifier is not initialized");
  }

  char buf[256];
  for (;;) {
    ssize_t n = read(read_fd_, buf, sizeof(buf));
    if (n > 0) {
      continue;
    }
    if (n == 0) {
      return Status::Closed("fd notifier closed");
    }
    if (errno == EINTR) {
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return Status::Ok();
    }
    return Status::IoError(std::strerror(errno));
  }
}

}  // namespace redis_proxy
