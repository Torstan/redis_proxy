#pragma once

#include <string>
#include <utility>

namespace redis_proxy {

enum class StatusCode {
  kOk,
  kInvalidArgument,
  kIoError,
  kProtocolError,
  kClosed
};

class Status {
public:
  Status() = default;
  Status(StatusCode code, std::string message)
      : code_(code), message_(std::move(message)) {}

  static Status Ok() { return Status(); }
  static Status InvalidArgument(std::string message) {
    return Status(StatusCode::kInvalidArgument, std::move(message));
  }
  static Status IoError(std::string message) {
    return Status(StatusCode::kIoError, std::move(message));
  }
  static Status ProtocolError(std::string message) {
    return Status(StatusCode::kProtocolError, std::move(message));
  }
  static Status Closed(std::string message) {
    return Status(StatusCode::kClosed, std::move(message));
  }

  bool ok() const { return code_ == StatusCode::kOk; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return message_; }

private:
  StatusCode code_ = StatusCode::kOk;
  std::string message_;
};

}  // namespace redis_proxy
