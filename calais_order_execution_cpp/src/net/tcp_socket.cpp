#include "calais/net/tcp_socket.h"

#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <utility>

namespace calais::net {
namespace {

IoResult ok(std::size_t n) noexcept { return {IoStatus::kOk, n, 0}; }
IoResult would_block() noexcept { return {IoStatus::kWouldBlock, 0, 0}; }
IoResult closed() noexcept { return {IoStatus::kClosed, 0, 0}; }
IoResult failed(int err) noexcept { return {IoStatus::kError, 0, err}; }

bool set_non_blocking(int fd) noexcept {
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0) {
    return false;
  }
  return ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

core::TuningResult set_int_option(int fd, int level, int name, int value,
                                  const char* label) noexcept {
  if (fd < 0) {
    return {false, "socket is not open"};
  }
  if (::setsockopt(fd, level, name, &value, sizeof(value)) != 0) {
    return {false, std::string(label) + ": " + std::strerror(errno)};
  }
  return {true, label};
}

}  // namespace

TcpSocket::~TcpSocket() { close(); }

TcpSocket::TcpSocket(TcpSocket&& o) noexcept
    : fd_(std::exchange(o.fd_, -1)),
      candidates_(std::move(o.candidates_)),
      candidate_index_(std::exchange(o.candidate_index_, 0)),
      last_connect_error_(std::exchange(o.last_connect_error_, 0)) {}

TcpSocket& TcpSocket::operator=(TcpSocket&& o) noexcept {
  if (this != &o) {
    close();
    fd_ = std::exchange(o.fd_, -1);
    candidates_ = std::move(o.candidates_);
    candidate_index_ = std::exchange(o.candidate_index_, 0);
    last_connect_error_ = std::exchange(o.last_connect_error_, 0);
  }
  return *this;
}

TcpSocket TcpSocket::adopt(int fd) noexcept {
  TcpSocket s;
  s.fd_ = fd;
  if (fd >= 0) {
    set_non_blocking(fd);
    // Same reasoning as connect_to_host: an accepted socket that leaves Nagle
    // enabled is just as slow as a connected one.
    static_cast<void>(s.set_no_delay(true));
  }
  return s;
}

IoResult TcpSocket::connect_to_host(std::string_view host, std::uint16_t port) {
  close();
  candidates_.clear();
  candidate_index_ = 0;
  last_connect_error_ = 0;

  const std::string host_str(host);
  const std::string port_str = std::to_string(port);

  struct addrinfo hints {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;

  // BLOCKING. Connection setup only -- see the header.
  struct addrinfo* result = nullptr;
  const int rc = ::getaddrinfo(host_str.c_str(), port_str.c_str(), &hints, &result);
  if (rc != 0 || result == nullptr) {
    return failed(EHOSTUNREACH);
  }

  // Keep EVERY resolved address. "localhost" commonly resolves to both ::1 and
  // 127.0.0.1, and a peer listening on only one of them makes the other
  // refuse -- asynchronously, so the failure surfaces long after connect()
  // returned EINPROGRESS. Falling through to the next address is the only way
  // to be robust to that.
  for (struct addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
    if (ai->ai_addrlen > sizeof(sockaddr_storage)) {
      continue;
    }
    Candidate c;
    c.family = ai->ai_family;
    c.socktype = ai->ai_socktype;
    c.protocol = ai->ai_protocol;
    c.addr_len = ai->ai_addrlen;
    std::memcpy(&c.addr, ai->ai_addr, ai->ai_addrlen);
    candidates_.push_back(c);
  }
  ::freeaddrinfo(result);

  if (candidates_.empty()) {
    return failed(EHOSTUNREACH);
  }
  return start_connect_attempt();
}

IoResult TcpSocket::start_connect_attempt() noexcept {
  while (candidate_index_ < candidates_.size()) {
    const Candidate& c = candidates_[candidate_index_];

    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }

    const int fd = ::socket(c.family, c.socktype, c.protocol);
    if (fd < 0) {
      last_connect_error_ = errno;
      ++candidate_index_;
      continue;
    }
    if (!set_non_blocking(fd)) {
      last_connect_error_ = errno;
      ::close(fd);
      ++candidate_index_;
      continue;
    }

#ifdef SO_NOSIGPIPE
    // macOS: without this, writing to a socket the peer has closed raises
    // SIGPIPE and kills the process. Linux uses MSG_NOSIGNAL on send instead.
    const int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &one, sizeof(one));
#endif

    fd_ = fd;
    // Set TCP_NODELAY before the handshake completes, so even the first write
    // is unaffected by Nagle.
    static_cast<void>(set_no_delay(true));

    const int cr = ::connect(fd, reinterpret_cast<const sockaddr*>(&c.addr),
                             c.addr_len);
    if (cr == 0) {
      ++candidate_index_;
      return ok(0);
    }
    if (errno == EINPROGRESS || errno == EALREADY || errno == EINTR) {
      return would_block();
    }

    last_connect_error_ = errno;
    ++candidate_index_;
  }

  close();
  return failed(last_connect_error_ != 0 ? last_connect_error_ : EHOSTUNREACH);
}

IoResult TcpSocket::connect_status() noexcept {
  if (fd_ < 0) {
    return failed(last_connect_error_ != 0 ? last_connect_error_ : EBADF);
  }

  // Writability first. SO_ERROR alone reports 0 for "no error YET", which on a
  // connection about to be refused looks exactly like success -- and the
  // refusal then surfaces as a mystery read error two states later. That is
  // the bug this ordering exists to prevent.
  struct pollfd pfd {};
  pfd.fd = fd_;
  pfd.events = POLLOUT;
  const int rc = ::poll(&pfd, 1, 0);
  if (rc == 0) {
    return would_block();  // still in progress
  }
  if (rc < 0) {
    if (errno == EINTR) {
      return would_block();
    }
    return failed(errno);
  }

  int err = 0;
  socklen_t len = sizeof(err);
  if (::getsockopt(fd_, SOL_SOCKET, SO_ERROR, &err, &len) != 0) {
    err = errno;
  }

  if (err == 0 && (pfd.revents & (POLLERR | POLLHUP)) == 0) {
    ++candidate_index_;  // this address worked; do not retry it
    return ok(0);
  }

  last_connect_error_ = err != 0 ? err : ECONNREFUSED;
  ++candidate_index_;
  if (candidate_index_ < candidates_.size()) {
    // Fall through to the next resolved address rather than giving up.
    return start_connect_attempt();
  }
  close();
  return failed(last_connect_error_);
}

IoResult TcpSocket::read(void* buffer, std::size_t len) noexcept {
  if (fd_ < 0) {
    return failed(EBADF);
  }
  if (len == 0) {
    return ok(0);
  }
  const ssize_t n = ::recv(fd_, buffer, len, 0);
  if (n > 0) {
    return ok(static_cast<std::size_t>(n));
  }
  if (n == 0) {
    return closed();
  }
  if (errno == EAGAIN || errno == EWOULDBLOCK) {
    return would_block();
  }
  if (errno == EINTR) {
    // Not an error and not end of stream; the caller should simply poll again.
    return would_block();
  }
  return failed(errno);
}

IoResult TcpSocket::write(const void* data, std::size_t len) noexcept {
  if (fd_ < 0) {
    return failed(EBADF);
  }
  if (len == 0) {
    return ok(0);
  }
#ifdef MSG_NOSIGNAL
  const ssize_t n = ::send(fd_, data, len, MSG_NOSIGNAL);
#else
  const ssize_t n = ::send(fd_, data, len, 0);
#endif
  if (n > 0) {
    // A PARTIAL write is normal and is reported as such. Looping here would
    // block the caller inside what is supposed to be a non-blocking call.
    return ok(static_cast<std::size_t>(n));
  }
  if (n == 0) {
    return ok(0);
  }
  if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
    return would_block();
  }
  if (errno == EPIPE || errno == ECONNRESET) {
    return closed();
  }
  return failed(errno);
}

void TcpSocket::shutdown_write() noexcept {
  if (fd_ >= 0) {
    ::shutdown(fd_, SHUT_WR);
  }
}

void TcpSocket::close() noexcept {
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}



// ---------------------------------------------------------------------------
core::TuningResult TcpSocket::set_no_delay(bool enabled) noexcept {
  return set_int_option(fd_, IPPROTO_TCP, TCP_NODELAY, enabled ? 1 : 0,
                        "TCP_NODELAY");
}

core::TuningResult TcpSocket::set_quick_ack([[maybe_unused]] bool enabled) noexcept {
#ifdef TCP_QUICKACK
  return set_int_option(fd_, IPPROTO_TCP, TCP_QUICKACK, enabled ? 1 : 0,
                        "TCP_QUICKACK");
#else
  return {false, "TCP_QUICKACK is Linux-only"};
#endif
}

core::TuningResult TcpSocket::set_busy_poll(
    [[maybe_unused]] int microseconds) noexcept {
#ifdef SO_BUSY_POLL
  return set_int_option(fd_, SOL_SOCKET, SO_BUSY_POLL, microseconds,
                        "SO_BUSY_POLL");
#else
  return {false, "SO_BUSY_POLL is Linux-only"};
#endif
}

core::TuningResult TcpSocket::set_recv_buffer(int bytes) noexcept {
  return set_int_option(fd_, SOL_SOCKET, SO_RCVBUF, bytes, "SO_RCVBUF");
}

core::TuningResult TcpSocket::set_send_buffer(int bytes) noexcept {
  return set_int_option(fd_, SOL_SOCKET, SO_SNDBUF, bytes, "SO_SNDBUF");
}

core::TuningResult TcpSocket::set_keep_alive(bool enabled,
                                             [[maybe_unused]] int idle_seconds,
                                             [[maybe_unused]] int interval_seconds,
                                             [[maybe_unused]] int probe_count) noexcept {
  auto base = set_int_option(fd_, SOL_SOCKET, SO_KEEPALIVE, enabled ? 1 : 0,
                             "SO_KEEPALIVE");
  if (!base.ok || !enabled) {
    return base;
  }

  std::string notes = "SO_KEEPALIVE";
#ifdef TCP_KEEPIDLE
  if (set_int_option(fd_, IPPROTO_TCP, TCP_KEEPIDLE, idle_seconds, "x").ok) {
    notes += "+idle";
  }
#elif defined(TCP_KEEPALIVE)
  // macOS spells it TCP_KEEPALIVE and it means the idle time.
  if (set_int_option(fd_, IPPROTO_TCP, TCP_KEEPALIVE, idle_seconds, "x").ok) {
    notes += "+idle";
  }
#endif
#ifdef TCP_KEEPINTVL
  if (set_int_option(fd_, IPPROTO_TCP, TCP_KEEPINTVL, interval_seconds, "x").ok) {
    notes += "+intvl";
  }
#endif
#ifdef TCP_KEEPCNT
  if (set_int_option(fd_, IPPROTO_TCP, TCP_KEEPCNT, probe_count, "x").ok) {
    notes += "+cnt";
  }
#endif
  return {true, notes};
}

TcpSocket::AppliedOptions TcpSocket::applied_options() const noexcept {
  AppliedOptions out;
  if (fd_ < 0) {
    out.notes = "socket is not open";
    return out;
  }

  int value = 0;
  socklen_t len = sizeof(value);
  if (::getsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &value, &len) == 0) {
    out.no_delay = value != 0;
  }
  len = sizeof(value);
  if (::getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &value, &len) == 0) {
    out.recv_buffer = value;
  }
  len = sizeof(value);
  if (::getsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &value, &len) == 0) {
    out.send_buffer = value;
  }
  // The kernel commonly doubles or clamps a requested buffer size, so what was
  // asked for and what is in force are different questions.
  out.notes = out.no_delay ? "nodelay=on" : "nodelay=OFF (Nagle active!)";
  return out;
}

}  // namespace calais::net
