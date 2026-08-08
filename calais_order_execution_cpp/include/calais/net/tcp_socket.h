// Non-blocking TCP socket.
//
// Thin by design. It does not own a buffer, does not retry, and does not
// interpret anything -- it moves bytes and reports what happened, so the layer
// above can busy-poll it without ever blocking.
//
// TCP_NODELAY IS ON BY DEFAULT, and that is the single most consequential line
// in this file. Nagle's algorithm holds a small write until the previous
// segment is acknowledged, which on a colocated link adds a delay measured in
// TENS OF MILLISECONDS to an order that took ten microseconds to build. It is
// off in the standard socket defaults and forgetting to disable it is the most
// common way to lose every other optimisation in this repo at once. Turning it
// back on requires an explicit call and a reason.
//
// Name resolution is deliberately absent from the hot path: connect_to_host()
// calls getaddrinfo, which BLOCKS. That is acceptable exactly once per
// connection, on a cold path, and unacceptable anywhere else.

#pragma once

#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "calais/core/platform.h"

namespace calais::net {

enum class IoStatus : std::uint8_t {
  kOk = 0,
  // Nothing to read, or the send buffer is full. Normal on a non-blocking
  // socket and the reason this is a status rather than an error.
  kWouldBlock = 1,
  // The peer closed cleanly.
  kClosed = 2,
  kError = 3,
};

struct IoResult {
  IoStatus status = IoStatus::kWouldBlock;
  std::size_t bytes = 0;
  int error = 0;  // errno when status is kError

  explicit operator bool() const noexcept { return status == IoStatus::kOk; }
};

class TcpSocket {
 public:
  TcpSocket() noexcept = default;
  ~TcpSocket();

  TcpSocket(const TcpSocket&) = delete;
  TcpSocket& operator=(const TcpSocket&) = delete;
  TcpSocket(TcpSocket&& o) noexcept;
  TcpSocket& operator=(TcpSocket&& o) noexcept;

  // Resolves `host` and starts a non-blocking connect.
  //
  // Returns kOk if the connection completed immediately (loopback usually
  // does), kWouldBlock if it is in progress -- poll with connect_status()
  // until it resolves -- or kError.
  //
  // BLOCKS in getaddrinfo. Connection setup only.
  IoResult connect_to_host(std::string_view host, std::uint16_t port);

  // Wraps an already-connected descriptor, taking ownership. Used by the test
  // server and by anything that accepts rather than connects.
  static TcpSocket adopt(int fd) noexcept;

  // For a connect still in progress: kOk once established, kWouldBlock while
  // still pending, kError once every resolved address has been tried.
  //
  // Polls for writability BEFORE reading SO_ERROR. Reading SO_ERROR on its own
  // is the classic mistake: it returns 0 for "no error YET", which on a
  // connection that is about to be refused is indistinguishable from success,
  // and the failure then surfaces as an unexplained read error several states
  // later.
  //
  // If an address fails, the NEXT resolved address is tried automatically. A
  // host with both A and AAAA records where one family is unreachable -- the
  // usual case for a machine with broken IPv6 -- would otherwise fail
  // outright even though a working address was available.
  IoResult connect_status() noexcept;

  IoResult read(void* buffer, std::size_t len) noexcept;
  IoResult write(const void* data, std::size_t len) noexcept;

  // Half-close: tells the peer we are done sending. The TLS layer needs this
  // to be separable from a full close so a close_notify can still be read.
  void shutdown_write() noexcept;
  void close() noexcept;

  bool valid() const noexcept { return fd_ >= 0; }
  int fd() const noexcept { return fd_; }

  // ---- tuning -----------------------------------------------------------
  // All applied at connect time with sensible defaults; exposed so a caller
  // can override deliberately.

  // Disabling this re-enables Nagle. See the warning at the top of this file.
  core::TuningResult set_no_delay(bool enabled) noexcept;

  // Linux: acknowledge immediately instead of waiting to piggyback. Saves up
  // to 40ms on a request/response pattern. No-op elsewhere.
  core::TuningResult set_quick_ack(bool enabled) noexcept;

  // Linux: let the kernel spin in the driver for this many microseconds
  // rather than sleeping, so a packet arriving mid-poll is picked up without
  // an interrupt and a wakeup. Meaningful only with a busy-polling reader.
  core::TuningResult set_busy_poll(int microseconds) noexcept;

  core::TuningResult set_recv_buffer(int bytes) noexcept;
  core::TuningResult set_send_buffer(int bytes) noexcept;

  // Keepalive is about detecting a peer that vanished without a FIN. The
  // defaults (two hours) are useless for trading; these set the interval
  // explicitly where the platform allows it.
  core::TuningResult set_keep_alive(bool enabled, int idle_seconds = 30,
                                    int interval_seconds = 10,
                                    int probe_count = 3) noexcept;

  // What the socket actually ended up with, for logging at startup. A
  // requested buffer size is a hint the kernel may double or clamp.
  struct AppliedOptions {
    bool no_delay = false;
    int recv_buffer = 0;
    int send_buffer = 0;
    std::string notes;
  };
  AppliedOptions applied_options() const noexcept;

 private:
  // Starts a non-blocking connect to candidates_[index_]. Returns kOk if it
  // completed immediately, kWouldBlock if in progress, kError if the socket
  // could not even be created.
  IoResult start_connect_attempt() noexcept;

  // A resolved address, stored so a failed attempt can fall through to the
  // next one without resolving again.
  struct Candidate {
    int family = 0;
    int socktype = 0;
    int protocol = 0;
    sockaddr_storage addr{};
    socklen_t addr_len = 0;
  };

  int fd_ = -1;
  std::vector<Candidate> candidates_;
  std::size_t candidate_index_ = 0;
  int last_connect_error_ = 0;
};

}  // namespace calais::net
