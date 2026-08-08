// A WebSocket connection: TCP + TLS + RFC 6455, driven by one poll() call.
//
// This is where the pieces meet. It is a flat state machine, not a coroutine,
// and that is the decision the whole design rests on:
//
//   kTcpConnecting -> kTlsHandshaking -> kWsHandshaking -> kOpen
//
// The three setup states are inherently sequential-with-waits and would read
// better as a coroutine. The kOpen state -- which is where every message
// spends its life -- would not: it never actually suspends, because the data
// is already in the buffer by the time poll() looks. A coroutine there would
// pay for machinery it never uses, and once the handle escapes into a
// scheduler the frame is a 45ns malloc with an unbounded tail (measured; see
// bench/bench_coroutine.cpp). So: no coroutines here at all, and the setup
// path pays a little readability for one consistent model.
//
// USAGE. Call poll() in a busy loop. It never blocks and never allocates once
// connected:
//
//     WsConnection conn;
//     conn.connect(cfg, &tls_ctx);
//     while (running) {
//       conn.poll(handler);
//       core::cpu_pause();
//     }
//
// Handler must provide:
//     void on_open()
//     void on_message(const std::byte* data, std::size_t len, WsOpcode op)
//     void on_close(const WsCloseInfo& info)
//     void on_error(const char* what)
//
// on_message hands out a pointer INTO the receive buffer. It is valid for the
// duration of the call and no longer -- copy what you need before returning.
// That is the whole point: a venue parser reads its six fields straight out of
// the socket buffer with nothing copied in between.
//
// Ping/pong is handled internally. A venue that does not get its pong closes
// the connection, and making that the application's problem is how it gets
// forgotten.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "calais/core/latency.h"
#include "calais/net/byte_buffer.h"
#include "calais/net/tcp_socket.h"
#include "calais/net/tls_stream.h"
#include "calais/net/ws_frame.h"
#include "calais/net/ws_handshake.h"
#include "calais/net/ws_message.h"

namespace calais::net {

enum class WsConnectionState : std::uint8_t {
  kIdle = 0,
  kTcpConnecting = 1,
  kTlsHandshaking = 2,
  kWsHandshaking = 3,
  kOpen = 4,
  kClosing = 5,
  kClosed = 6,
  kFailed = 7,
};

const char* to_string(WsConnectionState s) noexcept;

struct WsConnectionConfig {
  std::string host;
  std::uint16_t port = 443;
  std::string target = "/";
  bool use_tls = true;

  // Extra HTTP headers on the upgrade request. Where a venue's auth token goes.
  std::vector<std::pair<std::string, std::string>> extra_headers;

  // Receive buffer. Must comfortably exceed the largest frame the venue sends;
  // ensure_writable() fails rather than truncating, which fails the connection.
  //
  // Reserves simdjson's padding on top, so a venue parser can work straight out
  // of this buffer.
  std::size_t rx_buffer_bytes = 1u << 18;  // 256 KiB
  std::size_t tx_buffer_bytes = 1u << 16;  // 64 KiB

  // Bound on reassembly of a fragmented message.
  std::size_t max_message_bytes = 8u << 20;
};

// Counters worth exporting. Each one answers a question that otherwise gets
// answered by guessing during an incident.
struct WsConnectionStats {
  std::uint64_t messages_received = 0;
  std::uint64_t messages_sent = 0;
  std::uint64_t bytes_received = 0;
  std::uint64_t bytes_sent = 0;
  std::uint64_t pings_received = 0;
  std::uint64_t pongs_sent = 0;
  // Non-zero means the send buffer filled: the venue or the network is not
  // keeping up and orders are being delayed.
  std::uint64_t send_backpressure_events = 0;
  // Non-zero means the receive buffer had to memmove. Steady growth means it
  // is undersized for the message rate.
  std::uint64_t rx_compactions = 0;
};

class WsConnection {
 public:
  WsConnection() = default;

  WsConnection(const WsConnection&) = delete;
  WsConnection& operator=(const WsConnection&) = delete;

  // Starts connecting. `tls` may be null when use_tls is false; when it is not
  // null it must outlive the connection.
  //
  // Blocks only in getaddrinfo. Everything after that is driven by poll().
  void connect(const WsConnectionConfig& config, const TlsContext* tls);

  // Advances the state machine as far as available bytes allow. Returns true
  // while the connection is worth polling again.
  template <typename Handler>
  bool poll(Handler& handler);

  // Queues an outbound message. Returns false if the send buffer is full --
  // which is backpressure, not an error, and the caller must decide whether to
  // retry or drop. Never blocks and never grows the buffer.
  bool send_text(std::string_view text);
  bool send_binary(const void* data, std::size_t len);

  // Sends close_notify and begins an orderly shutdown.
  void close(WsCloseCode code = WsCloseCode::kNormal,
             std::string_view reason = {});

  // Drops everything immediately. For a connection already known to be dead.
  void abort() noexcept;

  WsConnectionState state() const noexcept { return state_; }
  bool open() const noexcept { return state_ == WsConnectionState::kOpen; }
  const std::string& last_error() const noexcept { return last_error_; }
  const WsConnectionStats& stats() const noexcept { return stats_; }
  const TcpSocket& socket() const noexcept { return socket_; }
  const TlsStream& tls() const noexcept { return tls_; }

 private:
  bool queue_frame(WsOpcode opcode, const void* payload, std::size_t len);
  // Moves bytes socket <-> TLS <-> buffers. Returns false if the connection
  // died.
  bool pump_io();
  bool flush_tx();
  bool fill_rx();
  void fail(std::string what);

  template <typename Handler>
  bool drive_ws_handshake(Handler& handler);
  template <typename Handler>
  bool drive_open(Handler& handler);

  WsConnectionConfig config_;
  const TlsContext* tls_ctx_ = nullptr;

  TcpSocket socket_;
  TlsStream tls_;
  WsMessageAssembler assembler_;

  // Plaintext both ways.
  ByteBuffer rx_;
  ByteBuffer tx_;
  // Ciphertext staging. Only used when TLS is on.
  ByteBuffer cipher_rx_;
  ByteBuffer cipher_tx_;

  WsClientHandshake handshake_;
  WsConnectionState state_ = WsConnectionState::kIdle;
  std::string last_error_;
  WsConnectionStats stats_;
  bool notified_open_ = false;
};

}  // namespace calais::net

#include "calais/net/ws_connection.inl"
