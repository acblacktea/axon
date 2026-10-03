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

#include "axon/core/latency.h"
#include "axon/net/byte_buffer.h"
#include "axon/net/tcp_socket.h"
#include "axon/net/tls_stream.h"
#include "axon/net/ws_frame.h"
#include "axon/net/ws_handshake.h"
#include "axon/net/ws_message.h"

namespace axon::net {

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

// ---------------------------------------------------------------------------
// Template members.
//
// These live in the header because a template's definition has to be visible
// wherever it is instantiated. They are templates on the handler rather than
// std::function so that on_message INLINES into the poll loop -- an indirect
// call per message would defeat the point of handing out a pointer into the
// receive buffer.
//
// Everything that is not a template stays in ws_connection.cpp: a header-only
// version would leak the implementation's includes to every consumer and turn
// every implementation edit into an interface change.
// ---------------------------------------------------------------------------

template <typename Handler>
bool WsConnection::drive_ws_handshake(Handler& handler) {
  if (!fill_rx()) {
    return false;
  }

  const std::string_view response(
      reinterpret_cast<const char*>(rx_.readable()), rx_.readable_size());
  const WsHandshakeResponse result =
      ws_validate_handshake_response(response, handshake_.expected_accept);

  switch (result.status) {
    case WsHandshakeStatus::kIncomplete:
      return true;

    case WsHandshakeStatus::kFailed:
      fail(result.error);
      handler.on_error(last_error_.c_str());
      return false;

    case WsHandshakeStatus::kOk:
      // Consume EXACTLY the headers. A venue may put its first frame in the
      // same segment, and eating it here would lose the message.
      rx_.consume(result.consumed);
      state_ = WsConnectionState::kOpen;
      if (!notified_open_) {
        notified_open_ = true;
        handler.on_open();
      }
      return true;
  }
  return true;
}

template <typename Handler>
bool WsConnection::drive_open(Handler& handler) {
  if (!pump_io()) {
    // The socket died. Anything already buffered is still worth delivering --
    // a close frame commonly arrives in the same read as the FIN.
    handler.on_error(last_error_.empty() ? "connection lost"
                                         : last_error_.c_str());
    return false;
  }

  // Drain every complete frame in the buffer before returning. Returning after
  // one would let a burst accumulate for a whole poll cycle each.
  for (;;) {
    const WsDecodeResult decoded =
        ws_decode_frame(rx_.readable(), rx_.readable_size());

    if (decoded.status == WsDecodeStatus::kIncomplete) {
      break;
    }
    if (decoded.status == WsDecodeStatus::kProtocolError) {
      fail(decoded.error != nullptr ? decoded.error : "websocket protocol error");
      handler.on_error(last_error_.c_str());
      close(WsCloseCode::kProtocolError, "protocol error");
      return false;
    }

    // Server-to-client frames must not be masked. One that is means we are not
    // talking to the server we think we are.
    if (decoded.header.masked) {
      fail("server sent a masked frame");
      handler.on_error(last_error_.c_str());
      close(WsCloseCode::kProtocolError, "masked frame from server");
      return false;
    }

    const WsMessageEvent event =
        assembler_.feed(decoded.header, decoded.payload);
    // Consume before dispatching: the handler must not see a buffer whose
    // cursor still points at a frame already handled.
    const std::size_t frame_size = decoded.total_size;

    switch (event.type) {
      case WsEvent::kNone:
        rx_.consume(frame_size);
        break;

      case WsEvent::kMessage: {
        ++stats_.messages_received;
        // The payload points into rx_ (single frame) or into the assembler
        // (reassembled). Either way it is valid for exactly this call.
        handler.on_message(event.payload, event.payload_size, event.opcode);
        rx_.consume(frame_size);
        break;
      }

      case WsEvent::kPing: {
        ++stats_.pings_received;
        // Answer immediately and with the same payload, as the RFC requires.
        // A venue that does not get its pong closes the connection, so this is
        // never the application's business.
        queue_frame(WsOpcode::kPong, event.payload, event.payload_size);
        ++stats_.pongs_sent;
        rx_.consume(frame_size);
        break;
      }

      case WsEvent::kPong:
        rx_.consume(frame_size);
        break;

      case WsEvent::kClose: {
        rx_.consume(frame_size);
        // Echo the close, then stop. The socket stays open long enough for the
        // reply to leave.
        if (state_ == WsConnectionState::kOpen) {
          close(event.close.code, {});
        }
        handler.on_close(event.close);
        return true;
      }

      case WsEvent::kProtocolError:
        fail(event.error != nullptr ? event.error : "message assembly error");
        handler.on_error(last_error_.c_str());
        close(WsCloseCode::kProtocolError, "assembly error");
        rx_.consume(frame_size);
        return false;
    }
  }

  stats_.rx_compactions = rx_.compactions();
  return true;
}

template <typename Handler>
bool WsConnection::poll(Handler& handler) {
  switch (state_) {
    case WsConnectionState::kIdle:
    case WsConnectionState::kClosed:
    case WsConnectionState::kFailed:
      return false;

    case WsConnectionState::kTcpConnecting: {
      const IoResult status = socket_.connect_status();
      if (status.status == IoStatus::kWouldBlock) {
        return true;
      }
      if (status.status != IoStatus::kOk) {
        fail("tcp connect failed: " + std::string(std::strerror(status.error)));
        handler.on_error(last_error_.c_str());
        return false;
      }
      if (config_.use_tls) {
        tls_.start_client(*tls_ctx_, config_.host);
        state_ = WsConnectionState::kTlsHandshaking;
      } else {
        // Plaintext: the upgrade request goes out as raw bytes, not as a
        // WebSocket frame -- framing only begins after the 101.
        if (!tx_.ensure_writable(handshake_.request.size())) {
          fail("tx buffer too small for the upgrade request");
          handler.on_error(last_error_.c_str());
          return false;
        }
        std::memcpy(tx_.writable(), handshake_.request.data(),
                    handshake_.request.size());
        tx_.commit(handshake_.request.size());
        state_ = WsConnectionState::kWsHandshaking;
        return pump_io();
      }
      return true;
    }

    case WsConnectionState::kTlsHandshaking: {
      if (!pump_io()) {
        handler.on_error(last_error_.c_str());
        return false;
      }
      const IoResult hs = tls_.handshake();
      if (hs.status == IoStatus::kError) {
        fail("tls handshake failed: " + tls_.last_error());
        handler.on_error(last_error_.c_str());
        return false;
      }
      // Flush whatever the handshake produced before deciding it is stalled.
      if (!pump_io()) {
        handler.on_error(last_error_.c_str());
        return false;
      }
      if (tls_.established()) {
        if (tx_.writable_size() < handshake_.request.size()) {
          fail("tx buffer too small for the upgrade request");
          handler.on_error(last_error_.c_str());
          return false;
        }
        std::memcpy(tx_.writable(), handshake_.request.data(),
                    handshake_.request.size());
        tx_.commit(handshake_.request.size());
        state_ = WsConnectionState::kWsHandshaking;
        return pump_io();
      }
      return true;
    }

    case WsConnectionState::kWsHandshaking:
      if (!pump_io()) {
        handler.on_error(last_error_.c_str());
        return false;
      }
      return drive_ws_handshake(handler);

    case WsConnectionState::kOpen:
      return drive_open(handler);

    case WsConnectionState::kClosing:
      // Keep flushing so the close frame actually leaves, then stop.
      if (!pump_io() || (tx_.empty() && !tls_.has_encrypted_pending() &&
                         cipher_tx_.empty())) {
        state_ = WsConnectionState::kClosed;
        return false;
      }
      return true;
  }
  return false;
}

}  // namespace axon::net
