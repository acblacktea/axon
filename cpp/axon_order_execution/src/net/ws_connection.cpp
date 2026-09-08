#include "axon/net/ws_connection.h"

#include <cerrno>
#include <cstring>

#include "axon/venue/json_padding.h"

namespace axon::net {

const char* to_string(WsConnectionState s) noexcept {
  switch (s) {
    case WsConnectionState::kIdle:
      return "idle";
    case WsConnectionState::kTcpConnecting:
      return "tcp-connecting";
    case WsConnectionState::kTlsHandshaking:
      return "tls-handshaking";
    case WsConnectionState::kWsHandshaking:
      return "ws-handshaking";
    case WsConnectionState::kOpen:
      return "open";
    case WsConnectionState::kClosing:
      return "closing";
    case WsConnectionState::kClosed:
      return "closed";
    case WsConnectionState::kFailed:
      return "failed";
  }
  return "?";
}

void WsConnection::connect(const WsConnectionConfig& config,
                           const TlsContext* tls) {
  config_ = config;
  tls_ctx_ = tls;
  last_error_.clear();
  stats_ = {};
  notified_open_ = false;

  if (config_.use_tls && tls_ctx_ == nullptr) {
    fail("use_tls is set but no TlsContext was supplied");
    return;
  }

  // The receive buffer carries simdjson's padding on top of its usable size,
  // so a venue parser can work straight out of it without a copy.
  rx_ = ByteBuffer(config_.rx_buffer_bytes + kJsonParserPadding);
  tx_ = ByteBuffer(config_.tx_buffer_bytes);
  if (config_.use_tls) {
    cipher_rx_ = ByteBuffer(config_.rx_buffer_bytes);
    cipher_tx_ = ByteBuffer(config_.tx_buffer_bytes);
  }
  assembler_ = WsMessageAssembler(config_.max_message_bytes);

  WsHandshakeRequest request;
  // Host must carry the port when it is not the scheme default, or a venue
  // behind a virtual host routes the request somewhere else.
  const bool default_port = (config_.use_tls && config_.port == 443) ||
                            (!config_.use_tls && config_.port == 80);
  request.host = default_port
                     ? config_.host
                     : config_.host + ":" + std::to_string(config_.port);
  request.target = config_.target;
  request.extra_headers = config_.extra_headers;
  handshake_ = ws_build_handshake(request);

  const IoResult connected = socket_.connect_to_host(config_.host, config_.port);
  if (connected.status == IoStatus::kError) {
    fail("tcp connect failed: " + std::string(std::strerror(connected.error)));
    return;
  }

  // Keepalive so a peer that vanishes without a FIN is noticed in seconds
  // rather than in the two hours the defaults would take.
  static_cast<void>(socket_.set_keep_alive(true));

  state_ = WsConnectionState::kTcpConnecting;
}

void WsConnection::fail(std::string what) {
  last_error_ = std::move(what);
  state_ = WsConnectionState::kFailed;
}

void WsConnection::abort() noexcept {
  socket_.close();
  tls_.reset();
  state_ = WsConnectionState::kClosed;
}

// ---------------------------------------------------------------------------
bool WsConnection::queue_frame(WsOpcode opcode, const void* payload,
                               std::size_t len) {
  const std::size_t needed = ws_header_size(len, /*masked=*/true) + len;
  if (!tx_.ensure_writable(needed)) {
    // Backpressure, not an error: the caller decides whether to retry or drop.
    // Growing the buffer here would defer the problem and hide it.
    ++stats_.send_backpressure_events;
    return false;
  }

  const std::size_t written =
      ws_encode_frame(tx_.writable(), tx_.writable_size(), opcode, /*fin=*/true,
                      payload, len, ws_random_mask_key());
  if (written == 0) {
    ++stats_.send_backpressure_events;
    return false;
  }
  tx_.commit(written);
  return true;
}

bool WsConnection::send_text(std::string_view text) {
  if (state_ != WsConnectionState::kOpen) {
    return false;
  }
  if (!queue_frame(WsOpcode::kText, text.data(), text.size())) {
    return false;
  }
  ++stats_.messages_sent;
  return true;
}

bool WsConnection::send_binary(const void* data, std::size_t len) {
  if (state_ != WsConnectionState::kOpen) {
    return false;
  }
  if (!queue_frame(WsOpcode::kBinary, data, len)) {
    return false;
  }
  ++stats_.messages_sent;
  return true;
}

void WsConnection::close(WsCloseCode code, std::string_view reason) {
  if (state_ != WsConnectionState::kOpen) {
    return;
  }
  std::byte payload[kMaxControlPayload];
  const std::size_t n =
      ws_build_close_payload(payload, sizeof(payload), code, reason);
  queue_frame(WsOpcode::kClose, payload, n);
  state_ = WsConnectionState::kClosing;
  static_cast<void>(pump_io());
}

// ---------------------------------------------------------------------------
// The pump. Plaintext <-> TLS <-> ciphertext <-> socket, all non-blocking.
// ---------------------------------------------------------------------------
bool WsConnection::flush_tx() {
  if (!config_.use_tls) {
    while (!tx_.empty()) {
      const IoResult w = socket_.write(tx_.readable(), tx_.readable_size());
      if (w.status == IoStatus::kWouldBlock) {
        return true;
      }
      if (w.status != IoStatus::kOk) {
        fail(w.status == IoStatus::kClosed
                 ? "peer closed while writing"
                 : "socket write failed: " + std::string(std::strerror(w.error)));
        return false;
      }
      tx_.consume(w.bytes);
      stats_.bytes_sent += w.bytes;
    }
    return true;
  }

  // Plaintext -> TLS. A partial SSL_write is normal; the rest stays queued.
  while (!tx_.empty()) {
    const IoResult w = tls_.write_plaintext(tx_.readable(), tx_.readable_size());
    if (w.status == IoStatus::kWouldBlock) {
      break;
    }
    if (w.status != IoStatus::kOk) {
      fail("tls write failed: " + tls_.last_error());
      return false;
    }
    tx_.consume(w.bytes);
  }

  // TLS -> ciphertext staging.
  for (;;) {
    if (cipher_tx_.writable_size() == 0 && !cipher_tx_.ensure_writable(1)) {
      break;
    }
    const IoResult t =
        tls_.take_encrypted(cipher_tx_.writable(), cipher_tx_.writable_size());
    if (t.status != IoStatus::kOk) {
      break;
    }
    cipher_tx_.commit(t.bytes);
  }

  // Ciphertext -> socket.
  while (!cipher_tx_.empty()) {
    const IoResult w =
        socket_.write(cipher_tx_.readable(), cipher_tx_.readable_size());
    if (w.status == IoStatus::kWouldBlock) {
      return true;
    }
    if (w.status != IoStatus::kOk) {
      fail(w.status == IoStatus::kClosed
               ? "peer closed while writing"
               : "socket write failed: " + std::string(std::strerror(w.error)));
      return false;
    }
    cipher_tx_.consume(w.bytes);
    stats_.bytes_sent += w.bytes;
  }
  return true;
}

bool WsConnection::fill_rx() {
  if (!config_.use_tls) {
    for (;;) {
      if (!rx_.ensure_writable(4096)) {
        fail("receive buffer full: a message exceeds rx_buffer_bytes");
        return false;
      }
      const IoResult r = socket_.read(rx_.writable(), rx_.writable_size());
      if (r.status == IoStatus::kWouldBlock) {
        return true;
      }
      if (r.status == IoStatus::kClosed) {
        fail("peer closed the connection");
        return false;
      }
      if (r.status != IoStatus::kOk) {
        fail("socket read failed: " + std::string(std::strerror(r.error)));
        return false;
      }
      rx_.commit(r.bytes);
      stats_.bytes_received += r.bytes;
    }
  }

  // Socket -> ciphertext staging -> TLS.
  bool peer_closed = false;
  for (;;) {
    if (!cipher_rx_.ensure_writable(4096)) {
      break;
    }
    const IoResult r =
        socket_.read(cipher_rx_.writable(), cipher_rx_.writable_size());
    if (r.status == IoStatus::kWouldBlock) {
      break;
    }
    if (r.status == IoStatus::kClosed) {
      peer_closed = true;
      break;
    }
    if (r.status != IoStatus::kOk) {
      fail("socket read failed: " + std::string(std::strerror(r.error)));
      return false;
    }
    cipher_rx_.commit(r.bytes);
    stats_.bytes_received += r.bytes;
  }

  while (!cipher_rx_.empty()) {
    const IoResult f =
        tls_.feed_encrypted(cipher_rx_.readable(), cipher_rx_.readable_size());
    if (f.status != IoStatus::kOk) {
      break;
    }
    cipher_rx_.consume(f.bytes);
  }

  // TLS -> plaintext. Only meaningful once the handshake is done; before that
  // SSL_read would drive the handshake itself, which handshake() already does.
  if (tls_.established()) {
    for (;;) {
      if (!rx_.ensure_writable(4096)) {
        fail("receive buffer full: a message exceeds rx_buffer_bytes");
        return false;
      }
      const IoResult p = tls_.read_plaintext(rx_.writable(), rx_.writable_size());
      if (p.status == IoStatus::kWouldBlock) {
        break;
      }
      if (p.status == IoStatus::kClosed) {
        peer_closed = true;
        break;
      }
      if (p.status != IoStatus::kOk) {
        fail("tls read failed: " + tls_.last_error());
        return false;
      }
      rx_.commit(p.bytes);
    }
  }

  if (peer_closed && rx_.empty()) {
    fail("peer closed the connection");
    return false;
  }
  return true;
}

bool WsConnection::pump_io() {
  if (!flush_tx()) {
    return false;
  }
  if (!fill_rx()) {
    return false;
  }
  // The handshake and any auto-pong may have produced more to send.
  return flush_tx();
}

}  // namespace axon::net
