// Template members of WsConnection. Included from ws_connection.h.
//
// These are templates on the handler rather than std::function so that
// on_message inlines into the poll loop -- an indirect call per message would
// defeat the point of handing out a pointer into the receive buffer.

#pragma once

namespace axon::net {

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
