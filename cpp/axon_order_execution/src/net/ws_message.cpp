#include "axon/net/ws_message.h"

#include <cstring>

namespace axon::net {

WsMessageEvent WsMessageAssembler::error(const char* what) noexcept {
  WsMessageEvent e;
  e.type = WsEvent::kProtocolError;
  e.error = what;
  // Drop any partial state: after a protocol error the connection is going
  // away, and leaving half a message buffered would confuse a reader that
  // inspects buffered_bytes() while logging the failure.
  reassembly_.clear();
  fragment_open_ = false;
  return e;
}

WsMessageEvent WsMessageAssembler::feed(const WsFrameHeader& header,
                                        const std::byte* payload) {
  WsMessageEvent event;

  // --- control frames ------------------------------------------------------
  //
  // Handled first and independently: they may arrive between fragments of a
  // data message and must not disturb it. ws_decode_frame has already
  // guaranteed they are unfragmented and <= 125 bytes.
  if (is_control_opcode(header.opcode)) {
    const auto len = static_cast<std::size_t>(header.payload_length);
    switch (header.opcode) {
      case WsOpcode::kPing:
        event.type = WsEvent::kPing;
        event.payload = payload;
        event.payload_size = len;
        return event;

      case WsOpcode::kPong:
        event.type = WsEvent::kPong;
        event.payload = payload;
        event.payload_size = len;
        return event;

      case WsOpcode::kClose: {
        const WsCloseInfo info = ws_parse_close_payload(payload, len);
        if (!info.valid) {
          return error("malformed close frame payload");
        }
        event.type = WsEvent::kClose;
        event.close = info;
        event.payload = payload;
        event.payload_size = len;
        return event;
      }

      default:
        return error("unhandled control opcode");
    }
  }

  const auto len = static_cast<std::size_t>(header.payload_length);

  // --- continuation --------------------------------------------------------
  if (header.opcode == WsOpcode::kContinuation) {
    if (!fragment_open_) {
      return error("continuation frame with no message in progress");
    }
    if (reassembly_.size() + len > max_message_size_) {
      return error("message exceeds the configured maximum size");
    }
    reassembly_.insert(reassembly_.end(), payload, payload + len);

    if (!header.fin) {
      event.type = WsEvent::kNone;
      return event;
    }

    fragment_open_ = false;
    ++fragmented_messages_;

    if (fragment_opcode_ == WsOpcode::kText &&
        !ws_is_valid_utf8(reassembly_.data(), reassembly_.size())) {
      return error("text message is not valid UTF-8");
    }

    event.type = WsEvent::kMessage;
    event.payload = reassembly_.data();
    event.payload_size = reassembly_.size();
    event.opcode = fragment_opcode_;
    return event;
  }

  // --- text or binary ------------------------------------------------------
  if (fragment_open_) {
    return error("new data frame while a fragmented message is in progress");
  }

  if (header.fin) {
    // The common path, and the one that must not copy: hand back a pointer
    // straight into the caller's receive buffer.
    if (header.opcode == WsOpcode::kText && !ws_is_valid_utf8(payload, len)) {
      return error("text message is not valid UTF-8");
    }
    event.type = WsEvent::kMessage;
    event.payload = payload;
    event.payload_size = len;
    event.opcode = header.opcode;
    return event;
  }

  // First fragment of a multi-frame message.
  if (len > max_message_size_) {
    return error("message exceeds the configured maximum size");
  }
  reassembly_.clear();
  reassembly_.insert(reassembly_.end(), payload, payload + len);
  fragment_opcode_ = header.opcode;
  fragment_open_ = true;

  event.type = WsEvent::kNone;
  return event;
}

}  // namespace axon::net
