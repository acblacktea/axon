// WebSocket message assembly: fragments in, whole messages out.
//
// Two things make this more than a concatenation loop, and both are places
// real clients get it wrong:
//
//   1. CONTROL FRAMES INTERLEAVE. A ping can arrive between two fragments of a
//      a data message and must be answered immediately, without disturbing the
//      partially-assembled message. A naive "read until FIN" loop either
//      stalls the ping or corrupts the message.
//
//   2. THE UNFRAGMENTED CASE MUST NOT COPY. Every message an exchange sends us
//      in practice arrives as a single frame, and the whole point of this
//      layer is to hand the JSON parser a pointer into the receive buffer.
//      Only when a message genuinely spans frames does anything get copied,
//      into a reassembly buffer sized once at construction.
//
// So `feed()` reports where the payload is rather than owning it: for the
// common single-frame case that is the caller's buffer, untouched.
//
// Not thread safe; one assembler per connection.

#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

#include "axon/net/ws_frame.h"

namespace axon::net {

enum class WsEvent : std::uint8_t {
  // Nothing to report -- the frame was a non-final fragment and was buffered.
  kNone = 0,
  // A complete text or binary message. payload/payload_size are valid.
  kMessage = 1,
  // A ping arrived; the caller must send a pong echoing the payload.
  kPing = 2,
  kPong = 3,
  // The peer initiated a close. `close` is valid.
  kClose = 4,
  // The peer violated the protocol. Close the connection; `error` says why.
  kProtocolError = 5,
};

struct WsMessageEvent {
  WsEvent type = WsEvent::kNone;

  // For kMessage: the assembled message. Points either into the caller's
  // buffer (single-frame message, zero copy) or into the assembler's
  // reassembly buffer (fragmented). Valid until the next feed() call.
  const std::byte* payload = nullptr;
  std::size_t payload_size = 0;

  // The message's original opcode: kText or kBinary. For a fragmented message
  // this is the opcode of the FIRST frame, not the continuation frames'.
  WsOpcode opcode = WsOpcode::kBinary;

  WsCloseInfo close;
  const char* error = nullptr;
};

class WsMessageAssembler {
 public:
  // `max_message_size` bounds reassembly. A peer that keeps sending
  // non-final fragments would otherwise grow the buffer without limit, which
  // is a trivial way to exhaust memory on a process that must not die.
  // Exceeding it is a protocol error, not a reallocation.
  explicit WsMessageAssembler(std::size_t max_message_size = 8u << 20)
      : max_message_size_(max_message_size) {}

  // Reserve the reassembly buffer up front so a fragmented message does not
  // allocate on the hot path. Optional -- without it the first fragmented
  // message pays for the growth.
  void reserve(std::size_t bytes) { reassembly_.reserve(bytes); }

  // Consumes one decoded frame. `payload` must already be unmasked.
  WsMessageEvent feed(const WsFrameHeader& header, const std::byte* payload);

  // True while a fragmented message is partially assembled.
  bool in_fragmented_message() const noexcept { return fragment_open_; }

  std::size_t buffered_bytes() const noexcept { return reassembly_.size(); }

  // Number of messages that had to be reassembled from multiple frames. If
  // this is climbing in production the receive buffer is probably too small,
  // since it is the socket read size that decides where fragments land.
  std::uint64_t fragmented_messages() const noexcept {
    return fragmented_messages_;
  }

  void reset() noexcept {
    reassembly_.clear();
    fragment_open_ = false;
  }

 private:
  WsMessageEvent error(const char* what) noexcept;

  std::vector<std::byte> reassembly_;
  std::size_t max_message_size_;
  WsOpcode fragment_opcode_ = WsOpcode::kBinary;
  bool fragment_open_ = false;
  std::uint64_t fragmented_messages_ = 0;
};

}  // namespace axon::net
