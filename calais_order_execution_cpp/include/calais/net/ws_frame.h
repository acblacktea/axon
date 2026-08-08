// RFC 6455 WebSocket framing, client side.
//
// Hand-rolled rather than Boost.Beast. Beast is the right answer at a ~100us
// internal budget; at 10us it is not, for one reason that has nothing to do
// with its steady-state speed: its dynamic_buffer allocates, and an allocation
// on the receive path puts an unbounded malloc tail inside the trade decision.
// The framing itself is genuinely small -- a client needs a fraction of the
// RFC, and that fraction is this file.
//
// What a client actually needs, and what is deliberately absent:
//
//   NEEDED    text/binary/close/ping/pong, fragmentation reassembly, client
//             masking, the three payload length encodings.
//
//   ABSENT    the server role (we never accept connections) and
//             permessage-deflate. Compression is not an oversight: deflating a
//             200-byte order costs tens of microseconds and buys nothing on a
//             colocated link. RSV1 arriving set is therefore a protocol error,
//             not something to negotiate.
//
// Decoding is zero-copy: ws_decode_frame() returns a pointer into the caller's
// buffer. Nothing is allocated anywhere in this file.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "calais/core/platform.h"

namespace calais::net {

enum class WsOpcode : std::uint8_t {
  kContinuation = 0x0,
  kText = 0x1,
  kBinary = 0x2,
  kClose = 0x8,
  kPing = 0x9,
  kPong = 0xA,
};

constexpr bool is_control_opcode(WsOpcode op) noexcept {
  return (static_cast<std::uint8_t>(op) & 0x8u) != 0;
}

constexpr bool is_data_opcode(WsOpcode op) noexcept {
  return op == WsOpcode::kContinuation || op == WsOpcode::kText ||
         op == WsOpcode::kBinary;
}

// RFC 6455 section 7.4.1.
enum class WsCloseCode : std::uint16_t {
  kNormal = 1000,
  kGoingAway = 1001,
  kProtocolError = 1002,
  kUnsupportedData = 1003,
  kNoStatusReceived = 1005,  // never sent on the wire
  kAbnormalClosure = 1006,   // never sent on the wire
  kInvalidPayload = 1007,
  kPolicyViolation = 1008,
  kMessageTooBig = 1009,
  kMandatoryExtension = 1010,
  kInternalError = 1011,
  kTlsHandshake = 1015,  // never sent on the wire
};

// Control frame payloads are capped by the RFC so they can always be handled
// without buffering.
inline constexpr std::size_t kMaxControlPayload = 125;

// Largest frame header: 2 fixed bytes + 8 length bytes + 4 mask bytes.
inline constexpr std::size_t kMaxFrameHeaderSize = 14;

struct WsFrameHeader {
  bool fin = false;
  bool rsv1 = false;
  bool rsv2 = false;
  bool rsv3 = false;
  WsOpcode opcode = WsOpcode::kContinuation;
  bool masked = false;
  std::uint64_t payload_length = 0;
  // Big-endian as it appears on the wire: byte 0 of the key is the high byte.
  std::uint32_t mask_key = 0;
  std::size_t header_size = 0;
};

enum class WsDecodeStatus : std::uint8_t {
  kOk = 0,
  // Not enough bytes yet. Read more and call again with the longer buffer.
  kIncomplete = 1,
  // The peer violated the protocol. The connection must be closed; retrying
  // the parse cannot help.
  kProtocolError = 2,
};

struct WsDecodeResult {
  WsDecodeStatus status = WsDecodeStatus::kIncomplete;
  WsFrameHeader header;
  // Points into the buffer passed to ws_decode_frame. Still masked if
  // header.masked is set.
  const std::byte* payload = nullptr;
  // header_size + payload_length. What to consume() on success.
  std::size_t total_size = 0;
  // Static string, valid forever. Null unless status is kProtocolError.
  const char* error = nullptr;
};

// Parses one frame from the front of [data, data+size).
//
// Never reads past `size` and never writes anything. Safe to call on a
// partially-received buffer -- that is the normal case, and it returns
// kIncomplete.
WsDecodeResult ws_decode_frame(const std::byte* data, std::size_t size) noexcept;

// Bytes a header will occupy for the given payload length.
constexpr std::size_t ws_header_size(std::uint64_t payload_length,
                                     bool masked) noexcept {
  std::size_t n = 2;
  if (payload_length > 65535) {
    n += 8;
  } else if (payload_length > 125) {
    n += 2;
  }
  if (masked) {
    n += 4;
  }
  return n;
}

// Writes a frame header. Returns bytes written, or 0 if `cap` is too small.
//
// Always emits the minimal length encoding, as the RFC requires of a sender.
// (The decoder is deliberately more forgiving -- see ws_frame.cpp.)
std::size_t ws_encode_header(std::byte* out, std::size_t cap, WsOpcode opcode,
                             bool fin, std::uint64_t payload_length,
                             bool masked, std::uint32_t mask_key) noexcept;

// XORs `len` bytes with the 4-byte masking key, in place.
//
// `offset` is the position of data[0] within the frame's payload, so a payload
// can be masked in pieces without losing the key phase.
//
// Processes 8 bytes per iteration once the key phase aligns. For a 200-byte
// order this is a handful of nanoseconds.
void ws_mask(std::byte* data, std::size_t len, std::uint32_t mask_key,
             std::uint64_t offset = 0) noexcept;

// Writes a complete masked frame (header + masked payload) into `out`.
// Returns total bytes written, or 0 if `cap` is too small.
//
// Client frames MUST be masked (RFC 6455 section 5.3), so there is no unmasked
// variant here. Under TLS the masking is pure defence in depth -- it exists to
// stop cache-poisoning intermediaries, which a TLS tunnel already prevents --
// but servers are entitled to close the connection over a missing mask.
std::size_t ws_encode_frame(std::byte* out, std::size_t cap, WsOpcode opcode,
                            bool fin, const void* payload, std::size_t len,
                            std::uint32_t mask_key) noexcept;

// A mask key from a thread-local PRNG seeded once from std::random_device.
//
// The RFC asks for an unpredictable key. This is not cryptographic, and does
// not need to be: the threat model masking addresses is a confused HTTP proxy,
// and every connection we open is wrapped in TLS where no such proxy can see
// the frame at all.
std::uint32_t ws_random_mask_key() noexcept;

// Builds a close frame payload: 2-byte big-endian code, then a UTF-8 reason.
// Returns bytes written, or 0 if the reason is too long to fit the RFC's
// 125-byte control payload limit.
std::size_t ws_build_close_payload(std::byte* out, std::size_t cap,
                                   WsCloseCode code,
                                   std::string_view reason = {}) noexcept;

struct WsCloseInfo {
  WsCloseCode code = WsCloseCode::kNoStatusReceived;
  std::string_view reason;
  bool valid = false;
};

// Parses a received close frame payload. An empty payload is legal and means
// "no status", per the RFC.
WsCloseInfo ws_parse_close_payload(const std::byte* payload,
                                   std::size_t len) noexcept;

// Validates UTF-8, as required for text frames and close reasons.
//
// Rejects overlong encodings, surrogates, and anything above U+10FFFF -- all
// of which are well-formed-looking byte sequences that a naive length-only
// check would pass.
bool ws_is_valid_utf8(const std::byte* data, std::size_t len) noexcept;

}  // namespace calais::net
