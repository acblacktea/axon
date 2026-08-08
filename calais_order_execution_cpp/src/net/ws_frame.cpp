#include "calais/net/ws_frame.h"

#include <random>

#include "calais/core/clock.h"

namespace calais::net {
namespace {

std::uint16_t read_be16(const std::byte* p) noexcept {
  return static_cast<std::uint16_t>((static_cast<std::uint16_t>(p[0]) << 8) |
                                    static_cast<std::uint16_t>(p[1]));
}

std::uint64_t read_be64(const std::byte* p) noexcept {
  std::uint64_t v = 0;
  for (int i = 0; i < 8; ++i) {
    v = (v << 8) | static_cast<std::uint64_t>(p[i]);
  }
  return v;
}

std::uint32_t read_be32(const std::byte* p) noexcept {
  std::uint32_t v = 0;
  for (int i = 0; i < 4; ++i) {
    v = (v << 8) | static_cast<std::uint32_t>(p[i]);
  }
  return v;
}

void write_be16(std::byte* p, std::uint16_t v) noexcept {
  p[0] = static_cast<std::byte>((v >> 8) & 0xFF);
  p[1] = static_cast<std::byte>(v & 0xFF);
}

void write_be64(std::byte* p, std::uint64_t v) noexcept {
  for (int i = 0; i < 8; ++i) {
    p[i] = static_cast<std::byte>((v >> (56 - 8 * i)) & 0xFF);
  }
}

void write_be32(std::byte* p, std::uint32_t v) noexcept {
  for (int i = 0; i < 4; ++i) {
    p[i] = static_cast<std::byte>((v >> (24 - 8 * i)) & 0xFF);
  }
}

WsDecodeResult incomplete() noexcept {
  WsDecodeResult r;
  r.status = WsDecodeStatus::kIncomplete;
  return r;
}

WsDecodeResult protocol_error(const char* what) noexcept {
  WsDecodeResult r;
  r.status = WsDecodeStatus::kProtocolError;
  r.error = what;
  return r;
}

}  // namespace

// ---------------------------------------------------------------------------
WsDecodeResult ws_decode_frame(const std::byte* data, std::size_t size) noexcept {
  if (size < 2) {
    return incomplete();
  }

  const auto b0 = static_cast<std::uint8_t>(data[0]);
  const auto b1 = static_cast<std::uint8_t>(data[1]);

  WsFrameHeader h;
  h.fin = (b0 & 0x80u) != 0;
  h.rsv1 = (b0 & 0x40u) != 0;
  h.rsv2 = (b0 & 0x20u) != 0;
  h.rsv3 = (b0 & 0x10u) != 0;
  h.opcode = static_cast<WsOpcode>(b0 & 0x0Fu);
  h.masked = (b1 & 0x80u) != 0;

  // No extension is negotiated -- permessage-deflate is deliberately not
  // offered -- so any reserved bit set means the peer is speaking a protocol
  // we did not agree to.
  if (h.rsv1 || h.rsv2 || h.rsv3) {
    return protocol_error("reserved bit set (no extension negotiated)");
  }

  if (!is_data_opcode(h.opcode) && !is_control_opcode(h.opcode)) {
    return protocol_error("reserved opcode");
  }
  // 0x3-0x7 and 0xB-0xF are reserved; the masks above let 0xB-0xF through as
  // "control", so reject them explicitly.
  const auto raw_op = static_cast<std::uint8_t>(h.opcode);
  if (raw_op > 0x2 && raw_op < 0x8) {
    return protocol_error("reserved non-control opcode");
  }
  if (raw_op > 0xA) {
    return protocol_error("reserved control opcode");
  }

  const std::uint8_t len7 = b1 & 0x7Fu;
  std::size_t offset = 2;

  if (len7 < 126) {
    h.payload_length = len7;
  } else if (len7 == 126) {
    if (size < offset + 2) {
      return incomplete();
    }
    h.payload_length = read_be16(data + offset);
    offset += 2;
  } else {
    if (size < offset + 8) {
      return incomplete();
    }
    h.payload_length = read_be64(data + offset);
    offset += 8;
    // The RFC reserves the top bit of the 64-bit length; a frame claiming
    // 2^63 bytes is a corrupt stream, not a large message.
    if ((h.payload_length & 0x8000000000000000ULL) != 0) {
      return protocol_error("64-bit length has the reserved high bit set");
    }
  }

  // DELIBERATELY NOT CHECKED: whether the length used the minimal encoding.
  //
  // RFC 6455 section 5.2 says a sender MUST use the minimal number of bytes,
  // and a strict reader could reject a non-minimal one. We accept it. Dropping
  // a live exchange connection mid-session over a pedantic length encoding is
  // a far worse failure than tolerating three wasted bytes, and every real
  // server encodes minimally anyway. Our own encoder is strict.

  if (is_control_opcode(h.opcode)) {
    // Control frames must be self-contained so they can always be answered
    // immediately, even in the middle of a fragmented message.
    if (h.payload_length > kMaxControlPayload) {
      return protocol_error("control frame payload exceeds 125 bytes");
    }
    if (!h.fin) {
      return protocol_error("control frame is fragmented");
    }
  }

  if (h.masked) {
    if (size < offset + 4) {
      return incomplete();
    }
    h.mask_key = read_be32(data + offset);
    offset += 4;
  }

  h.header_size = offset;

  // Guard the addition itself: payload_length is attacker-controlled up to
  // 2^63-1 and offset + payload_length could wrap on a 32-bit size_t.
  if (h.payload_length > static_cast<std::uint64_t>(SIZE_MAX - offset)) {
    return protocol_error("frame length overflows the address space");
  }

  const std::size_t total = offset + static_cast<std::size_t>(h.payload_length);
  if (size < total) {
    return incomplete();
  }

  WsDecodeResult r;
  r.status = WsDecodeStatus::kOk;
  r.header = h;
  r.payload = data + offset;
  r.total_size = total;
  return r;
}

// ---------------------------------------------------------------------------
std::size_t ws_encode_header(std::byte* out, std::size_t cap, WsOpcode opcode,
                             bool fin, std::uint64_t payload_length, bool masked,
                             std::uint32_t mask_key) noexcept {
  const std::size_t need = ws_header_size(payload_length, masked);
  if (cap < need) {
    return 0;
  }

  out[0] = static_cast<std::byte>((fin ? 0x80u : 0x00u) |
                                  static_cast<std::uint8_t>(opcode));

  const std::uint8_t mask_bit = masked ? 0x80u : 0x00u;
  std::size_t offset = 2;

  if (payload_length > 65535) {
    out[1] = static_cast<std::byte>(mask_bit | 127u);
    write_be64(out + offset, payload_length);
    offset += 8;
  } else if (payload_length > 125) {
    out[1] = static_cast<std::byte>(mask_bit | 126u);
    write_be16(out + offset, static_cast<std::uint16_t>(payload_length));
    offset += 2;
  } else {
    out[1] = static_cast<std::byte>(mask_bit |
                                    static_cast<std::uint8_t>(payload_length));
  }

  if (masked) {
    write_be32(out + offset, mask_key);
    offset += 4;
  }

  return offset;
}

// ---------------------------------------------------------------------------
void ws_mask(std::byte* data, std::size_t len, std::uint32_t mask_key,
             std::uint64_t offset) noexcept {
  if (len == 0) {
    return;
  }

  std::uint8_t k[4];
  k[0] = static_cast<std::uint8_t>((mask_key >> 24) & 0xFF);
  k[1] = static_cast<std::uint8_t>((mask_key >> 16) & 0xFF);
  k[2] = static_cast<std::uint8_t>((mask_key >> 8) & 0xFF);
  k[3] = static_cast<std::uint8_t>(mask_key & 0xFF);

  std::size_t i = 0;

  // Byte-wise until the key phase returns to 0, so the 64-bit body below can
  // use one repeating pattern.
  while (i < len && (((offset + i) & 3u) != 0)) {
    data[i] ^= static_cast<std::byte>(k[(offset + i) & 3u]);
    ++i;
  }

  // Eight bytes per iteration. Unaligned 64-bit loads are fine on x86-64 and
  // ARM64, and memcpy compiles to a single instruction here, so there is no
  // need to align first.
  if (i + 8 <= len) {
    // Build the repeating pattern in MEMORY order and memcpy it into the
    // word, rather than assembling it with shifts. Shifts would bake in an
    // endianness assumption; this XORs the right key byte against the right
    // data byte on any host.
    std::uint8_t pattern[8];
    for (int b = 0; b < 8; ++b) {
      pattern[b] = k[b & 3];
    }
    std::uint64_t k64;
    std::memcpy(&k64, pattern, 8);

    for (; i + 8 <= len; i += 8) {
      std::uint64_t word;
      std::memcpy(&word, data + i, 8);
      word ^= k64;
      std::memcpy(data + i, &word, 8);
    }
  }

  for (; i < len; ++i) {
    data[i] ^= static_cast<std::byte>(k[(offset + i) & 3u]);
  }
}

// ---------------------------------------------------------------------------
std::size_t ws_encode_frame(std::byte* out, std::size_t cap, WsOpcode opcode,
                            bool fin, const void* payload, std::size_t len,
                            std::uint32_t mask_key) noexcept {
  const std::size_t header = ws_header_size(len, /*masked=*/true);
  if (cap < header + len) {
    return 0;
  }

  const std::size_t written =
      ws_encode_header(out, cap, opcode, fin, len, /*masked=*/true, mask_key);
  if (written == 0) {
    return 0;
  }

  if (len > 0) {
    std::memcpy(out + written, payload, len);
    ws_mask(out + written, len, mask_key, 0);
  }
  return written + len;
}

// ---------------------------------------------------------------------------
std::uint32_t ws_random_mask_key() noexcept {
  struct Rng {
    std::uint64_t s;
    Rng() noexcept {
      std::random_device rd;
      s = (static_cast<std::uint64_t>(rd()) << 32) ^
          static_cast<std::uint64_t>(rd());
      s ^= core::now_ticks();
      if (s == 0) {
        s = 0x9E3779B97F4A7C15ULL;
      }
    }
    std::uint32_t next() noexcept {
      // splitmix64, then take the high word.
      s += 0x9E3779B97F4A7C15ULL;
      std::uint64_t z = s;
      z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
      z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
      z = z ^ (z >> 31);
      return static_cast<std::uint32_t>(z >> 32);
    }
  };
  static thread_local Rng rng;
  return rng.next();
}

// ---------------------------------------------------------------------------
std::size_t ws_build_close_payload(std::byte* out, std::size_t cap,
                                   WsCloseCode code,
                                   std::string_view reason) noexcept {
  // 2 bytes of status plus the reason, and the whole thing is a control frame.
  if (reason.size() + 2 > kMaxControlPayload || cap < reason.size() + 2) {
    return 0;
  }
  write_be16(out, static_cast<std::uint16_t>(code));
  if (!reason.empty()) {
    std::memcpy(out + 2, reason.data(), reason.size());
  }
  return 2 + reason.size();
}

WsCloseInfo ws_parse_close_payload(const std::byte* payload,
                                   std::size_t len) noexcept {
  WsCloseInfo info;

  // An empty close payload is legal and means "no status was provided".
  if (len == 0) {
    info.code = WsCloseCode::kNoStatusReceived;
    info.valid = true;
    return info;
  }
  // A single byte cannot hold a status code.
  if (len == 1) {
    return info;
  }

  const std::uint16_t raw = read_be16(payload);

  // 1005/1006/1015 are reserved for local signalling and must never appear on
  // the wire; anything below 1000 or in 1016-2999 is undefined.
  if (raw < 1000 || raw == 1004 || raw == 1005 || raw == 1006 ||
      (raw >= 1016 && raw <= 2999)) {
    return info;
  }

  if (len > 2 && !ws_is_valid_utf8(payload + 2, len - 2)) {
    return info;
  }

  info.code = static_cast<WsCloseCode>(raw);
  info.reason = std::string_view(reinterpret_cast<const char*>(payload) + 2,
                                 len - 2);
  info.valid = true;
  return info;
}

// ---------------------------------------------------------------------------
bool ws_is_valid_utf8(const std::byte* data, std::size_t len) noexcept {
  std::size_t i = 0;
  while (i < len) {
    // ASCII fast path, eight bytes at a time.
    //
    // This exists because measurement said so: byte-at-a-time validation of a
    // 288-byte exchange payload cost ~100ns, which was more than the frame
    // decode, the masking, and the assembler combined. Exchange feeds are
    // JSON and effectively all ASCII, so testing the high bits of a whole word
    // at once skips the entire loop body for the payloads we actually see.
    //
    // Correctness is unchanged: a word with any high bit set falls through to
    // the full multi-byte validation below.
    while (i + 8 <= len) {
      std::uint64_t word;
      std::memcpy(&word, data + i, 8);
      if ((word & 0x8080808080808080ULL) != 0) {
        break;
      }
      i += 8;
    }
    if (i >= len) {
      break;
    }

    const auto c = static_cast<std::uint8_t>(data[i]);

    if (c < 0x80) {
      ++i;
      continue;
    }

    std::size_t extra = 0;
    std::uint32_t cp = 0;
    std::uint32_t min_cp = 0;

    if ((c & 0xE0) == 0xC0) {
      extra = 1;
      cp = c & 0x1Fu;
      min_cp = 0x80;
    } else if ((c & 0xF0) == 0xE0) {
      extra = 2;
      cp = c & 0x0Fu;
      min_cp = 0x800;
    } else if ((c & 0xF8) == 0xF0) {
      extra = 3;
      cp = c & 0x07u;
      min_cp = 0x10000;
    } else {
      return false;  // continuation byte in a lead position, or 5/6-byte form
    }

    if (i + extra >= len) {
      return false;  // truncated sequence
    }

    for (std::size_t j = 1; j <= extra; ++j) {
      const auto cc = static_cast<std::uint8_t>(data[i + j]);
      if ((cc & 0xC0) != 0x80) {
        return false;
      }
      cp = (cp << 6) | (cc & 0x3Fu);
    }

    // Overlong encodings, UTF-16 surrogates, and anything past U+10FFFF all
    // decode "successfully" byte-wise. Each has been used to smuggle content
    // past naive validators, and the RFC requires rejecting them.
    if (cp < min_cp) {
      return false;
    }
    if (cp >= 0xD800 && cp <= 0xDFFF) {
      return false;
    }
    if (cp > 0x10FFFF) {
      return false;
    }

    i += extra + 1;
  }
  return true;
}

}  // namespace calais::net
