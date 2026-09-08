// SHA-1 and base64, implemented here rather than pulled from OpenSSL.
//
// Two reasons, neither of them "OpenSSL is bad":
//
//   1. These are needed by exactly one thing -- the Sec-WebSocket-Accept check
//      in the opening handshake, which runs once per connection. Linking a
//      crypto library into the framing layer for that would make the layer
//      that must stay trivially testable depend on the layer with the most
//      build-environment friction. OpenSSL arrives with TLS, where it is
//      genuinely required.
//
//   2. Both algorithms have authoritative published test vectors (RFC 3174 for
//      SHA-1, RFC 4648 for base64), so "did I implement it right" is a
//      question with a definitive answer rather than a judgement call.
//
// SHA-1 IS NOT USED AS A SECURITY PRIMITIVE HERE. RFC 6455's handshake uses it
// as a fixed transformation to prove the peer understood the WebSocket
// protocol, not to authenticate anything. Its collision weakness is
// irrelevant to that. Do not reach for this function for anything else --
// request signing uses HMAC-SHA256 from OpenSSL.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace axon::net {

using Sha1Digest = std::array<std::byte, 20>;
using Sha256Digest = std::array<std::byte, 32>;

Sha1Digest sha1(const void* data, std::size_t len) noexcept;

inline Sha1Digest sha1(std::string_view s) noexcept {
  return sha1(s.data(), s.size());
}

// SHA-256 and HMAC-SHA256.
//
// UNLIKE the SHA-1 above, these ARE security primitives: Bybit and OKX
// authenticate their private WebSocket streams with an HMAC over a timestamped
// string, so a wrong implementation means a rejected login at best. Verified
// against the published FIPS 180-4 and RFC 4231 vectors.
//
// Still local rather than OpenSSL, for the same reason as SHA-1: they run once
// per connection, and keeping the venue layer free of a crypto library keeps
// it trivially buildable and testable. When TLS arrives it brings OpenSSL, and
// switching to EVP_MAC then is a one-file change.
Sha256Digest sha256(const void* data, std::size_t len) noexcept;

inline Sha256Digest sha256(std::string_view s) noexcept {
  return sha256(s.data(), s.size());
}

Sha256Digest hmac_sha256(std::string_view key, std::string_view message) noexcept;

// Lowercase hex. Bybit wants its signature in this form; OKX wants base64.
std::string to_hex(const void* data, std::size_t len);

template <std::size_t N>
std::string to_hex(const std::array<std::byte, N>& d) {
  return to_hex(d.data(), d.size());
}

// Standard base64 with '+' and '/' and '=' padding (RFC 4648 section 4).
std::string base64_encode(const void* data, std::size_t len);

inline std::string base64_encode(std::string_view s) {
  return base64_encode(s.data(), s.size());
}

// Returns false on invalid input: a bad character, bad length, or misplaced
// padding. Used only to sanity-check test fixtures, never on the hot path.
bool base64_decode(std::string_view in, std::string& out);

}  // namespace axon::net
