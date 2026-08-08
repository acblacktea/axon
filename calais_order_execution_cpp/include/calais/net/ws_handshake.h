// RFC 6455 opening handshake, client side.
//
// Runs once per connection, so none of it is performance-sensitive and all of
// it is allowed to allocate. It IS correctness-sensitive: a client that
// accepts a response it should have rejected will happily start framing
// against something that is not a WebSocket server, and the failure surfaces
// later as unexplained garbage rather than as a failed connect.
//
// So the validation here is strict in the one direction that matters --
// rejecting anything unexpected -- while the frame decoder is deliberately
// lenient about harmless deviations. Handshake failures are cheap (reconnect);
// mid-session failures are not.
//
// NO EXTENSIONS AND NO SUBPROTOCOLS ARE OFFERED. permessage-deflate in
// particular is not requested, because compressing a 200-byte order costs tens
// of microseconds on a colocated link and saves nothing. A server that
// nonetheless returns Sec-WebSocket-Extensions is rejected: it would be
// framing with RSV1 set, which the decoder cannot interpret.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace calais::net {

struct WsHandshakeRequest {
  // Value for the Host header, including the port when it is not the scheme
  // default: "www.deribit.com" or "127.0.0.1:8080".
  std::string host;
  // Request target, e.g. "/ws/api/v2". Must begin with '/'.
  std::string target = "/";
  // Appended verbatim. Where an exchange's auth header would go.
  std::vector<std::pair<std::string, std::string>> extra_headers;
};

struct WsClientHandshake {
  // The bytes to send, terminated by a blank line.
  std::string request;
  // The generated Sec-WebSocket-Key (base64 of 16 random bytes).
  std::string key;
  // base64(SHA1(key + RFC 6455 GUID)) -- what the server must echo back.
  std::string expected_accept;
};

// Generates a random nonce and builds the request.
WsClientHandshake ws_build_handshake(const WsHandshakeRequest& req);

// Deterministic variant, for tests and for reproducing a captured session.
WsClientHandshake ws_build_handshake(const WsHandshakeRequest& req,
                                     const std::byte (&nonce)[16]);

// Computes the Sec-WebSocket-Accept value a server must return for a given
// key. Exposed so tests can build correct server responses without
// reimplementing it.
std::string ws_compute_accept(std::string_view key);

enum class WsHandshakeStatus : std::uint8_t {
  // The response headers are not complete yet. Read more and call again.
  kIncomplete = 0,
  kOk = 1,
  // The response is complete and unacceptable. Close the connection.
  kFailed = 2,
};

struct WsHandshakeResponse {
  WsHandshakeStatus status = WsHandshakeStatus::kIncomplete;
  // Bytes occupied by the response headers, including the terminating blank
  // line. On kOk, everything after this is the first WebSocket frame -- which
  // a server is allowed to send in the same TCP segment, so the caller must
  // consume exactly this much and no more.
  std::size_t consumed = 0;
  // HTTP status line code, when one was parsed. Useful for logging a 401 or a
  // 429 distinctly from a malformed response.
  int http_status = 0;
  std::string error;
};

// Validates a server's response against the key we sent.
//
// `max_header_bytes` bounds how long we will wait for the terminating blank
// line before declaring failure, so a server that never sends one cannot make
// us buffer without limit.
WsHandshakeResponse ws_validate_handshake_response(
    std::string_view data, std::string_view expected_accept,
    std::size_t max_header_bytes = 16u * 1024u);

}  // namespace calais::net
