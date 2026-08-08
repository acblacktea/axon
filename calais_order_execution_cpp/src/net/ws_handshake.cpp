#include "calais/net/ws_handshake.h"

#include <algorithm>
#include <cctype>
#include <random>

#include "calais/core/clock.h"
#include "calais/net/crypto_lite.h"

namespace calais::net {
namespace {

// RFC 6455 section 1.3. Fixed by the spec; not a secret.
constexpr std::string_view kWsGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

char lower(char c) noexcept {
  return static_cast<char>(
      std::tolower(static_cast<unsigned char>(c)));
}

bool iequals(std::string_view a, std::string_view b) noexcept {
  if (a.size() != b.size()) {
    return false;
  }
  for (std::size_t i = 0; i < a.size(); ++i) {
    if (lower(a[i]) != lower(b[i])) {
      return false;
    }
  }
  return true;
}

std::string_view trim(std::string_view s) noexcept {
  std::size_t b = 0;
  while (b < s.size() && (s[b] == ' ' || s[b] == '\t')) {
    ++b;
  }
  std::size_t e = s.size();
  while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t')) {
    --e;
  }
  return s.substr(b, e - b);
}

// Connection: keep-alive, Upgrade  -- the token we want may be one of several.
bool has_token(std::string_view header_value, std::string_view token) noexcept {
  std::size_t pos = 0;
  while (pos <= header_value.size()) {
    const std::size_t comma = header_value.find(',', pos);
    const std::string_view piece =
        trim(header_value.substr(pos, comma == std::string_view::npos
                                          ? std::string_view::npos
                                          : comma - pos));
    if (iequals(piece, token)) {
      return true;
    }
    if (comma == std::string_view::npos) {
      break;
    }
    pos = comma + 1;
  }
  return false;
}

WsHandshakeResponse fail(std::string why, std::size_t consumed = 0,
                         int http_status = 0) {
  WsHandshakeResponse r;
  r.status = WsHandshakeStatus::kFailed;
  r.error = std::move(why);
  r.consumed = consumed;
  r.http_status = http_status;
  return r;
}

}  // namespace

// ---------------------------------------------------------------------------
std::string ws_compute_accept(std::string_view key) {
  std::string input;
  input.reserve(key.size() + kWsGuid.size());
  input.append(key);
  input.append(kWsGuid);
  const Sha1Digest d = sha1(input);
  return base64_encode(d.data(), d.size());
}

// ---------------------------------------------------------------------------
WsClientHandshake ws_build_handshake(const WsHandshakeRequest& req,
                                     const std::byte (&nonce)[16]) {
  WsClientHandshake out;
  out.key = base64_encode(nonce, sizeof(nonce));
  out.expected_accept = ws_compute_accept(out.key);

  std::string& r = out.request;
  r.reserve(256);
  r += "GET ";
  r += req.target.empty() ? "/" : req.target;
  r += " HTTP/1.1\r\n";
  r += "Host: ";
  r += req.host;
  r += "\r\n";
  r += "Upgrade: websocket\r\n";
  r += "Connection: Upgrade\r\n";
  r += "Sec-WebSocket-Key: ";
  r += out.key;
  r += "\r\n";
  r += "Sec-WebSocket-Version: 13\r\n";

  // No Sec-WebSocket-Extensions header at all: not offering compression is a
  // deliberate latency decision, see ws_handshake.h.

  for (const auto& [name, value] : req.extra_headers) {
    r += name;
    r += ": ";
    r += value;
    r += "\r\n";
  }
  r += "\r\n";
  return out;
}

WsClientHandshake ws_build_handshake(const WsHandshakeRequest& req) {
  // RFC 6455 section 4.1 wants a nonce "selected randomly". It is not a
  // secret: it exists so a cached HTTP response cannot be replayed as a
  // successful handshake. random_device once per connection is ample.
  std::byte nonce[16];
  std::random_device rd;
  for (std::size_t i = 0; i < sizeof(nonce); i += 4) {
    const std::uint32_t v = rd();
    nonce[i] = static_cast<std::byte>(v & 0xFF);
    nonce[i + 1] = static_cast<std::byte>((v >> 8) & 0xFF);
    nonce[i + 2] = static_cast<std::byte>((v >> 16) & 0xFF);
    nonce[i + 3] = static_cast<std::byte>((v >> 24) & 0xFF);
  }
  return ws_build_handshake(req, nonce);
}

// ---------------------------------------------------------------------------
WsHandshakeResponse ws_validate_handshake_response(
    std::string_view data, std::string_view expected_accept,
    std::size_t max_header_bytes) {
  const std::size_t end = data.find("\r\n\r\n");
  if (end == std::string_view::npos) {
    if (data.size() > max_header_bytes) {
      return fail("response headers exceeded " +
                  std::to_string(max_header_bytes) + " bytes without a blank line");
    }
    WsHandshakeResponse r;
    r.status = WsHandshakeStatus::kIncomplete;
    return r;
  }

  const std::size_t consumed = end + 4;
  const std::string_view headers = data.substr(0, end);

  // --- status line ---------------------------------------------------------
  const std::size_t first_eol = headers.find("\r\n");
  const std::string_view status_line =
      first_eol == std::string_view::npos ? headers : headers.substr(0, first_eol);

  if (!status_line.starts_with("HTTP/1.1 ") &&
      !status_line.starts_with("HTTP/1.0 ")) {
    return fail("not an HTTP response", consumed);
  }
  if (status_line.size() < 12) {
    return fail("malformed status line", consumed);
  }

  int code = 0;
  for (std::size_t i = 9; i < 12; ++i) {
    const char c = status_line[i];
    if (c < '0' || c > '9') {
      return fail("malformed status code", consumed);
    }
    code = code * 10 + (c - '0');
  }

  if (code != 101) {
    // Surface the code: a 401 from a bad API key and a 429 from rate limiting
    // need very different responses from the reconnect logic, and both look
    // identical if we only report "handshake failed".
    return fail("server refused the upgrade with HTTP " + std::to_string(code),
                consumed, code);
  }

  // --- headers -------------------------------------------------------------
  bool saw_upgrade = false;
  bool saw_connection = false;
  bool saw_accept = false;

  std::size_t pos = (first_eol == std::string_view::npos) ? headers.size()
                                                          : first_eol + 2;
  while (pos < headers.size()) {
    std::size_t eol = headers.find("\r\n", pos);
    if (eol == std::string_view::npos) {
      eol = headers.size();
    }
    const std::string_view line = headers.substr(pos, eol - pos);
    pos = eol + 2;

    if (line.empty()) {
      continue;
    }
    const std::size_t colon = line.find(':');
    if (colon == std::string_view::npos) {
      return fail("malformed header line", consumed, code);
    }
    const std::string_view name = trim(line.substr(0, colon));
    const std::string_view value = trim(line.substr(colon + 1));

    if (iequals(name, "Upgrade")) {
      if (!iequals(value, "websocket")) {
        return fail("Upgrade header is not 'websocket'", consumed, code);
      }
      saw_upgrade = true;
    } else if (iequals(name, "Connection")) {
      if (!has_token(value, "Upgrade")) {
        return fail("Connection header lacks the Upgrade token", consumed, code);
      }
      saw_connection = true;
    } else if (iequals(name, "Sec-WebSocket-Accept")) {
      // Case-SENSITIVE: this is a base64 digest, not a token. A
      // case-insensitive compare here would accept a wrong value.
      if (value != expected_accept) {
        return fail("Sec-WebSocket-Accept does not match the key we sent",
                    consumed, code);
      }
      saw_accept = true;
    } else if (iequals(name, "Sec-WebSocket-Extensions")) {
      if (!value.empty()) {
        // We offered none. Accepting one would mean the server frames with
        // RSV bits set, which the decoder rejects -- better to fail here,
        // where the error is legible.
        return fail("server returned an extension we did not offer: " +
                        std::string(value),
                    consumed, code);
      }
    } else if (iequals(name, "Sec-WebSocket-Protocol")) {
      if (!value.empty()) {
        return fail("server returned a subprotocol we did not offer: " +
                        std::string(value),
                    consumed, code);
      }
    }
  }

  if (!saw_upgrade) {
    return fail("response is missing the Upgrade header", consumed, code);
  }
  if (!saw_connection) {
    return fail("response is missing the Connection header", consumed, code);
  }
  if (!saw_accept) {
    return fail("response is missing Sec-WebSocket-Accept", consumed, code);
  }

  WsHandshakeResponse r;
  r.status = WsHandshakeStatus::kOk;
  r.consumed = consumed;
  r.http_status = code;
  return r;
}

}  // namespace calais::net
