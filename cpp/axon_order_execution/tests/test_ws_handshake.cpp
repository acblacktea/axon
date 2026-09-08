#include "axon/net/ws_handshake.h"

#include <gtest/gtest.h>

#include <string>

#include "axon/net/crypto_lite.h"

using namespace axon::net;

namespace {

std::string hex(const Sha1Digest& d) {
  static const char* kHex = "0123456789abcdef";
  std::string s;
  for (std::byte b : d) {
    const auto v = static_cast<std::uint8_t>(b);
    s.push_back(kHex[v >> 4]);
    s.push_back(kHex[v & 0xF]);
  }
  return s;
}

// The example handshake from RFC 6455 section 1.3.
constexpr std::string_view kRfcKey = "dGhlIHNhbXBsZSBub25jZQ==";
constexpr std::string_view kRfcAccept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

std::string good_response(std::string_view accept) {
  return std::string("HTTP/1.1 101 Switching Protocols\r\n") +
         "Upgrade: websocket\r\n" + "Connection: Upgrade\r\n" +
         "Sec-WebSocket-Accept: " + std::string(accept) + "\r\n\r\n";
}

}  // namespace

// ---------------------------------------------------------------------------
// SHA-1, against the published RFC 3174 / FIPS 180-1 vectors.
// ---------------------------------------------------------------------------

TEST(Sha1, MatchesPublishedVectors) {
  EXPECT_EQ(hex(sha1("")), "da39a3ee5e6b4b0d3255bfef95601890afd80709");
  EXPECT_EQ(hex(sha1("abc")), "a9993e364706816aba3e25717850c26c9cd0d89d");
  EXPECT_EQ(hex(sha1("abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")),
            "84983e441c3bd26ebaae4aa1f95129e5e54670f1");
  EXPECT_EQ(hex(sha1("The quick brown fox jumps over the lazy dog")),
            "2fd4e1c67a2d28fced849ee1bb76e7391b93eb12");
}

TEST(Sha1, HandlesBlockBoundaries) {
  // The padding logic branches at 55/56 bytes (length field fits or does not)
  // and at 64 (a whole extra block). Each of these lengths exercises a
  // different path through it.
  const std::string a55(55, 'a');
  const std::string a56(56, 'a');
  const std::string a63(63, 'a');
  const std::string a64(64, 'a');
  const std::string a119(119, 'a');
  const std::string a120(120, 'a');

  EXPECT_EQ(hex(sha1(a55)), "c1c8bbdc22796e28c0e15163d20899b65621d65a");
  EXPECT_EQ(hex(sha1(a56)), "c2db330f6083854c99d4b5bfb6e8f29f201be699");
  EXPECT_EQ(hex(sha1(a63)), "03f09f5b158a7a8cdad920bddc29b81c18a551f5");
  EXPECT_EQ(hex(sha1(a64)), "0098ba824b5c16427bd7a1122a5a442a25ec644d");
  EXPECT_EQ(hex(sha1(a119)), "ee971065aaa017e0632a8ca6c77bb3bf8b1dfc56");
  EXPECT_EQ(hex(sha1(a120)), "f34c1488385346a55709ba056ddd08280dd4c6d6");
}

TEST(Sha1, LongInputSpanningManyBlocks) {
  const std::string million(1000000, 'a');
  EXPECT_EQ(hex(sha1(million)), "34aa973cd4c4daa4f61eeb2bdbad27316534016f");
}

// ---------------------------------------------------------------------------
// base64, against RFC 4648 section 10.
// ---------------------------------------------------------------------------

TEST(Base64, MatchesRfc4648Vectors) {
  EXPECT_EQ(base64_encode(""), "");
  EXPECT_EQ(base64_encode("f"), "Zg==");
  EXPECT_EQ(base64_encode("fo"), "Zm8=");
  EXPECT_EQ(base64_encode("foo"), "Zm9v");
  EXPECT_EQ(base64_encode("foob"), "Zm9vYg==");
  EXPECT_EQ(base64_encode("fooba"), "Zm9vYmE=");
  EXPECT_EQ(base64_encode("foobar"), "Zm9vYmFy");
}

TEST(Base64, RoundTrips) {
  for (const char* s : {"", "f", "fo", "foo", "foob", "fooba", "foobar",
                        "the quick brown fox", "\x00\x01\x02\xff"}) {
    const std::string encoded = base64_encode(std::string_view(s));
    std::string decoded;
    ASSERT_TRUE(base64_decode(encoded, decoded)) << encoded;
    EXPECT_EQ(decoded, std::string_view(s));
  }
}

TEST(Base64, RejectsMalformedInput) {
  std::string out;
  EXPECT_FALSE(base64_decode("Zm9vYg=", out)) << "length not a multiple of 4";
  EXPECT_FALSE(base64_decode("Zm9v!g==", out)) << "invalid character";
  EXPECT_FALSE(base64_decode("Z===", out)) << "too much padding";
  EXPECT_FALSE(base64_decode("Zm==Zm9v", out)) << "data after padding";
}

TEST(Base64, EncodesBinaryWithHighBits) {
  const std::byte data[] = {std::byte{0xFF}, std::byte{0xFE}, std::byte{0xFD}};
  EXPECT_EQ(base64_encode(data, sizeof(data)), "//79");
}

// ---------------------------------------------------------------------------
// Sec-WebSocket-Accept
// ---------------------------------------------------------------------------

TEST(WsHandshake, ComputeAcceptMatchesTheRfcExample) {
  EXPECT_EQ(ws_compute_accept(kRfcKey), kRfcAccept);
}

TEST(WsHandshake, BuildsAValidRequest) {
  std::byte nonce[16];
  for (std::size_t i = 0; i < sizeof(nonce); ++i) {
    nonce[i] = static_cast<std::byte>(i);
  }

  WsHandshakeRequest req;
  req.host = "www.deribit.com";
  req.target = "/ws/api/v2";
  const auto hs = ws_build_handshake(req, nonce);

  EXPECT_TRUE(hs.request.starts_with("GET /ws/api/v2 HTTP/1.1\r\n"));
  EXPECT_NE(hs.request.find("Host: www.deribit.com\r\n"), std::string::npos);
  EXPECT_NE(hs.request.find("Upgrade: websocket\r\n"), std::string::npos);
  EXPECT_NE(hs.request.find("Connection: Upgrade\r\n"), std::string::npos);
  EXPECT_NE(hs.request.find("Sec-WebSocket-Version: 13\r\n"), std::string::npos);
  EXPECT_NE(hs.request.find("Sec-WebSocket-Key: " + hs.key + "\r\n"),
            std::string::npos);
  EXPECT_TRUE(hs.request.ends_with("\r\n\r\n"));

  // Compression is deliberately not requested -- see ws_handshake.h.
  EXPECT_EQ(hs.request.find("Sec-WebSocket-Extensions"), std::string::npos);
  EXPECT_EQ(hs.request.find("permessage-deflate"), std::string::npos);

  // The key must be 16 bytes base64-encoded, per RFC 6455 section 4.1.
  EXPECT_EQ(hs.key.size(), 24u);
  std::string decoded;
  ASSERT_TRUE(base64_decode(hs.key, decoded));
  EXPECT_EQ(decoded.size(), 16u);

  EXPECT_EQ(hs.expected_accept, ws_compute_accept(hs.key));
}

TEST(WsHandshake, IncludesExtraHeaders) {
  std::byte nonce[16] = {};
  WsHandshakeRequest req;
  req.host = "api.example.com";
  req.extra_headers = {{"Authorization", "Bearer token123"},
                       {"X-Custom", "value"}};

  const auto hs = ws_build_handshake(req, nonce);
  EXPECT_NE(hs.request.find("Authorization: Bearer token123\r\n"),
            std::string::npos);
  EXPECT_NE(hs.request.find("X-Custom: value\r\n"), std::string::npos);
}

TEST(WsHandshake, EmptyTargetBecomesRoot) {
  std::byte nonce[16] = {};
  WsHandshakeRequest req;
  req.host = "h";
  req.target = "";
  EXPECT_TRUE(ws_build_handshake(req, nonce).request.starts_with("GET / HTTP/1.1"));
}

TEST(WsHandshake, RandomHandshakesUseDifferentKeys) {
  WsHandshakeRequest req;
  req.host = "h";
  const auto a = ws_build_handshake(req);
  const auto b = ws_build_handshake(req);
  EXPECT_NE(a.key, b.key) << "a fixed nonce would let a cached response replay";
}

// ---------------------------------------------------------------------------
// Response validation -- accepting a bad one is the expensive failure
// ---------------------------------------------------------------------------

TEST(WsHandshake, AcceptsAWellFormedResponse) {
  const auto response = good_response(kRfcAccept);
  const auto r = ws_validate_handshake_response(response, kRfcAccept);

  EXPECT_EQ(r.status, WsHandshakeStatus::kOk) << r.error;
  EXPECT_EQ(r.consumed, response.size());
  EXPECT_EQ(r.http_status, 101);
}

TEST(WsHandshake, ReportsIncompleteUntilTheBlankLineArrives) {
  const auto response = good_response(kRfcAccept);
  for (std::size_t n = 0; n < response.size(); ++n) {
    const auto r =
        ws_validate_handshake_response(response.substr(0, n), kRfcAccept);
    EXPECT_EQ(r.status, WsHandshakeStatus::kIncomplete) << "prefix " << n;
  }
  EXPECT_EQ(ws_validate_handshake_response(response, kRfcAccept).status,
            WsHandshakeStatus::kOk);
}

TEST(WsHandshake, ConsumedStopsAtTheHeaderBoundary) {
  // A server may send the first frame in the same TCP segment as the
  // handshake response. Consuming past the blank line would eat it.
  auto response = good_response(kRfcAccept);
  response += "\x81\x05Hello";

  const auto r = ws_validate_handshake_response(response, kRfcAccept);
  ASSERT_EQ(r.status, WsHandshakeStatus::kOk);
  EXPECT_EQ(r.consumed, response.size() - 7);
  EXPECT_EQ(response.substr(r.consumed), std::string("\x81\x05Hello"));
}

TEST(WsHandshake, HeaderNamesAreCaseInsensitive) {
  const std::string response =
      "HTTP/1.1 101 Switching Protocols\r\n"
      "UPGRADE: WebSocket\r\n"
      "connection: upgrade\r\n"
      "sec-websocket-accept: " +
      std::string(kRfcAccept) + "\r\n\r\n";
  EXPECT_EQ(ws_validate_handshake_response(response, kRfcAccept).status,
            WsHandshakeStatus::kOk);
}

TEST(WsHandshake, ConnectionHeaderMayCarryMultipleTokens) {
  const std::string response =
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Upgrade: websocket\r\n"
      "Connection: keep-alive, Upgrade\r\n"
      "Sec-WebSocket-Accept: " +
      std::string(kRfcAccept) + "\r\n\r\n";
  EXPECT_EQ(ws_validate_handshake_response(response, kRfcAccept).status,
            WsHandshakeStatus::kOk);
}

TEST(WsHandshake, AcceptValueIsComparedCaseSensitively) {
  // It is a base64 digest, not a token. A case-insensitive compare here would
  // accept a value that does not actually prove the server hashed our key.
  std::string upper(kRfcAccept);
  for (char& c : upper) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  const auto r = ws_validate_handshake_response(good_response(upper), kRfcAccept);
  EXPECT_EQ(r.status, WsHandshakeStatus::kFailed);
}

TEST(WsHandshake, RejectsWrongAcceptValue) {
  const auto r = ws_validate_handshake_response(
      good_response("wrongvaluewrongvaluewrong==="), kRfcAccept);
  EXPECT_EQ(r.status, WsHandshakeStatus::kFailed);
  EXPECT_NE(r.error.find("Sec-WebSocket-Accept"), std::string::npos);
}

TEST(WsHandshake, RejectsNon101StatusAndReportsTheCode) {
  // The reconnect logic needs to tell a 401 (bad key, do not retry) from a 429
  // (rate limited, back off) -- both look identical if we only say "failed".
  struct Case {
    const char* line;
    int code;
  };
  for (const Case& c : {Case{"HTTP/1.1 401 Unauthorized", 401},
                        Case{"HTTP/1.1 429 Too Many Requests", 429},
                        Case{"HTTP/1.1 500 Internal Server Error", 500},
                        Case{"HTTP/1.1 200 OK", 200}}) {
    const std::string response =
        std::string(c.line) + "\r\nContent-Length: 0\r\n\r\n";
    const auto r = ws_validate_handshake_response(response, kRfcAccept);
    EXPECT_EQ(r.status, WsHandshakeStatus::kFailed);
    EXPECT_EQ(r.http_status, c.code);
  }
}

TEST(WsHandshake, RejectsMissingHeaders) {
  const std::string no_upgrade =
      "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\n"
      "Sec-WebSocket-Accept: " +
      std::string(kRfcAccept) + "\r\n\r\n";
  EXPECT_EQ(ws_validate_handshake_response(no_upgrade, kRfcAccept).status,
            WsHandshakeStatus::kFailed);

  const std::string no_connection =
      "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
      "Sec-WebSocket-Accept: " +
      std::string(kRfcAccept) + "\r\n\r\n";
  EXPECT_EQ(ws_validate_handshake_response(no_connection, kRfcAccept).status,
            WsHandshakeStatus::kFailed);

  const std::string no_accept =
      "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
      "Connection: Upgrade\r\n\r\n";
  EXPECT_EQ(ws_validate_handshake_response(no_accept, kRfcAccept).status,
            WsHandshakeStatus::kFailed);
}

TEST(WsHandshake, RejectsWrongUpgradeValue) {
  const std::string response =
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Upgrade: h2c\r\nConnection: Upgrade\r\n"
      "Sec-WebSocket-Accept: " +
      std::string(kRfcAccept) + "\r\n\r\n";
  EXPECT_EQ(ws_validate_handshake_response(response, kRfcAccept).status,
            WsHandshakeStatus::kFailed);
}

TEST(WsHandshake, RejectsConnectionWithoutTheUpgradeToken) {
  const std::string response =
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Upgrade: websocket\r\nConnection: keep-alive\r\n"
      "Sec-WebSocket-Accept: " +
      std::string(kRfcAccept) + "\r\n\r\n";
  EXPECT_EQ(ws_validate_handshake_response(response, kRfcAccept).status,
            WsHandshakeStatus::kFailed);
}

TEST(WsHandshake, RejectsUnrequestedExtension) {
  // We never offer permessage-deflate. A server that turns it on anyway will
  // frame with RSV1 set, which the decoder rejects -- better to fail here,
  // where the error message says what actually happened.
  const std::string response =
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Upgrade: websocket\r\nConnection: Upgrade\r\n"
      "Sec-WebSocket-Extensions: permessage-deflate\r\n"
      "Sec-WebSocket-Accept: " +
      std::string(kRfcAccept) + "\r\n\r\n";
  const auto r = ws_validate_handshake_response(response, kRfcAccept);
  EXPECT_EQ(r.status, WsHandshakeStatus::kFailed);
  EXPECT_NE(r.error.find("permessage-deflate"), std::string::npos);
}

TEST(WsHandshake, RejectsUnrequestedSubprotocol) {
  const std::string response =
      "HTTP/1.1 101 Switching Protocols\r\n"
      "Upgrade: websocket\r\nConnection: Upgrade\r\n"
      "Sec-WebSocket-Protocol: chat\r\n"
      "Sec-WebSocket-Accept: " +
      std::string(kRfcAccept) + "\r\n\r\n";
  EXPECT_EQ(ws_validate_handshake_response(response, kRfcAccept).status,
            WsHandshakeStatus::kFailed);
}

TEST(WsHandshake, RejectsNonHttpAndMalformedResponses) {
  EXPECT_EQ(ws_validate_handshake_response("GARBAGE\r\n\r\n", kRfcAccept).status,
            WsHandshakeStatus::kFailed);
  EXPECT_EQ(
      ws_validate_handshake_response("HTTP/1.1 1X1 Weird\r\n\r\n", kRfcAccept)
          .status,
      WsHandshakeStatus::kFailed);
  EXPECT_EQ(
      ws_validate_handshake_response(
          "HTTP/1.1 101 OK\r\nUpgrade websocket\r\n\r\n", kRfcAccept)
          .status,
      WsHandshakeStatus::kFailed)
      << "header line with no colon";
}

TEST(WsHandshake, FailsRatherThanBufferingForever) {
  // A server that opens a connection and never sends a blank line must not be
  // able to make us buffer without limit.
  const std::string endless(2048, 'x');
  const auto r = ws_validate_handshake_response(endless, kRfcAccept, 1024);
  EXPECT_EQ(r.status, WsHandshakeStatus::kFailed);
  EXPECT_NE(r.error.find("1024"), std::string::npos);
}

TEST(WsHandshake, EndToEndAgainstOurOwnAcceptComputation) {
  // Full loop: build a request, act as the server, validate the response.
  WsHandshakeRequest req;
  req.host = "test.example.com";
  req.target = "/ws";
  const auto hs = ws_build_handshake(req);

  const std::string server_reply = good_response(ws_compute_accept(hs.key));
  const auto r = ws_validate_handshake_response(server_reply, hs.expected_accept);
  EXPECT_EQ(r.status, WsHandshakeStatus::kOk) << r.error;
}
