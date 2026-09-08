// End-to-end WebSocket connection tests.
//
// Real TCP on loopback, real TLS with certificate verification ON, real RFC
// 6455 framing against the in-process server in ws_test_server.h. Nothing is
// mocked -- a mocked transport only proves the mock and the client agree, and
// the bugs worth catching live exactly where they would agree to skip.

#include "axon/net/ws_connection.h"

#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <vector>

#include "axon/core/clock.h"
#include "ws_test_server.h"

using namespace axon;
using axon::test::WsTestServer;

namespace {

struct Recorder {
  bool opened = false;
  bool closed = false;
  std::vector<std::string> messages;
  std::vector<std::string> errors;
  net::WsCloseInfo close_info;

  void on_open() { opened = true; }
  void on_message(const std::byte* data, std::size_t len, net::WsOpcode) {
    messages.emplace_back(reinterpret_cast<const char*>(data), len);
  }
  void on_close(const net::WsCloseInfo& info) {
    closed = true;
    close_info = info;
  }
  void on_error(const char* what) { errors.emplace_back(what); }
};

// Polls until `done` or the deadline. Everything here is non-blocking, so a
// busy loop is exactly how it is meant to be driven.
template <typename Predicate>
bool pump_until(net::WsConnection& conn, Recorder& rec, Predicate done,
                int timeout_ms = 5000) {
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
  while (std::chrono::steady_clock::now() < deadline) {
    const bool alive = conn.poll(rec);
    if (done()) {
      return true;
    }
    if (!alive) {
      return done();
    }
    core::cpu_pause();
  }
  return done();
}

net::WsConnectionConfig config_for(const WsTestServer& server, bool tls = true) {
  net::WsConnectionConfig cfg;
  cfg.host = "localhost";
  cfg.port = server.port();
  cfg.target = "/ws";
  cfg.use_tls = tls;
  return cfg;
}

// A client context that trusts ONLY the server's generated certificate.
// Verification stays enabled -- switching it off to make the test pass would
// mean the test never exercises the code that matters.
net::TlsContext trusting_context(const WsTestServer& server) {
  net::TlsContext ctx;
  ctx.add_trusted_certificate_pem(server.certificate_pem());
  return ctx;
}

}  // namespace

TEST(WsConnection, ConnectsOverTlsAndEchoesAMessage) {
  WsTestServer server;
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);

  ASSERT_TRUE(pump_until(conn, rec, [&] { return rec.opened; }))
      << (rec.errors.empty() ? conn.last_error() : rec.errors.front());
  EXPECT_EQ(conn.state(), net::WsConnectionState::kOpen);
  EXPECT_TRUE(conn.tls().established());

  // A real negotiated version and cipher, not a stub.
  const std::string tls_description = conn.tls().description();
  EXPECT_NE(tls_description.find("TLS"), std::string::npos) << tls_description;

  ASSERT_TRUE(conn.send_text(R"({"method":"public/test"})"));
  ASSERT_TRUE(pump_until(conn, rec, [&] { return !rec.messages.empty(); }))
      << conn.last_error();

  EXPECT_EQ(rec.messages.front(), R"({"method":"public/test"})");
  EXPECT_EQ(conn.stats().messages_sent, 1u);
  EXPECT_EQ(conn.stats().messages_received, 1u);
  EXPECT_GT(conn.stats().bytes_sent, 0u);
}

TEST(WsConnection, WorksWithoutTls) {
  // ws:// rather than wss://. No venue uses it, but the plaintext path is what
  // makes a failure attributable to TLS or not.
  WsTestServer server({.use_tls = false});
  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server, /*tls=*/false), nullptr);

  ASSERT_TRUE(pump_until(conn, rec, [&] { return rec.opened; }))
      << conn.last_error();
  ASSERT_TRUE(conn.send_text("hello"));
  ASSERT_TRUE(pump_until(conn, rec, [&] { return !rec.messages.empty(); }));
  EXPECT_EQ(rec.messages.front(), "hello");
}

TEST(WsConnection, RejectsAnUntrustedCertificate) {
  // The property that matters most in this file. A client that accepts an
  // unknown certificate looks identical to one that verifies -- right up until
  // it is talking to something else.
  WsTestServer server;
  net::TlsContext ctx;  // trusts nothing; the server's cert is self-signed

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);

  pump_until(conn, rec, [&] { return !rec.errors.empty(); }, 5000);

  EXPECT_FALSE(rec.opened) << "an untrusted certificate must not open";
  EXPECT_EQ(conn.state(), net::WsConnectionState::kFailed);
  EXPECT_FALSE(rec.errors.empty());
  EXPECT_NE(conn.last_error().find("tls"), std::string::npos)
      << conn.last_error();
}

TEST(WsConnection, RejectsAHostnameMismatch) {
  // Chain verification and hostname verification are SEPARATE in OpenSSL.
  // Trusting the certificate but issuing it for another name must still fail,
  // or the client would accept any valid certificate for any host.
  WsTestServer server({.common_name = "not-localhost.example"});
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);

  pump_until(conn, rec, [&] { return !rec.errors.empty(); }, 5000);

  EXPECT_FALSE(rec.opened) << "a certificate for another host must not open";
  EXPECT_EQ(conn.state(), net::WsConnectionState::kFailed);
}

TEST(WsConnection, AnswersPingsAutomatically) {
  // A venue that does not get its pong closes the connection, so this can
  // never be the application's job.
  WsTestServer server({.ping_on_open = true});
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);

  ASSERT_TRUE(pump_until(conn, rec, [&] { return rec.opened; }))
      << conn.last_error();
  ASSERT_TRUE(pump_until(conn, rec, [&] {
    return conn.stats().pongs_sent > 0 && server.pongs_received() > 0;
  })) << "expected an automatic pong";

  EXPECT_GE(conn.stats().pings_received, 1u);
  EXPECT_GE(conn.stats().pongs_sent, 1u);
  // And the ping must not have surfaced as an application message.
  EXPECT_TRUE(rec.messages.empty());
}

TEST(WsConnection, DoesNotLoseAFrameSentWithTheHandshakeResponse) {
  // A server may put its first frame in the same segment as the 101. A client
  // that consumes the whole buffer instead of exactly the headers eats it, and
  // the symptom is one missing message at connect -- easy to blame on the
  // venue.
  WsTestServer server({.greeting = R"({"jsonrpc":"2.0","method":"hello"})"});
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);

  ASSERT_TRUE(pump_until(conn, rec, [&] { return !rec.messages.empty(); }))
      << conn.last_error();
  EXPECT_EQ(rec.messages.front(), R"({"jsonrpc":"2.0","method":"hello"})");
}

TEST(WsConnection, ReportsAHandshakeRejectionWithItsStatus) {
  // A 401 (bad key, do not retry) and a 429 (rate limited, back off) need
  // different responses from the reconnect logic; both look identical if the
  // client only says "failed".
  WsTestServer server({.reject_handshake = true});
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);

  pump_until(conn, rec, [&] { return !rec.errors.empty(); }, 5000);
  EXPECT_FALSE(rec.opened);
  EXPECT_EQ(conn.state(), net::WsConnectionState::kFailed);
  EXPECT_NE(conn.last_error().find("401"), std::string::npos)
      << conn.last_error();
}

TEST(WsConnection, RejectsAnUnrequestedExtension) {
  // We never offer permessage-deflate. A server that turns it on anyway would
  // frame with RSV1 set, which the decoder rejects -- failing here gives a
  // legible error instead of a mid-session protocol error.
  WsTestServer server({.offer_extension = true});
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);

  pump_until(conn, rec, [&] { return !rec.errors.empty(); }, 5000);
  EXPECT_FALSE(rec.opened);
  EXPECT_NE(conn.last_error().find("permessage-deflate"), std::string::npos)
      << conn.last_error();
}

TEST(WsConnection, RejectsAWrongAcceptValue) {
  WsTestServer server({.wrong_accept = true});
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);

  pump_until(conn, rec, [&] { return !rec.errors.empty(); }, 5000);
  EXPECT_FALSE(rec.opened);
  EXPECT_NE(conn.last_error().find("Sec-WebSocket-Accept"), std::string::npos)
      << conn.last_error();
}

TEST(WsConnection, RoundTripsManyMessagesWithoutLoss) {
  // Exercises buffer compaction, partial writes, and TLS record boundaries --
  // none of which show up on a single message.
  WsTestServer server;
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);
  ASSERT_TRUE(pump_until(conn, rec, [&] { return rec.opened; }))
      << conn.last_error();

  constexpr int kCount = 500;
  int sent = 0;
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(20);

  while (rec.messages.size() < kCount &&
         std::chrono::steady_clock::now() < deadline) {
    if (sent < kCount) {
      const std::string msg = "message-" + std::to_string(sent);
      if (conn.send_text(msg)) {
        ++sent;
      }
    }
    if (!conn.poll(rec)) {
      break;
    }
    core::cpu_pause();
  }

  ASSERT_EQ(sent, kCount);
  ASSERT_EQ(rec.messages.size(), static_cast<std::size_t>(kCount))
      << conn.last_error();
  // Order must be preserved and nothing duplicated.
  for (int i = 0; i < kCount; ++i) {
    EXPECT_EQ(rec.messages[static_cast<std::size_t>(i)],
              "message-" + std::to_string(i));
  }
  EXPECT_TRUE(rec.errors.empty());
}

TEST(WsConnection, HandlesLargeMessages) {
  // 64 KiB spans multiple TLS records and forces the 16-bit frame length path.
  WsTestServer server;
  net::TlsContext ctx = trusting_context(server);

  net::WsConnectionConfig cfg = config_for(server);
  cfg.rx_buffer_bytes = 1u << 20;
  cfg.tx_buffer_bytes = 1u << 20;

  net::WsConnection conn;
  Recorder rec;
  conn.connect(cfg, &ctx);
  ASSERT_TRUE(pump_until(conn, rec, [&] { return rec.opened; }))
      << conn.last_error();

  const std::string big(64 * 1024, 'x');
  ASSERT_TRUE(conn.send_text(big));
  ASSERT_TRUE(pump_until(conn, rec, [&] { return !rec.messages.empty(); }, 15000))
      << conn.last_error();
  EXPECT_EQ(rec.messages.front().size(), big.size());
  EXPECT_EQ(rec.messages.front(), big);
}

TEST(WsConnection, CloseHandshakeIsOrderly) {
  WsTestServer server;
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);
  ASSERT_TRUE(pump_until(conn, rec, [&] { return rec.opened; }))
      << conn.last_error();

  conn.close(net::WsCloseCode::kGoingAway, "bye");

  // Wait on the SERVER, not on the client's state. The client reaching
  // kClosed only means the bytes left our socket; the server is a separate
  // thread and may not have read them yet. Asserting on the client's state
  // and then checking the server is a race that passes locally and fails in
  // CI.
  const auto deadline =
      std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (!server.saw_close() && std::chrono::steady_clock::now() < deadline) {
    conn.poll(rec);
    core::cpu_pause();
  }

  EXPECT_TRUE(server.saw_close()) << "the server should have received a close";
  EXPECT_EQ(conn.state(), net::WsConnectionState::kClosed);
  EXPECT_TRUE(rec.errors.empty());
}

TEST(WsConnection, SendBackpressureIsReportedNotFatal) {
  // A full send buffer is backpressure, not an error: the caller must decide
  // whether to retry or drop. Growing the buffer would defer the problem to
  // the worst possible moment.
  WsTestServer server;
  net::TlsContext ctx = trusting_context(server);

  net::WsConnectionConfig cfg = config_for(server);
  cfg.tx_buffer_bytes = 4096;

  net::WsConnection conn;
  Recorder rec;
  conn.connect(cfg, &ctx);
  ASSERT_TRUE(pump_until(conn, rec, [&] { return rec.opened; }))
      << conn.last_error();

  const std::string chunk(1024, 'y');
  int refused = 0;
  for (int i = 0; i < 64; ++i) {
    if (!conn.send_text(chunk)) {
      ++refused;
    }
  }
  EXPECT_GT(refused, 0) << "a 4 KiB buffer should have refused something";
  EXPECT_GT(conn.stats().send_backpressure_events, 0u);
  // And the connection must still be usable afterwards.
  EXPECT_EQ(conn.state(), net::WsConnectionState::kOpen);
  EXPECT_TRUE(rec.errors.empty());
}

TEST(WsConnection, ConnectionRefusedIsReported) {
  net::TlsContext ctx;
  net::WsConnection conn;
  Recorder rec;

  net::WsConnectionConfig cfg;
  cfg.host = "127.0.0.1";
  cfg.port = 1;  // nothing listens here
  cfg.use_tls = false;

  conn.connect(cfg, nullptr);
  pump_until(conn, rec, [&] { return !rec.errors.empty(); }, 5000);

  EXPECT_FALSE(rec.opened);
  EXPECT_EQ(conn.state(), net::WsConnectionState::kFailed);
}

TEST(WsConnection, ReceiveBufferIsPaddedForTheJsonParser) {
  // A venue parser reads straight out of the receive buffer, and simdjson
  // reads past the end of the document. Without the reserved padding that is
  // a heap overread -- which ASan catches here and production does not.
  WsTestServer server({.greeting = R"({"a":1})"});
  net::TlsContext ctx = trusting_context(server);

  net::WsConnection conn;
  Recorder rec;
  conn.connect(config_for(server), &ctx);
  ASSERT_TRUE(pump_until(conn, rec, [&] { return !rec.messages.empty(); }))
      << conn.last_error();
  EXPECT_EQ(rec.messages.front(), R"({"a":1})");
}
