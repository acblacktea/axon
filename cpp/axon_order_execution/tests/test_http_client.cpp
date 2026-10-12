// The REST client against a loopback server.
//
// The one that mattered: a response larger than the 64 KiB receive buffer.
// Binance's /fapi/v2/positionRisk lists every symbol, and on testnet that grew
// to ~290 KB. The client failed it as "connection lost", so the position
// reconciler and the risk layer's positions quietly stopped updating. A full
// buffer is backpressure -- drain and keep reading -- never a lost connection.

#include <gtest/gtest.h>

#include <string>

#include "axon/core/clock.h"
#include "axon/net/http_client.h"
#include "http_test_server.h"

namespace {

using axon::net::HttpClient;
using axon::net::HttpRequest;
using axon::net::HttpResponse;

HttpResponse get(HttpClient& client, std::uint16_t port, bool tls) {
  HttpRequest req;
  req.host = "localhost";
  req.port = port;
  req.use_tls = tls;
  req.path = "/fapi/v2/positionRisk";
  req.timeout_seconds = 10.0;
  HttpResponse out;
  bool done = false;
  client.submit(req, [&](const HttpResponse& r) {
    out = r;
    done = true;
  });
  const auto deadline = axon::core::monotonic_ns() + 15'000'000'000LL;
  while (!done && axon::core::monotonic_ns() < deadline) {
    client.poll();
  }
  return out;
}

std::string big_json(std::size_t bytes) {
  std::string s = "[";
  while (s.size() < bytes) {
    s += R"({"symbol":"SNTUSDT","positionAmt":"0","entryPrice":"0.0","markPrice":"0"},)";
  }
  s.back() = ']';
  return s;
}

TEST(HttpClient, AResponseLargerThanTheBufferArrivesWholeOverTls) {
  const std::string body = big_json(300 * 1024);
  HttpTestServer server(body, /*use_tls=*/true);
  axon::net::TlsContext ctx;
  ctx.add_trusted_certificate_pem(server.certificate_pem());
  HttpClient client(&ctx);

  const HttpResponse r = get(client, server.port(), true);
  EXPECT_EQ(r.error, "");
  EXPECT_EQ(r.status, 200);
  EXPECT_EQ(r.body.size(), body.size());
  EXPECT_EQ(r.body, body);
}

TEST(HttpClient, AResponseLargerThanTheBufferArrivesWholeOverPlainTcp) {
  const std::string body = big_json(300 * 1024);
  HttpTestServer server(body, /*use_tls=*/false);
  HttpClient client(nullptr);

  const HttpResponse r = get(client, server.port(), false);
  EXPECT_EQ(r.error, "");
  EXPECT_EQ(r.status, 200);
  EXPECT_EQ(r.body, body);
}

}  // namespace
