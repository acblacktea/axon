// WebSocket order entry on the three perpetual venues.
//
// These mappings were transcribed from each venue's DOCUMENTATION, not from
// the running Python (which places orders over REST on all three). Everything
// else in the venue layer came from code that has been reconciling against
// live venues for a long time; this did not. That makes these tests weaker
// evidence than the rest of the suite -- they prove the bytes are what this
// build intends, not that the venue accepts them.
//
// What they DO prove, and it is the part worth having:
//
//   * The envelope wraps the REST body verbatim, so the two transports cannot
//     send different orders for the same request.
//   * Binance signs EXACTLY what it sends. That is checked by recomputing the
//     HMAC from the emitted JSON rather than by comparing against a constant,
//     so a future edit that adds a parameter to one pass and not the other
//     fails here instead of at the venue with a -1022.
//   * A request that cannot be represented is REJECTED, never truncated.

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "axon/models/order.h"
#include "axon/net/crypto_lite.h"
#include "axon/venue/binance/binance_builder.h"
#include "axon/venue/bybit/bybit_builder.h"
#include "axon/venue/okx/okx_builder.h"

namespace {

using axon::core::Price;
using axon::core::Qty;
using axon::models::OrderRequest;
using axon::models::OrderSide;
using axon::models::OrderType;

Price D(const char* s) { return *Price::from_string(s); }

OrderRequest limit_buy() {
  OrderRequest req;
  req.instrument = "BTCUSDT";
  req.side = OrderSide::kBuy;
  req.order_type = OrderType::kLimit;
  req.amount = *Qty::from_string("2.5");
  req.price = D("64000.5");
  return req;
}

}  // namespace

// ===========================================================================
// OKX -- rides the private stream connection, args identical to the REST body
// ===========================================================================
TEST(OkxWsOrderEntry, PlaceWrapsTheRestBodyVerbatim) {
  auto req = limit_buy();
  req.instrument = "BTC-USDT-SWAP";

  axon::venue::okx::OkxBuilder builder;
  char body[axon::venue::okx::kMaxRequestBytes];
  const std::size_t body_len =
      axon::venue::okx::OkxBuilder::place_order_body(body, sizeof(body), req);
  ASSERT_GT(body_len, 0u);

  char buf[axon::venue::okx::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = builder.ws_place_order(buf, sizeof(buf), req, id);
  ASSERT_GT(n, 0u);
  const std::string message(buf, n);

  EXPECT_EQ(message, std::string(R"({"id":")") + std::to_string(id) +
                         R"(","op":"order","args":[)" +
                         std::string(body, body_len) + "]}");
}

TEST(OkxWsOrderEntry, IdsAreDistinctAndMonotonic) {
  axon::venue::okx::OkxBuilder builder;
  const auto req = limit_buy();
  char buf[axon::venue::okx::kMaxRequestBytes];
  std::int64_t first = 0;
  std::int64_t second = 0;
  ASSERT_GT(builder.ws_place_order(buf, sizeof(buf), req, first), 0u);
  ASSERT_GT(builder.ws_place_order(buf, sizeof(buf), req, second), 0u);
  EXPECT_EQ(second, first + 1);
}

TEST(OkxWsOrderEntry, CancelAndAmendWrapTheirBodies) {
  axon::venue::okx::OkxBuilder builder;
  char buf[axon::venue::okx::kMaxRequestBytes];
  std::int64_t id = 0;

  std::size_t n = builder.ws_cancel_order(buf, sizeof(buf), "BTC-USDT-SWAP",
                                          "ord-1", id);
  ASSERT_GT(n, 0u);
  EXPECT_EQ(std::string(buf, n),
            std::string(R"({"id":")") + std::to_string(id) +
                R"(","op":"cancel-order","args":[{"instId":"BTC-USDT-SWAP","ordId":"ord-1"}]})");

  n = builder.ws_amend_order(buf, sizeof(buf), "BTC-USDT-SWAP", "ord-1",
                             std::nullopt, D("101.5"), id);
  ASSERT_GT(n, 0u);
  EXPECT_EQ(std::string(buf, n),
            std::string(R"({"id":")") + std::to_string(id) +
                R"(","op":"amend-order","args":[{"instId":"BTC-USDT-SWAP","ordId":"ord-1","newPx":"101.5"}]})");
}

TEST(OkxWsOrderEntry, TooSmallABufferIsRejectedNotTruncated) {
  axon::venue::okx::OkxBuilder builder;
  const auto req = limit_buy();
  // Enough for the envelope, nowhere near enough for the body.
  char buf[24];
  std::int64_t id = 0;
  EXPECT_EQ(builder.ws_place_order(buf, sizeof(buf), req, id), 0u);
}

// ===========================================================================
// Bybit -- its own /v5/trade connection, header block, no per-request signature
// ===========================================================================
TEST(BybitWsOrderEntry, PlaceCarriesTheHeaderAndWrapsTheRestBody) {
  const auto req = limit_buy();

  char body[axon::venue::bybit::kMaxRequestBytes];
  const std::size_t body_len =
      axon::venue::bybit::BybitBuilder::place_order_body(body, sizeof(body), req);
  ASSERT_GT(body_len, 0u);

  axon::venue::bybit::BybitBuilder builder;
  char buf[axon::venue::bybit::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n =
      builder.ws_place_order(buf, sizeof(buf), req, 1700000000000LL, id);
  ASSERT_GT(n, 0u);

  EXPECT_EQ(std::string(buf, n),
            std::string(R"({"reqId":")") + std::to_string(id) +
                R"(","header":{"X-BAPI-TIMESTAMP":"1700000000000","X-BAPI-RECV-WINDOW":"5000"},)"
                R"("op":"order.create","args":[)" +
                std::string(body, body_len) + "]}");
}

// The connection is authenticated once at login, so an individual request
// carries no signature. If one ever appears here, the login model changed.
TEST(BybitWsOrderEntry, RequestsAreNotIndividuallySigned) {
  axon::venue::bybit::BybitBuilder builder;
  const auto req = limit_buy();
  char buf[axon::venue::bybit::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n =
      builder.ws_place_order(buf, sizeof(buf), req, 1700000000000LL, id);
  ASSERT_GT(n, 0u);
  EXPECT_EQ(std::string(buf, n).find("sign"), std::string::npos);
}

TEST(BybitWsOrderEntry, CancelAndAmendUseTheirOwnOps) {
  axon::venue::bybit::BybitBuilder builder;
  char buf[axon::venue::bybit::kMaxRequestBytes];
  std::int64_t id = 0;

  std::size_t n = builder.ws_cancel_order(buf, sizeof(buf), "BTCUSDT", "ord-1",
                                          1700000000000LL, id);
  ASSERT_GT(n, 0u);
  EXPECT_NE(std::string(buf, n).find(R"("op":"order.cancel")"), std::string::npos);

  n = builder.ws_amend_order(buf, sizeof(buf), "BTCUSDT", "ord-1",
                             *Qty::from_string("3"), std::nullopt,
                             1700000000000LL, id);
  ASSERT_GT(n, 0u);
  EXPECT_NE(std::string(buf, n).find(R"("op":"order.amend")"), std::string::npos);
  EXPECT_NE(std::string(buf, n).find(R"("qty":"3")"), std::string::npos);
}

// ===========================================================================
// Binance -- its own ws-fapi connection, and every request signs itself
// ===========================================================================
namespace {

// Pulls "key":"value" pairs out of the params object, in the order they appear.
// A deliberately dumb scanner: using the project's JSON reader here would let
// a bug in that reader hide a bug in the builder.
std::vector<std::pair<std::string, std::string>> scan_params(
    std::string_view message) {
  std::vector<std::pair<std::string, std::string>> out;
  const auto start = message.find(R"("params":{)");
  if (start == std::string_view::npos) {
    return out;
  }
  std::size_t i = start + std::string_view(R"("params":{)").size();
  while (i < message.size() && message[i] != '}') {
    if (message[i] != '"') {
      ++i;
      continue;
    }
    const std::size_t key_begin = ++i;
    while (i < message.size() && message[i] != '"') ++i;
    const std::string key(message.substr(key_begin, i - key_begin));
    i += 2;  // closing quote and ':'
    if (i >= message.size() || message[i] != '"') {
      break;
    }
    const std::size_t val_begin = ++i;
    while (i < message.size() && message[i] != '"') ++i;
    const std::string value(message.substr(val_begin, i - val_begin));
    ++i;
    out.emplace_back(key, value);
    if (i < message.size() && message[i] == ',') ++i;
  }
  return out;
}

}  // namespace

// THE TEST THAT MATTERS. Rebuilds the signing string from the JSON that was
// actually emitted and checks the signature in the message matches it. A pass
// means the bytes signed and the bytes sent describe the same order; nothing
// weaker is worth having, because the venue's answer to a mismatch is -1022
// with no indication of which field disagreed.
TEST(BinanceWsOrderEntry, SignatureCoversExactlyWhatIsSent) {
  const auto req = limit_buy();
  axon::venue::binance::BinanceBuilder builder;
  char buf[axon::venue::binance::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = builder.ws_place_order(
      buf, sizeof(buf), req, "the-api-key", "the-api-secret", 1700000000000LL, id);
  ASSERT_GT(n, 0u);
  const std::string message(buf, n);

  const auto params = scan_params(message);
  ASSERT_FALSE(params.empty());

  std::string signature;
  std::string signing;
  for (const auto& [key, value] : params) {
    if (key == "signature") {
      signature = value;
      continue;
    }
    if (!signing.empty()) {
      signing += "&";
    }
    signing += key;
    signing += "=";
    signing += value;
  }
  ASSERT_FALSE(signature.empty());

  const std::string expected = axon::net::to_hex(
      axon::net::hmac_sha256("the-api-secret", signing));
  EXPECT_EQ(signature, expected);
}

// The test above proves the message is SELF-CONSISTENT: whatever was emitted
// is what was signed. That would still pass if the signing string were built
// in a way the venue does not expect, so this pins the exact bytes against a
// signature computed independently (Python's hmac module) -- the same kind of
// reference the REST builder used to carry.
TEST(BinanceWsOrderEntry, SigningStringMatchesAnIndependentReference) {
  const auto req = limit_buy();
  axon::venue::binance::BinanceBuilder builder;
  char buf[axon::venue::binance::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = builder.ws_place_order(
      buf, sizeof(buf), req, "the-api-key", "the-api-secret", 1700000000000LL, id);
  ASSERT_GT(n, 0u);

  // hmac.new(b"the-api-secret", payload, hashlib.sha256).hexdigest() over:
  //   apiKey=the-api-key&price=64000.5&quantity=2.5&recvWindow=5000&side=BUY
  //   &symbol=BTCUSDT&timeInForce=GTC&timestamp=1700000000000&type=LIMIT
  EXPECT_NE(
      std::string(buf, n).find(
          R"("signature":"d144f506951720c8cea62419eb99f6e16c52300956a3fbe70ca5faf9cae6bf19")"),
      std::string::npos)
      << std::string(buf, n);
}

// Binance sorts parameters by name before signing, so the emitted order is
// part of the contract rather than a style choice.
TEST(BinanceWsOrderEntry, ParametersAreEmittedInAlphabeticalOrder) {
  auto req = limit_buy();
  req.label = "my-order-1";
  axon::venue::binance::BinanceBuilder builder;
  char buf[axon::venue::binance::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = builder.ws_place_order(
      buf, sizeof(buf), req, "k", "s", 1700000000000LL, id);
  ASSERT_GT(n, 0u);

  const auto params = scan_params(std::string_view(buf, n));
  std::vector<std::string> keys;
  for (const auto& [key, value] : params) {
    if (key != "signature") {
      keys.push_back(key);
    }
  }
  const std::vector<std::string> expected{
      "apiKey", "newClientOrderId", "price",     "quantity", "recvWindow",
      "side",   "symbol",           "timeInForce", "timestamp", "type"};
  EXPECT_EQ(keys, expected);

  // The signature is appended after the parameters it covers, never inside.
  ASSERT_FALSE(params.empty());
  EXPECT_EQ(params.back().first, "signature");
}

TEST(BinanceWsOrderEntry, MarketOrderOmitsPriceAndTimeInForce) {
  OrderRequest req;
  req.instrument = "BTCUSDT";
  req.side = OrderSide::kSell;
  req.order_type = OrderType::kMarket;
  req.amount = *Qty::from_string("1");

  axon::venue::binance::BinanceBuilder builder;
  char buf[axon::venue::binance::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = builder.ws_place_order(
      buf, sizeof(buf), req, "k", "s", 1700000000000LL, id);
  ASSERT_GT(n, 0u);

  const auto params = scan_params(std::string_view(buf, n));
  for (const auto& [key, value] : params) {
    EXPECT_NE(key, "price");
    EXPECT_NE(key, "timeInForce");
  }
}

TEST(BinanceWsOrderEntry, PostOnlyBecomesGtx) {
  auto req = limit_buy();
  req.post_only = true;
  axon::venue::binance::BinanceBuilder builder;
  char buf[axon::venue::binance::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = builder.ws_place_order(
      buf, sizeof(buf), req, "k", "s", 1700000000000LL, id);
  ASSERT_GT(n, 0u);
  EXPECT_NE(std::string(buf, n).find(R"("timeInForce":"GTX")"), std::string::npos);
}

// A label containing a character JSON would escape cannot be both signed raw
// and sent escaped. Rejecting is the only honest answer -- signing one thing
// and sending another produces a rejection nobody can trace.
TEST(BinanceWsOrderEntry, LabelNeedingJsonEscapingIsRejected) {
  axon::venue::binance::BinanceBuilder builder;
  char buf[axon::venue::binance::kMaxRequestBytes];
  std::int64_t id = 0;

  for (const char* bad : {R"(a"b)", "a\\b", "a&b", "a b", "a\nb"}) {
    auto req = limit_buy();
    req.label = bad;
    EXPECT_EQ(builder.ws_place_order(buf, sizeof(buf), req, "k", "s",
                                     1700000000000LL, id),
              0u)
        << "label " << bad << " should have been rejected";
  }

  // The safe set still goes through.
  auto req = limit_buy();
  req.label = "abc-123_x.y";
  EXPECT_GT(builder.ws_place_order(buf, sizeof(buf), req, "k", "s",
                                   1700000000000LL, id),
            0u);
}

TEST(BinanceWsOrderEntry, TooSmallABufferIsRejectedNotTruncated) {
  const auto req = limit_buy();
  axon::venue::binance::BinanceBuilder builder;
  char buf[40];
  std::int64_t id = 0;
  EXPECT_EQ(builder.ws_place_order(buf, sizeof(buf), req, "k", "s",
                                   1700000000000LL, id),
            0u);
}

TEST(BinanceWsOrderEntry, CancelAndModifySignThemselvesToo) {
  axon::venue::binance::BinanceBuilder builder;
  char buf[axon::venue::binance::kMaxRequestBytes];
  std::int64_t id = 0;

  std::size_t n = builder.ws_cancel_order(buf, sizeof(buf), "BTCUSDT", "12345",
                                          "k", "the-secret", 1700000000000LL, id);
  ASSERT_GT(n, 0u);
  EXPECT_NE(std::string(buf, n).find(R"("method":"order.cancel")"),
            std::string::npos);

  n = builder.ws_modify_order(buf, sizeof(buf), "BTCUSDT", "12345",
                              OrderSide::kBuy, *Qty::from_string("1"),
                              D("100"), "k", "the-secret", 1700000000000LL, id);
  ASSERT_GT(n, 0u);
  const std::string message(buf, n);
  EXPECT_NE(message.find(R"("method":"order.modify")"), std::string::npos);

  // Same property as the place test: signed exactly what is sent.
  const auto params = scan_params(message);
  std::string signature;
  std::string signing;
  for (const auto& [key, value] : params) {
    if (key == "signature") {
      signature = value;
      continue;
    }
    if (!signing.empty()) signing += "&";
    signing += key + "=" + value;
  }
  EXPECT_EQ(signature,
            axon::net::to_hex(axon::net::hmac_sha256("the-secret", signing)));
}
