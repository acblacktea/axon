// Binance / Bybit / OKX perpetual venue tests.
//
// Every payload here is shaped like the real feed and every mapping asserted
// was transcribed from the corresponding Python OMS. When one of these fails
// after a change, the question is "does the Python do this?" -- the two
// implementations must agree or a strategy sees different state depending on
// which engine it is talking to.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "axon/net/crypto_lite.h"
#include "axon/venue/binance/binance_builder.h"
#include "axon/venue/binance/binance_parser.h"
#include "axon/venue/bybit/bybit_builder.h"
#include "axon/venue/bybit/bybit_parser.h"
#include "axon/venue/okx/okx_builder.h"
#include "axon/venue/okx/okx_parser.h"

using namespace axon;
using axon::models::Liquidity;
using axon::models::OrderSide;
using axon::models::OrderStatus;
using axon::models::OrderType;
using axon::venue::VenueMessageKind;

namespace {

struct Collector {
  std::vector<transport::OrderUpdateMsg> orders;
  std::vector<transport::FillMsg> fills;
  std::vector<models::AccountSummary> accounts;

  void on_order(const transport::OrderUpdateMsg& m) { orders.push_back(m); }
  void on_fill(const transport::FillMsg& m) { fills.push_back(m); }
  void on_account(const models::AccountSummary& a) { accounts.push_back(a); }
};

// simdjson reads past the end of the document; every input must carry padding.
template <typename Parser>
venue::VenueParseResult run(Parser& p, std::string_view json, Collector& c) {
  std::string buf(json);
  const std::size_t len = buf.size();
  buf.resize(len + venue::kJsonPadding, '\0');
  return p.parse(buf.data(), len, buf.size(), c);
}

core::Price D(const char* s) { return *core::Price::from_string(s); }

}  // namespace

// ===========================================================================
// crypto: SHA-256 and HMAC-SHA256, against published vectors
// ===========================================================================

TEST(Sha256, MatchesPublishedVectors) {
  using net::sha256;
  using net::to_hex;
  EXPECT_EQ(to_hex(sha256("")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
  EXPECT_EQ(to_hex(sha256("abc")),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  // 56 and 64 bytes straddle the padding branch, as with SHA-1.
  EXPECT_EQ(to_hex(sha256(std::string(56, 'a'))),
            "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a");
  EXPECT_EQ(to_hex(sha256(std::string(64, 'a'))),
            "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb");
  EXPECT_EQ(to_hex(sha256(std::string(1000000, 'a'))),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
}

TEST(HmacSha256, MatchesRfc4231Vectors) {
  using net::hmac_sha256;
  using net::to_hex;

  EXPECT_EQ(to_hex(hmac_sha256(std::string(20, '\x0b'), "Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7");

  EXPECT_EQ(to_hex(hmac_sha256("Jefe", "what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843");

  EXPECT_EQ(to_hex(hmac_sha256(std::string(20, '\xaa'), std::string(50, '\xdd'))),
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe");

  // Case 6: a key LONGER than the 64-byte block must be hashed first. Getting
  // this branch wrong produces a signature that works in testing with short
  // keys and fails with a real one.
  EXPECT_EQ(to_hex(hmac_sha256(
                std::string(131, '\xaa'),
                "Test Using Larger Than Block-Size Key - Hash Key First")),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54");
}

// ===========================================================================
// Binance
// ===========================================================================

namespace {
constexpr std::string_view kBinanceOrderFrame = R"({
"e":"ORDER_TRADE_UPDATE","T":1786106096123,"E":1786106096124,
"o":{"s":"BTCUSDT","c":"chase-maker","S":"BUY","o":"LIMIT","f":"GTX",
"q":"2.5","p":"64000.5","ap":"0","x":"NEW","X":"NEW","i":12345678,
"l":"0","z":"0","L":"0","n":"0","N":"USDT","T":1786106096123,"t":0,
"m":false,"R":false,"wt":"CONTRACT_PRICE","ot":"LIMIT","ps":"BOTH"}})";

constexpr std::string_view kBinanceTradeFrame = R"({
"e":"ORDER_TRADE_UPDATE","T":1786106096456,
"o":{"s":"BTCUSDT","c":"chase-maker","S":"BUY","o":"LIMIT","f":"GTC",
"q":"2.5","p":"64000.5","ap":"64000.25","x":"TRADE","X":"PARTIALLY_FILLED",
"i":12345678,"l":"1.25","z":"1.25","L":"64000.25","n":"0.0123","N":"USDT",
"T":1786106096456,"t":987654321,"m":true}})";
}  // namespace

TEST(BinanceParser, ParsesOrderUpdate) {
  venue::binance::BinanceParser p;
  Collector c;
  const auto r = run(p, kBinanceOrderFrame, c);

  ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate) << (r.error ? r.error : "");
  ASSERT_EQ(c.orders.size(), 1u);
  EXPECT_TRUE(c.fills.empty()) << "execution type NEW carries no fill";

  const auto& o = c.orders[0];
  EXPECT_EQ(o.exchange.view(), "binance");
  // The order id is a NUMBER on the wire and text in the model.
  EXPECT_EQ(o.order_id.view(), "12345678");
  EXPECT_EQ(o.instrument.view(), "BTCUSDT");
  EXPECT_EQ(static_cast<OrderSide>(o.side), OrderSide::kBuy);
  EXPECT_EQ(static_cast<OrderType>(o.order_type), OrderType::kLimit);
  EXPECT_EQ(static_cast<OrderStatus>(o.status), OrderStatus::kOpen);
  EXPECT_EQ(o.amount_raw, D("2.5").raw());
  EXPECT_EQ(o.price_raw, D("64000.5").raw());
  EXPECT_EQ(o.filled_amount_raw, 0);
  // "ap":"0" means no average price yet, not an average of zero.
  EXPECT_EQ(o.has_average_price, 0);
  EXPECT_EQ(o.exchange_ts_ns, 1786106096123LL * 1'000'000LL);
}

TEST(BinanceParser, OneFrameCarriesBothOrderAndFill) {
  // ORDER_TRADE_UPDATE with x=="TRADE" is an order update AND a fill. Missing
  // this is how a fill silently never gets recorded.
  venue::binance::BinanceParser p;
  Collector c;
  const auto r = run(p, kBinanceTradeFrame, c);

  EXPECT_EQ(r.kind, VenueMessageKind::kTradeUpdate);
  EXPECT_EQ(r.emitted, 2u);
  ASSERT_EQ(c.orders.size(), 1u);
  ASSERT_EQ(c.fills.size(), 1u);

  EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status),
            OrderStatus::kPartiallyFilled);
  EXPECT_EQ(c.orders[0].filled_amount_raw, D("1.25").raw());
  EXPECT_EQ(c.orders[0].average_price_raw, D("64000.25").raw());

  const auto& f = c.fills[0];
  EXPECT_EQ(f.trade_id.view(), "987654321");
  EXPECT_EQ(f.order_id.view(), "12345678");
  // `l` and `L` are the LAST fill, not the cumulative totals.
  EXPECT_EQ(f.amount_raw, D("1.25").raw());
  EXPECT_EQ(f.price_raw, D("64000.25").raw());
  EXPECT_EQ(f.fee_raw, D("0.0123").raw());
  EXPECT_EQ(f.fee_currency.view(), "USDT");
  EXPECT_EQ(static_cast<Liquidity>(f.liquidity), Liquidity::kMaker);
  // The order must be emitted before the fill that references it.
  EXPECT_LT(c.orders[0].hdr.seq, c.fills[0].hdr.seq);
}

TEST(BinanceParser, MapsEveryKnownStatus) {
  const std::vector<std::pair<std::string, OrderStatus>> cases{
      {"NEW", OrderStatus::kOpen},
      {"PARTIALLY_FILLED", OrderStatus::kPartiallyFilled},
      {"FILLED", OrderStatus::kFilled},
      {"CANCELED", OrderStatus::kCancelled},
      {"REJECTED", OrderStatus::kRejected},
      {"EXPIRED", OrderStatus::kCancelled},
      {"EXPIRED_IN_MATCH", OrderStatus::kCancelled},
  };
  for (const auto& [status, expected] : cases) {
    venue::binance::BinanceParser p;
    Collector c;
    const std::string json =
        R"({"e":"ORDER_TRADE_UPDATE","o":{"s":"BTCUSDT","S":"BUY","o":"LIMIT",)"
        R"("q":"1","p":"1","x":"NEW","X":")" +
        status + R"(","i":1,"T":1}})";
    const auto r = run(p, json, c);
    ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate) << status;
    EXPECT_FALSE(r.unknown_order_state) << status;
    EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status), expected) << status;
  }
}

TEST(BinanceParser, UnknownStatusIsReported) {
  venue::binance::BinanceParser p;
  Collector c;
  const std::string json =
      R"({"e":"ORDER_TRADE_UPDATE","o":{"s":"X","S":"BUY","o":"LIMIT",)"
      R"("q":"1","p":"1","x":"NEW","X":"SOME_NEW_STATE","i":1,"T":1}})";
  const auto r = run(p, json, c);
  EXPECT_EQ(r.kind, VenueMessageKind::kOrderUpdate);
  EXPECT_TRUE(r.unknown_order_state);
  EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status), OrderStatus::kOpen);
}

TEST(BinanceParser, ListenKeyExpiryIsDistinctFromAnError) {
  // The correct response is to re-establish the session, not to log and carry
  // on, so it must not be reported as a parse error or an unknown message.
  venue::binance::BinanceParser p;
  Collector c;
  const auto r = run(p, R"({"e":"listenKeyExpired","E":1786106096123})", c);
  EXPECT_EQ(r.kind, VenueMessageKind::kSessionExpired);
}

TEST(BinanceParser, ParsesAccountUpdateBalances) {
  venue::binance::BinanceParser p;
  Collector c;
  const auto r = run(p, R"({"e":"ACCOUNT_UPDATE","T":1,"a":{"B":[
      {"a":"USDT","wb":"10000.5","cw":"9500.25"},
      {"a":"BTC","wb":"0.5","cw":"0.5"}]}})",
                     c);
  EXPECT_EQ(r.kind, VenueMessageKind::kPortfolioUpdate);
  ASSERT_EQ(c.accounts.size(), 2u);
  EXPECT_EQ(c.accounts[0].currency, "USDT");
  EXPECT_EQ(c.accounts[0].balance, D("10000.5"));
  EXPECT_EQ(c.accounts[0].available_funds, D("9500.25"));
  EXPECT_EQ(c.accounts[1].currency, "BTC");
}

TEST(BinanceParser, ZeroPriceMeansUnset) {
  // A market order arrives with "p":"0"; treating that as a price would create
  // an order at zero.
  venue::binance::BinanceParser p;
  Collector c;
  const std::string json =
      R"({"e":"ORDER_TRADE_UPDATE","o":{"s":"X","S":"SELL","o":"MARKET",)"
      R"("q":"1","p":"0","x":"NEW","X":"NEW","i":9,"T":1}})";
  ASSERT_EQ(run(p, json, c).kind, VenueMessageKind::kOrderUpdate);
  EXPECT_EQ(c.orders[0].has_price, 0);
  EXPECT_EQ(static_cast<OrderType>(c.orders[0].order_type), OrderType::kMarket);
  EXPECT_EQ(static_cast<OrderSide>(c.orders[0].side), OrderSide::kSell);
}

// ===========================================================================
// Bybit
// ===========================================================================

TEST(BybitParser, ParsesOrderTopic) {
  venue::bybit::BybitParser p;
  Collector c;
  const auto r = run(p, R"({"topic":"order","data":[{
      "category":"linear","symbol":"BTCUSDT","orderId":"BYBIT-1","side":"Buy",
      "orderType":"Limit","qty":"2.5","price":"64000.5","cumExecQty":"1.25",
      "avgPrice":"64000.25","orderStatus":"PartiallyFilled",
      "orderLinkId":"chase-maker","createdTime":"1786106096123",
      "updatedTime":"1786106096456"}]})",
                     c);

  ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate) << (r.error ? r.error : "");
  ASSERT_EQ(c.orders.size(), 1u);
  const auto& o = c.orders[0];
  EXPECT_EQ(o.exchange.view(), "bybit");
  EXPECT_EQ(o.order_id.view(), "BYBIT-1");
  EXPECT_EQ(static_cast<OrderStatus>(o.status), OrderStatus::kPartiallyFilled);
  EXPECT_EQ(o.amount_raw, D("2.5").raw());
  EXPECT_EQ(o.price_raw, D("64000.5").raw());
  EXPECT_EQ(o.filled_amount_raw, D("1.25").raw());
  // Timestamps arrive as QUOTED millisecond epochs.
  EXPECT_EQ(o.exchange_ts_ns, 1786106096456LL * 1'000'000LL);
}

TEST(BybitParser, DropsNonLinearCategories) {
  // One private stream carries linear, inverse, option and spot. A spot fill
  // applied to a perp position would be silently wrong.
  venue::bybit::BybitParser p;
  Collector c;
  const auto r = run(p, R"({"topic":"order","data":[
      {"category":"spot","symbol":"BTCUSDT","orderId":"S-1","side":"Buy",
       "qty":"1","orderStatus":"New","createdTime":"1"},
      {"category":"linear","symbol":"BTCUSDT","orderId":"L-1","side":"Buy",
       "qty":"1","orderStatus":"New","createdTime":"1"},
      {"category":"option","symbol":"BTC-X","orderId":"O-1","side":"Buy",
       "qty":"1","orderStatus":"New","createdTime":"1"}]})",
                     c);

  EXPECT_EQ(r.emitted, 1u);
  ASSERT_EQ(c.orders.size(), 1u);
  EXPECT_EQ(c.orders[0].order_id.view(), "L-1");
}

TEST(BybitParser, MapsEveryKnownStatus) {
  const std::vector<std::pair<std::string, OrderStatus>> cases{
      {"New", OrderStatus::kOpen},
      {"PartiallyFilled", OrderStatus::kPartiallyFilled},
      {"Filled", OrderStatus::kFilled},
      {"Cancelled", OrderStatus::kCancelled},
      {"Rejected", OrderStatus::kRejected},
      {"Deactivated", OrderStatus::kCancelled},
      {"Untriggered", OrderStatus::kPending},
      {"Triggered", OrderStatus::kOpen},
  };
  for (const auto& [status, expected] : cases) {
    venue::bybit::BybitParser p;
    Collector c;
    const std::string json =
        R"({"topic":"order","data":[{"category":"linear","symbol":"X",)"
        R"("orderId":"1","side":"Buy","qty":"1","orderStatus":")" +
        status + R"(","createdTime":"1"}]})";
    const auto r = run(p, json, c);
    ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate) << status;
    EXPECT_FALSE(r.unknown_order_state) << status;
    EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status), expected) << status;
  }
}

TEST(BybitParser, PriceZeroMeansUnset) {
  venue::bybit::BybitParser p;
  Collector c;
  const auto r = run(p, R"({"topic":"order","data":[{"category":"linear",
      "symbol":"X","orderId":"1","side":"Sell","orderType":"Market","qty":"1",
      "price":"0","avgPrice":"0","orderStatus":"New","createdTime":"1"}]})",
                     c);
  ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate);
  EXPECT_EQ(c.orders[0].has_price, 0);
  EXPECT_EQ(c.orders[0].has_average_price, 0);
}

TEST(BybitParser, ParsesExecutionTopic) {
  venue::bybit::BybitParser p;
  Collector c;
  const auto r = run(p, R"({"topic":"execution","data":[{
      "category":"linear","execId":"EXEC-1","orderId":"BYBIT-1","symbol":"BTCUSDT",
      "side":"Buy","execQty":"1.25","execPrice":"64000.25","execFee":"0.0123",
      "feeCurrency":"USDT","isMaker":true,"execTime":"1786106096456"}]})",
                     c);

  ASSERT_EQ(r.kind, VenueMessageKind::kTradeUpdate);
  ASSERT_EQ(c.fills.size(), 1u);
  const auto& f = c.fills[0];
  EXPECT_EQ(f.trade_id.view(), "EXEC-1");
  EXPECT_EQ(f.amount_raw, D("1.25").raw());
  EXPECT_EQ(f.fee_raw, D("0.0123").raw());
  EXPECT_EQ(static_cast<Liquidity>(f.liquidity), Liquidity::kMaker);
  EXPECT_EQ(f.exchange_ts_ns, 1786106096456LL * 1'000'000LL);
}

TEST(BybitParser, IsMakerAcceptsBooleanOrString) {
  // Bybit sends a JSON boolean on some paths and the string "true" on others;
  // the Python accepts both.
  for (const char* value : {"true", "\"true\""}) {
    venue::bybit::BybitParser p;
    Collector c;
    const std::string json =
        R"({"topic":"execution","data":[{"category":"linear","execId":"1",)"
        R"("orderId":"2","symbol":"X","side":"Buy","execQty":"1","execPrice":"1",)"
        R"("isMaker":)" +
        std::string(value) + R"(,"execTime":"1"}]})";
    ASSERT_EQ(run(p, json, c).kind, VenueMessageKind::kTradeUpdate) << value;
    ASSERT_EQ(c.fills.size(), 1u) << value;
    EXPECT_EQ(static_cast<Liquidity>(c.fills[0].liquidity), Liquidity::kMaker)
        << value;
  }

  venue::bybit::BybitParser p;
  Collector c;
  ASSERT_EQ(run(p, R"({"topic":"execution","data":[{"category":"linear",
      "execId":"1","orderId":"2","symbol":"X","side":"Buy","execQty":"1",
      "execPrice":"1","isMaker":false,"execTime":"1"}]})",
                c)
                .kind,
            VenueMessageKind::kTradeUpdate);
  EXPECT_EQ(static_cast<Liquidity>(c.fills[0].liquidity), Liquidity::kTaker);
}

TEST(BybitParser, HandlesOpRepliesAndPong) {
  venue::bybit::BybitParser p;
  Collector c;
  EXPECT_EQ(run(p, R"({"op":"pong","success":true})", c).kind,
            VenueMessageKind::kHeartbeat);
  EXPECT_EQ(run(p, R"({"op":"auth","success":true,"req_id":"7"})", c).kind,
            VenueMessageKind::kSessionEvent);

  const auto err =
      run(p, R"({"op":"auth","success":false,"ret_msg":"invalid sign","req_id":"8"})",
          c);
  EXPECT_EQ(err.kind, VenueMessageKind::kRpcError);
  EXPECT_EQ(err.rpc_error_text, "invalid sign");
  EXPECT_EQ(err.rpc_id, 8);
}

TEST(BybitBuilder, WsAuthSignatureMatchesTheReference) {
  // "GET/realtime" + expires, HMAC-SHA256, lowercase hex.
  EXPECT_EQ(venue::bybit::BybitBuilder::ws_signature("test-secret", 1786106096123LL),
            "49a306f2e33774b3492b30b06948a6d1f6116e549323e6cfb3ab40f421755f9b");

  venue::bybit::BybitBuilder b;
  char buf[venue::bybit::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n =
      b.ws_auth(buf, sizeof(buf), "my-key", "test-secret", 1786106096123LL, id);
  ASSERT_GT(n, 0u);
  const std::string msg(buf, n);
  EXPECT_NE(msg.find(R"("op":"auth")"), std::string::npos) << msg;
  EXPECT_NE(msg.find("49a306f2e33774b3492b30b06948a6d1f6116e549323e6cfb3ab40f421755f9b"),
            std::string::npos)
      << msg;
  // req_id is a STRING on Bybit, unlike every other venue's integer.
  EXPECT_NE(msg.find(R"("req_id":"1")"), std::string::npos) << msg;
}

TEST(BybitBuilder, PlaceOrderBodyQuotesNumbers) {
  models::OrderRequest req;
  req.instrument = "BTCUSDT";
  req.side = OrderSide::kBuy;
  req.order_type = OrderType::kLimit;
  req.amount = *core::Qty::from_string("2.5");
  req.price = D("64000.5");
  req.post_only = true;
  req.label = "chase-maker";

  char buf[venue::bybit::kMaxRequestBytes];
  const std::size_t n =
      venue::bybit::BybitBuilder::place_order_body(buf, sizeof(buf), req);
  const std::string body(buf, n);

  EXPECT_NE(body.find(R"("category":"linear")"), std::string::npos) << body;
  EXPECT_NE(body.find(R"("qty":"2.5")"), std::string::npos) << body;
  EXPECT_NE(body.find(R"("price":"64000.5")"), std::string::npos) << body;
  EXPECT_NE(body.find(R"("timeInForce":"PostOnly")"), std::string::npos) << body;
  EXPECT_NE(body.find(R"("orderLinkId":"chase-maker")"), std::string::npos);

  // A market order without post_only takes IOC, a limit order GTC.
  req.post_only = false;
  req.order_type = OrderType::kMarket;
  const std::size_t m =
      venue::bybit::BybitBuilder::place_order_body(buf, sizeof(buf), req);
  EXPECT_NE(std::string(buf, m).find(R"("timeInForce":"IOC")"),
            std::string::npos);
}

// ===========================================================================
// OKX
// ===========================================================================

TEST(OkxParser, ParsesOrdersChannel) {
  venue::okx::OkxParser p;
  Collector c;
  const auto r = run(p, R"({"arg":{"channel":"orders","instType":"SWAP"},
      "data":[{"instId":"BTC-USDT-SWAP","ordId":"OKX-1","clOrdId":"chase",
      "side":"buy","ordType":"limit","sz":"2.5","px":"64000.5",
      "accFillSz":"1.25","avgPx":"64000.25","state":"partially_filled",
      "cTime":"1786106096123","uTime":"1786106096456"}]})",
                     c);

  ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate) << (r.error ? r.error : "");
  ASSERT_EQ(c.orders.size(), 1u);
  const auto& o = c.orders[0];
  EXPECT_EQ(o.exchange.view(), "okx");
  EXPECT_EQ(o.order_id.view(), "OKX-1");
  EXPECT_EQ(o.instrument.view(), "BTC-USDT-SWAP");
  EXPECT_EQ(static_cast<OrderStatus>(o.status), OrderStatus::kPartiallyFilled);
  EXPECT_EQ(o.amount_raw, D("2.5").raw());
  EXPECT_EQ(o.price_raw, D("64000.5").raw());
  EXPECT_EQ(o.exchange_ts_ns, 1786106096456LL * 1'000'000LL);
}

TEST(OkxParser, EmptyStringMeansAbsentNotZero) {
  // OKX sends "px":"" for an order with no price. Reading that as 0 would
  // create an order priced at zero.
  venue::okx::OkxParser p;
  Collector c;
  const auto r = run(p, R"({"arg":{"channel":"orders"},"data":[{
      "instId":"BTC-USDT-SWAP","ordId":"1","side":"sell","ordType":"market",
      "sz":"1","px":"","accFillSz":"0","avgPx":"","state":"live","cTime":"1"}]})",
                     c);
  ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate);
  EXPECT_EQ(c.orders[0].has_price, 0);
  EXPECT_EQ(c.orders[0].has_average_price, 0);
  EXPECT_EQ(c.orders[0].price_raw, 0);
}

TEST(OkxParser, PostOnlyIsAnOrderTypeNotAFlag) {
  venue::okx::OkxParser p;
  Collector c;
  const auto r = run(p, R"({"arg":{"channel":"orders"},"data":[{
      "instId":"X","ordId":"1","side":"buy","ordType":"post_only","sz":"1",
      "px":"100","state":"live","cTime":"1"}]})",
                     c);
  ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate);
  EXPECT_EQ(static_cast<OrderType>(c.orders[0].order_type), OrderType::kLimit);
}

TEST(OkxParser, MapsEveryKnownStatus) {
  const std::vector<std::pair<std::string, OrderStatus>> cases{
      {"live", OrderStatus::kOpen},
      {"partially_filled", OrderStatus::kPartiallyFilled},
      {"filled", OrderStatus::kFilled},
      {"canceled", OrderStatus::kCancelled},
      {"mmp_canceled", OrderStatus::kCancelled},
  };
  for (const auto& [state, expected] : cases) {
    venue::okx::OkxParser p;
    Collector c;
    const std::string json =
        R"({"arg":{"channel":"orders"},"data":[{"instId":"X","ordId":"1",)"
        R"("side":"buy","ordType":"limit","sz":"1","px":"1","state":")" +
        state + R"(","cTime":"1"}]})";
    const auto r = run(p, json, c);
    ASSERT_EQ(r.kind, VenueMessageKind::kOrderUpdate) << state;
    EXPECT_FALSE(r.unknown_order_state) << state;
    EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status), expected) << state;
  }
}

TEST(OkxParser, FeesAreNormalisedToPositive) {
  // OKX reports a fee PAID as negative. Left signed, fee accounting would flip
  // sign against the other three venues.
  venue::okx::OkxParser p;
  Collector c;
  const auto r = run(p, R"({"arg":{"channel":"fills"},"data":[{
      "instId":"BTC-USDT-SWAP","ordId":"OKX-1","tradeId":"T-1","side":"buy",
      "fillSz":"1.25","fillPx":"64000.25","fee":"-0.0123","feeCcy":"USDT",
      "execType":"M","ts":"1786106096456"}]})",
                     c);

  ASSERT_EQ(r.kind, VenueMessageKind::kTradeUpdate);
  ASSERT_EQ(c.fills.size(), 1u);
  EXPECT_EQ(c.fills[0].fee_raw, D("0.0123").raw()) << "fee must mean cost";
  EXPECT_EQ(static_cast<Liquidity>(c.fills[0].liquidity), Liquidity::kMaker);
}

TEST(OkxParser, ExecTypeMOnlyIsMaker) {
  for (const auto& [exec_type, expected] :
       std::vector<std::pair<std::string, Liquidity>>{
           {"M", Liquidity::kMaker}, {"T", Liquidity::kTaker},
           {"", Liquidity::kTaker}}) {
    venue::okx::OkxParser p;
    Collector c;
    const std::string json =
        R"({"arg":{"channel":"fills"},"data":[{"instId":"X","ordId":"1",)"
        R"("tradeId":"T","side":"buy","fillSz":"1","fillPx":"1","fee":"0",)"
        R"("execType":")" +
        exec_type + R"(","ts":"1"}]})";
    ASSERT_EQ(run(p, json, c).kind, VenueMessageKind::kTradeUpdate) << exec_type;
    EXPECT_EQ(static_cast<Liquidity>(c.fills[0].liquidity), expected)
        << "execType=" << exec_type;
  }
}

TEST(OkxParser, FallsBackToBillId) {
  venue::okx::OkxParser p;
  Collector c;
  const auto r = run(p, R"({"arg":{"channel":"fills"},"data":[{
      "instId":"X","ordId":"1","tradeId":"","billId":"BILL-9","side":"buy",
      "fillSz":"1","fillPx":"1","fee":"0","execType":"T","ts":"1"}]})",
                     c);
  ASSERT_EQ(r.kind, VenueMessageKind::kTradeUpdate);
  ASSERT_EQ(c.fills.size(), 1u);
  EXPECT_EQ(c.fills[0].trade_id.view(), "BILL-9");
}

TEST(OkxParser, PlainTextHeartbeatNeverReachesTheJsonParser) {
  // OKX's liveness exchange is the literal text "ping"/"pong", not JSON.
  venue::okx::OkxParser p;
  Collector c;
  EXPECT_EQ(run(p, "ping", c).kind, VenueMessageKind::kHeartbeat);
  EXPECT_EQ(run(p, "pong", c).kind, VenueMessageKind::kHeartbeat);
  EXPECT_TRUE(venue::okx::is_plain_text_heartbeat("ping"));
  EXPECT_FALSE(venue::okx::is_plain_text_heartbeat("{}"));
}

TEST(OkxParser, EventMessagesAreSessionEventsNotData) {
  venue::okx::OkxParser p;
  Collector c;
  EXPECT_EQ(run(p, R"({"event":"login","code":"0","msg":""})", c).kind,
            VenueMessageKind::kSessionEvent);
  EXPECT_EQ(run(p, R"({"event":"subscribe","arg":{"channel":"orders"}})", c).kind,
            VenueMessageKind::kSessionEvent);

  const auto err =
      run(p, R"({"event":"error","code":"60009","msg":"Login failed."})", c);
  EXPECT_EQ(err.kind, VenueMessageKind::kRpcError);
  EXPECT_EQ(err.rpc_error_text, "Login failed.");
}

TEST(OkxBuilder, WsLoginSignatureMatchesTheReference) {
  // base64, not hex -- OKX differs from Bybit and Binance here.
  EXPECT_EQ(venue::okx::OkxBuilder::ws_signature("test-secret", 1786106096LL),
            "uSMzCIUzpm/NcxhJsJU9W9aQXU+UCIYkiXMd+xd3TfQ=");

  char buf[venue::okx::kMaxRequestBytes];
  const std::size_t n = venue::okx::OkxBuilder::ws_login(
      buf, sizeof(buf), "my-key", "my-passphrase", "test-secret", 1786106096LL);
  ASSERT_GT(n, 0u);
  const std::string msg(buf, n);
  EXPECT_NE(msg.find(R"("op":"login")"), std::string::npos) << msg;
  EXPECT_NE(msg.find(R"("passphrase":"my-passphrase")"), std::string::npos);
  EXPECT_NE(msg.find("uSMzCIUzpm/NcxhJsJU9W9aQXU+UCIYkiXMd+xd3TfQ="),
            std::string::npos)
      << msg;
  // The timestamp must be SECONDS and a string; milliseconds are rejected with
  // an unhelpful error.
  EXPECT_NE(msg.find(R"("timestamp":"1786106096")"), std::string::npos) << msg;
}

TEST(OkxBuilder, PlaceOrderBody) {
  models::OrderRequest req;
  req.instrument = "BTC-USDT-SWAP";
  req.side = OrderSide::kBuy;
  req.order_type = OrderType::kLimit;
  req.amount = *core::Qty::from_string("2.5");
  req.price = D("64000.5");
  req.label = "chase";

  char buf[venue::okx::kMaxRequestBytes];
  std::size_t n = venue::okx::OkxBuilder::place_order_body(buf, sizeof(buf), req);
  std::string body(buf, n);
  EXPECT_NE(body.find(R"("tdMode":"cross")"), std::string::npos) << body;
  EXPECT_NE(body.find(R"("ordType":"limit")"), std::string::npos) << body;
  EXPECT_NE(body.find(R"("sz":"2.5")"), std::string::npos) << body;
  EXPECT_NE(body.find(R"("px":"64000.5")"), std::string::npos) << body;
  EXPECT_NE(body.find(R"("clOrdId":"chase")"), std::string::npos) << body;

  // post_only replaces ordType rather than adding a flag.
  req.post_only = true;
  n = venue::okx::OkxBuilder::place_order_body(buf, sizeof(buf), req);
  body = std::string(buf, n);
  EXPECT_NE(body.find(R"("ordType":"post_only")"), std::string::npos) << body;
  EXPECT_EQ(body.find(R"("post_only":true)"), std::string::npos) << body;
}

TEST(OkxBuilder, SubscribeScopesByInstType) {
  const std::string_view channels[] = {venue::okx::kOrdersChannel,
                                       venue::okx::kFillsChannel,
                                       venue::okx::kAccountChannel};
  char buf[venue::okx::kMaxRequestBytes];
  const std::size_t n =
      venue::okx::OkxBuilder::ws_subscribe(buf, sizeof(buf), channels, 3);
  const std::string msg(buf, n);
  EXPECT_NE(msg.find(R"({"channel":"orders","instType":"SWAP"})"),
            std::string::npos)
      << msg;
  // The account channel is not scoped by instrument type.
  EXPECT_NE(msg.find(R"({"channel":"account"})"), std::string::npos) << msg;
}

// ===========================================================================
// Cross-venue invariants
// ===========================================================================

TEST(AllPerpVenues, RejectMalformedJsonWithoutThrowing) {
  venue::binance::BinanceParser bn;
  venue::bybit::BybitParser by;
  venue::okx::OkxParser ok;
  Collector c;

  for (const char* bad : {"", "{", "not json", R"({"topic":)"}) {
    EXPECT_EQ(run(bn, bad, c).kind, VenueMessageKind::kParseError) << bad;
    EXPECT_EQ(run(by, bad, c).kind, VenueMessageKind::kParseError) << bad;
    // OKX must not mistake an empty frame for its plain-text heartbeat.
    EXPECT_EQ(run(ok, bad, c).kind, VenueMessageKind::kParseError) << bad;
  }
}

TEST(AllPerpVenues, RejectBuffersWithoutSimdjsonPadding) {
  Collector c;
  std::string json = R"({"topic":"order","data":[]})";

  venue::bybit::BybitParser by;
  EXPECT_EQ(by.parse(json.data(), json.size(), json.size(), c).kind,
            VenueMessageKind::kParseError);

  venue::binance::BinanceParser bn;
  EXPECT_EQ(bn.parse(json.data(), json.size(), json.size(), c).kind,
            VenueMessageKind::kParseError);
}

TEST(AllPerpVenues, RejectOverlongInstrumentNames) {
  // A truncated symbol would route an order to the wrong contract.
  const std::string long_symbol(transport::kInstrumentCap + 1, 'X');
  Collector c;

  venue::bybit::BybitParser by;
  const std::string bybit_json =
      R"({"topic":"order","data":[{"category":"linear","symbol":")" +
      long_symbol +
      R"(","orderId":"1","side":"Buy","qty":"1","orderStatus":"New","createdTime":"1"}]})";
  EXPECT_EQ(run(by, bybit_json, c).emitted, 0u);
  EXPECT_TRUE(c.orders.empty());

  venue::okx::OkxParser ok;
  const std::string okx_json =
      R"({"arg":{"channel":"orders"},"data":[{"instId":")" + long_symbol +
      R"(","ordId":"1","side":"buy","ordType":"limit","sz":"1","px":"1","state":"live","cTime":"1"}]})";
  EXPECT_EQ(run(ok, okx_json, c).emitted, 0u);
  EXPECT_TRUE(c.orders.empty());
}

TEST(AllPerpVenues, SequenceNumbersAdvanceMonotonically) {
  // Downstream detects gaps with these; a repeated sequence number would hide
  // a dropped message.
  venue::bybit::BybitParser p;
  Collector c;
  const std::string json =
      R"({"topic":"order","data":[)"
      R"({"category":"linear","symbol":"X","orderId":"1","side":"Buy","qty":"1","orderStatus":"New","createdTime":"1"},)"
      R"({"category":"linear","symbol":"X","orderId":"2","side":"Buy","qty":"1","orderStatus":"New","createdTime":"1"}]})";

  run(p, json, c);
  run(p, json, c);
  ASSERT_EQ(c.orders.size(), 4u);
  for (std::size_t i = 1; i < c.orders.size(); ++i) {
    EXPECT_GT(c.orders[i].hdr.seq, c.orders[i - 1].hdr.seq) << "at " << i;
  }
}
