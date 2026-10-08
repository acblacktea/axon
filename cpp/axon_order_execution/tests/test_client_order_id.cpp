// The client order id, end to end through the venue layer.
//
// Every order now goes out with a client id -- the strategy's label, or our
// internal_order_id -- and every venue echoes it on its updates. That echo is
// what ties an update to the request that produced it, which is the only way
// to resolve a placement whose reply was lost: without it, a timeout leaves a
// strategy unable to tell "not placed" from "placed and live", and the natural
// retry doubles the position.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "axon/models/order.h"
#include "axon/venue/binance/binance_builder.h"
#include "axon/venue/binance/binance_parser.h"
#include "axon/venue/bybit/bybit_builder.h"
#include "axon/venue/bybit/bybit_parser.h"
#include "axon/venue/deribit/deribit_builder.h"
#include "axon/venue/deribit/deribit_parser.h"
#include "axon/venue/okx/okx_builder.h"
#include "axon/venue/okx/okx_parser.h"

using namespace axon;

namespace {

struct Collector {
  std::vector<transport::OrderUpdateMsg> orders;
  void on_order(const transport::OrderUpdateMsg& m) { orders.push_back(m); }
  void on_fill(const transport::FillMsg&) {}
  void on_account(const models::AccountSummary&) {}
};

template <typename Parser>
std::vector<transport::OrderUpdateMsg> parse(Parser& p, std::string_view json) {
  std::string buf(json);
  const std::size_t len = buf.size();
  buf.resize(len + venue::kJsonPadding, '\0');
  Collector c;
  p.parse(buf.data(), len, buf.size(), c);
  return c.orders;
}

models::OrderRequest limit_buy(const char* instrument) {
  models::OrderRequest r;
  r.instrument = instrument;
  r.side = models::OrderSide::kBuy;
  r.order_type = models::OrderType::kLimit;
  r.amount = *core::Qty::from_string("2.5");
  r.price = core::Price::from_string("64000.5");
  return r;
}

const std::string kId = "0123456789abcdef0123456789abcdef";

std::string binance_frame(const std::string& c) {
  return R"({"e":"ORDER_TRADE_UPDATE","T":1786106096123,"E":1786106096124,
"o":{"s":"BTCUSDT","c":")" + c + R"(","S":"BUY","o":"LIMIT","f":"GTX",
"q":"2.5","p":"64000.5","ap":"0","x":"NEW","X":"NEW","i":12345678,
"l":"0","z":"0","L":"0","n":"0","N":"USDT","T":1786106096123,"t":0,"m":false}})";
}
std::string bybit_frame(const std::string& c) {
  return R"({"topic":"order","data":[{"category":"linear","symbol":"BTCUSDT",
"orderId":"BYBIT-1","side":"Buy","orderType":"Limit","qty":"2.5","price":"64000.5",
"cumExecQty":"0","avgPrice":"0","orderStatus":"New","orderLinkId":")" + c + R"(",
"createdTime":"1786106096123","updatedTime":"1786106096456"}]})";
}
std::string okx_frame(const std::string& c) {
  return R"({"arg":{"channel":"orders","instType":"SWAP"},"data":[{
"instId":"BTC-USDT-SWAP","ordId":"OKX-1","clOrdId":")" + c + R"(","side":"buy",
"ordType":"limit","sz":"2.5","px":"64000.5","accFillSz":"0","avgPx":"",
"state":"live","cTime":"1786106096123","uTime":"1786106096456"}]})";
}
std::string deribit_frame(const std::string& c) {
  return R"({"jsonrpc":"2.0","method":"subscription","params":{
"channel":"user.orders.BTC-PERPETUAL.raw","data":{"order_id":"DERIBIT-1",
"instrument_name":"BTC-PERPETUAL","direction":"buy","order_type":"limit",
"amount":2.5,"filled_amount":0,"price":64000.5,"average_price":0,
"order_state":"open","label":")" + c + R"(","creation_timestamp":1786106096123,
"last_update_timestamp":1786106096456}}})";
}

}  // namespace

// ===========================================================================
// The request side
// ===========================================================================

TEST(OrderRequestClientId, DefaultsToTheInternalOrderId) {
  auto r = limit_buy("BTCUSDT");
  EXPECT_EQ(r.venue_client_id(), r.internal_order_id);
  EXPECT_EQ(r.internal_order_id.size(), 32u);
  r.label = "chase-maker";
  EXPECT_EQ(r.venue_client_id(), "chase-maker");
  r.label = "";
  EXPECT_EQ(r.venue_client_id(), r.internal_order_id);
}

// OKX's clOrdId -- 1 to 32 alphanumerics -- is the strictest of the four.
TEST(OrderRequestClientId, InternalOrderIdMustSuitEveryVenue) {
  auto r = limit_buy("BTCUSDT");
  EXPECT_FALSE(r.validate().has_value());
  r.internal_order_id = "";
  EXPECT_TRUE(r.validate().has_value());
  r.internal_order_id = std::string(33, 'a');
  EXPECT_TRUE(r.validate().has_value());
  r.internal_order_id = "has-dash";
  EXPECT_TRUE(r.validate().has_value());
  r.internal_order_id = "Mixed123Case";
  EXPECT_FALSE(r.validate().has_value());
}

TEST(BuilderClientId, BinanceSendsItAsNewClientOrderId) {
  venue::binance::BinanceBuilder b;
  char buf[venue::binance::kMaxRequestBytes];
  std::int64_t id = 0;
  auto r = limit_buy("BTCUSDT");
  r.internal_order_id = kId;
  std::string out(buf, b.ws_place_order(buf, sizeof buf, r, "k", "s", 1, id));
  EXPECT_NE(out.find(R"("newClientOrderId":")" + kId + "\""), std::string::npos) << out;

  r.label = "chase-maker";
  out.assign(buf, b.ws_place_order(buf, sizeof buf, r, "k", "s", 1, id));
  EXPECT_NE(out.find(R"("newClientOrderId":"chase-maker")"), std::string::npos) << out;
}

TEST(BuilderClientId, BybitSendsItAsOrderLinkId) {
  venue::bybit::BybitBuilder b;
  char buf[venue::bybit::kMaxRequestBytes];
  auto r = limit_buy("BTCUSDT");
  r.internal_order_id = kId;
  std::string out(buf, b.place_order_body(buf, sizeof buf, r));
  EXPECT_NE(out.find(R"("orderLinkId":")" + kId + "\""), std::string::npos) << out;
}

TEST(BuilderClientId, OkxSendsItAsClOrdId) {
  venue::okx::OkxBuilder b;
  char buf[venue::okx::kMaxRequestBytes];
  auto r = limit_buy("BTC-USDT-SWAP");
  r.internal_order_id = kId;
  std::string out(buf, b.place_order_body(buf, sizeof buf, r));
  EXPECT_NE(out.find(R"("clOrdId":")" + kId + "\""), std::string::npos) << out;
}

TEST(BuilderClientId, DeribitSendsItAsLabel) {
  venue::deribit::DeribitBuilder b;
  char buf[venue::deribit::kMaxRequestBytes];
  std::int64_t id = 0;
  auto r = limit_buy("BTC-PERPETUAL");
  r.internal_order_id = kId;
  std::string out(buf, b.place_order(buf, sizeof buf, r, id));
  EXPECT_NE(out.find(R"("label":")" + kId + "\""), std::string::npos) << out;
}

// ===========================================================================
// The echo: each venue's order update carries it back
// ===========================================================================

TEST(ParserClientId, BinanceReadsC) {
  venue::binance::BinanceParser p;
  auto o = parse(p, binance_frame(kId));
  ASSERT_EQ(o.size(), 1u);
  EXPECT_EQ(o[0].internal_order_id.view(), kId);
}

TEST(ParserClientId, BybitReadsOrderLinkId) {
  venue::bybit::BybitParser p;
  auto o = parse(p, bybit_frame(kId));
  ASSERT_EQ(o.size(), 1u);
  EXPECT_EQ(o[0].internal_order_id.view(), kId);
}

TEST(ParserClientId, OkxReadsClOrdId) {
  venue::okx::OkxParser p;
  auto o = parse(p, okx_frame(kId));
  ASSERT_EQ(o.size(), 1u);
  EXPECT_EQ(o[0].internal_order_id.view(), kId);
}

TEST(ParserClientId, DeribitReadsLabel) {
  venue::deribit::DeribitParser p;
  auto o = parse(p, deribit_frame(kId));
  ASSERT_EQ(o.size(), 1u);
  EXPECT_EQ(o[0].internal_order_id.view(), kId);
}

// A label the OMS maps back to its request is carried through as-is.
TEST(ParserClientId, ALabelIsCarriedForTheOmsToMap) {
  venue::bybit::BybitParser p;
  auto o = parse(p, bybit_frame("chase-maker"));
  ASSERT_EQ(o.size(), 1u);
  EXPECT_EQ(o[0].internal_order_id.view(), "chase-maker");
}

// An order placed elsewhere (the venue's web UI, another system) can carry an
// id longer than the field. It is left empty rather than truncated -- a
// truncated id could collide with one of ours.
TEST(ParserClientId, AnIdTooLongForTheFieldIsLeftEmpty) {
  venue::binance::BinanceParser p;
  auto o = parse(p, binance_frame("web_usdt_rvrfakrvetf7acfj4bt7hm4xxxxxxxx"));
  ASSERT_EQ(o.size(), 1u);
  EXPECT_TRUE(o[0].internal_order_id.empty());
  EXPECT_EQ(o[0].order_id.view(), "12345678");  // the update itself still parses
}

TEST(ParserClientId, MissingClientIdLeavesItEmpty) {
  venue::okx::OkxParser p;
  auto o = parse(p, R"({"arg":{"channel":"orders","instType":"SWAP"},"data":[{
"instId":"BTC-USDT-SWAP","ordId":"OKX-1","side":"buy","ordType":"limit","sz":"1",
"px":"1","accFillSz":"0","avgPx":"","state":"live","cTime":"1","uTime":"2"}]})");
  ASSERT_EQ(o.size(), 1u);
  EXPECT_TRUE(o[0].internal_order_id.empty());
}
