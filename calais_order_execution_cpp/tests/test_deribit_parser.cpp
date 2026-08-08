// Deribit inbound parser tests.
//
// Payloads are shaped like the real feed, and every field mapping asserted
// here was transcribed from oms/deribit/deribit_ws.py. When one of these fails
// after a change, the question to ask is "did the Python do this?" -- because
// the two implementations must agree or a strategy will see different state
// depending on which engine it is talking to.

#include "calais/venue/deribit/deribit_parser.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace calais;
using namespace calais::venue::deribit;
using calais::models::Liquidity;
using calais::models::OrderSide;
using calais::models::OrderStatus;
using calais::models::OrderType;

namespace {

// Collects everything the parser emits so a test can assert on it.
struct Collector {
  std::vector<transport::OrderUpdateMsg> orders;
  std::vector<transport::FillMsg> fills;
  std::vector<models::AccountSummary> accounts;

  void on_order(const transport::OrderUpdateMsg& m) { orders.push_back(m); }
  void on_fill(const transport::FillMsg& m) { fills.push_back(m); }
  void on_account(const models::AccountSummary& a) { accounts.push_back(a); }
};

// simdjson reads past the end of the document, so every input must carry
// padding. Getting this wrong is a heap overread, which is why the wrapper
// makes it a parameter rather than an assumption.
class Padded {
 public:
  explicit Padded(std::string_view json) : buf_(json) {
    len_ = json.size();
    buf_.resize(len_ + venue::kJsonPadding, '\0');
  }
  const char* data() const { return buf_.data(); }
  std::size_t len() const { return len_; }
  std::size_t capacity() const { return buf_.size(); }

 private:
  std::string buf_;
  std::size_t len_;
};

ParseResult run(DeribitParser& p, std::string_view json, Collector& c) {
  Padded padded(json);
  return p.parse(padded.data(), padded.len(), padded.capacity(), c);
}

constexpr std::string_view kOrderFrame = R"({
  "jsonrpc":"2.0","method":"subscription",
  "params":{"channel":"user.orders.BTC-27JUN25-100000-C.raw",
  "data":{"order_id":"DERIBIT-12345","instrument_name":"BTC-27JUN25-100000-C",
  "direction":"buy","order_type":"limit","amount":2.5,"filled_amount":0,
  "price":0.0345,"average_price":0,"order_state":"open","label":"chase-maker",
  "post_only":true,"reject_post_only":false,
  "creation_timestamp":1786106096123,"last_update_timestamp":1786106096456}}})";

constexpr std::string_view kTradeFrame = R"({
  "jsonrpc":"2.0","method":"subscription",
  "params":{"channel":"user.trades.BTC-27JUN25-100000-C.raw",
  "data":[{"trade_id":"TRADE-9876","order_id":"DERIBIT-12345",
  "instrument_name":"BTC-27JUN25-100000-C","direction":"buy","amount":1.25,
  "price":0.0344,"fee":0.0000123,"fee_currency":"BTC","liquidity":"M",
  "timestamp":1786106096456,"index_price":64000.5,"mark_price":64000.6,
  "iv":68.5,"profit_loss":-0.0012,"label":"chase-maker"}]}})";

}  // namespace

// ---------------------------------------------------------------------------
// Order updates
// ---------------------------------------------------------------------------

TEST(DeribitParser, ParsesOrderUpdate) {
  DeribitParser p;
  Collector c;
  const auto r = run(p, kOrderFrame, c);

  ASSERT_EQ(r.kind, MessageKind::kOrderUpdate) << (r.error ? r.error : "");
  ASSERT_EQ(c.orders.size(), 1u);
  EXPECT_EQ(r.emitted, 1u);
  EXPECT_FALSE(r.unknown_order_state);

  const auto& o = c.orders[0];
  EXPECT_EQ(o.hdr.kind(), transport::HotMsgType::kOrderUpdate);
  EXPECT_EQ(o.exchange.view(), "deribit");
  EXPECT_EQ(o.order_id.view(), "DERIBIT-12345");
  EXPECT_EQ(o.instrument.view(), "BTC-27JUN25-100000-C");
  EXPECT_EQ(static_cast<OrderSide>(o.side), OrderSide::kBuy);
  EXPECT_EQ(static_cast<OrderType>(o.order_type), OrderType::kLimit);
  EXPECT_EQ(static_cast<OrderStatus>(o.status), OrderStatus::kOpen);

  // Prices must come through EXACTLY -- this is the whole point of routing
  // them through raw token text instead of a double.
  EXPECT_EQ(o.amount_raw, core::Qty::from_string("2.5")->raw());
  EXPECT_EQ(o.price_raw, core::Price::from_string("0.0345")->raw());
  EXPECT_EQ(o.filled_amount_raw, 0);
  EXPECT_EQ(o.has_price, 1);

  // last_update_timestamp wins over creation_timestamp.
  EXPECT_EQ(o.exchange_ts_ns, 1786106096456LL * 1'000'000LL);
}

TEST(DeribitParser, DerivesPartiallyFilledFromFilledAmount) {
  // Deribit has no partially-filled state; _parse_order derives it.
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":"X","direction":"buy",)"
      R"("order_type":"limit","amount":10,"filled_amount":3.25,"price":1,)"
      R"("order_state":"open","creation_timestamp":1786106096123}}})";

  ASSERT_EQ(run(p, json, c).kind, MessageKind::kOrderUpdate);
  ASSERT_EQ(c.orders.size(), 1u);
  EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status),
            OrderStatus::kPartiallyFilled);
  EXPECT_EQ(c.orders[0].filled_amount_raw,
            core::Qty::from_string("3.25")->raw());
}

TEST(DeribitParser, TerminalStatesAreNotRewrittenByAFill) {
  // refine_with_fill applies only to open orders -- a filled order with a
  // non-zero fill is still filled, not partially filled.
  for (const auto& [state, expected] :
       std::vector<std::pair<std::string, OrderStatus>>{
           {"filled", OrderStatus::kFilled},
           {"cancelled", OrderStatus::kCancelled},
           {"rejected", OrderStatus::kRejected}}) {
    DeribitParser p;
    Collector c;
    const std::string json =
        R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
        R"("data":{"order_id":"1","instrument_name":"X","direction":"sell",)"
        R"("order_type":"limit","amount":10,"filled_amount":10,"price":1,)"
        R"("order_state":")" +
        state + R"(","creation_timestamp":1786106096123}}})";

    ASSERT_EQ(run(p, json, c).kind, MessageKind::kOrderUpdate) << state;
    ASSERT_EQ(c.orders.size(), 1u) << state;
    EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status), expected) << state;
  }
}

TEST(DeribitParser, MapsEveryKnownOrderState) {
  const std::vector<std::pair<std::string, OrderStatus>> cases{
      {"open", OrderStatus::kOpen},
      {"filled", OrderStatus::kFilled},
      {"cancelled", OrderStatus::kCancelled},
      {"rejected", OrderStatus::kRejected},
      {"untriggered", OrderStatus::kPending},
  };
  for (const auto& [state, expected] : cases) {
    DeribitParser p;
    Collector c;
    const std::string json =
        R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
        R"("data":{"order_id":"1","instrument_name":"X","direction":"buy",)"
        R"("order_type":"limit","amount":1,"filled_amount":0,"price":1,)"
        R"("order_state":")" +
        state + R"(","creation_timestamp":1}}})";

    const auto r = run(p, json, c);
    ASSERT_EQ(r.kind, MessageKind::kOrderUpdate) << state;
    EXPECT_FALSE(r.unknown_order_state) << state;
    EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status), expected) << state;
  }
}

TEST(DeribitParser, UnknownOrderStateBecomesOpenButIsReported) {
  // Matching the Python's default, but NOT silently: an unrecognised state is
  // surfaced so it can be counted and alerted on rather than discovered later.
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":"X","direction":"buy",)"
      R"("order_type":"limit","amount":1,"filled_amount":0,"price":1,)"
      R"("order_state":"some_new_state","creation_timestamp":1}}})";

  const auto r = run(p, json, c);
  ASSERT_EQ(r.kind, MessageKind::kOrderUpdate);
  EXPECT_TRUE(r.unknown_order_state);
  EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status), OrderStatus::kOpen);
}

TEST(DeribitParser, MissingOrderStateDefaultsToOpenWithoutFlagging) {
  // data.get("order_state", "open") -- absent is the documented default, so it
  // is not an anomaly.
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":"X","direction":"buy",)"
      R"("order_type":"limit","amount":1,"filled_amount":0,"price":1,)"
      R"("creation_timestamp":1}}})";

  const auto r = run(p, json, c);
  ASSERT_EQ(r.kind, MessageKind::kOrderUpdate);
  EXPECT_FALSE(r.unknown_order_state);
  EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status), OrderStatus::kOpen);
}

TEST(DeribitParser, DirectionAndTypeFallBackTheSameWayAsPython) {
  // OrderSide.BUY if direction == "buy" else SELL;
  // OrderType.LIMIT if order_type == "limit" else MARKET.
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":"X","direction":"anything",)"
      R"("order_type":"market","amount":1,"filled_amount":0,)"
      R"("order_state":"open","creation_timestamp":1}}})";

  ASSERT_EQ(run(p, json, c).kind, MessageKind::kOrderUpdate);
  EXPECT_EQ(static_cast<OrderSide>(c.orders[0].side), OrderSide::kSell);
  EXPECT_EQ(static_cast<OrderType>(c.orders[0].order_type), OrderType::kMarket);
  EXPECT_EQ(c.orders[0].has_price, 0) << "no price field means no price";
}

TEST(DeribitParser, FallsBackToCreationTimestamp) {
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":"X","direction":"buy",)"
      R"("order_type":"limit","amount":1,"filled_amount":0,"price":1,)"
      R"("order_state":"open","creation_timestamp":1786106096123}}})";

  ASSERT_EQ(run(p, json, c).kind, MessageKind::kOrderUpdate);
  EXPECT_EQ(c.orders[0].exchange_ts_ns, 1786106096123LL * 1'000'000LL);
}

TEST(DeribitParser, RejectsOrderWithoutRequiredFields) {
  // creation_timestamp, order_id, instrument_name and direction are indexed
  // directly by the Python and would raise; here they fail the parse.
  const std::vector<std::string> missing{
      R"("instrument_name":"X","direction":"buy","creation_timestamp":1)",
      R"("order_id":"1","direction":"buy","creation_timestamp":1)",
      R"("order_id":"1","instrument_name":"X","creation_timestamp":1)",
      R"("order_id":"1","instrument_name":"X","direction":"buy")",
  };
  for (const auto& body : missing) {
    DeribitParser p;
    Collector c;
    const std::string json =
        R"({"method":"subscription","params":{"channel":"user.orders.X.raw","data":{)" +
        body + R"(}}})";
    const auto r = run(p, json, c);
    EXPECT_EQ(r.kind, MessageKind::kParseError) << body;
    EXPECT_TRUE(c.orders.empty()) << body;
  }
}

TEST(DeribitParser, PricesArriveExactNotViaDouble) {
  // 0.1 + 0.2 style drift must be impossible. These values are chosen so a
  // double round-trip would show up in the raw integer.
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":"X","direction":"buy",)"
      R"("order_type":"limit","amount":0.000000001,"filled_amount":0.123456789,)"
      R"("price":9999999.999999999,"order_state":"open","creation_timestamp":1}}})";

  ASSERT_EQ(run(p, json, c).kind, MessageKind::kOrderUpdate);
  EXPECT_EQ(c.orders[0].amount_raw, 1);
  EXPECT_EQ(c.orders[0].filled_amount_raw, 123456789);
  EXPECT_EQ(c.orders[0].price_raw, 9999999999999999LL);
}

TEST(DeribitParser, AcceptsNumbersQuotedAsStrings) {
  // Deribit sends bare JSON numbers, but Binance and Bybit quote theirs. The
  // shared as_decimal() must handle both, and the string form is the one that
  // is exact by construction -- no float is involved either way.
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":"X","direction":"buy",)"
      R"("order_type":"limit","amount":"2.5","filled_amount":"1.25",)"
      R"("price":"0.0345","order_state":"open","creation_timestamp":1}}})";

  ASSERT_EQ(run(p, json, c).kind, MessageKind::kOrderUpdate);
  ASSERT_EQ(c.orders.size(), 1u);
  EXPECT_EQ(c.orders[0].amount_raw, core::Qty::from_string("2.5")->raw());
  EXPECT_EQ(c.orders[0].filled_amount_raw, core::Qty::from_string("1.25")->raw());
  EXPECT_EQ(c.orders[0].price_raw, core::Price::from_string("0.0345")->raw());
  EXPECT_EQ(static_cast<OrderStatus>(c.orders[0].status),
            OrderStatus::kPartiallyFilled);
}

TEST(DeribitParser, AcceptsScientificNotation) {
  // A raw-text decimal parse handles exponents; a naive digit scanner would
  // silently read 1e-8 as 1.
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":"X","direction":"buy",)"
      R"("order_type":"limit","amount":1e-8,"filled_amount":0,"price":1.5e2,)"
      R"("order_state":"open","creation_timestamp":1}}})";

  ASSERT_EQ(run(p, json, c).kind, MessageKind::kOrderUpdate);
  EXPECT_EQ(c.orders[0].amount_raw, 10) << "1e-8 == 10 raw units at 9 decimals";
  EXPECT_EQ(c.orders[0].price_raw, core::Price::from_string("150")->raw());
}

// ---------------------------------------------------------------------------
// Trades
// ---------------------------------------------------------------------------

TEST(DeribitParser, ParsesTradeArray) {
  DeribitParser p;
  Collector c;
  const auto r = run(p, kTradeFrame, c);

  ASSERT_EQ(r.kind, MessageKind::kTradeUpdate) << (r.error ? r.error : "");
  ASSERT_EQ(c.fills.size(), 1u);
  EXPECT_EQ(r.emitted, 1u);

  const auto& f = c.fills[0];
  EXPECT_EQ(f.hdr.kind(), transport::HotMsgType::kFill);
  EXPECT_EQ(f.exchange.view(), "deribit");
  EXPECT_EQ(f.trade_id.view(), "TRADE-9876");
  EXPECT_EQ(f.order_id.view(), "DERIBIT-12345");
  EXPECT_EQ(f.instrument.view(), "BTC-27JUN25-100000-C");
  EXPECT_EQ(static_cast<OrderSide>(f.side), OrderSide::kBuy);
  EXPECT_EQ(f.amount_raw, core::Qty::from_string("1.25")->raw());
  EXPECT_EQ(f.price_raw, core::Price::from_string("0.0344")->raw());
  EXPECT_EQ(f.fee_raw, core::Price::from_string("0.0000123")->raw());
  EXPECT_EQ(f.fee_currency.view(), "BTC");
  EXPECT_EQ(static_cast<Liquidity>(f.liquidity), Liquidity::kMaker);
  EXPECT_EQ(f.exchange_ts_ns, 1786106096456LL * 1'000'000LL);
}

TEST(DeribitParser, LiquidityIsTakerOnlyForExactlyT) {
  // _parse_fill: TAKER if liquidity == "T" else MAKER.
  for (const auto& [value, expected] :
       std::vector<std::pair<std::string, Liquidity>>{
           {"T", Liquidity::kTaker},
           {"M", Liquidity::kMaker},
           {"t", Liquidity::kMaker},
           {"", Liquidity::kMaker}}) {
    DeribitParser p;
    Collector c;
    const std::string json =
        R"({"method":"subscription","params":{"channel":"user.trades.X.raw",)"
        R"("data":[{"trade_id":"1","order_id":"2","instrument_name":"X",)"
        R"("direction":"buy","amount":1,"price":1,"fee":0,"fee_currency":"BTC",)"
        R"("liquidity":")" +
        value + R"(","timestamp":1}]}})";

    ASSERT_EQ(run(p, json, c).kind, MessageKind::kTradeUpdate) << value;
    ASSERT_EQ(c.fills.size(), 1u) << value;
    EXPECT_EQ(static_cast<Liquidity>(c.fills[0].liquidity), expected)
        << "liquidity=" << value;
  }
}

TEST(DeribitParser, ParsesMultipleTradesInOneFrame) {
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.trades.X.raw","data":[)"
      R"({"trade_id":"1","order_id":"A","instrument_name":"X","direction":"buy",)"
      R"("amount":1,"price":10,"fee":0,"fee_currency":"BTC","liquidity":"M","timestamp":1},)"
      R"({"trade_id":"2","order_id":"A","instrument_name":"X","direction":"buy",)"
      R"("amount":2,"price":11,"fee":0,"fee_currency":"BTC","liquidity":"T","timestamp":2},)"
      R"({"trade_id":"3","order_id":"B","instrument_name":"Y","direction":"sell",)"
      R"("amount":3,"price":12,"fee":0,"fee_currency":"ETH","liquidity":"M","timestamp":3})"
      R"(]}})";

  const auto r = run(p, json, c);
  ASSERT_EQ(r.kind, MessageKind::kTradeUpdate);
  EXPECT_EQ(r.emitted, 3u);
  ASSERT_EQ(c.fills.size(), 3u);
  EXPECT_EQ(c.fills[0].trade_id.view(), "1");
  EXPECT_EQ(c.fills[1].trade_id.view(), "2");
  EXPECT_EQ(static_cast<Liquidity>(c.fills[1].liquidity), Liquidity::kTaker);
  EXPECT_EQ(c.fills[2].instrument.view(), "Y");
  EXPECT_EQ(c.fills[2].fee_currency.view(), "ETH");
  // Sequence numbers must be distinct so the consumer can order them.
  EXPECT_NE(c.fills[0].hdr.seq, c.fills[1].hdr.seq);
  EXPECT_NE(c.fills[1].hdr.seq, c.fills[2].hdr.seq);
}

TEST(DeribitParser, SkipsMalformedTradesButKeepsTheGoodOnes) {
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.trades.X.raw","data":[)"
      R"({"order_id":"A","instrument_name":"X","direction":"buy","amount":1,"price":1,"timestamp":1},)"
      R"({"trade_id":"2","order_id":"A","instrument_name":"X","direction":"buy",)"
      R"("amount":2,"price":11,"fee":0,"fee_currency":"BTC","liquidity":"T","timestamp":2})"
      R"(]}})";

  const auto r = run(p, json, c);
  EXPECT_EQ(r.kind, MessageKind::kTradeUpdate);
  EXPECT_EQ(r.emitted, 1u) << "the trade with no trade_id must be dropped";
  ASSERT_EQ(c.fills.size(), 1u);
  EXPECT_EQ(c.fills[0].trade_id.view(), "2");
}

TEST(DeribitParser, MissingTradeTimestampStaysAtEpoch) {
  // DIVERGENCE from the Python, which substitutes utcnow() and thereby makes
  // an exchange-side delay look like zero latency. Epoch is obviously wrong,
  // which is the point.
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.trades.X.raw",)"
      R"("data":[{"trade_id":"1","order_id":"2","instrument_name":"X",)"
      R"("direction":"buy","amount":1,"price":1,"fee":0,"fee_currency":"BTC",)"
      R"("liquidity":"M"}]}})";

  ASSERT_EQ(run(p, json, c).kind, MessageKind::kTradeUpdate);
  ASSERT_EQ(c.fills.size(), 1u);
  EXPECT_EQ(c.fills[0].exchange_ts_ns, 0);
}

// ---------------------------------------------------------------------------
// Portfolio
// ---------------------------------------------------------------------------

TEST(DeribitParser, ParsesPortfolioUpdate) {
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.portfolio.BTC",)"
      R"("data":{"currency":"BTC","equity":12.3456789,"balance":12,)"
      R"("available_funds":10.5,"initial_margin":1.5,"maintenance_margin":0.75,)"
      R"("margin_balance":12.1,"delta_total":0.4523,"options_delta":0.35,)"
      R"("options_gamma":0.0001,"options_vega":-0.005,"options_theta":-0.02,)"
      R"("futures_pl":0.1,"options_pl":-0.05,"total_pl":0.05}}})";

  const auto r = run(p, json, c);
  ASSERT_EQ(r.kind, MessageKind::kPortfolioUpdate) << (r.error ? r.error : "");
  ASSERT_EQ(c.accounts.size(), 1u);

  const auto& a = c.accounts[0];
  EXPECT_EQ(a.currency, "BTC");
  EXPECT_EQ(a.exchange, "deribit");
  EXPECT_EQ(a.equity, *core::Price::from_string("12.3456789"));
  EXPECT_EQ(a.balance, *core::Price::from_string("12"));
  EXPECT_EQ(a.options_vega, *core::Price::from_string("-0.005"));
  EXPECT_EQ(a.total_pl, *core::Price::from_string("0.05"));
}

TEST(DeribitParser, MissingPortfolioFieldsDefaultToZero) {
  DeribitParser p;
  Collector c;
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.portfolio.BTC",)"
      R"("data":{"currency":"ETH","equity":1}}})";

  ASSERT_EQ(run(p, json, c).kind, MessageKind::kPortfolioUpdate);
  ASSERT_EQ(c.accounts.size(), 1u);
  EXPECT_EQ(c.accounts[0].currency, "ETH");
  EXPECT_EQ(c.accounts[0].equity, *core::Price::from_string("1"));
  EXPECT_TRUE(c.accounts[0].total_pl.is_zero());
}

// ---------------------------------------------------------------------------
// Envelope handling
// ---------------------------------------------------------------------------

TEST(DeribitParser, DetectsHeartbeatTestRequest) {
  // Deribit closes the connection if a test_request goes unanswered, so this
  // must be distinguishable from an ordinary heartbeat.
  DeribitParser p;
  Collector c;
  const auto r = run(
      p, R"({"jsonrpc":"2.0","method":"heartbeat","params":{"type":"test_request"}})",
      c);
  EXPECT_EQ(r.kind, MessageKind::kHeartbeatTestRequest);
}

TEST(DeribitParser, PlainHeartbeatNeedsNoResponse) {
  DeribitParser p;
  Collector c;
  const auto r = run(
      p, R"({"jsonrpc":"2.0","method":"heartbeat","params":{"type":"heartbeat"}})",
      c);
  EXPECT_EQ(r.kind, MessageKind::kHeartbeat);
}

TEST(DeribitParser, ParsesRpcResult) {
  DeribitParser p;
  Collector c;
  const auto r = run(
      p, R"({"jsonrpc":"2.0","id":42,"result":{"access_token":"x","expires_in":900}})",
      c);
  EXPECT_EQ(r.kind, MessageKind::kRpcResult);
  EXPECT_EQ(r.rpc_id, 42);
}

TEST(DeribitParser, ParsesRpcError) {
  DeribitParser p;
  Collector c;
  const auto r = run(
      p,
      R"({"jsonrpc":"2.0","id":7,"error":{"code":13004,"message":"invalid_credentials"}})",
      c);
  EXPECT_EQ(r.kind, MessageKind::kRpcError);
  EXPECT_EQ(r.rpc_id, 7);
  EXPECT_EQ(r.rpc_error_text, "invalid_credentials");
}

TEST(DeribitParser, UnknownMethodsAreIgnoredNotFatal) {
  DeribitParser p;
  Collector c;
  EXPECT_EQ(run(p, R"({"jsonrpc":"2.0","method":"something_new","params":{}})", c)
                .kind,
            MessageKind::kUnknown);
  EXPECT_EQ(run(p, R"({"jsonrpc":"2.0"})", c).kind, MessageKind::kUnknown);
}

TEST(DeribitParser, UnknownChannelIsIgnored) {
  DeribitParser p;
  Collector c;
  const auto r = run(
      p,
      R"({"method":"subscription","params":{"channel":"book.BTC.raw","data":{}}})",
      c);
  EXPECT_EQ(r.kind, MessageKind::kUnknown);
  EXPECT_TRUE(c.orders.empty());
}

TEST(DeribitParser, MalformedJsonIsAnOrdinaryReturnValue) {
  // A garbage frame arrives on a network socket as a matter of course; it must
  // never throw out of the receive loop.
  DeribitParser p;
  Collector c;
  for (const char* bad : {"", "{", "not json", R"({"method":)",
                          R"({"method":"subscription","params":)"}) {
    const auto r = run(p, bad, c);
    EXPECT_EQ(r.kind, MessageKind::kParseError) << bad;
  }
}

TEST(DeribitParser, WrongDataShapeIsAParseError) {
  DeribitParser p;
  Collector c;
  // orders channel with an array instead of an object
  EXPECT_EQ(
      run(p, R"({"method":"subscription","params":{"channel":"user.orders.X.raw","data":[]}})",
          c)
          .kind,
      MessageKind::kParseError);
  // trades channel with an object instead of an array
  EXPECT_EQ(
      run(p, R"({"method":"subscription","params":{"channel":"user.trades.X.raw","data":{}}})",
          c)
          .kind,
      MessageKind::kParseError);
}

TEST(DeribitParser, RejectsBuffersWithoutSimdjsonPadding) {
  // Feeding simdjson an unpadded buffer is a heap overread. The wrapper makes
  // that impossible rather than merely documented.
  DeribitParser p;
  Collector c;
  std::string json(kOrderFrame);
  EXPECT_EQ(p.parse(json.data(), json.size(), json.size(), c).kind,
            MessageKind::kParseError);
}

TEST(DeribitParser, ReusesOneParserAcrossManyMessages) {
  // The parser holds simdjson's scratch buffers; reusing it is what keeps
  // parsing allocation-free after the first message.
  DeribitParser p;
  Collector c;
  for (int i = 0; i < 5000; ++i) {
    const auto r = run(p, kOrderFrame, c);
    ASSERT_EQ(r.kind, MessageKind::kOrderUpdate) << "iteration " << i;
  }
  EXPECT_EQ(c.orders.size(), 5000u);
  // Sequence numbers must keep advancing so downstream can detect gaps.
  EXPECT_EQ(c.orders.front().hdr.seq, 1u);
  EXPECT_EQ(c.orders.back().hdr.seq, 5000u);
}

TEST(DeribitParser, LongFieldsAreRejectedRatherThanTruncated) {
  // A truncated instrument name would route an order to the wrong contract.
  DeribitParser p;
  Collector c;
  const std::string long_instrument(transport::kInstrumentCap + 1, 'X');
  const std::string json =
      R"({"method":"subscription","params":{"channel":"user.orders.X.raw",)"
      R"("data":{"order_id":"1","instrument_name":")" +
      long_instrument +
      R"(","direction":"buy","order_type":"limit","amount":1,)"
      R"("filled_amount":0,"price":1,"order_state":"open","creation_timestamp":1}}})";

  EXPECT_EQ(run(p, json, c).kind, MessageKind::kParseError);
  EXPECT_TRUE(c.orders.empty());
}
