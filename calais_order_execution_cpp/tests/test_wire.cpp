#include "calais/transport/wire.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

using namespace calais::transport;
using calais::core::Price;
using calais::core::Qty;
using calais::core::Timestamp;
using calais::models::Liquidity;
using calais::models::OrderSide;
using calais::models::OrderStatus;
using calais::models::OrderType;

namespace {

std::vector<std::string> keys_of(const Json& j) {
  std::vector<std::string> k;
  for (auto it = j.begin(); it != j.end(); ++it) {
    k.push_back(it.key());
  }
  return k;
}

calais::models::Order sample_order() {
  calais::models::Order o;
  o.order_id = "DERIBIT-12345";
  o.exchange = "deribit";
  o.instrument = "BTC-27JUN25-100000-C";
  o.side = OrderSide::kBuy;
  o.order_type = OrderType::kLimit;
  o.amount = *Qty::from_string("2.5");
  o.status = OrderStatus::kPartiallyFilled;
  o.internal_order_id = "0123456789abcdef0123456789abcdef";
  o.price = *Price::from_string("0.0345");
  o.filled_amount = *Qty::from_string("1.25");
  o.average_price = *Price::from_string("0.0344");
  o.client_order_id = "client-1";
  o.label = "chase-maker";
  o.liquidity = Liquidity::kMaker;
  o.post_only = true;
  o.reject_post_only = false;
  o.strategy_id = "alpha";
  o.created_at = *Timestamp::from_iso8601("2026-08-07T12:34:56");
  o.updated_at = *Timestamp::from_iso8601("2026-08-07T12:35:01.123456");
  return o;
}

}  // namespace

// ---------------------------------------------------------------------------
// Key order. This is what makes the emitted object structurally identical to
// Python's, which walks dataclass fields in declaration order.
// ---------------------------------------------------------------------------

TEST(Wire, OrderKeyOrderMatchesPythonDataclass) {
  const std::vector<std::string> expected = {
      "order_id",     "exchange",         "instrument",   "side",
      "order_type",   "amount",           "status",       "internal_order_id",
      "price",        "filled_amount",    "average_price", "client_order_id",
      "label",        "liquidity",        "post_only",    "reject_post_only",
      "strategy_id",  "created_at",       "updated_at"};
  EXPECT_EQ(keys_of(to_json(sample_order())), expected);
}

TEST(Wire, OrderRequestKeyOrderMatchesPythonDataclass) {
  calais::models::OrderRequest r;
  r.instrument = "BTC-PERPETUAL";
  r.amount = Qty::from_integer(1);
  r.price = Price::from_integer(100);

  const std::vector<std::string> expected = {
      "instrument", "side",          "amount",           "order_type",
      "price",      "client_order_id", "label",          "post_only",
      "reject_post_only", "internal_order_id", "strategy_id"};
  EXPECT_EQ(keys_of(to_json(r)), expected);
}

TEST(Wire, TickerKeyOrderMatchesPythonDataclass) {
  calais::models::Ticker t;
  t.instrument = "BTC-PERPETUAL";
  const std::vector<std::string> expected = {
      "instrument",     "best_bid_price", "best_bid_amount", "best_ask_price",
      "best_ask_amount", "last_price",    "mark_price",      "timestamp"};
  EXPECT_EQ(keys_of(to_json(t)), expected);
}

TEST(Wire, FillKeyOrderMatchesPythonDataclass) {
  calais::models::Fill f;
  const std::vector<std::string> expected = {
      "trade_id",  "order_id",  "exchange",    "instrument", "side",
      "amount",    "price",     "fee",         "fee_currency", "liquidity",
      "timestamp", "index_price", "mark_price", "iv",        "profit_loss",
      "label",     "strategy_id"};
  EXPECT_EQ(keys_of(to_json(f)), expected);
}

// ---------------------------------------------------------------------------
// Value encoding
// ---------------------------------------------------------------------------

TEST(Wire, EnumsEmitTheirWireSpelling) {
  const Json j = to_json(sample_order());
  EXPECT_EQ(j["side"], "buy");
  EXPECT_EQ(j["order_type"], "limit");
  EXPECT_EQ(j["status"], "partially_filled");
  EXPECT_EQ(j["liquidity"], "maker");
}

TEST(Wire, DatetimesEmitIsoformat) {
  const Json j = to_json(sample_order());
  EXPECT_EQ(j["created_at"], "2026-08-07T12:34:56");
  EXPECT_EQ(j["updated_at"], "2026-08-07T12:35:01.123456");
}

TEST(Wire, EmptyOptionalsEmitNullRatherThanBeingOmitted) {
  // Python's serialize() writes every dataclass field. Omitting a null here
  // would produce a smaller object than Python does.
  calais::models::Order o = sample_order();
  o.internal_order_id.reset();
  o.price.reset();
  o.average_price.reset();
  o.client_order_id.reset();
  o.label.reset();
  o.strategy_id.reset();

  const Json j = to_json(o);
  EXPECT_TRUE(j["internal_order_id"].is_null());
  EXPECT_TRUE(j["price"].is_null());
  EXPECT_TRUE(j["average_price"].is_null());
  EXPECT_TRUE(j["client_order_id"].is_null());
  EXPECT_TRUE(j["label"].is_null());
  EXPECT_TRUE(j["strategy_id"].is_null());
  EXPECT_EQ(keys_of(j).size(), 19u) << "no key may disappear";
}

TEST(Wire, DecimalsEmitAsJsonNumbers) {
  const Json j = to_json(sample_order());
  EXPECT_TRUE(j["amount"].is_number());
  EXPECT_TRUE(j["price"].is_number());
  EXPECT_DOUBLE_EQ(j["amount"].get<double>(), 2.5);
  EXPECT_DOUBLE_EQ(j["price"].get<double>(), 0.0345);
}

// ---------------------------------------------------------------------------
// Round trips
// ---------------------------------------------------------------------------

TEST(Wire, OrderRoundTrip) {
  const auto o = sample_order();
  const auto back = order_from_json(to_json(o));
  ASSERT_TRUE(back.has_value());

  EXPECT_EQ(back->order_id, o.order_id);
  EXPECT_EQ(back->exchange, o.exchange);
  EXPECT_EQ(back->instrument, o.instrument);
  EXPECT_EQ(back->side, o.side);
  EXPECT_EQ(back->order_type, o.order_type);
  EXPECT_EQ(back->amount, o.amount);
  EXPECT_EQ(back->status, o.status);
  EXPECT_EQ(back->internal_order_id, o.internal_order_id);
  EXPECT_EQ(back->price, o.price);
  EXPECT_EQ(back->filled_amount, o.filled_amount);
  EXPECT_EQ(back->average_price, o.average_price);
  EXPECT_EQ(back->client_order_id, o.client_order_id);
  EXPECT_EQ(back->label, o.label);
  EXPECT_EQ(back->liquidity, o.liquidity);
  EXPECT_EQ(back->post_only, o.post_only);
  EXPECT_EQ(back->reject_post_only, o.reject_post_only);
  EXPECT_EQ(back->strategy_id, o.strategy_id);
  EXPECT_EQ(back->created_at, o.created_at);
  EXPECT_EQ(back->updated_at, o.updated_at);
}

TEST(Wire, TickerRoundTrip) {
  calais::models::Ticker t;
  t.instrument = "BTC-PERPETUAL";
  t.best_bid_price = *Price::from_string("64000.5");
  t.best_bid_amount = *Qty::from_string("12.5");
  t.best_ask_price = *Price::from_string("64001.0");
  t.best_ask_amount = *Qty::from_string("8.25");
  t.last_price = *Price::from_string("64000.75");
  t.mark_price = *Price::from_string("64000.6");
  t.timestamp = *Timestamp::from_iso8601("2026-08-07T12:34:56.500000");

  const auto back = ticker_from_json(to_json(t));
  ASSERT_TRUE(back.has_value());
  EXPECT_EQ(back->instrument, t.instrument);
  EXPECT_EQ(back->best_bid_price, t.best_bid_price);
  EXPECT_EQ(back->best_ask_amount, t.best_ask_amount);
  EXPECT_EQ(back->last_price, t.last_price);
  EXPECT_EQ(back->timestamp, t.timestamp);
  EXPECT_EQ(back->spread(), *Price::from_string("0.5"));
}

TEST(Wire, FillRoundTrip) {
  calais::models::Fill f;
  f.trade_id = "TRADE-9876";
  f.order_id = "DERIBIT-12345";
  f.exchange = "deribit";
  f.instrument = "BTC-27JUN25-100000-C";
  f.side = OrderSide::kSell;
  f.amount = *Qty::from_string("1.5");
  f.price = *Price::from_string("0.0345");
  f.fee = *Price::from_string("0.0000123");
  f.fee_currency = "BTC";
  f.liquidity = Liquidity::kTaker;
  f.timestamp = *Timestamp::from_iso8601("2026-08-07T12:34:56.123456");
  f.index_price = *Price::from_string("64000");
  f.iv = *Price::from_string("68.5");
  f.strategy_id = "alpha";

  const auto back = fill_from_json(to_json(f));
  ASSERT_TRUE(back.has_value());
  EXPECT_EQ(back->trade_id, f.trade_id);
  EXPECT_EQ(back->side, f.side);
  EXPECT_EQ(back->amount, f.amount);
  EXPECT_EQ(back->fee, f.fee);
  EXPECT_EQ(back->liquidity, f.liquidity);
  EXPECT_EQ(back->timestamp, f.timestamp);
  EXPECT_EQ(back->index_price, f.index_price);
  EXPECT_EQ(back->iv, f.iv);
  EXPECT_FALSE(back->mark_price.has_value());
  EXPECT_EQ(back->signed_amount(), -f.amount);
}

TEST(Wire, AccountSummaryAndPositionRoundTrip) {
  calais::models::AccountSummary a;
  a.currency = "BTC";
  a.exchange = "deribit";
  a.equity = *Price::from_string("12.3456789");
  a.balance = *Price::from_string("12.0");
  a.options_vega = *Price::from_string("-0.005");
  a.timestamp = *Timestamp::from_iso8601("2026-08-07T12:34:56");

  const auto ab = account_summary_from_json(to_json(a));
  ASSERT_TRUE(ab.has_value());
  EXPECT_EQ(ab->currency, a.currency);
  EXPECT_EQ(ab->equity, a.equity);
  EXPECT_EQ(ab->options_vega, a.options_vega);
  EXPECT_EQ(ab->timestamp, a.timestamp);

  calais::models::Position p;
  p.instrument = "BTC-27JUN25-100000-C";
  p.exchange = "deribit";
  p.kind = "option";
  p.direction = "buy";
  p.size = *Qty::from_string("2.5");
  p.delta = *Price::from_string("0.4523");
  p.timestamp = *Timestamp::from_iso8601("2026-08-07T12:34:56");

  const auto pb = position_from_json(to_json(p));
  ASSERT_TRUE(pb.has_value());
  EXPECT_EQ(pb->kind, p.kind);
  EXPECT_EQ(pb->size, p.size);
  EXPECT_EQ(pb->delta, p.delta);
  EXPECT_FALSE(pb->is_flat());
}

// ---------------------------------------------------------------------------
// Parse robustness. A malformed frame arrives on a network socket as a matter
// of course; it must never throw out of the receive loop.
// ---------------------------------------------------------------------------

TEST(Wire, RejectsMissingRequiredFields) {
  Json j = to_json(sample_order());
  j.erase("order_id");
  EXPECT_FALSE(order_from_json(j).has_value());

  j = to_json(sample_order());
  j.erase("status");
  EXPECT_FALSE(order_from_json(j).has_value());

  j = to_json(sample_order());
  j["status"] = nullptr;
  EXPECT_FALSE(order_from_json(j).has_value());
}

TEST(Wire, RejectsWrongTypes) {
  Json j = to_json(sample_order());
  j["order_id"] = 12345;
  EXPECT_FALSE(order_from_json(j).has_value());

  j = to_json(sample_order());
  j["amount"] = "not-a-number";
  EXPECT_FALSE(order_from_json(j).has_value());

  j = to_json(sample_order());
  j["post_only"] = "yes";
  EXPECT_FALSE(order_from_json(j).has_value());

  j = to_json(sample_order());
  j["created_at"] = "not-a-date";
  EXPECT_FALSE(order_from_json(j).has_value());
}

TEST(Wire, RejectsUnknownEnumValues) {
  Json j = to_json(sample_order());
  j["status"] = "untriggered";
  EXPECT_FALSE(order_from_json(j).has_value());

  j = to_json(sample_order());
  j["side"] = "BUY";
  EXPECT_FALSE(order_from_json(j).has_value());
}

TEST(Wire, IgnoresUnknownExtraKeys) {
  // A newer Python peer adding a field must not break an older C++ engine.
  Json j = to_json(sample_order());
  j["some_future_field"] = 42;
  j["another_one"] = Json::object();
  EXPECT_TRUE(order_from_json(j).has_value());
}

TEST(Wire, RejectsNonObjects) {
  EXPECT_FALSE(order_from_json(Json::array()).has_value());
  EXPECT_FALSE(order_from_json(Json(42)).has_value());
  EXPECT_FALSE(order_from_json(Json()).has_value());
}

TEST(Wire, AcceptsDecimalsAsStringsForExactness) {
  // Exchange REST payloads commonly send numbers as strings. That path is the
  // exact one -- it never goes through a double.
  Json j = to_json(sample_order());
  j["price"] = "0.000000001";
  const auto back = order_from_json(j);
  ASSERT_TRUE(back.has_value());
  EXPECT_EQ(back->price->raw(), 1);
}

TEST(Wire, AcceptsNumericEpochTimestamps) {
  Json j = to_json(sample_order());
  j["created_at"] = 1786106096123LL;
  const auto back = order_from_json(j);
  ASSERT_TRUE(back.has_value());
  EXPECT_EQ(back->created_at.ns(), 1786106096123LL * 1'000'000LL);
}

// ---------------------------------------------------------------------------
// Protocol messages
// ---------------------------------------------------------------------------

TEST(Wire, CommandRoundTripThroughBytes) {
  Command c;
  c.command_type = "place_order";
  c.payload = Json::object();
  c.payload["order_request"] = to_json(calais::models::OrderRequest{
      .instrument = "BTC-PERPETUAL",
      .amount = Qty::from_integer(1),
      .price = Price::from_integer(64000)});
  c.request_id = "req-1";
  c.strategy_id = "alpha";

  const std::string bytes = serialize_command(c);
  const auto back = deserialize_command(bytes);
  ASSERT_TRUE(back.has_value());
  EXPECT_EQ(back->command_type, c.command_type);
  EXPECT_EQ(back->request_id, c.request_id);
  EXPECT_EQ(back->strategy_id, c.strategy_id);
  EXPECT_EQ(back->payload, c.payload);
  EXPECT_EQ(back->typed(), calais::models::CommandType::kPlaceOrder);

  EXPECT_EQ(keys_of(to_json(c)),
            (std::vector<std::string>{"command_type", "payload", "request_id",
                                      "strategy_id"}));
}

TEST(Wire, CommandKeepsUnknownTypeAsAString) {
  // A strategy on newer code may send a command this build has never heard of.
  // That must parse cleanly so the engine can answer "unknown command".
  Command c;
  c.command_type = "some_future_command";
  c.request_id = "req-2";

  const auto back = deserialize_command(serialize_command(c));
  ASSERT_TRUE(back.has_value());
  EXPECT_EQ(back->command_type, "some_future_command");
  EXPECT_FALSE(back->typed().has_value());
}

TEST(Wire, ResponseRoundTrip) {
  const Response ok = Response::ok("req-1", to_json(sample_order()));
  const auto back = deserialize_response(serialize_response(ok));
  ASSERT_TRUE(back.has_value());
  EXPECT_TRUE(back->success);
  EXPECT_EQ(back->request_id, "req-1");
  EXPECT_FALSE(back->error.has_value());
  EXPECT_TRUE(order_from_json(back->data).has_value());

  const Response bad = Response::fail("req-2", "instrument not found");
  const auto back2 = deserialize_response(serialize_response(bad));
  ASSERT_TRUE(back2.has_value());
  EXPECT_FALSE(back2->success);
  ASSERT_TRUE(back2->error.has_value());
  EXPECT_EQ(*back2->error, "instrument not found");
  EXPECT_TRUE(back2->data.is_null());

  EXPECT_EQ(keys_of(to_json(ok)), (std::vector<std::string>{"request_id", "success",
                                                            "data", "error"}));
}

TEST(Wire, ResponseDataCanBeAnyJsonShape) {
  for (const Json& data : {Json(true), Json(42), Json("text"),
                           Json::array({1, 2, 3}), Json()}) {
    const auto back = deserialize_response(
        serialize_response(Response::ok("r", data)));
    ASSERT_TRUE(back.has_value());
    EXPECT_EQ(back->data, data);
  }
}

TEST(Wire, EventRoundTrip) {
  Event e;
  e.event_type = "order_update";
  e.data = to_json(sample_order());
  e.strategy_id = "alpha";

  const auto back = deserialize_event(serialize_event(e));
  ASSERT_TRUE(back.has_value());
  EXPECT_EQ(back->event_type, "order_update");
  EXPECT_EQ(back->strategy_id, "alpha");
  EXPECT_EQ(back->typed(), calais::models::EventType::kOrderUpdate);
  EXPECT_TRUE(order_from_json(back->data).has_value());

  EXPECT_EQ(keys_of(to_json(e)),
            (std::vector<std::string>{"event_type", "data", "strategy_id"}));
}

TEST(Wire, MalformedBytesReturnNulloptRatherThanThrow) {
  for (const char* bad : {"", "{", "not json", "[1,2,3", "{\"a\":}",
                          "\xff\xfe garbage"}) {
    EXPECT_FALSE(deserialize_command(bad).has_value()) << bad;
    EXPECT_FALSE(deserialize_response(bad).has_value()) << bad;
    EXPECT_FALSE(deserialize_event(bad).has_value()) << bad;
  }
}

TEST(Wire, WellFormedJsonWithWrongSchemaReturnsNullopt) {
  EXPECT_FALSE(deserialize_command("{}").has_value());
  EXPECT_FALSE(deserialize_command("[]").has_value());
  EXPECT_FALSE(deserialize_command("null").has_value());
  EXPECT_FALSE(deserialize_response("{\"request_id\":\"r\"}").has_value());
  EXPECT_FALSE(deserialize_event("{\"data\":{}}").has_value());
}

TEST(Wire, SerializedOutputIsCompact) {
  // nlohmann dumps without spaces. The Python peer's json.dumps defaults to
  // ", " / ": ", so the two differ in whitespace only -- documented in wire.h
  // and handled by the cross-implementation test.
  const std::string bytes = serialize_event(Event{.event_type = "order_update",
                                                  .data = Json::object(),
                                                  .strategy_id = "alpha"});
  EXPECT_EQ(bytes.find(", "), std::string::npos);
  EXPECT_EQ(bytes.find(": "), std::string::npos);
}
