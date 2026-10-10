// OmsService: how every feed event becomes order state and strategy traffic.
//
// Until this class existed this logic lived in lambdas inside the engine and
// had no tests at all. Each test pins one property a strategy depends on: the
// hot path sees an update before anything else does, a fill reaches the
// strategy that traded and reaches it once, a missed fill is recovered, and a
// placement whose reply is lost still finds its way home.
//
// No network: the feed is driven by calling the sink directly, and the
// reconcilers read a scripted VenueRest.

#include <gtest/gtest.h>

#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "axon/config.h"
#include "axon/models/order.h"
#include "axon/oms/oms_service.h"
#include "axon/risk/risk_feed.h"
#include "axon/risk/risk_manager.h"
#include "axon/oms/venue_connections.h"
#include "axon/transport/hot_messages.h"

namespace {

using axon::Config;
using axon::core::Price;
using axon::core::Qty;
using axon::models::Fill;
using axon::models::Order;
using axon::models::OrderRequest;
using axon::models::OrderSide;
using axon::models::OrderStatus;
using axon::models::OrderType;
using axon::oms::OmsService;
using Outcome = axon::oms::OmsService::PlaceOutcome;

// ---------------------------------------------------------------------------
// Records everything the OMS publishes, in order, and lets a test look at the
// OMS from inside a callback.
// ---------------------------------------------------------------------------
struct Recorder : axon::oms::OmsListener {
  std::vector<std::string> calls;  // "message", "update", "changed", "fill", "positions"
  std::vector<axon::transport::OrderUpdateMsg> messages;
  std::vector<Order> changed;
  std::vector<Fill> fills;
  std::vector<bool> fill_had_msg;
  std::vector<bool> fill_recovered;
  std::vector<std::string> fill_msg_strategy;
  std::function<void()> on_message_hook;

  void on_order_message(const axon::transport::OrderUpdateMsg& m) override {
    calls.push_back("message");
    messages.push_back(m);
    if (on_message_hook) on_message_hook();
  }
  void on_order_update(const Order&) override { calls.push_back("update"); }
  void on_order_changed(const Order& o) override {
    calls.push_back("changed");
    changed.push_back(o);
  }
  void on_fill(const axon::transport::FillMsg* msg, const Fill& f, bool recovered) override {
    calls.push_back("fill");
    fills.push_back(f);
    fill_had_msg.push_back(msg != nullptr);
    fill_recovered.push_back(recovered);
    fill_msg_strategy.emplace_back(msg ? std::string(msg->strategy_id.view()) : "");
  }
  void on_positions(const std::string&, const std::vector<axon::models::Position>&) override {
    calls.push_back("positions");
  }
};

// A VenueRest whose answers the test scripts.
class ScriptedRest : public axon::oms::VenueRest {
 public:
  std::vector<Order> open_orders;
  std::vector<Fill> trades;
  std::vector<axon::models::Position> positions;
  int open_order_calls = 0;

  void get_open_orders(const std::string&, OrdersCallback cb) override {
    ++open_order_calls;
    cb(open_orders, {});
  }
  void get_order(const Order& o, OrderCallback cb) override {
    Order closed = o;
    closed.status = OrderStatus::kCancelled;
    closed.strategy_id.reset();
    closed.updated_at = axon::core::Timestamp::from_ns(o.updated_at.ns() + 1'000'000);
    cb(closed, {});
  }
  void get_positions(const std::string&, PositionsCallback cb) override { cb(positions, {}); }
  void get_user_trades(const std::string&, std::int64_t, std::int64_t, FillsCallback cb) override {
    cb(trades, {});
  }
  void get_ticker(const std::string&, TickerCallback cb) override { cb(std::nullopt, "n/a"); }
  const std::string& exchange_name() const override { return name_; }

 private:
  std::string name_ = "binance";
};

Config config() {
  Config c;
  axon::ExchangeConfig e;
  e.name = "binance";
  c.exchanges["binance"] = e;
  c.portfolio.currencies = {"BTCUSDT"};
  return c;
}

OrderRequest request(const char* strategy = "alpha") {
  OrderRequest r;
  r.instrument = "BTCUSDT";
  r.side = OrderSide::kBuy;
  r.order_type = OrderType::kLimit;
  r.amount = *Qty::from_string("0.01");
  r.price = Price::from_string("60000");
  r.strategy_id = strategy;
  return r;
}

// An order update as the Binance parser emits it: the client order id we sent
// in internal_order_id, no strategy.
axon::transport::OrderUpdateMsg venue_update(const std::string& client_id,
                                             OrderStatus status = OrderStatus::kOpen,
                                             const char* filled = "0", std::int64_t ms = 1000) {
  Order o;
  o.order_id = "V-1";
  o.exchange = "binance";
  o.instrument = "BTCUSDT";
  o.amount = *Qty::from_string("0.01");
  o.filled_amount = *Qty::from_string(filled);
  o.price = Price::from_string("60000");
  o.status = status;
  o.internal_order_id = client_id;
  o.updated_at = axon::core::Timestamp::from_millis(ms);
  axon::transport::OrderUpdateMsg m{};
  EXPECT_TRUE(axon::transport::encode_order_update(o, 1, m));
  return m;
}

Fill trade(const char* trade_id, const char* order_id = "V-1") {
  Fill f;
  f.trade_id = trade_id;
  f.order_id = order_id;
  f.exchange = "binance";
  f.instrument = "BTCUSDT";
  f.side = OrderSide::kBuy;
  f.amount = *Qty::from_string("0.01");
  f.price = *Price::from_string("60000");
  f.fee_currency = "USDT";
  f.timestamp = axon::core::Timestamp::from_millis(1500);
  return f;
}

axon::transport::FillMsg venue_fill(const char* trade_id) {
  axon::transport::FillMsg m{};
  EXPECT_TRUE(axon::transport::encode_fill(trade(trade_id), 2, m));
  return m;
}

class OmsServiceTest : public ::testing::Test {
 protected:
  OmsServiceTest() : cfg(config()), oms(cfg) { oms.add_listener(&rec); }

  void start_with_rest() { oms.start({{"binance", &rest}}); }

  Config cfg;
  OmsService oms;
  Recorder rec;
  ScriptedRest rest;
};

// ===========================================================================
// Order updates
// ===========================================================================

// The shared-memory hot path publishes from on_order_message. Nothing on the
// control-plane side -- decoding, the store -- may happen before it.
TEST_F(OmsServiceTest, TheRawUpdateIsPublishedBeforeTheStoreChanges) {
  std::optional<Order> stored_at_publish = Order{};
  rec.on_message_hook = [&] { stored_at_publish = oms.get_order("V-1"); };

  oms.on_order_update("binance", venue_update("abc"));

  EXPECT_FALSE(stored_at_publish.has_value()) << "the store was updated before the hot path";
  EXPECT_EQ(rec.calls, (std::vector<std::string>{"message", "update", "changed"}));
  EXPECT_TRUE(oms.get_order("V-1").has_value());
}

// The venue echoes the client id we sent; the OMS maps it back to the request
// so the update reaches the strategy that placed it -- on both paths.
TEST_F(OmsServiceTest, AnUpdateIsRoutedToTheStrategyThatPlacedIt) {
  const auto req = request("alpha");
  oms.before_place("binance", req);
  oms.on_order_update("binance", venue_update(req.internal_order_id));

  ASSERT_EQ(rec.messages.size(), 1u);
  EXPECT_EQ(rec.messages[0].strategy_id.view(), "alpha");
  EXPECT_EQ(rec.messages[0].internal_order_id.view(), req.internal_order_id);
  ASSERT_EQ(rec.changed.size(), 1u);
  EXPECT_EQ(rec.changed[0].strategy_id.value_or(""), "alpha");
}

// The engine's whole point for a lost reply: the order went out, no verdict
// came back, and its updates must still route.
TEST_F(OmsServiceTest, AnUnknownOutcomeKeepsTheRouting) {
  const auto req = request("alpha");
  oms.before_place("binance", req);
  oms.after_place("binance", req, Outcome::kUnknown, std::nullopt);
  oms.on_order_update("binance", venue_update(req.internal_order_id));
  EXPECT_EQ(rec.messages.at(0).strategy_id.view(), "alpha");
}

TEST_F(OmsServiceTest, ARefusedPlacementForgetsTheRouting) {
  const auto req = request("alpha");
  oms.before_place("binance", req);
  oms.after_place("binance", req, Outcome::kRefused, std::nullopt);
  oms.on_order_update("binance", venue_update(req.internal_order_id));
  EXPECT_TRUE(rec.messages.at(0).strategy_id.empty());
}

TEST_F(OmsServiceTest, AnAcceptedPlacementRecordsTheOrder) {
  const auto req = request("alpha");
  Order accepted;
  accepted.order_id = "V-9";
  accepted.exchange = "binance";
  accepted.instrument = "BTCUSDT";
  accepted.strategy_id = "alpha";
  oms.before_place("binance", req);
  oms.after_place("binance", req, Outcome::kAccepted, accepted);
  ASSERT_TRUE(oms.get_order("V-9").has_value());
  EXPECT_EQ(oms.get_order("V-9")->status, OrderStatus::kPending);
}

// An update that changes nothing the store tracks is still an update (the
// risk layer needs it) but not a change (strategies do not).
TEST_F(OmsServiceTest, ARepeatedUpdateIsNotAChange) {
  oms.on_order_update("binance", venue_update("abc", OrderStatus::kOpen, "0", 1000));
  oms.on_order_update("binance", venue_update("abc", OrderStatus::kOpen, "0", 1001));
  EXPECT_EQ(rec.changed.size(), 1u);
  EXPECT_EQ(rec.messages.size(), 2u);
}

// ===========================================================================
// Fills
// ===========================================================================

TEST_F(OmsServiceTest, AFillReachesTheStrategyOfItsOrder) {
  const auto req = request("alpha");
  oms.before_place("binance", req);
  oms.on_order_update("binance", venue_update(req.internal_order_id));
  oms.on_fill("binance", venue_fill("T1"));

  ASSERT_EQ(rec.fills.size(), 1u);
  EXPECT_EQ(rec.fills[0].strategy_id.value_or(""), "alpha");
  EXPECT_EQ(rec.fill_msg_strategy[0], "alpha") << "the hot-path copy went to nobody";
  EXPECT_TRUE(rec.fill_had_msg[0]);
  EXPECT_FALSE(rec.fill_recovered[0]);
  EXPECT_EQ(oms.fills_by_strategy("alpha").size(), 1u);
}

TEST_F(OmsServiceTest, AFillIsDeliveredOnce) {
  oms.on_fill("binance", venue_fill("T1"));
  oms.on_fill("binance", venue_fill("T1"));
  EXPECT_EQ(rec.fills.size(), 1u);
  EXPECT_EQ(oms.fills_duplicate(), 1u);
}

// A fill the feed never delivered, found by the reconciler, attributed and
// published like any other.
TEST_F(OmsServiceTest, AMissedFillIsRecoveredAndAttributed) {
  start_with_rest();
  const auto req = request("alpha");
  oms.before_place("binance", req);
  oms.on_order_update("binance", venue_update(req.internal_order_id));

  rest.trades = {trade("T7")};
  oms.on_session_live("binance");
  oms.poll();

  ASSERT_EQ(rec.fills.size(), 1u);
  EXPECT_TRUE(rec.fill_recovered[0]);
  EXPECT_TRUE(rec.fill_had_msg[0]);
  EXPECT_EQ(rec.fills[0].strategy_id.value_or(""), "alpha") << "recovered fill went to everyone";
  EXPECT_EQ(rec.fill_msg_strategy[0], "alpha");
}

// The feed and the reconciler both report fills; a trade must still reach a
// strategy once.
TEST_F(OmsServiceTest, AFeedFillIsNotRecoveredAgain) {
  start_with_rest();
  oms.on_fill("binance", venue_fill("T1"));
  rest.trades = {trade("T1")};
  oms.on_session_live("binance");
  oms.poll();
  EXPECT_EQ(rec.fills.size(), 1u);
  ASSERT_NE(oms.reconciler("binance"), nullptr);
  EXPECT_EQ(oms.reconciler("binance")->stats().fills_recovered, 0u);
}

// ===========================================================================
// Reconciliation, positions, accounts
// ===========================================================================

TEST_F(OmsServiceTest, SessionLiveReconcilesImmediately) {
  start_with_rest();
  oms.poll();
  EXPECT_EQ(rest.open_order_calls, 0) << "reconciled before it was due";
  oms.on_session_live("binance");
  oms.poll();
  EXPECT_EQ(rest.open_order_calls, 1);
}

// A correction reconciliation makes is a change like any other: it is
// published, so strategies and the risk layer both hear of it.
TEST_F(OmsServiceTest, AReconciliationCorrectionIsPublished) {
  start_with_rest();
  oms.on_order_update("binance", venue_update("abc"));
  rec.changed.clear();

  rest.open_orders = {};  // the venue no longer lists V-1: it closed unseen
  oms.on_session_live("binance");
  oms.poll();

  ASSERT_EQ(rec.changed.size(), 1u);
  EXPECT_EQ(rec.changed[0].status, OrderStatus::kCancelled);
  EXPECT_TRUE(oms.active_orders().empty());
}

TEST_F(OmsServiceTest, PositionsAreStoredAndPublished) {
  start_with_rest();
  axon::models::Position p;
  p.exchange = "binance";
  p.instrument = "BTCUSDT";
  p.size = *Qty::from_string("0.5");
  p.direction = "buy";
  rest.positions = {p};
  oms.on_session_live("binance");
  oms.poll();

  ASSERT_EQ(oms.positions("binance").size(), 1u);
  EXPECT_NE(std::find(rec.calls.begin(), rec.calls.end(), "positions"), rec.calls.end());
}

TEST_F(OmsServiceTest, AccountUpdatesAreStored) {
  axon::models::AccountSummary a;
  a.exchange = "binance";
  a.currency = "USDT";
  a.equity = *Price::from_string("1000");
  oms.on_account("binance", a);
  ASSERT_TRUE(oms.account("binance", "USDT").has_value());
  EXPECT_EQ(oms.account("binance", "USDT")->equity.raw(), a.equity.raw());
}

TEST_F(OmsServiceTest, AVenueWithoutRestHasNoReconciler) {
  oms.start({});
  EXPECT_EQ(oms.reconciler("binance"), nullptr);
  oms.on_session_live("binance");  // nothing to trigger; must not crash
  oms.poll();
}

// ===========================================================================
// The risk layer, fed through the same listener interface
// ===========================================================================

// An order the venue closed while the feed was down leaves the risk layer's
// working-order book when reconciliation finds it -- not only on a feed update.
TEST_F(OmsServiceTest, RiskHearsOfReconciliationCorrections) {
  axon::risk::RiskManager risk;
  axon::risk::RiskFeed feed(risk, [] { return 1000.0; });
  oms.add_listener(&feed);
  start_with_rest();

  oms.on_order_update("binance", venue_update("abc"));
  EXPECT_EQ(risk.working_order_count(), 1u);

  oms.on_session_live("binance");
  oms.poll();
  EXPECT_EQ(risk.working_order_count(), 0u) << "a closed order still counts against limits";
}

TEST_F(OmsServiceTest, RiskTakesFillsAndPositions) {
  axon::risk::RiskManager risk;
  axon::risk::RiskFeed feed(risk, [] { return 1000.0; });
  oms.add_listener(&feed);
  oms.on_fill("binance", venue_fill("T1"));
  EXPECT_EQ(risk.position("binance", "BTCUSDT").raw(), Qty::from_string("0.01")->raw());

  feed.on_positions("binance", {});
  EXPECT_EQ(risk.position("binance", "BTCUSDT").raw(), 0);
}

// A recovered fill can be hours old: it moves the position but is no
// reference price for now.
TEST(RiskFeedTest, RecoveredFillsDoNotSetTheReferencePrice) {
  axon::RiskConfig c;
  c.defaults.max_price_deviation = 0.05;
  axon::risk::RiskManager risk(c);
  axon::risk::RiskFeed feed(risk, [] { return 1000.0; });

  feed.on_fill(nullptr, trade("T1"), /*recovered=*/true);
  auto r = request();
  EXPECT_EQ(risk.check_new_order("binance", r, 1000.0)->code, "no_reference_price");

  feed.on_fill(nullptr, trade("T2"), /*recovered=*/false);
  EXPECT_FALSE(risk.check_new_order("binance", r, 1000.0).has_value());
}

// ===========================================================================
// VenueConnections, without touching the network
// ===========================================================================

struct NullSink : axon::oms::VenueEventSink {
  void on_order_update(const std::string&, const axon::transport::OrderUpdateMsg&) override {}
  void on_fill(const std::string&, const axon::transport::FillMsg&) override {}
  void on_account(const std::string&, const axon::models::AccountSummary&) override {}
  void on_session_live(const std::string&) override {}
};

// Starting only begins each connection; nothing reaches the network until
// poll(), which this test never calls.
TEST(VenueConnectionsTest, BuildsEachVenuesConnectionsAndSkipsUnknownOnes) {
  Config c;
  for (const char* name : {"binance", "bybit", "okx", "deribit", "nosuchvenue"}) {
    axon::ExchangeConfig e;
    e.name = name;
    c.exchanges[name] = e;
  }
  axon::oms::VenueConnections conns(c);
  NullSink sink;
  conns.start(sink, [](const std::string&, std::int64_t, bool, std::string_view,
                       std::string_view) {});

  EXPECT_EQ(conns.venues(), (std::vector<std::string>{"binance", "bybit", "deribit", "okx"}));
  for (const auto& v : conns.venues()) {
    EXPECT_NE(conns.session(v), nullptr) << v;
    EXPECT_NE(conns.rest(v), nullptr) << v;
  }
  // Only the venues that need a second connection for order entry get one.
  EXPECT_NE(conns.trade_session("binance"), nullptr);
  EXPECT_NE(conns.trade_session("bybit"), nullptr);
  EXPECT_EQ(conns.trade_session("okx"), nullptr);
  EXPECT_EQ(conns.trade_session("deribit"), nullptr);
  EXPECT_EQ(conns.session("nosuchvenue"), nullptr);
  EXPECT_EQ(conns.rest("nosuchvenue"), nullptr);
  EXPECT_EQ(conns.rests().size(), 4u);

  conns.stop();
  EXPECT_TRUE(conns.venues().empty());
  conns.stop();  // twice is fine
}

// A configured trust anchor that cannot be read must stop startup, not show up
// later as every connection failing verification.
TEST(VenueConnectionsTest, AnUnreadableExtraCaFileIsAStartupError) {
  Config c;
  c.runtime.extra_ca_file = "/nonexistent/corporate-root.pem";
  EXPECT_THROW(axon::oms::VenueConnections conns(c), std::runtime_error);
}

}  // namespace
