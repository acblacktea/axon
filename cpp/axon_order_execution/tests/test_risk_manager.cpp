// Pre-trade risk. Each test is one way a strategy bug would otherwise reach
// the venue: too large, too fast, mispriced, too much position, or a runaway
// that someone needs to stop right now.

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <optional>
#include <string>
#include <vector>

#include "axon/config.h"
#include "axon/ems/ems_service.h"
#include "axon/models/order.h"
#include "axon/oms/risk_manager.h"
#include "axon/oms/venue_session.h"

namespace {

using axon::RiskConfig;
using axon::RiskLimits;
using axon::core::Price;
using axon::core::Qty;
using axon::models::Order;
using axon::models::OrderRequest;
using axon::models::OrderSide;
using axon::models::OrderStatus;
using axon::models::OrderType;
using axon::oms::RiskManager;

Qty Q(const char* s) { return *Qty::from_string(s); }
Price P(const char* s) { return *Price::from_string(s); }

OrderRequest order(OrderSide side, const char* qty, std::optional<const char*> price = "60000",
                   const char* instrument = "BTCUSDT", const char* strategy = "alpha") {
  OrderRequest r;
  r.instrument = instrument;
  r.side = side;
  r.amount = Q(qty);
  if (price) {
    r.order_type = OrderType::kLimit;
    r.price = P(*price);
  } else {
    r.order_type = OrderType::kMarket;
  }
  r.strategy_id = strategy;
  return r;
}

RiskConfig with_defaults(RiskLimits defaults) {
  RiskConfig c;
  c.defaults = defaults;
  return c;
}

std::string code(const std::optional<axon::oms::RiskRejection>& r) {
  return r ? r->code : "allowed";
}

constexpr double kNow = 1000.0;

// ===========================================================================
// Order size
// ===========================================================================

TEST(RiskOrderSize, QuantityAboveTheCapIsRefused) {
  RiskLimits l;
  l.max_order_qty = Q("0.5");
  RiskManager risk(with_defaults(l));
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.5"), kNow)), "allowed");
  const auto r = risk.check_new_order("binance", order(OrderSide::kBuy, "0.51"), kNow);
  EXPECT_EQ(code(r), "order_qty");
  EXPECT_NE(r->message.find("0.51"), std::string::npos) << r->message;
}

TEST(RiskOrderSize, LimitNotionalUsesTheOrdersOwnPrice) {
  RiskLimits l;
  l.max_order_notional = P("10000");
  RiskManager risk(with_defaults(l));
  // 0.1 x 60000 = 6000; 0.2 x 60000 = 12000.
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.1"), kNow)), "allowed");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kSell, "0.2"), kNow)),
            "order_notional");
}

TEST(RiskOrderSize, MarketNotionalUsesTheReferencePrice) {
  RiskLimits l;
  l.max_order_notional = P("10000");
  RiskManager risk(with_defaults(l));
  risk.update_reference_price("binance", "BTCUSDT", P("60000"), kNow);
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.1", std::nullopt), kNow)),
            "allowed");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.2", std::nullopt), kNow)),
            "order_notional");
}

// Fail closed: a market order cannot be valued without a price.
TEST(RiskOrderSize, MarketOrderWithoutAReferenceIsRefused) {
  RiskLimits l;
  l.max_order_notional = P("10000");
  RiskManager risk(with_defaults(l));
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.01", std::nullopt), kNow)),
            "no_reference_price");
}

TEST(RiskOrderSize, AStaleReferenceCountsAsNone) {
  RiskLimits l;
  l.max_order_notional = P("10000");
  RiskConfig c = with_defaults(l);
  c.reference_price_max_age_seconds = 30;
  RiskManager risk(c);
  risk.update_reference_price("binance", "BTCUSDT", P("60000"), kNow - 31);
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.01", std::nullopt), kNow)),
            "no_reference_price");
}

TEST(RiskOrderSize, NotionalOverflowIsRefusedNotWrapped) {
  RiskLimits l;
  l.max_order_notional = P("10000");
  RiskManager risk(with_defaults(l));
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "9000000", "9000000"), kNow)),
            "order_notional");
}

// ===========================================================================
// Price deviation
// ===========================================================================

TEST(RiskPrice, LimitPriceFarFromTheReferenceIsRefused) {
  RiskLimits l;
  l.max_price_deviation = 0.05;
  RiskManager risk(with_defaults(l));
  risk.update_reference_price("binance", "BTCUSDT", P("60000"), kNow);
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "1", "62999"), kNow)), "allowed");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "1", "63001"), kNow)),
            "price_deviation");
  // A price mis-scaled by 10x is the case this exists for.
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kSell, "1", "6000"), kNow)),
            "price_deviation");
}

TEST(RiskPrice, NoReferenceMeansNoLimitOrdersEither) {
  RiskLimits l;
  l.max_price_deviation = 0.05;
  RiskManager risk(with_defaults(l));
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "1"), kNow)),
            "no_reference_price");
}

// Deviation has nothing to compare for a market order.
TEST(RiskPrice, MarketOrdersAreNotPriceChecked) {
  RiskLimits l;
  l.max_price_deviation = 0.05;
  RiskManager risk(with_defaults(l));
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "1", std::nullopt), kNow)),
            "allowed");
}

// ===========================================================================
// Position
// ===========================================================================

class RiskPosition : public ::testing::Test {
 protected:
  RiskPosition() {
    RiskLimits l;
    l.max_position = Q("1");
    risk = RiskManager(with_defaults(l));
  }
  axon::models::Position held(const char* size, const char* direction) {
    axon::models::Position p;
    p.instrument = "BTCUSDT";
    p.size = Q(size);
    p.direction = direction;
    return p;
  }
  RiskManager risk;
};

TEST_F(RiskPosition, VenuePositionCountsAgainstTheLimit) {
  risk.on_positions("binance", {held("0.8", "buy")});
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.2"), kNow)), "allowed");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.3"), kNow)), "position");
}

// Reducing a position is always allowed by this check, however large it is.
TEST_F(RiskPosition, OrdersThatReduceThePositionPass) {
  risk.on_positions("binance", {held("0.9", "buy")});
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kSell, "1.9"), kNow)), "allowed");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kSell, "2.0"), kNow)), "position");
}

// A position already over the limit -- the limit was lowered, or the position
// predates it -- must still be reducible, or the engine would trap it there.
TEST_F(RiskPosition, AnOverLimitPositionCanStillBeReduced) {
  risk.on_positions("binance", {held("3", "sell")});
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "1"), kNow)), "allowed");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kSell, "0.01"), kNow)), "position");
}

TEST_F(RiskPosition, ShortPositionsAreLimitedToo) {
  risk.on_positions("binance", {held("0.9", "sell")});
  EXPECT_EQ(risk.position("binance", "BTCUSDT").raw(), -Q("0.9").raw());
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kSell, "0.2"), kNow)), "position");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "1.5"), kNow)), "allowed");
}

// The burst a runaway strategy sends goes out before the first ack. Each order
// counts from the moment it is sent.
TEST_F(RiskPosition, UnacknowledgedOrdersCountImmediately) {
  for (int i = 0; i < 4; ++i) {
    const auto r = order(OrderSide::kBuy, "0.25");
    ASSERT_EQ(code(risk.check_new_order("binance", r, kNow)), "allowed") << i;
    risk.on_order_sent("binance", r, kNow);
  }
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.01"), kNow)), "position");
}

TEST_F(RiskPosition, AVenueRejectionReleasesTheExposure) {
  const auto r = order(OrderSide::kBuy, "1");
  risk.on_order_sent("binance", r, kNow);
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.1"), kNow)), "position");
  risk.on_order_rejected("binance", r);
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.1"), kNow)), "allowed");
}

// The first update replaces the sent entry (same internal_order_id) rather
// than adding a second one; terminal updates remove it.
TEST_F(RiskPosition, UpdatesTrackTheWorkingOrder) {
  const auto r = order(OrderSide::kBuy, "0.6");
  risk.on_order_sent("binance", r, kNow);
  EXPECT_EQ(risk.working_order_count(), 1u);

  Order o;
  o.order_id = "V-1";
  o.exchange = "binance";
  o.instrument = "BTCUSDT";
  o.side = OrderSide::kBuy;
  o.amount = Q("0.6");
  o.filled_amount = Q("0.2");
  o.status = OrderStatus::kPartiallyFilled;
  o.internal_order_id = r.internal_order_id;
  risk.on_order_update(o);
  EXPECT_EQ(risk.working_order_count(), 1u);
  EXPECT_EQ(risk.exposure("binance", "BTCUSDT").long_side.raw(), Q("0.4").raw());

  o.status = OrderStatus::kCancelled;
  risk.on_order_update(o);
  EXPECT_EQ(risk.working_order_count(), 0u);
}

// Fills move the position between position refreshes; the next snapshot
// replaces the whole picture for that exchange.
TEST_F(RiskPosition, FillsMoveThePositionUntilTheNextSnapshot) {
  axon::models::Fill f;
  f.exchange = "binance";
  f.instrument = "BTCUSDT";
  f.side = OrderSide::kBuy;
  f.amount = Q("0.7");
  f.price = P("60000");
  risk.on_fill(f);
  EXPECT_EQ(risk.position("binance", "BTCUSDT").raw(), Q("0.7").raw());
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.4"), kNow)), "position");

  risk.on_positions("binance", {});  // venue says flat
  EXPECT_EQ(risk.position("binance", "BTCUSDT").raw(), 0);
}

TEST_F(RiskPosition, PositionsAreScopedByExchangeAndInstrument) {
  risk.on_positions("binance", {held("1", "buy")});
  EXPECT_EQ(code(risk.check_new_order("okx", order(OrderSide::kBuy, "1"), kNow)), "allowed");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "1", "3000", "ETHUSDT"), kNow)),
            "allowed");
}

// An amend replaces the order's remaining quantity; it does not stack on it.
TEST_F(RiskPosition, AnAmendIsJudgedAsTheOrderItWouldBecome) {
  Order o;
  o.order_id = "V-1";
  o.exchange = "binance";
  o.instrument = "BTCUSDT";
  o.side = OrderSide::kBuy;
  o.order_type = OrderType::kLimit;
  o.amount = Q("0.8");
  o.price = P("60000");
  o.status = OrderStatus::kOpen;
  risk.on_order_update(o);

  // 0.8 -> 0.9 is within 1; stacking would read it as 1.7.
  EXPECT_EQ(code(risk.check_modify("binance", o, Q("0.9"), std::nullopt, kNow)), "allowed");
  EXPECT_EQ(code(risk.check_modify("binance", o, Q("1.1"), std::nullopt, kNow)), "position");
}

// ===========================================================================
// Rate, kill switch, configuration
// ===========================================================================

TEST(RiskRate, OrdersPerStrategyPerSecondAreCapped) {
  RiskConfig c;
  c.max_orders_per_strategy_per_second = 3;
  RiskManager risk(c);
  for (int i = 0; i < 3; ++i) {
    const auto r = order(OrderSide::kBuy, "0.01");
    ASSERT_EQ(code(risk.check_new_order("binance", r, kNow + i * 0.1)), "allowed");
    risk.on_order_sent("binance", r, kNow + i * 0.1);
  }
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.01"), kNow + 0.3)), "rate");
  // Another strategy has its own budget.
  EXPECT_EQ(code(risk.check_new_order("binance",
                                      order(OrderSide::kBuy, "0.01", "60000", "BTCUSDT", "beta"),
                                      kNow + 0.3)),
            "allowed");
  // And the window slides.
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.01"), kNow + 1.05)),
            "allowed");
}

TEST(RiskKillSwitch, RefusesEverythingNewUntilReleased) {
  RiskManager risk;
  risk.engage_kill_switch("operator");
  const auto r = risk.check_new_order("binance", order(OrderSide::kBuy, "0.01"), kNow);
  EXPECT_EQ(code(r), "kill_switch");
  EXPECT_NE(r->message.find("operator"), std::string::npos);
  risk.release_kill_switch();
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "0.01"), kNow)), "allowed");
}

// `enabled: false` turns the limits off -- never the kill switch.
TEST(RiskKillSwitch, WorksEvenWithLimitsDisabled) {
  RiskConfig c;
  c.enabled = false;
  c.defaults.max_order_qty = Q("0.001");
  RiskManager risk(c);
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "5"), kNow)), "allowed");
  risk.engage_kill_switch("test");
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "5"), kNow)), "kill_switch");
}

TEST(RiskConfigTest, InstrumentEntriesOverrideTheDefaultsFieldByField) {
  RiskConfig c;
  c.defaults.max_order_qty = Q("1");
  c.defaults.max_position = Q("5");
  RiskLimits btc;
  btc.max_order_qty = Q("0.1");
  c.instruments["BTCUSDT"] = btc;
  RiskLimits binance_btc;
  binance_btc.max_position = Q("2");
  c.instruments["binance:BTCUSDT"] = binance_btc;
  RiskManager risk(c);

  const auto l = risk.limits_for("binance", "BTCUSDT");
  EXPECT_EQ(l.max_order_qty->raw(), Q("0.1").raw());  // instrument entry
  EXPECT_EQ(l.max_position->raw(), Q("2").raw());     // exchange:instrument entry
  const auto okx = risk.limits_for("okx", "BTCUSDT");
  EXPECT_EQ(okx.max_position->raw(), Q("5").raw());   // default
  const auto eth = risk.limits_for("binance", "ETHUSDT");
  EXPECT_EQ(eth.max_order_qty->raw(), Q("1").raw());
}

TEST(RiskConfigTest, ParsedFromTheCppSection) {
  const auto path = std::filesystem::temp_directory_path() / "axon_risk_config_test.yaml";
  std::ofstream(path) << R"(
exchanges:
  binance: {env: testnet}
cpp:
  risk:
    max_orders_per_strategy_per_second: 20
    kill_switch_file: /tmp/axon.kill
    cancel_all_on_kill: false
    defaults:
      max_order_notional: 50000
      max_price_deviation: 0.05
    instruments:
      "binance:BTCUSDT":
        max_order_qty: "0.5"
        max_position: 2
)";
  const auto cfg = axon::load_config(path.string());
  std::filesystem::remove(path);

  EXPECT_TRUE(cfg.risk.enabled);
  EXPECT_EQ(cfg.risk.max_orders_per_strategy_per_second, 20);
  EXPECT_EQ(cfg.risk.kill_switch_file, "/tmp/axon.kill");
  EXPECT_FALSE(cfg.risk.cancel_all_on_kill);
  EXPECT_EQ(cfg.risk.defaults.max_order_notional->raw(), P("50000").raw());
  EXPECT_DOUBLE_EQ(*cfg.risk.defaults.max_price_deviation, 0.05);
  const auto& btc = cfg.risk.instruments.at("binance:BTCUSDT");
  EXPECT_EQ(btc.max_order_qty->raw(), Q("0.5").raw());
  EXPECT_EQ(btc.max_position->raw(), Q("2").raw());
  EXPECT_FALSE(btc.max_price_deviation.has_value());
}

TEST(RiskConfigTest, ABadNumberIsAConfigError) {
  const auto path = std::filesystem::temp_directory_path() / "axon_risk_config_bad.yaml";
  std::ofstream(path) << "cpp:\n  risk:\n    defaults:\n      max_order_qty: lots\n";
  EXPECT_THROW(axon::load_config(path.string()), std::runtime_error);
  std::filesystem::remove(path);
}

TEST(RiskConfigTest, AbsentSectionMeansNoLimits) {
  const auto path = std::filesystem::temp_directory_path() / "axon_risk_config_none.yaml";
  std::ofstream(path) << "exchanges:\n  binance: {env: testnet}\n";
  const auto cfg = axon::load_config(path.string());
  std::filesystem::remove(path);
  EXPECT_TRUE(cfg.risk.instruments.empty());
  EXPECT_FALSE(cfg.risk.defaults.max_order_qty.has_value());
  RiskManager risk(cfg.risk);
  EXPECT_EQ(code(risk.check_new_order("binance", order(OrderSide::kBuy, "1000"), kNow)), "allowed");
}

// ===========================================================================
// In the EMS: a refused order never reaches the venue
// ===========================================================================

class SendingSession : public axon::oms::VenueSession {
 public:
  SendingSession() : VenueSession(cfg(), {}, {}, nullptr, {}) {}
  bool live() const noexcept override { return true; }
  bool send_raw(std::string_view p) override {
    sent.emplace_back(p);
    return true;
  }
  std::vector<std::string> sent;

 protected:
  bool send_authentication() override { return true; }
  bool send_subscriptions() override { return true; }
  bool handle_frame(const std::byte*, std::size_t, std::size_t) override { return true; }
  std::string host() const override { return "invalid.test"; }
  std::string path() const override { return "/"; }

 private:
  static axon::ExchangeConfig cfg() {
    axon::ExchangeConfig c;
    c.name = "binance";
    return c;
  }
};

class RiskInEms : public ::testing::Test {
 protected:
  RiskInEms() {
    axon::Config c;
    axon::ExchangeConfig e;
    e.name = "binance";
    e.api_key = "k";
    e.api_secret = "s";
    c.exchanges["binance"] = e;
    ems = std::make_unique<axon::ems::EmsService>(c, nullptr);
    ems->register_trade_session("binance", &session);
    RiskLimits l;
    l.max_order_qty = Q("1");
    l.max_position = Q("1.5");
    risk = RiskManager(with_defaults(l));
    ems->set_risk_manager(&risk);
    ems->set_order_lookup([this](const std::string& id) -> std::optional<Order> {
      if (id == "V-1") return working;
      return std::nullopt;
    });
  }

  std::optional<axon::ems::OrderResult> place(const OrderRequest& r) {
    std::optional<axon::ems::OrderResult> out;
    ems->place_order("binance", r, [&](const axon::ems::OrderResult& res) { out = res; });
    return out;
  }

  SendingSession session;
  RiskManager risk;
  std::unique_ptr<axon::ems::EmsService> ems;
  Order working;
};

TEST_F(RiskInEms, ARefusedOrderIsAnsweredLocallyAndNotSent) {
  const auto r = place(order(OrderSide::kBuy, "2"));
  ASSERT_TRUE(r.has_value());
  EXPECT_FALSE(r->success);
  EXPECT_FALSE(r->outcome_unknown);
  EXPECT_EQ(r->error.rfind("risk: ", 0), 0u) << r->error;
  EXPECT_TRUE(session.sent.empty());
}

// The EMS reports what it sends, so the burst adds up.
TEST_F(RiskInEms, SentOrdersCountTowardThePosition) {
  EXPECT_FALSE(place(order(OrderSide::kBuy, "1")).has_value());  // sent, awaiting reply
  const auto second = place(order(OrderSide::kBuy, "1"));
  ASSERT_TRUE(second.has_value());
  EXPECT_NE(second->error.find("position"), std::string::npos) << second->error;
  EXPECT_EQ(session.sent.size(), 1u);
}

// A risk refusal is not a submission: the same request may be sent once the
// limit allows it.
TEST_F(RiskInEms, ARiskRefusalDoesNotMarkTheIdAsSubmitted) {
  risk.engage_kill_switch("test");
  const auto req = order(OrderSide::kBuy, "0.1");
  ASSERT_TRUE(place(req).has_value());
  risk.release_kill_switch();
  EXPECT_FALSE(place(req).has_value());  // sent this time
  EXPECT_EQ(session.sent.size(), 1u);
}

// Reducing risk must always be possible.
TEST_F(RiskInEms, CancelsGoThroughWithTheKillSwitchEngaged) {
  working.order_id = "V-1";
  working.exchange = "binance";
  working.instrument = "BTCUSDT";
  risk.engage_kill_switch("test");
  std::optional<bool> ok;
  ems->cancel_order("binance", "V-1", [&](bool success, const std::string&) { ok = success; });
  EXPECT_FALSE(ok.has_value());  // in flight, not refused
  ASSERT_EQ(session.sent.size(), 1u);
  EXPECT_NE(session.sent[0].find("order.cancel"), std::string::npos);
}

TEST_F(RiskInEms, AmendsAreRiskChecked) {
  working.order_id = "V-1";
  working.exchange = "binance";
  working.instrument = "BTCUSDT";
  working.side = OrderSide::kBuy;
  working.order_type = OrderType::kLimit;
  working.amount = Q("0.5");
  working.price = P("60000");
  working.status = OrderStatus::kOpen;
  risk.on_order_update(working);

  std::optional<axon::ems::OrderResult> out;
  ems->modify_order("binance", "V-1", Q("1.6"), std::nullopt,
                    [&](const axon::ems::OrderResult& r) { out = r; });
  ASSERT_TRUE(out.has_value());
  EXPECT_FALSE(out->success);
  EXPECT_TRUE(session.sent.empty());
}

}  // namespace
