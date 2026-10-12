// The risk controls that look beyond a single order: trading against
// ourselves, losing too much, and trading blind while an exchange's feed is
// down. Each test is one way money would otherwise be lost.

#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "axon/config.h"
#include "axon/ems/exchange_ops.h"
#include "axon/models/fill.h"
#include "axon/models/order.h"
#include "axon/models/portfolio.h"
#include "axon/risk/risk_manager.h"

namespace {

using axon::RiskConfig;
using axon::core::Price;
using axon::core::Qty;
using axon::models::Fill;
using axon::models::Order;
using axon::models::OrderRequest;
using axon::models::OrderSide;
using axon::models::OrderStatus;
using axon::models::OrderType;
using axon::models::SelfTradePrevention;
using axon::risk::KillSource;
using axon::risk::RiskManager;

Qty Q(const char* s) { return *Qty::from_string(s); }
Price P(const char* s) { return *Price::from_string(s); }

OrderRequest limit(OrderSide side, const char* price, const char* id,
                   const char* instrument = "BTCUSDT") {
  OrderRequest r;
  r.instrument = instrument;
  r.side = side;
  r.order_type = OrderType::kLimit;
  r.amount = Q("0.01");
  r.price = P(price);
  r.internal_order_id = id;
  r.strategy_id = "alpha";
  return r;
}

OrderRequest market(OrderSide side, const char* id) {
  OrderRequest r = limit(side, "1", id);
  r.order_type = OrderType::kMarket;
  r.price.reset();
  return r;
}

std::string code(const std::optional<axon::risk::RiskRejection>& r) {
  return r ? r->code : "allowed";
}

constexpr double kNow = 1000.0;
constexpr std::int64_t kStartMs = 1'700'000'000'000;  // 2023-11-14T22:13:20Z

// ===========================================================================
// Self-trade prevention
// ===========================================================================

class SelfTrade : public ::testing::Test {
 protected:
  RiskManager risk{RiskConfig{}};
  void SetUp() override {
    // Our own sell resting at 60100.
    risk.on_order_sent("binance", limit(OrderSide::kSell, "60100", "ask1"), kNow);
  }
};

TEST_F(SelfTrade, ABuyReachingOurOwnSellIsRefused) {
  const auto r = risk.check_new_order("binance", limit(OrderSide::kBuy, "60100", "b1"), kNow);
  EXPECT_EQ(code(r), "self_trade");
  EXPECT_NE(r->message.find("60100"), std::string::npos) << r->message;
  EXPECT_EQ(code(risk.check_new_order("binance", limit(OrderSide::kBuy, "60200", "b2"), kNow)),
            "self_trade");
}

TEST_F(SelfTrade, ABuyBelowOurSellIsFine) {
  EXPECT_EQ(code(risk.check_new_order("binance", limit(OrderSide::kBuy, "60099", "b1"), kNow)),
            "allowed");
}

TEST_F(SelfTrade, SameSideNeverSelfTrades) {
  EXPECT_EQ(code(risk.check_new_order("binance", limit(OrderSide::kSell, "50000", "s2"), kNow)),
            "allowed");
}

TEST_F(SelfTrade, OnlyTheSameExchangeAndInstrumentCount) {
  EXPECT_EQ(code(risk.check_new_order("bybit", limit(OrderSide::kBuy, "60200", "b1"), kNow)),
            "allowed");
  EXPECT_EQ(code(risk.check_new_order(
                "binance", limit(OrderSide::kBuy, "60200", "b2", "ETHUSDT"), kNow)),
            "allowed");
}

// A post-only order is rejected by the exchange rather than allowed to take.
TEST_F(SelfTrade, PostOnlyIsExempt) {
  auto r = limit(OrderSide::kBuy, "60200", "b1");
  r.post_only = true;
  EXPECT_EQ(code(risk.check_new_order("binance", r, kNow)), "allowed");
}

TEST_F(SelfTrade, AFilledOrCancelledOrderNoLongerCounts) {
  Order o;
  o.exchange = "binance";
  o.instrument = "BTCUSDT";
  o.internal_order_id = "ask1";
  o.order_id = "1";
  o.side = OrderSide::kSell;
  o.order_type = OrderType::kLimit;
  o.amount = Q("0.01");
  o.price = P("60100");
  o.status = OrderStatus::kCancelled;
  risk.on_order_update(o);
  EXPECT_EQ(code(risk.check_new_order("binance", limit(OrderSide::kBuy, "60200", "b1"), kNow)),
            "allowed");
}

// Amending our sell down through our own buy: the sell being amended is not
// a counterparty to itself, but the buy is.
TEST_F(SelfTrade, AnAmendIsCheckedAsTheOrderItBecomes) {
  risk.on_order_sent("binance", limit(OrderSide::kBuy, "60000", "bid1"), kNow);
  Order ask;
  ask.exchange = "binance";
  ask.instrument = "BTCUSDT";
  ask.internal_order_id = "ask1";
  ask.side = OrderSide::kSell;
  ask.order_type = OrderType::kLimit;
  ask.amount = Q("0.01");
  ask.price = P("60100");
  EXPECT_EQ(code(risk.check_modify("binance", ask, std::nullopt, P("60050"), kNow)), "allowed");
  EXPECT_EQ(code(risk.check_modify("binance", ask, std::nullopt, P("60000"), kNow)),
            "self_trade");
}

TEST_F(SelfTrade, AMarketOrderTakesOurSellWhenItIsAtTheTop) {
  risk.update_reference_price("binance", "BTCUSDT", P("60150"), kNow);  // our ask is below it
  EXPECT_EQ(code(risk.check_new_order("binance", market(OrderSide::kBuy, "m1"), kNow)),
            "self_trade");
}

TEST_F(SelfTrade, AMarketOrderPassesWhenOurSellIsAwayFromTheTop) {
  risk.update_reference_price("binance", "BTCUSDT", P("60000"), kNow);
  EXPECT_EQ(code(risk.check_new_order("binance", market(OrderSide::kBuy, "m1"), kNow)),
            "allowed");
}

TEST_F(SelfTrade, AMarketOrderWithNoReferenceFailsClosed) {
  EXPECT_EQ(code(risk.check_new_order("binance", market(OrderSide::kBuy, "m1"), kNow)),
            "self_trade");
}

TEST(SelfTradeConfig, CanBeTurnedOff) {
  RiskConfig c;
  c.self_trade_prevention = false;
  RiskManager risk(c);
  risk.on_order_sent("binance", limit(OrderSide::kSell, "60100", "ask1"), kNow);
  EXPECT_EQ(code(risk.check_new_order("binance", limit(OrderSide::kBuy, "60200", "b1"), kNow)),
            "allowed");
}

// --- the exchange's own guard, sent with every order -------------------------

TEST(SelfTradeParameter, EachExchangeSendsItsOwnSpelling) {
  axon::ExchangeConfig cfg;
  cfg.api_key = "k";
  cfg.api_secret = "s";
  cfg.self_trade_prevention = SelfTradePrevention::kCancelTaker;
  axon::ems::ExchangeState state;
  for (const char* name : {"binance", "bybit", "okx"}) {
    axon::ems::ops_for(name)->configure(state, cfg);
  }
  auto req = limit(OrderSide::kBuy, "60000", "abc");

  char buf[axon::ems::kMaxRequestBytes];
  std::int64_t id = 0;
  std::string out(buf, state.binance.ws_place_order(buf, sizeof buf, req, "k", "s", 1, id));
  // Inside the signed parameters, in alphabetical place.
  const auto stp = out.find(R"("selfTradePreventionMode":"EXPIRE_TAKER")");
  ASSERT_NE(stp, std::string::npos) << out;
  EXPECT_LT(out.find(R"("recvWindow")"), stp);
  EXPECT_GT(out.find(R"("side")"), stp);
  EXPECT_LT(stp, out.find(R"("signature")"));

  out.assign(buf, state.bybit.place_order_body(buf, sizeof buf, req));
  EXPECT_NE(out.find(R"("smpType":"CancelTaker")"), std::string::npos) << out;

  out.assign(buf, state.okx.place_order_body(buf, sizeof buf, req));
  EXPECT_NE(out.find(R"("stpMode":"cancel_taker")"), std::string::npos) << out;
}

TEST(SelfTradeParameter, ExchangeDefaultSendsNothing) {
  axon::ems::ExchangeState state;  // builders start at the exchange default
  auto req = limit(OrderSide::kBuy, "60000", "abc");
  char buf[axon::ems::kMaxRequestBytes];
  std::int64_t id = 0;
  std::string out(buf, state.binance.ws_place_order(buf, sizeof buf, req, "k", "s", 1, id));
  EXPECT_EQ(out.find("selfTradePreventionMode"), std::string::npos) << out;
  out.assign(buf, state.bybit.place_order_body(buf, sizeof buf, req));
  EXPECT_EQ(out.find("smpType"), std::string::npos) << out;
  out.assign(buf, state.okx.place_order_body(buf, sizeof buf, req));
  EXPECT_EQ(out.find("stpMode"), std::string::npos) << out;
}

TEST(SelfTradeParameter, TheConfigDefaultIsCancelTaker) {
  EXPECT_EQ(axon::ExchangeConfig{}.self_trade_prevention, SelfTradePrevention::kCancelTaker);
}

// ===========================================================================
// Profit and loss, and the loss limit
// ===========================================================================

Fill fill(OrderSide side, const char* qty, const char* price, std::int64_t at_ms = kStartMs + 1,
          const char* fee = "0", const char* fee_ccy = "USDT", const char* exchange = "binance") {
  Fill f;
  f.trade_id = "t";
  f.exchange = exchange;
  f.instrument = "BTCUSDT";
  f.side = side;
  f.amount = Q(qty);
  f.price = P(price);
  f.fee = P(fee);
  f.fee_currency = fee_ccy;
  f.timestamp = axon::core::Timestamp::from_millis(at_ms);
  return f;
}

RiskConfig loss_limit(double max_loss) {
  RiskConfig c;
  c.max_loss = max_loss;
  return c;
}

TEST(Pnl, ARoundTripIsRealised) {
  RiskManager risk(RiskConfig{});
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "1", "60000"));
  risk.on_fill(fill(OrderSide::kSell, "1", "60500"));
  EXPECT_DOUBLE_EQ(risk.pnl(kNow).total, 500.0);
  EXPECT_EQ(risk.pnl(kNow).unvalued, 0u);
}

TEST(Pnl, AnOpenPositionIsMarkedToTheReference) {
  RiskManager risk(RiskConfig{});
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "2", "60000"));
  risk.update_reference_price("binance", "BTCUSDT", P("59000"), kNow);
  EXPECT_DOUBLE_EQ(risk.pnl(kNow).total, -2000.0);
}

TEST(Pnl, StablecoinFeesAreACostOthersAreLeftOut) {
  RiskManager risk(RiskConfig{});
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "1", "60000", kStartMs + 1, "12", "USDT"));
  risk.on_fill(fill(OrderSide::kSell, "1", "60000", kStartMs + 2, "0.01", "BNB"));
  EXPECT_DOUBLE_EQ(risk.pnl(kNow).total, -12.0);
}

// The position held when the engine starts is valued from the first price
// seen, so a market move against it is a loss today.
TEST(Pnl, ThePositionHeldAtTheStartCounts) {
  RiskManager risk(RiskConfig{});
  risk.start_pnl_session(kStartMs);
  axon::models::Position p;
  p.instrument = "BTCUSDT";
  p.direction = "buy";
  p.size = Q("1");
  risk.on_positions("binance", {p});
  risk.update_reference_price("binance", "BTCUSDT", P("60000"), kNow);
  risk.update_reference_price("binance", "BTCUSDT", P("59000"), kNow + 1);
  EXPECT_DOUBLE_EQ(risk.pnl(kNow + 1).total, -1000.0);
}

TEST(Pnl, FillsFromBeforeTheSessionAreHistory) {
  RiskManager risk(RiskConfig{});
  risk.start_pnl_session(kStartMs);
  // Recovered at startup: in the position already, not in today's PnL.
  risk.on_fill(fill(OrderSide::kBuy, "1", "50000", kStartMs - 60'000));
  risk.update_reference_price("binance", "BTCUSDT", P("60000"), kNow);
  EXPECT_DOUBLE_EQ(risk.pnl(kNow).total, 0.0);
}

TEST(Pnl, APositionWithoutAFreshPriceIsUnvaluedNotZero) {
  RiskConfig c;
  c.reference_price_max_age_seconds = 10;
  RiskManager risk(c);
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "1", "60000"));
  risk.update_reference_price("binance", "BTCUSDT", P("60000"), kNow);
  EXPECT_EQ(risk.pnl(kNow + 60).unvalued, 1u);
}

TEST(Pnl, ExcludedExchangesAreNotCounted) {
  RiskManager risk(RiskConfig{});  // deribit excluded by default
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "1", "60000", kStartMs + 1, "0", "USDT", "deribit"));
  risk.on_fill(fill(OrderSide::kSell, "1", "50000", kStartMs + 2, "0", "USDT", "deribit"));
  EXPECT_DOUBLE_EQ(risk.pnl(kNow).total, 0.0);
}

TEST(LossLimit, BreachingItEngagesTheKillSwitch) {
  RiskManager risk(loss_limit(1000));
  risk.start_pnl_session(kStartMs);
  int cancels = 0;
  risk.set_on_kill_engaged([&](const std::string&) { ++cancels; });

  risk.on_fill(fill(OrderSide::kBuy, "1", "60000"));
  risk.update_reference_price("binance", "BTCUSDT", P("59100"), kNow);
  risk.check_loss(kNow);
  EXPECT_FALSE(risk.kill_switch_engaged()) << "-900 is inside the limit";

  risk.update_reference_price("binance", "BTCUSDT", P("58900"), kNow + 1);
  risk.check_loss(kNow + 1);
  ASSERT_TRUE(risk.kill_switch_engaged());
  EXPECT_EQ(risk.kill_switch_source(), KillSource::kLoss);
  EXPECT_NE(risk.kill_switch_reason().find("-1100.00"), std::string::npos)
      << risk.kill_switch_reason();
  EXPECT_EQ(cancels, 1) << "working orders are cancelled";
  EXPECT_EQ(code(risk.check_new_order("binance", limit(OrderSide::kBuy, "58000", "x"), kNow)),
            "kill_switch");
}

// The operator has seen the loss and chosen to continue: the switch must not
// slam shut again on the next tick.
TEST(LossLimit, AReleaseHoldsForTheRestOfTheSession) {
  RiskManager risk(loss_limit(1000));
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "1", "60000"));
  risk.update_reference_price("binance", "BTCUSDT", P("58000"), kNow);
  risk.check_loss(kNow);
  ASSERT_TRUE(risk.kill_switch_engaged());
  risk.release_kill_switch();
  risk.check_loss(kNow + 1);
  EXPECT_FALSE(risk.kill_switch_engaged());
}

TEST(LossLimit, TheKillFileCannotReleaseIt) {
  RiskManager risk(loss_limit(1000));
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "1", "60000"));
  risk.update_reference_price("binance", "BTCUSDT", P("58000"), kNow);
  risk.check_loss(kNow);
  risk.observe_kill_file(false, "axon.kill");
  EXPECT_TRUE(risk.kill_switch_engaged());
}

TEST(LossLimit, ANewUtcDayStartsANewSession) {
  RiskManager risk(loss_limit(1000));
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "1", "60000"));
  risk.update_reference_price("binance", "BTCUSDT", P("58000"), kNow);
  risk.check_loss(kNow);
  risk.release_kill_switch();

  // 00:00 UTC: the 1 BTC is now held from 58000.
  const std::int64_t midnight = (kStartMs / 86'400'000 + 1) * 86'400'000;
  risk.on_wall_clock(midnight + 5);
  EXPECT_DOUBLE_EQ(risk.pnl(kNow + 1).total, 0.0);
  risk.update_reference_price("binance", "BTCUSDT", P("56900"), kNow + 2);
  risk.check_loss(kNow + 2);
  EXPECT_TRUE(risk.kill_switch_engaged()) << "a new session, a new limit";
}

TEST(LossLimit, NoneConfiguredNeverTrips) {
  RiskManager risk(RiskConfig{});
  risk.start_pnl_session(kStartMs);
  risk.on_fill(fill(OrderSide::kBuy, "1", "60000"));
  risk.update_reference_price("binance", "BTCUSDT", P("1"), kNow);
  risk.check_loss(kNow);
  EXPECT_FALSE(risk.kill_switch_engaged());
}

// ===========================================================================
// An exchange whose feed is down
// ===========================================================================

TEST(ExchangeDown, NewOrdersHaltAfterTheGracePeriod) {
  RiskManager risk(RiskConfig{});  // 5s by default
  risk.observe_exchange_link("binance", false, kNow);
  EXPECT_EQ(code(risk.check_new_order("binance", limit(OrderSide::kBuy, "60000", "a"), kNow)),
            "allowed") << "a blip is not an outage";
  risk.observe_exchange_link("binance", false, kNow + 5);
  const auto r = risk.check_new_order("binance", limit(OrderSide::kBuy, "60000", "b"), kNow + 5);
  EXPECT_EQ(code(r), "exchange_down");
  EXPECT_TRUE(risk.exchange_halted("binance"));
  EXPECT_EQ(code(risk.check_new_order("bybit", limit(OrderSide::kBuy, "60000", "c"), kNow + 5)),
            "allowed") << "other exchanges are unaffected";
}

TEST(ExchangeDown, TheFeedComingBackResumes) {
  RiskManager risk(RiskConfig{});
  risk.observe_exchange_link("binance", false, kNow);
  risk.observe_exchange_link("binance", false, kNow + 6);
  ASSERT_TRUE(risk.exchange_halted("binance"));
  risk.observe_exchange_link("binance", true, kNow + 7);
  EXPECT_FALSE(risk.exchange_halted("binance"));
  // And a new outage starts its own grace period.
  risk.observe_exchange_link("binance", false, kNow + 8);
  EXPECT_FALSE(risk.exchange_halted("binance"));
}

TEST(ExchangeDown, AppliesEvenWithLimitsDisabled) {
  RiskConfig c;
  c.enabled = false;
  RiskManager risk(c);
  risk.observe_exchange_link("binance", false, kNow);
  risk.observe_exchange_link("binance", false, kNow + 10);
  EXPECT_EQ(code(risk.check_new_order("binance", limit(OrderSide::kBuy, "60000", "a"), kNow)),
            "exchange_down");
}

TEST(ExchangeDown, ZeroDisablesIt) {
  RiskConfig c;
  c.exchange_down_halt_seconds = 0;
  RiskManager risk(c);
  risk.observe_exchange_link("binance", false, kNow);
  risk.observe_exchange_link("binance", false, kNow + 3600);
  EXPECT_FALSE(risk.exchange_halted("binance"));
}

}  // namespace
