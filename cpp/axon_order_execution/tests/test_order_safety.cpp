// The two ways order state used to go silently wrong, pinned down.
//
// 1. A TIMEOUT WAS REPORTED AS A FAILURE. An order whose reply never came may
//    be live at the venue; reporting "failed" invites the strategy to retry,
//    and the retry doubles the position. Now: the timeout is reported as
//    OUTCOME UNKNOWN, a resubmission of the same internal_order_id is refused,
//    and the order's updates are tied back to the request by the client id
//    every order now carries.
//
// 2. RECONCILIATION ONLY SAW ONE DIRECTION. Applying the venue's open-orders
//    snapshot corrected the orders it listed, but an order the venue filled or
//    cancelled while the feed was down is simply absent from that list -- so it
//    stayed "open" locally forever. Now each such order is looked up by id for
//    its final state, as the Python OrderReconciler does.
//
// No network: the EMS sends through a fake live session, the reconciler reads
// a scripted VenueRest.

#include <gtest/gtest.h>

#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "axon/config.h"
#include "axon/ems/ems_service.h"
#include "axon/models/order.h"
#include "axon/oms/order_store.h"
#include "axon/oms/reconciler.h"
#include "axon/oms/venue_rest.h"
#include "axon/oms/venue_session.h"

namespace {

using namespace std::chrono_literals;
using axon::Config;
using axon::ExchangeConfig;
using axon::core::Price;
using axon::core::Qty;
using axon::ems::EmsService;
using axon::ems::OrderResult;
using axon::models::Order;
using axon::models::OrderRequest;
using axon::models::OrderSide;
using axon::models::OrderStatus;
using axon::models::OrderType;

// ---------------------------------------------------------------------------
// A session that is always live and records what it is asked to send.
// ---------------------------------------------------------------------------
class FakeLiveSession : public axon::oms::VenueSession {
 public:
  explicit FakeLiveSession(std::string name)
      : VenueSession(make_config(std::move(name)), {}, {}, nullptr, {}) {}

  bool live() const noexcept override { return true; }
  bool send_raw(std::string_view payload) override {
    sent.emplace_back(payload);
    return true;
  }

  // The request id of the n-th message sent: ws-fapi's {"id":"<n>",...}.
  std::int64_t id_of(std::size_t n) const {
    const auto& s = sent.at(n);
    const auto at = s.find(R"("id":")") + 6;
    return std::stoll(s.substr(at, s.find('"', at) - at));
  }

  std::vector<std::string> sent;

 protected:
  bool send_authentication() override { return true; }
  bool send_subscriptions() override { return true; }
  bool handle_frame(const std::byte*, std::size_t, std::size_t) override { return true; }
  std::string host() const override { return "invalid.test"; }
  std::string path() const override { return "/"; }

 private:
  static ExchangeConfig make_config(std::string name) {
    ExchangeConfig c;
    c.name = std::move(name);
    return c;
  }
};

Config binance_config(int request_timeout_seconds = 30) {
  Config c;
  ExchangeConfig e;
  e.name = "binance";
  e.api_key = "key";
  e.api_secret = "secret";
  c.exchanges["binance"] = e;
  c.websocket.request_timeout_seconds = request_timeout_seconds;
  return c;
}

OrderRequest limit_buy() {
  OrderRequest r;
  r.instrument = "BTCUSDT";
  r.side = OrderSide::kBuy;
  r.order_type = OrderType::kLimit;
  r.amount = *Qty::from_string("0.002");
  r.price = Price::from_string("60000");
  r.strategy_id = "alpha";
  return r;
}

class EmsSafetyTest : public ::testing::Test {
 protected:
  void make(int timeout_seconds = 30) {
    ems = std::make_unique<EmsService>(binance_config(timeout_seconds), nullptr);
    session = std::make_unique<FakeLiveSession>("binance");
    ems->register_trade_session("binance", session.get());
  }

  // For a placement answered synchronously (refused locally) or never at all.
  // A callback the EMS fires later must not capture this function's local.
  std::optional<OrderResult> place(const OrderRequest& r) {
    std::optional<OrderResult> out;
    ems->place_order("binance", r, [&](const OrderResult& res) { out = res; });
    return out;
  }

  std::unique_ptr<EmsService> ems;
  std::unique_ptr<FakeLiveSession> session;
};

// ===========================================================================
// EMS: outcome unknown, idempotency
// ===========================================================================

TEST_F(EmsSafetyTest, UnansweredPlacementIsOutcomeUnknownNotFailed) {
  make(/*timeout_seconds=*/0);
  std::optional<OrderResult> out;
  ems->place_order("binance", limit_buy(), [&](const OrderResult& r) { out = r; });
  ASSERT_EQ(session->sent.size(), 1u);
  std::this_thread::sleep_for(5ms);
  ems->poll();

  ASSERT_TRUE(out.has_value());
  EXPECT_FALSE(out->success);
  EXPECT_TRUE(out->outcome_unknown) << "a timeout must not read as a rejection";
}

TEST_F(EmsSafetyTest, ResubmittingAnUnresolvedOrderIsRefused) {
  make(0);
  const auto req = limit_buy();
  // The EMS keeps this callback until the timeout fires, so what it writes to
  // must outlive the call -- not place()'s local.
  std::optional<OrderResult> first;
  ems->place_order("binance", req, [&](const OrderResult& r) { first = r; });
  std::this_thread::sleep_for(5ms);
  ems->poll();  // times out -> unknown
  ASSERT_TRUE(first.has_value());
  ASSERT_TRUE(first->outcome_unknown);

  // The strategy's natural reaction -- try again -- must not reach the venue.
  const auto retry = place(req);
  ASSERT_TRUE(retry.has_value());
  EXPECT_FALSE(retry->success);
  EXPECT_FALSE(retry->outcome_unknown);
  EXPECT_NE(retry->error.find("duplicate internal_order_id"), std::string::npos) << retry->error;
  EXPECT_EQ(session->sent.size(), 1u) << "the duplicate was sent to the venue";
}

TEST_F(EmsSafetyTest, ResubmittingWhileTheFirstIsInFlightIsRefused) {
  make();
  const auto req = limit_buy();
  EXPECT_FALSE(place(req).has_value());  // in flight, no reply yet
  const auto again = place(req);
  ASSERT_TRUE(again.has_value());
  EXPECT_FALSE(again->success);
  EXPECT_EQ(session->sent.size(), 1u);
}

// A definite no from the venue means nothing is live: the same id may retry.
TEST_F(EmsSafetyTest, AVenueRejectionAllowsARetry) {
  make();
  const auto req = limit_buy();
  std::optional<OrderResult> first;
  ems->place_order("binance", req, [&](const OrderResult& r) { first = r; });
  ems->on_rpc_reply("binance", session->id_of(0), false, "{}", "Margin is insufficient.");
  ASSERT_TRUE(first.has_value());
  EXPECT_FALSE(first->success);
  EXPECT_FALSE(first->outcome_unknown);

  place(req);
  EXPECT_EQ(session->sent.size(), 2u) << "a rejected order could not be retried";
}

TEST_F(EmsSafetyTest, DistinctOrdersAreNotMistakenForDuplicates) {
  make();
  place(limit_buy());
  place(limit_buy());  // a new request: a new internal_order_id
  EXPECT_EQ(session->sent.size(), 2u);
}

// The venue order id comes back with the acceptance, so a strategy can tie
// what it placed to what it later sees.
TEST_F(EmsSafetyTest, BinanceAcceptanceCarriesTheVenueOrder) {
  make();
  const auto req = limit_buy();
  std::optional<OrderResult> out;
  ems->place_order("binance", req, [&](const OrderResult& r) { out = r; });
  ems->on_rpc_reply("binance", session->id_of(0), true,
                    R"({"id":")" + std::to_string(session->id_of(0)) +
                        R"(","status":200,"result":{"orderId":28618940365,"symbol":"BTCUSDT",)"
                        R"("status":"NEW","clientOrderId":")" + req.internal_order_id +
                        R"(","price":"60000.00","avgPrice":"0.00","origQty":"0.002",)"
                        R"("executedQty":"0","type":"LIMIT","side":"BUY","updateTime":1791196866625}})",
                    "");
  ASSERT_TRUE(out.has_value());
  ASSERT_TRUE(out->success);
  ASSERT_TRUE(out->order.has_value());
  EXPECT_EQ(out->order->order_id, "28618940365");
  EXPECT_EQ(out->order->status, OrderStatus::kOpen);
  EXPECT_EQ(out->order->internal_order_id.value_or(""), req.internal_order_id);
  EXPECT_EQ(out->order->strategy_id.value_or(""), "alpha");
  EXPECT_EQ(out->order->price->raw(), req.price->raw());
}

// ===========================================================================
// OrderStore: an update finds its way back to the request
// ===========================================================================

Order venue_update(const std::string& client_id, OrderStatus status) {
  Order o;
  o.order_id = "V-1";
  o.exchange = "binance";
  o.instrument = "BTCUSDT";
  o.amount = *Qty::from_string("1");
  o.status = status;
  o.internal_order_id = client_id;  // what the parser puts there
  o.updated_at = axon::core::Timestamp::now();
  return o;
}

TEST(OrderStoreSubmissions, UpdateForAnUnknownOutcomeOrderReachesItsStrategy) {
  axon::oms::OrderStore store;
  std::vector<Order> seen;
  store.register_update_callback([&](const Order& o) { seen.push_back(o); });

  auto req = limit_buy();
  store.register_submission("binance", req);
  // The placement reply was lost; the feed reports the order anyway.
  store.update_from_ws(venue_update(req.internal_order_id, OrderStatus::kOpen));

  ASSERT_EQ(seen.size(), 1u);
  EXPECT_EQ(seen[0].internal_order_id.value_or(""), req.internal_order_id);
  EXPECT_EQ(seen[0].strategy_id.value_or(""), "alpha");
}

// With a label the venue echoes the LABEL; it is mapped back to the real id.
TEST(OrderStoreSubmissions, ALabelIsMappedBackToTheInternalOrderId) {
  axon::oms::OrderStore store;
  auto req = limit_buy();
  req.label = "chasemaker";
  store.register_submission("binance", req);
  store.update_from_ws(venue_update("chasemaker", OrderStatus::kOpen));

  const auto o = store.get_order("V-1");
  ASSERT_TRUE(o.has_value());
  EXPECT_EQ(o->internal_order_id.value_or(""), req.internal_order_id);
  EXPECT_EQ(o->strategy_id.value_or(""), "alpha");
}

TEST(OrderStoreSubmissions, IdsAreScopedPerExchange) {
  axon::oms::OrderStore store;
  auto req = limit_buy();
  store.register_submission("okx", req);
  EXPECT_FALSE(store.find_submission("binance", req.internal_order_id).has_value());
  EXPECT_TRUE(store.find_submission("okx", req.internal_order_id).has_value());
}

TEST(OrderStoreSubmissions, ATerminalUpdateReleasesTheSubmission) {
  axon::oms::OrderStore store;
  auto req = limit_buy();
  store.register_submission("binance", req);
  store.update_from_ws(venue_update(req.internal_order_id, OrderStatus::kOpen));
  EXPECT_EQ(store.submission_count(), 1u);
  auto done = venue_update(req.internal_order_id, OrderStatus::kFilled);
  done.updated_at = axon::core::Timestamp::from_ns(done.updated_at.ns() + 1'000'000);
  store.update_from_ws(done);
  EXPECT_EQ(store.submission_count(), 0u);
}

TEST(OrderStoreSubmissions, ForgetDropsARejectedSubmission) {
  axon::oms::OrderStore store;
  auto req = limit_buy();
  store.register_submission("binance", req);
  store.forget_submission("binance", req);
  EXPECT_EQ(store.submission_count(), 0u);
}

// An order placed outside this engine is left exactly as the venue sent it.
TEST(OrderStoreSubmissions, AForeignClientIdIsLeftAlone) {
  axon::oms::OrderStore store;
  store.update_from_ws(venue_update("webui123", OrderStatus::kOpen));
  const auto o = store.get_order("V-1");
  ASSERT_TRUE(o.has_value());
  EXPECT_EQ(o->internal_order_id.value_or(""), "webui123");
  EXPECT_FALSE(o->strategy_id.has_value());
}

// ===========================================================================
// Reconciler: orders that closed while the feed was not looking
// ===========================================================================

// A VenueRest whose answers the test scripts. Callbacks may be held and fired
// later to exercise passes that span several replies.
class ScriptedRest : public axon::oms::VenueRest {
 public:
  std::map<std::string, std::vector<Order>> open_by_currency;
  std::map<std::string, std::string> open_errors;  // currency -> error
  std::map<std::string, Order> final_state;        // order id -> state
  std::vector<std::string> looked_up;
  bool hold_open_orders = false;
  std::vector<std::function<void()>> held;

  void get_open_orders(const std::string& currency, OrdersCallback cb) override {
    auto fire = [this, currency, cb] {
      const auto err = open_errors.count(currency) ? open_errors[currency] : std::string();
      cb(err.empty() ? open_by_currency[currency] : std::vector<Order>{}, err);
    };
    if (hold_open_orders) {
      held.push_back(fire);
    } else {
      fire();
    }
  }
  void get_order(const Order& order, OrderCallback cb) override {
    looked_up.push_back(order.order_id);
    const auto it = final_state.find(order.order_id);
    if (it == final_state.end()) {
      cb(std::nullopt, "HTTP 400: Order does not exist.");
    } else {
      cb(it->second, {});
    }
  }
  void get_ticker(const std::string&, TickerCallback cb) override { cb(std::nullopt, "n/a"); }
  void get_positions(const std::string&, PositionsCallback cb) override { cb({}, {}); }
  void get_user_trades(const std::string&, std::int64_t, std::int64_t, FillsCallback cb) override {
    cb({}, {});
  }
  const std::string& exchange_name() const override { return name_; }

 private:
  std::string name_ = "binance";
};

Order open_order(const std::string& id, const std::string& exchange = "binance") {
  Order o;
  o.order_id = id;
  o.exchange = exchange;
  o.instrument = "BTCUSDT";
  o.amount = *Qty::from_string("1");
  o.status = OrderStatus::kOpen;
  o.strategy_id = "alpha";
  o.updated_at = axon::core::Timestamp::from_millis(1'000);
  return o;
}

Order closed(Order o, OrderStatus status, const char* filled) {
  o.status = status;
  o.filled_amount = *Qty::from_string(filled);
  o.strategy_id.reset();  // the venue does not know it; the store carries it
  o.updated_at = axon::core::Timestamp::from_millis(2'000);
  return o;
}

class ReconcilerTest : public ::testing::Test {
 protected:
  void run_pass(std::vector<std::string> currencies = {"USDT"}) {
    Config config;
    config.reconciliation.enabled = true;
    config.fill_reconciliation.enabled = false;
    config.portfolio.currencies = std::move(currencies);
    ExchangeConfig ex;
    ex.name = "binance";
    reconciler = std::make_unique<axon::oms::Reconciler>(config, ex, &rest, &store,
                                                         axon::oms::ReconcilerCallbacks{});
    reconciler->on_session_live();
    reconciler->poll();
  }

  OrderStatus local_status(const std::string& id) {
    auto o = store.get_order(id);
    return o ? o->status : OrderStatus::kPending;
  }

  axon::oms::OrderStore store;
  ScriptedRest rest;
  std::unique_ptr<axon::oms::Reconciler> reconciler;
};

TEST_F(ReconcilerTest, AnOrderThatFilledUnseenIsResolvedToFilled) {
  store.update_order(open_order("A"));
  rest.open_by_currency["USDT"] = {};  // the venue no longer lists it
  rest.final_state["A"] = closed(open_order("A"), OrderStatus::kFilled, "1");

  std::vector<Order> notified;
  store.register_update_callback([&](const Order& o) { notified.push_back(o); });
  run_pass();

  EXPECT_EQ(rest.looked_up, std::vector<std::string>{"A"});
  EXPECT_TRUE(store.active_orders().empty()) << "the order is still open locally";
  ASSERT_EQ(notified.size(), 1u);
  EXPECT_EQ(notified[0].status, OrderStatus::kFilled);
  EXPECT_EQ(notified[0].strategy_id.value_or(""), "alpha") << "the strategy was not told";
  EXPECT_EQ(reconciler->stats().closed_orders_recovered, 1u);
}

// Filled and cancelled are not interchangeable: the lookup is what tells them
// apart, which is why the order is not simply marked closed.
TEST_F(ReconcilerTest, AnOrderCancelledUnseenIsResolvedToCancelled) {
  store.update_order(open_order("A"));
  rest.final_state["A"] = closed(open_order("A"), OrderStatus::kCancelled, "0");
  run_pass();
  EXPECT_TRUE(store.active_orders().empty());
  EXPECT_EQ(reconciler->stats().closed_orders_recovered, 1u);
}

TEST_F(ReconcilerTest, OrdersTheVenueListsAreNotLookedUp) {
  store.update_order(open_order("A"));
  rest.open_by_currency["USDT"] = {open_order("A")};
  run_pass();
  EXPECT_TRUE(rest.looked_up.empty());
  EXPECT_EQ(store.active_orders().size(), 1u);
}

TEST_F(ReconcilerTest, OtherExchangesOrdersAreNotThisReconcilersBusiness) {
  store.update_order(open_order("OKX-1", "okx"));
  run_pass();
  EXPECT_TRUE(rest.looked_up.empty());
}

// An order absent from an INCOMPLETE snapshot proves nothing.
TEST_F(ReconcilerTest, NoLookupsWhenAnyCurrencysSnapshotFailed) {
  store.update_order(open_order("A"));
  rest.open_errors["BTC"] = "HTTP 502";
  run_pass({"USDT", "BTC"});
  EXPECT_TRUE(rest.looked_up.empty());
  EXPECT_EQ(store.active_orders().size(), 1u);
}

// Waits for every currency before deciding anything is missing -- an order
// listed by the second reply must not be looked up because of the first.
TEST_F(ReconcilerTest, MissingOrdersAreJudgedOnlyOnceEveryReplyIsIn) {
  store.update_order(open_order("A"));
  rest.hold_open_orders = true;
  rest.open_by_currency["BTC"] = {open_order("A")};
  run_pass({"USDT", "BTC"});
  ASSERT_EQ(rest.held.size(), 2u);

  rest.held[0]();  // USDT: does not list A
  EXPECT_TRUE(rest.looked_up.empty()) << "judged on a partial snapshot";
  rest.held[1]();  // BTC: lists A
  EXPECT_TRUE(rest.looked_up.empty());
}

TEST_F(ReconcilerTest, AFailedLookupLeavesTheOrderOpenForNextTime) {
  store.update_order(open_order("A"));  // no final_state: the lookup errors
  run_pass();
  EXPECT_EQ(rest.looked_up.size(), 1u);
  EXPECT_EQ(store.active_orders().size(), 1u);
  EXPECT_GE(reconciler->stats().failures, 1u);
}

// A truncated snapshot must not become one signed request per local order.
TEST_F(ReconcilerTest, LookupsPerPassAreBounded) {
  for (int i = 0; i < 50; ++i) {
    const auto id = "O" + std::to_string(i);
    store.update_order(open_order(id));
    rest.final_state[id] = closed(open_order(id), OrderStatus::kCancelled, "0");
  }
  run_pass();
  EXPECT_EQ(rest.looked_up.size(), 20u);
  EXPECT_EQ(store.active_orders().size(), 30u);
}

// The race: an order placed after the snapshot was taken is absent from it but
// still open. The lookup says so, and nothing changes.
TEST_F(ReconcilerTest, AnOrderNewerThanTheSnapshotStaysOpen) {
  store.update_order(open_order("A"));
  rest.final_state["A"] = open_order("A");
  run_pass();
  EXPECT_EQ(rest.looked_up.size(), 1u);
  EXPECT_EQ(local_status("A"), OrderStatus::kOpen);
  EXPECT_EQ(reconciler->stats().closed_orders_recovered, 0u);
}

}  // namespace
