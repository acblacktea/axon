// Binance USDT-M against the LIVE TESTNET.
//
// Everything else in this repo proves the bytes are right in isolation. This
// proves the venue accepts them: the listenKey and user data stream, the
// ws-fapi order-entry connection, every signed request, and the round trip
// from an order sent to the update and fill that come back on the feed.
//
// The WebSocket order-entry mapping for Binance was transcribed from the
// venue's documentation and there is no REST order path beside it, so this
// suite is the first evidence that order entry works at all.
//
// SAFETY
//   * The exchange env is hard-coded to "testnet". Whatever key is in the
//     environment, nothing here can reach production hosts.
//   * Runs only with AXON_TESTNET=1 and AXON_BINANCE_API_KEY/_SECRET set;
//     otherwise every test is skipped, so a plain `ctest` never touches the
//     network.
//   * Resting orders are placed ~3% away from the touch with post-only, and
//     every order a test creates is cancelled by that test. The one test that
//     trades buys the minimum and sells it straight back.
//   * Pre-existing orders and positions on the account are never touched.
//
//   source ~/.config/axon/binance-testnet.env
//   AXON_TESTNET=1 ./build/tests/axon_testnet_tests

#include <gtest/gtest.h>

#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <nlohmann/json.hpp>

#include "axon/config.h"
#include "axon/core/clock.h"
#include "axon/ems/ems_service.h"
#include "axon/models/order.h"
#include "axon/net/http_client.h"
#include "axon/net/tls_stream.h"
#include "axon/oms/venue_rest.h"
#include "axon/oms/venue_session.h"
#include "axon/transport/hot_messages.h"
#include "axon/util/logging.h"

namespace {

using namespace std::chrono_literals;
using axon::core::Price;
using axon::core::Qty;
using axon::ems::OrderResult;
using axon::models::Order;
using axon::models::OrderRequest;
using axon::models::OrderSide;
using axon::models::OrderStatus;
using axon::models::OrderType;
using Json = nlohmann::json;

constexpr const char* kSymbol = "BTCUSDT";

std::string env(const char* name) {
  const char* v = std::getenv(name);
  return v == nullptr ? std::string() : std::string(v);
}

std::int64_t now_ms() { return axon::core::wall_clock_ns() / 1'000'000LL; }

// A client order id unique to this run, so a test can find ITS order among
// whatever else is on the account.
std::string fresh_label(const char* tag) {
  static int n = 0;
  return std::string("axt") + tag + std::to_string(now_ms()) + "x" + std::to_string(n++);
}

// Formats `v` floored to `step` with the step's decimals, as a venue string.
std::string to_step(double v, double step, int decimals) {
  const double floored = std::floor(v / step + 1e-9) * step;
  char buf[64];
  std::snprintf(buf, sizeof buf, "%.*f", decimals, floored);
  return buf;
}

// ---------------------------------------------------------------------------
// The engine's Binance wiring, minus ZMQ and persistence: REST, the user data
// stream, the ws-fapi trade connection and the EMS, driven by one busy-poll
// loop exactly as engine_main drives them.
// ---------------------------------------------------------------------------
class Harness {
 public:
  Harness() {
    exchange_.name = "binance";
    exchange_.env = "testnet";  // never production, whatever the key is
    exchange_.api_key = env("AXON_BINANCE_API_KEY");
    exchange_.api_secret = env("AXON_BINANCE_API_SECRET");
    config_.exchanges["binance"] = exchange_;
    config_.websocket.request_timeout_seconds = 15;

    tls_.use_default_trust_store();
    http_ = std::make_unique<axon::net::HttpClient>(&tls_);
    rest_ = axon::oms::make_venue_rest(exchange_, http_.get());
    ems_ = std::make_unique<axon::ems::EmsService>(config_, http_.get());
    ems_->set_order_lookup([this](const std::string& id) -> std::optional<Order> {
      const auto it = known_.find(id);
      if (it == known_.end()) return std::nullopt;
      return it->second;
    });

    axon::oms::VenueSessionHandlers feed;
    feed.on_order = [this](const axon::transport::OrderUpdateMsg& m) {
      auto o = axon::transport::decode_order_update(m);
      known_[o.order_id] = o;
      updates_.push_back(std::move(o));
    };
    feed.on_fill = [this](const axon::transport::FillMsg& m) {
      fills_.push_back(axon::transport::decode_fill(m));
    };
    feed.on_account = [this](const axon::models::AccountSummary&) { ++account_updates_; };
    feed.on_error = [this](const std::string& e) { errors_.push_back("feed: " + e); };
    feed_ = axon::oms::make_venue_session(exchange_, config_.websocket, config_.runtime,
                                          &tls_, rest_.get(), std::move(feed));

    axon::oms::VenueSessionHandlers trade;
    trade.on_rpc_reply = [this](std::int64_t id, bool ok, std::string_view payload,
                                std::string_view error) {
      replies_.push_back(std::string(payload));
      ems_->on_rpc_reply("binance", id, ok, payload, error);
    };
    trade.on_error = [this](const std::string& e) { errors_.push_back("trade: " + e); };
    trade_ = axon::oms::make_trade_session(exchange_, config_.websocket, config_.runtime,
                                           &tls_, std::move(trade));

    ems_->register_session("binance", feed_.get());
    ems_->register_trade_session("binance", trade_.get());
    feed_->start();
    trade_->start();
  }

  ~Harness() {
    trade_->stop();
    feed_->stop();
  }

  void poll_once() {
    feed_->poll();
    trade_->poll();
    http_->poll();
    ems_->poll();
  }

  bool pump(const std::function<bool()>& done, std::chrono::milliseconds timeout = 20s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!done()) {
      if (std::chrono::steady_clock::now() > deadline) return false;
      poll_once();
    }
    return true;
  }

  void pump_for(std::chrono::milliseconds d) { pump([] { return false; }, d); }

  bool live() const { return feed_->live() && trade_->live(); }

  // --- order entry, synchronously --------------------------------------------
  OrderResult place(const OrderRequest& req) {
    std::optional<OrderResult> out;
    ems_->place_order("binance", req, [&](const OrderResult& r) { out = r; });
    pump([&] { return out.has_value(); });
    return out.value_or(OrderResult{false, std::nullopt, "no reply"});
  }

  std::pair<bool, std::string> cancel(const std::string& order_id) {
    std::optional<std::pair<bool, std::string>> out;
    ems_->cancel_order("binance", order_id,
                       [&](bool ok, const std::string& e) { out = std::make_pair(ok, e); });
    pump([&] { return out.has_value(); });
    return out.value_or(std::make_pair(false, std::string("no reply")));
  }

  OrderResult modify(const std::string& order_id, std::optional<Qty> amount,
                     std::optional<Price> price) {
    std::optional<OrderResult> out;
    ems_->modify_order("binance", order_id, amount, price,
                       [&](const OrderResult& r) { out = r; });
    pump([&] { return out.has_value(); });
    return out.value_or(OrderResult{false, std::nullopt, "no reply"});
  }

  // The venue's order id for the order placed with client id `label`. The EMS
  // does not surface it (see BinanceTestnet.PlaceResultCarriesTheOrderId), so
  // it is read from the raw ws-fapi reply.
  std::optional<std::string> order_id_for(const std::string& label) {
    for (auto it = replies_.rbegin(); it != replies_.rend(); ++it) {
      const auto j = Json::parse(*it, nullptr, false);
      if (j.is_discarded() || !j.contains("result") || !j["result"].is_object()) continue;
      const auto& r = j["result"];
      if (r.value("clientOrderId", "") == label && r.contains("orderId")) {
        auto id = std::to_string(r["orderId"].get<std::int64_t>());
        created_.push_back(id);
        return id;
      }
    }
    return std::nullopt;
  }

  // Cancels every order this run created that is still working. Runs at suite
  // teardown, so a test that fails half-way never leaves an order resting on
  // the account. Orders this run did not create are never touched.
  void cancel_leftovers() {
    pump_for(1s);  // let in-flight updates land first
    for (const auto& id : created_) {
      const auto o = latest(id);
      if (o && (o->status == OrderStatus::kOpen || o->status == OrderStatus::kPartiallyFilled)) {
        cancel(id);
        await_status(id, OrderStatus::kCancelled, 5s);
      }
    }
  }

  // Latest feed state for an order, once one has arrived.
  std::optional<Order> latest(const std::string& order_id) const {
    for (auto it = updates_.rbegin(); it != updates_.rend(); ++it)
      if (it->order_id == order_id) return *it;
    return std::nullopt;
  }

  bool await_status(const std::string& order_id, OrderStatus status,
                    std::chrono::milliseconds timeout = 20s) {
    return pump([&] {
      auto o = latest(order_id);
      return o.has_value() && o->status == status;
    }, timeout);
  }

  // --- REST, synchronously ---------------------------------------------------
  std::pair<std::vector<Order>, std::string> open_orders() {
    std::optional<std::pair<std::vector<Order>, std::string>> out;
    rest_->get_open_orders("", [&](std::vector<Order> o, const std::string& e) {
      out = std::make_pair(std::move(o), e);
    });
    pump([&] { return out.has_value(); });
    return out.value_or(std::make_pair(std::vector<Order>{}, std::string("no reply")));
  }

  std::pair<std::vector<axon::models::Position>, std::string> positions() {
    std::optional<std::pair<std::vector<axon::models::Position>, std::string>> out;
    rest_->get_positions("", [&](std::vector<axon::models::Position> p, const std::string& e) {
      out = std::make_pair(std::move(p), e);
    });
    pump([&] { return out.has_value(); });
    return out.value_or(std::make_pair(std::vector<axon::models::Position>{},
                                       std::string("no reply")));
  }

  std::pair<std::vector<axon::models::Fill>, std::string> user_trades(std::int64_t from_ms) {
    std::optional<std::pair<std::vector<axon::models::Fill>, std::string>> out;
    rest_->get_user_trades(kSymbol, from_ms, now_ms() + 1000,
                           [&](std::vector<axon::models::Fill> f, const std::string& e) {
                             out = std::make_pair(std::move(f), e);
                           });
    pump([&] { return out.has_value(); });
    return out.value_or(std::make_pair(std::vector<axon::models::Fill>{},
                                       std::string("no reply")));
  }

  std::pair<std::optional<Order>, std::string> get_order(const Order& order) {
    std::optional<std::pair<std::optional<Order>, std::string>> out;
    rest_->get_order(order, [&](std::optional<Order> o, const std::string& e) {
      out = std::make_pair(std::move(o), e);
    });
    pump([&] { return out.has_value(); });
    return out.value_or(std::make_pair(std::optional<Order>{}, std::string("no reply")));
  }

  std::optional<axon::models::Ticker> ticker() {
    std::optional<axon::models::Ticker> out;
    bool done = false;
    rest_->get_ticker(kSymbol, [&](std::optional<axon::models::Ticker> t, const std::string&) {
      out = t;
      done = true;
    });
    pump([&] { return done; });
    return out;
  }

  // Signed position size for kSymbol, in the account's one-way mode.
  std::optional<double> position_size() {
    auto [ps, err] = positions();
    if (!err.empty()) return std::nullopt;
    for (auto& p : ps)
      if (p.instrument == kSymbol)
        return (p.direction == "sell" ? -1.0 : 1.0) * p.size.to_double();
    return 0.0;
  }

  // A post-only buy far enough below the touch that it rests, inside the
  // venue's PERCENT_PRICE band (0.95x).
  OrderRequest resting_buy(const std::string& label, double discount = 0.97) {
    auto t = ticker();
    EXPECT_TRUE(t.has_value());
    const double px = t ? t->best_bid_price.to_double() * discount : 0;
    OrderRequest r;
    r.instrument = kSymbol;
    r.side = OrderSide::kBuy;
    r.order_type = OrderType::kLimit;
    r.post_only = true;
    r.label = label;
    r.price = Price::from_string(to_step(px, 0.1, 1));
    // MIN_NOTIONAL is 50 USDT; 80 leaves room after the discount.
    r.amount = *Qty::from_string(to_step(80.0 / std::max(px, 1.0) + 0.0001, 0.0001, 4));
    return r;
  }

  const std::vector<Order>& updates() const { return updates_; }
  const std::vector<axon::models::Fill>& fills() const { return fills_; }
  const std::vector<std::string>& errors() const { return errors_; }
  int account_updates() const { return account_updates_; }
  std::map<std::string, Order>& known() { return known_; }

 private:
  axon::ExchangeConfig exchange_;
  axon::Config config_;
  axon::net::TlsContext tls_;
  std::unique_ptr<axon::net::HttpClient> http_;
  std::unique_ptr<axon::oms::VenueRest> rest_;
  std::unique_ptr<axon::ems::EmsService> ems_;
  std::unique_ptr<axon::oms::VenueSession> feed_;
  std::unique_ptr<axon::oms::VenueSession> trade_;

  std::vector<Order> updates_;
  std::vector<axon::models::Fill> fills_;
  std::vector<std::string> replies_;
  std::vector<std::string> created_;
  std::vector<std::string> errors_;
  std::map<std::string, Order> known_;
  int account_updates_ = 0;
};

// Binance's REST reads trail its WebSocket by a moment: an order the feed has
// just reported cancelled can still be listed open, and one just placed can be
// "unknown" to a lookup. Retries a REST check a bounded number of times,
// spaced out -- signed reads carry request weight, and a tight loop of them is
// how an IP gets banned.
bool eventually(Harness* h, const std::function<bool()>& check, int attempts = 5) {
  for (int i = 0; i < attempts; ++i) {
    if (i > 0) h->pump_for(500ms);
    if (check()) return true;
  }
  return false;
}

// One connected harness for the whole suite: connecting is the slow part, and
// it is also what the first test asserts on.
class BinanceTestnet : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    if (env("AXON_TESTNET") != "1" || env("AXON_BINANCE_API_KEY").empty() ||
        env("AXON_BINANCE_API_SECRET").empty())
      return;
    axon::util::LoggingOptions logging;
    logging.level = axon::util::LogLevel::kWarn;
    axon::util::init_logging(logging);
    h_ = new Harness();
    h_->pump([] { return h_->live(); }, 30s);
  }

  static void TearDownTestSuite() {
    if (h_ != nullptr) h_->cancel_leftovers();
    delete h_;
    h_ = nullptr;
  }

  void SetUp() override {
    if (h_ == nullptr)
      GTEST_SKIP() << "set AXON_TESTNET=1 and AXON_BINANCE_API_KEY/_SECRET to run";
    ASSERT_TRUE(h_->live() || h_->pump([] { return h_->live(); }, 30s))
        << "sessions not live; errors: " << join(h_->errors());
  }

  static std::string join(const std::vector<std::string>& v) {
    std::string s;
    for (auto& x : v) s += x + "; ";
    return s;
  }

  // Places a resting order and returns its venue id, or fails the test.
  std::string place_resting(const std::string& label, double discount = 0.97) {
    const auto req = h_->resting_buy(label, discount);
    const auto r = h_->place(req);
    EXPECT_TRUE(r.success) << r.error;
    const auto id = h_->order_id_for(label);
    EXPECT_TRUE(id.has_value()) << "no ws-fapi reply carried clientOrderId " << label;
    return id.value_or("");
  }

  static inline Harness* h_ = nullptr;
};

// ===========================================================================
// Connectivity and authentication
// ===========================================================================

// The listenKey comes from a REST POST with the API key header; the user data
// stream is then opened with it. ws-fapi needs no login at all. Both reaching
// kLive is the first proof the endpoints and the key are right.
TEST_F(BinanceTestnet, FeedAndTradeSessionsGoLive) {
  EXPECT_TRUE(h_->live());
  EXPECT_TRUE(h_->errors().empty()) << join(h_->errors());
}

// Every signed REST read the reconcilers depend on. A signing mistake shows up
// here as -1022.
TEST_F(BinanceTestnet, SignedRestReadsAreAccepted) {
  auto [orders, oerr] = h_->open_orders();
  EXPECT_EQ(oerr, "");
  auto [positions, perr] = h_->positions();
  EXPECT_EQ(perr, "");
  EXPECT_FALSE(positions.empty());
  auto [trades, terr] = h_->user_trades(now_ms() - 3600'000);
  EXPECT_EQ(terr, "");
}

TEST_F(BinanceTestnet, TickerHasATwoSidedBook) {
  auto t = h_->ticker();
  ASSERT_TRUE(t.has_value());
  EXPECT_GT(t->best_bid_price.to_double(), 0);
  EXPECT_GT(t->best_ask_price.to_double(), t->best_bid_price.to_double());
}

// ===========================================================================
// Order lifecycle: place -> feed NEW -> modify -> cancel -> feed CANCELED
// ===========================================================================

TEST_F(BinanceTestnet, PlacedOrderIsAcceptedAndReportedOnTheFeed) {
  const auto label = fresh_label("p");
  const auto req = h_->resting_buy(label);
  const auto id = place_resting(label);
  ASSERT_FALSE(id.empty());

  ASSERT_TRUE(h_->await_status(id, OrderStatus::kOpen)) << "no NEW on the user data stream";
  const auto o = *h_->latest(id);
  EXPECT_EQ(o.instrument, kSymbol);
  EXPECT_EQ(o.side, OrderSide::kBuy);
  EXPECT_EQ(o.order_type, OrderType::kLimit);
  ASSERT_TRUE(o.price.has_value());
  EXPECT_EQ(o.price->raw(), req.price->raw());
  EXPECT_EQ(o.amount.raw(), req.amount.raw());
  EXPECT_EQ(o.filled_amount.raw(), 0);

  // The reconciliation snapshot sees it too, once REST catches up.
  EXPECT_TRUE(eventually(h_, [&] {
    auto [orders, err] = h_->open_orders();
    for (auto& x : orders)
      if (x.order_id == id) return true;
    return false;
  })) << "openOrders does not list " << id;

  EXPECT_TRUE(h_->cancel(id).first);
  EXPECT_TRUE(h_->await_status(id, OrderStatus::kCancelled));
}

TEST_F(BinanceTestnet, CancelIsAcceptedAndReportedOnTheFeed) {
  const auto id = place_resting(fresh_label("c"));
  ASSERT_FALSE(id.empty());
  ASSERT_TRUE(h_->await_status(id, OrderStatus::kOpen));

  auto [ok, err] = h_->cancel(id);
  EXPECT_TRUE(ok) << err;
  ASSERT_TRUE(h_->await_status(id, OrderStatus::kCancelled));

  EXPECT_TRUE(eventually(h_, [&] {
    auto [orders, oerr] = h_->open_orders();
    if (!oerr.empty()) return false;
    for (auto& x : orders)
      if (x.order_id == id) return false;
    return true;
  })) << "cancelled order still listed open";
}

// order.modify needs price AND quantity; the EMS fills the omitted one from
// the order it already knows.
TEST_F(BinanceTestnet, ModifyMovesThePriceAndKeepsTheQuantity) {
  const auto id = place_resting(fresh_label("m"));
  ASSERT_FALSE(id.empty());
  ASSERT_TRUE(h_->await_status(id, OrderStatus::kOpen));
  const auto before = *h_->latest(id);

  const auto new_price = *Price::from_string(to_step(before.price->to_double() - 10.0, 0.1, 1));
  const auto r = h_->modify(id, std::nullopt, new_price);
  EXPECT_TRUE(r.success) << r.error;

  ASSERT_TRUE(h_->pump([&] {
    auto o = h_->latest(id);
    return o && o->price && o->price->raw() == new_price.raw();
  })) << "the feed never reported the amended price";
  EXPECT_EQ(h_->latest(id)->amount.raw(), before.amount.raw());

  EXPECT_TRUE(h_->cancel(id).first);
  EXPECT_TRUE(h_->await_status(id, OrderStatus::kCancelled));
}

// GTX is Binance's post-only: an order that would take liquidity must never
// fill. The venue either rejects the request (-5022, what testnet does) or
// accepts and expires it; either way nothing trades.
TEST_F(BinanceTestnet, PostOnlyThatWouldCrossNeverFills) {
  const auto label = fresh_label("x");
  auto t = h_->ticker();
  ASSERT_TRUE(t.has_value());
  auto req = h_->resting_buy(label);
  req.price = *Price::from_string(to_step(t->best_ask_price.to_double() * 1.002, 0.1, 1));
  req.amount = *Qty::from_string(to_step(80.0 / t->best_ask_price.to_double() + 0.0001, 0.0001, 4));

  const size_t fills_before = h_->fills().size();
  const auto r = h_->place(req);
  if (!r.success) {
    EXPECT_NE(r.error.find("Post Only"), std::string::npos) << r.error;
  } else {
    const auto id = h_->order_id_for(label);
    ASSERT_TRUE(id.has_value());
    ASSERT_TRUE(h_->await_status(*id, OrderStatus::kCancelled));
    EXPECT_EQ(h_->latest(*id)->filled_amount.raw(), 0);
  }
  h_->pump_for(1s);
  EXPECT_EQ(h_->fills().size(), fills_before);
}

// ===========================================================================
// Rejections
// ===========================================================================

TEST_F(BinanceTestnet, VenueRejectionCarriesTheVenuesReason) {
  auto req = h_->resting_buy(fresh_label("r"));
  req.amount = *Qty::from_string("0.00001");  // below LOT_SIZE stepSize 0.0001
  const auto r = h_->place(req);
  EXPECT_FALSE(r.success);
  EXPECT_FALSE(r.error.empty());
  EXPECT_NE(r.error, "the venue did not reply in time");
}

// PERCENT_PRICE (multiplierUp 1.05) caps how far ABOVE the mark a buy may be.
// It does not bound a buy from below -- a deep bid is accepted.
TEST_F(BinanceTestnet, BuyAboveThePercentPriceCapIsRejected) {
  auto t = h_->ticker();
  ASSERT_TRUE(t.has_value());
  auto req = h_->resting_buy(fresh_label("b"));
  req.price = *Price::from_string(to_step(t->best_ask_price.to_double() * 1.2, 0.1, 1));
  const auto r = h_->place(req);
  EXPECT_FALSE(r.success);
  EXPECT_FALSE(r.error.empty());
  EXPECT_NE(r.error, "the venue did not reply in time");
}

TEST_F(BinanceTestnet, CancellingAnUnknownOrderFails) {
  Order fake;
  fake.order_id = "1";
  fake.instrument = kSymbol;
  h_->known()["1"] = fake;
  auto [ok, err] = h_->cancel("1");
  EXPECT_FALSE(ok);
  EXPECT_FALSE(err.empty());
}

// ===========================================================================
// Fills
// ===========================================================================

// The one test that trades: buys the minimum at market and sells it straight
// back, so the account's position is where it started.
TEST_F(BinanceTestnet, MarketOrderFillIsReportedAndReconcilable) {
  const auto start_ms = now_ms() - 5000;
  // REST checks are EXPECTs, not ASSERTs, so a broken snapshot path does not
  // hide whether the feed side works.
  const auto before = h_->position_size();
  EXPECT_TRUE(before.has_value()) << "positionRisk unreadable";

  auto t = h_->ticker();
  ASSERT_TRUE(t.has_value());
  const auto qty = *Qty::from_string(
      to_step(60.0 / t->best_ask_price.to_double() + 0.0001, 0.0001, 4));

  auto market = [&](OrderSide side, const std::string& label) {
    OrderRequest r;
    r.instrument = kSymbol;
    r.side = side;
    r.order_type = OrderType::kMarket;
    r.amount = qty;
    r.label = label;
    return r;
  };

  const auto buy_label = fresh_label("f");
  const size_t fills_before = h_->fills().size();
  const auto r = h_->place(market(OrderSide::kBuy, buy_label));
  ASSERT_TRUE(r.success) << r.error;
  const auto id = h_->order_id_for(buy_label);
  ASSERT_TRUE(id.has_value());

  // Restore the position whatever happens below.
  struct Unwind {
    Harness* h;
    std::function<OrderRequest()> make;
    ~Unwind() { h->place(make()); h->pump_for(2s); }
  } unwind{h_, [&] { return market(OrderSide::kSell, fresh_label("u")); }};

  ASSERT_TRUE(h_->await_status(*id, OrderStatus::kFilled));
  ASSERT_TRUE(h_->pump([&] { return h_->fills().size() > fills_before; }));

  Qty filled{};
  std::vector<std::string> trade_ids;
  for (size_t i = fills_before; i < h_->fills().size(); ++i) {
    const auto& f = h_->fills()[i];
    if (f.order_id != *id) continue;
    EXPECT_EQ(f.instrument, kSymbol);
    EXPECT_EQ(f.side, OrderSide::kBuy);
    EXPECT_GT(f.price.to_double(), 0);
    EXPECT_FALSE(f.trade_id.empty());
    EXPECT_EQ(f.liquidity, axon::models::Liquidity::kTaker);
    filled = filled + f.amount;
    trade_ids.push_back(f.trade_id);
  }
  EXPECT_EQ(filled.raw(), qty.raw()) << "fills on the feed do not add up to the order";
  EXPECT_EQ(h_->latest(*id)->filled_amount.raw(), qty.raw());

  // The fill reconciler's source agrees on the trade ids -- the dedup key that
  // stops a recovered fill being counted twice.
  // userTrades can lag the feed by a moment. Retry a bounded number of times,
  // spaced out: it carries request weight, and a tight loop of signed requests
  // is how an IP gets banned (HTTP 418).
  bool listed = false;
  std::string trades_error;
  for (int attempt = 0; attempt < 8 && !listed; ++attempt) {
    if (attempt > 0) h_->pump_for(1s);
    auto [trades, err] = h_->user_trades(start_ms);
    trades_error = err;
    if (!err.empty()) break;  // a failing endpoint will not start working on retry
    size_t matched = 0;
    for (auto& tr : trades)
      for (auto& tid : trade_ids) matched += (tr.trade_id == tid && tr.order_id == *id);
    listed = matched == trade_ids.size();
  }
  EXPECT_TRUE(listed) << "userTrades does not list the feed's trade ids: " << trades_error;

  // ACCOUNT_UPDATE arrives with the position change.
  EXPECT_TRUE(h_->pump([&] { return h_->account_updates() > 0; }, 10s));

  const auto after = h_->position_size();
  EXPECT_TRUE(after.has_value()) << "positionRisk unreadable";
  if (before && after) {
    EXPECT_NEAR(*after - *before, qty.to_double(), 1e-9);
  }
}

// ===========================================================================
// Client order ids, idempotency and single-order lookup
// ===========================================================================

// The acceptance carries the venue's order, so a strategy can tie what it
// placed to what it later sees.
TEST_F(BinanceTestnet, PlaceResultCarriesTheOrderId) {
  const auto label = fresh_label("g");
  const auto r = h_->place(h_->resting_buy(label));
  ASSERT_TRUE(r.success) << r.error;
  const auto id = h_->order_id_for(label);
  ASSERT_TRUE(r.order.has_value()) << "OrderResult carries no order";
  EXPECT_EQ(r.order->order_id, id.value_or(""));
  EXPECT_EQ(r.order->instrument, kSymbol);
  EXPECT_EQ(r.order->status, OrderStatus::kOpen);
  if (id) {
    h_->cancel(*id);
    h_->await_status(*id, OrderStatus::kCancelled);
  }
}

// With no label, the venue is given our internal_order_id and echoes it on
// every update -- the handle that resolves an order whose reply was lost.
TEST_F(BinanceTestnet, FeedUpdatesCarryTheInternalOrderId) {
  auto req = h_->resting_buy("");
  req.label.reset();
  const auto r = h_->place(req);
  ASSERT_TRUE(r.success) << r.error;
  const auto id = h_->order_id_for(req.internal_order_id);
  ASSERT_TRUE(id.has_value()) << "the venue did not echo the internal_order_id";
  ASSERT_TRUE(h_->await_status(*id, OrderStatus::kOpen));
  EXPECT_EQ(h_->latest(*id)->internal_order_id.value_or(""), req.internal_order_id);

  h_->cancel(*id);
  ASSERT_TRUE(h_->await_status(*id, OrderStatus::kCancelled));
  EXPECT_EQ(h_->latest(*id)->internal_order_id.value_or(""), req.internal_order_id);
}

// The lookup the order reconciler uses for orders that left the open-orders
// snapshot unseen: it must report the state, open or final.
TEST_F(BinanceTestnet, SingleOrderLookupReportsOpenAndFinalState) {
  const auto label = fresh_label("l");
  const auto id = place_resting(label);
  ASSERT_FALSE(id.empty());
  ASSERT_TRUE(h_->await_status(id, OrderStatus::kOpen));
  const auto local = *h_->latest(id);

  std::optional<Order> open;
  std::string err;
  eventually(h_, [&] {
    std::tie(open, err) = h_->get_order(local);
    return open.has_value();
  });
  ASSERT_TRUE(open.has_value()) << err;
  EXPECT_EQ(open->order_id, id);
  EXPECT_EQ(open->status, OrderStatus::kOpen);
  EXPECT_EQ(open->internal_order_id.value_or(""), label);

  ASSERT_TRUE(h_->cancel(id).first);
  ASSERT_TRUE(h_->await_status(id, OrderStatus::kCancelled));
  std::optional<Order> done;
  std::string err2;
  eventually(h_, [&] {
    std::tie(done, err2) = h_->get_order(local);
    return done.has_value() && done->status == OrderStatus::kCancelled;
  });
  ASSERT_TRUE(done.has_value()) << err2;
  EXPECT_EQ(done->status, OrderStatus::kCancelled);
}

// The retry a strategy reaches for after a timeout must not become a second
// order. The EMS refuses it before it is sent.
TEST_F(BinanceTestnet, ResubmittingTheSameRequestIsRefused) {
  const auto label = fresh_label("d");
  const auto req = h_->resting_buy(label);
  const auto first = h_->place(req);
  ASSERT_TRUE(first.success) << first.error;
  const auto id = h_->order_id_for(label);
  ASSERT_TRUE(id.has_value());

  const auto again = h_->place(req);
  EXPECT_FALSE(again.success);
  EXPECT_NE(again.error.find("duplicate internal_order_id"), std::string::npos) << again.error;

  // REST trails the feed, so wait for the order to be listed -- then it must
  // be listed exactly once.
  int with_label = 0;
  eventually(h_, [&] {
    auto [orders, err] = h_->open_orders();
    with_label = 0;
    for (auto& o : orders) with_label += o.internal_order_id.value_or("") == label;
    return err.empty() && with_label > 0;
  });
  EXPECT_EQ(with_label, 1) << "the venue holds more than one order for one request";

  h_->cancel(*id);
  h_->await_status(*id, OrderStatus::kCancelled);
}

}  // namespace
