// The exchange's request limits, counted on our side.
//
// Before this the engine had only the risk layer's per-strategy rate, which
// knows nothing about the exchange: several strategies each inside their own
// limit could together run the account into Binance's 429, and then its 418
// IP ban. These pin the budget, the headroom that keeps cancels possible, what
// each exchange's replies say about usage, and the EMS refusing locally.

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "axon/config.h"
#include "axon/ems/ems_service.h"
#include "axon/ems/exchange_ops.h"
#include "axon/ems/rate_limiter.h"
#include "axon/oms/exchange_session.h"

namespace {

using axon::Config;
using axon::ExchangeConfig;
using axon::RateLimitRule;
using axon::core::Price;
using axon::core::Qty;
using axon::ems::EmsService;
using axon::ems::ExchangeState;
using axon::ems::OrderResult;
using axon::ems::RateLimiter;
using axon::ems::RateLimitFeedback;
using Kind = axon::ems::RateLimiter::Kind;

// ===========================================================================
// The limiter
// ===========================================================================

TEST(RateLimiter, NewOrdersStopAtTheHeadroom) {
  RateLimiter l({{10.0, 10}}, 0.8);
  for (int i = 0; i < 8; ++i) {
    ASSERT_FALSE(l.check(Kind::kNewOrder, 0.0).has_value()) << "refused at " << i;
    l.record(0.0);
  }
  const auto refused = l.check(Kind::kNewOrder, 0.0);
  ASSERT_TRUE(refused.has_value());
  EXPECT_NE(refused->find("8 requests in the last 10s against a limit of 10"),
            std::string::npos)
      << *refused;
}

// When quoting has spent the budget, getting out must still be possible.
TEST(RateLimiter, CancelsMayUseTheWholeBudget) {
  RateLimiter l({{10.0, 10}}, 0.8);
  for (int i = 0; i < 8; ++i) l.record(0.0);
  ASSERT_TRUE(l.check(Kind::kNewOrder, 0.0).has_value());
  EXPECT_FALSE(l.check(Kind::kCancel, 0.0).has_value());
  l.record(0.0);
  l.record(0.0);
  EXPECT_TRUE(l.check(Kind::kCancel, 0.0).has_value()) << "10 of 10 is the limit";
}

TEST(RateLimiter, TheWindowSlides) {
  RateLimiter l({{10.0, 10}}, 0.8);
  for (int i = 0; i < 8; ++i) l.record(static_cast<double>(i));  // t = 0..7
  EXPECT_TRUE(l.check(Kind::kNewOrder, 9.9).has_value());
  // At t=10 the request from t=0 has left the window.
  EXPECT_FALSE(l.check(Kind::kNewOrder, 10.0).has_value());
}

TEST(RateLimiter, EveryWindowMustHaveRoom) {
  RateLimiter l({{1.0, 100}, {60.0, 10}}, 1.0);
  for (int i = 0; i < 10; ++i) l.record(static_cast<double>(i) * 2.0);
  // The 1s window is empty by t=30; the minute window is full.
  EXPECT_TRUE(l.check(Kind::kNewOrder, 30.0).has_value());
}

// Another process on the same account spends the same budget.
TEST(RateLimiter, TheExchangesCountTightensOursForOneWindow) {
  RateLimiter l({{10.0, 300}}, 0.8);
  RateLimitFeedback f;
  f.usage.push_back({10.0, 250, 300});
  l.observe(f, 100.0);
  EXPECT_TRUE(l.check(Kind::kNewOrder, 101.0).has_value());
  // The report is good for one window, after which our own count rules again.
  EXPECT_FALSE(l.check(Kind::kNewOrder, 110.5).has_value());
}

TEST(RateLimiter, TheExchangesLimitReplacesTheConfiguredOne) {
  RateLimiter l({{10.0, 300}}, 1.0);
  RateLimitFeedback f;
  f.usage.push_back({10.0, 0, 5});  // a lower tier than configured
  l.observe(f, 0.0);
  for (int i = 0; i < 5; ++i) l.record(0.0);
  EXPECT_TRUE(l.check(Kind::kNewOrder, 0.5).has_value());
}

TEST(RateLimiter, AThrottlePausesEverythingUntilRetryAfter) {
  RateLimiter l({{10.0, 300}}, 0.8);
  RateLimitFeedback f;
  f.throttled = true;
  f.retry_after_seconds = 5.0;
  l.observe(f, 100.0);
  EXPECT_TRUE(l.check(Kind::kNewOrder, 104.9).has_value());
  EXPECT_TRUE(l.check(Kind::kCancel, 104.9).has_value()) << "a 429 is about every request";
  EXPECT_FALSE(l.check(Kind::kNewOrder, 105.0).has_value());
}

TEST(RateLimiter, AThrottleWithoutADurationStillPauses) {
  RateLimiter l({{10.0, 300}}, 0.8);
  RateLimitFeedback f;
  f.throttled = true;
  l.observe(f, 0.0);
  EXPECT_TRUE(l.check(Kind::kNewOrder, 0.5).has_value());
  EXPECT_FALSE(l.check(Kind::kNewOrder, 1.0).has_value());
}

TEST(RateLimiter, UtilisationIsTheBusiestWindow) {
  RateLimiter l({{1.0, 10}, {60.0, 100}}, 1.0);
  for (int i = 0; i < 5; ++i) l.record(0.0);
  EXPECT_DOUBLE_EQ(l.utilisation(0.0), 0.5);
}

// ===========================================================================
// What each exchange's replies say
// ===========================================================================

RateLimitFeedback read(const char* exchange, std::string_view payload,
                       std::int64_t now_ms = 1'700'000'000'000) {
  ExchangeState state;
  RateLimitFeedback out;
  const auto* ops = axon::ems::ops_for(exchange);
  EXPECT_NE(ops, nullptr);
  EXPECT_NE(ops->read_rate_limits, nullptr) << exchange;
  ops->read_rate_limits(payload, state, now_ms, out);
  return out;
}

TEST(ExchangeRateLimits, EveryExchangeHasDefaults) {
  for (const char* name : {"binance", "bybit", "okx", "deribit"}) {
    const auto* ops = axon::ems::ops_for(name);
    ASSERT_NE(ops, nullptr) << name;
    EXPECT_FALSE(ops->default_rate_limits.empty()) << name;
  }
}

TEST(ExchangeRateLimits, BinanceReadsTheOrderCountersAndSkipsRequestWeight) {
  const auto f = read("binance", R"({"id":"1000001","status":200,"result":{"orderId":1},
"rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","intervalNum":1,"limit":2400,"count":7},
{"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,"limit":300,"count":12},
{"rateLimitType":"ORDERS","interval":"MINUTE","intervalNum":1,"limit":1200,"count":40}]})");
  EXPECT_FALSE(f.throttled);
  ASSERT_EQ(f.usage.size(), 2u);
  EXPECT_DOUBLE_EQ(f.usage[0].window_seconds, 10.0);
  EXPECT_EQ(f.usage[0].used, 12);
  EXPECT_EQ(f.usage[0].limit, 300);
  EXPECT_DOUBLE_EQ(f.usage[1].window_seconds, 60.0);
  EXPECT_EQ(f.usage[1].used, 40);
}

TEST(ExchangeRateLimits, BinanceA429CarriesItsRetryAfter) {
  const auto f = read("binance", R"({"id":"1000002","status":429,
"error":{"code":-1015,"msg":"Too many new orders","data":{"retryAfter":1700000003000}},
"rateLimits":[]})",
                      1'700'000'000'000);
  EXPECT_TRUE(f.throttled);
  EXPECT_DOUBLE_EQ(f.retry_after_seconds, 3.0);
}

TEST(ExchangeRateLimits, BinanceA418IsAThrottleToo) {
  EXPECT_TRUE(read("binance", R"({"id":"1","status":418,"error":{"code":-1003}})").throttled);
}

TEST(ExchangeRateLimits, BybitReadsTheQuotaHeader) {
  const auto f = read("bybit", R"({"reqId":"1000001","retCode":0,"retMsg":"OK",
"op":"order.create","data":{"orderId":"x"},
"header":{"X-Bapi-Limit":"10","X-Bapi-Limit-Status":"3","X-Bapi-Limit-Reset-Timestamp":"1700000000500"}})");
  ASSERT_EQ(f.usage.size(), 1u);
  EXPECT_DOUBLE_EQ(f.usage[0].window_seconds, 1.0);
  EXPECT_EQ(f.usage[0].limit, 10);
  EXPECT_EQ(f.usage[0].used, 7);
  EXPECT_FALSE(f.throttled);
}

TEST(ExchangeRateLimits, BybitTooManyVisitsPausesUntilTheReset) {
  const auto f = read("bybit", R"({"reqId":"1","retCode":10006,"retMsg":"Too many visits!",
"op":"order.create","header":{"X-Bapi-Limit":"10","X-Bapi-Limit-Status":"0",
"X-Bapi-Limit-Reset-Timestamp":"1700000000750"}})",
                      1'700'000'000'000);
  EXPECT_TRUE(f.throttled);
  EXPECT_DOUBLE_EQ(f.retry_after_seconds, 0.75);
}

TEST(ExchangeRateLimits, OkxThrottleCodeAtEitherLevel) {
  EXPECT_TRUE(read("okx", R"({"id":"1","op":"order","code":"50011","msg":"","data":[]})")
                  .throttled);
  EXPECT_TRUE(read("okx", R"({"id":"1","op":"order","code":"1","msg":"",
"data":[{"ordId":"","clOrdId":"a","sCode":"50011","sMsg":"Rate limit reached"}]})")
                  .throttled);
  EXPECT_FALSE(read("okx", R"({"id":"1","op":"order","code":"1","msg":"",
"data":[{"ordId":"","clOrdId":"a","sCode":"51008","sMsg":"Insufficient balance"}]})")
                   .throttled);
}

TEST(ExchangeRateLimits, DeribitTooManyRequests) {
  EXPECT_TRUE(read("deribit", R"({"jsonrpc":"2.0","id":1000001,
"error":{"message":"too_many_requests","code":10028}})")
                  .throttled);
  EXPECT_FALSE(read("deribit", R"({"jsonrpc":"2.0","id":1000001,
"error":{"message":"not_enough_funds","code":10009}})")
                   .throttled);
}

// ===========================================================================
// The EMS refuses locally
// ===========================================================================

class LiveSession : public axon::oms::ExchangeSession {
 public:
  LiveSession() : ExchangeSession(cfg(), {}, {}, nullptr, {}) {}
  bool live() const noexcept override { return true; }
  bool send_raw(std::string_view payload) override {
    sent.emplace_back(payload);
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
  static ExchangeConfig cfg() {
    ExchangeConfig c;
    c.name = "binance";
    return c;
  }
};

Config binance_with_limit(int limit) {
  Config c;
  ExchangeConfig e;
  e.name = "binance";
  e.api_key = "key";
  e.api_secret = "secret";
  e.rate_limits = {RateLimitRule{10.0, limit}};
  c.exchanges["binance"] = e;
  return c;
}

axon::models::OrderRequest order(int n) {
  axon::models::OrderRequest r;
  r.instrument = "BTCUSDT";
  r.side = axon::models::OrderSide::kBuy;
  r.order_type = axon::models::OrderType::kLimit;
  r.amount = *Qty::from_string("0.002");
  r.price = Price::from_string("60000");
  r.internal_order_id = "rl" + std::to_string(n);
  return r;
}

TEST(EmsRateLimit, NewOrdersAreRefusedLocallyButCancelsStillGoOut) {
  EmsService ems(binance_with_limit(5), nullptr);
  LiveSession session;
  ems.register_trade_session("binance", &session);
  ems.set_order_lookup([](const std::string& id) {
    axon::models::Order o;
    o.order_id = id;
    o.instrument = "BTCUSDT";
    return std::optional<axon::models::Order>(o);
  });

  // 80% of 5 is 4.
  std::vector<OrderResult> results;
  for (int i = 0; i < 5; ++i) {
    ems.place_order("binance", order(i), [&](const OrderResult& r) { results.push_back(r); });
  }
  EXPECT_EQ(session.sent.size(), 4u);
  ASSERT_EQ(results.size(), 1u) << "only the refusal answers synchronously";
  EXPECT_FALSE(results[0].success);
  EXPECT_NE(results[0].error.find("exchange rate limit"), std::string::npos)
      << results[0].error;

  bool cancel_refused = false;
  ems.cancel_order("binance", "123", [&](bool ok, const std::string&) {
    cancel_refused = !ok;
  });
  EXPECT_FALSE(cancel_refused);
  EXPECT_EQ(session.sent.size(), 5u) << "the cancel went out";
  EXPECT_GT(ems.rate_limit_utilisation("binance"), 0.99);
}

TEST(EmsRateLimit, AThrottleReplyStopsTheNextOrder) {
  EmsService ems(binance_with_limit(300), nullptr);
  LiveSession session;
  ems.register_trade_session("binance", &session);

  ems.place_order("binance", order(1), [](const OrderResult&) {});
  ASSERT_EQ(session.sent.size(), 1u);
  // An unrelated id: the throttle must be heard whoever it answers.
  ems.on_rpc_reply("binance", 42, false,
                   R"({"id":"42","status":429,"error":{"code":-1015,"msg":"Too many new orders"}})",
                   "Too many new orders");

  OrderResult result;
  ems.place_order("binance", order(2), [&](const OrderResult& r) { result = r; });
  EXPECT_EQ(session.sent.size(), 1u);
  EXPECT_NE(result.error.find("throttled"), std::string::npos) << result.error;
}

}  // namespace
