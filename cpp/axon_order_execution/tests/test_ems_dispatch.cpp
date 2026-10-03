// Order-entry dispatch: which venue gets which treatment.
//
// The EMS used to answer that question with `exchange == "..."` in sixteen
// places. It now answers it once, from a table, and these tests pin the four
// behaviours that table encodes -- because until this file existed NOTHING
// covered EmsService at all. The suite's other 395 tests exercise the venue
// BUILDERS, which is a different layer: they prove the bytes are right, not
// that the right builder was picked.
//
// Everything here runs without a network. A session that was never started is
// not live, which is exactly the state these assertions need.

#include <gtest/gtest.h>

#include <optional>
#include <string>

#include "axon/config.h"
#include "axon/ems/ems_service.h"
#include "axon/models/order.h"
#include "axon/oms/venue_session.h"

namespace {

using axon::Config;
using axon::ExchangeConfig;
using axon::core::Price;
using axon::core::Qty;
using axon::ems::EmsService;
using axon::ems::OrderResult;
using axon::models::OrderRequest;
using axon::models::OrderSide;
using axon::models::OrderType;

// A session that can be constructed but never connects. `live()` is false, so
// order entry is always unavailable -- which is what lets these tests read the
// REASON the EMS gives without any I/O.
class StubSession : public axon::oms::VenueSession {
 public:
  explicit StubSession(std::string name)
      : VenueSession(make_config(std::move(name)), {}, {}, nullptr, {}) {}

 protected:
  bool send_authentication() override { return true; }
  bool send_subscriptions() override { return true; }
  bool handle_frame(const std::byte*, std::size_t, std::size_t) override {
    return true;
  }
  std::string host() const override { return "invalid.test"; }
  std::string path() const override { return "/"; }

 private:
  static ExchangeConfig make_config(std::string name) {
    ExchangeConfig c;
    c.name = std::move(name);
    return c;
  }
};

Config config_with(const std::string& venue) {
  Config c;
  ExchangeConfig e;
  e.name = venue;
  e.api_key = "key";
  e.api_secret = "secret";
  c.exchanges[venue] = e;
  return c;
}

OrderRequest limit_buy() {
  OrderRequest req;
  req.instrument = "BTC-PERPETUAL";
  req.side = OrderSide::kBuy;
  req.order_type = OrderType::kLimit;
  req.amount = *Qty::from_string("1");
  req.price = *Price::from_string("64000");
  return req;
}

// Collects the single result each entry point promises to deliver.
struct Capture {
  int calls = 0;
  OrderResult result;

  auto order_cb() {
    return [this](const OrderResult& r) {
      ++calls;
      result = r;
    };
  }
  auto bool_cb() {
    return [this](bool ok, const std::string& err) {
      ++calls;
      result.success = ok;
      result.error = err;
    };
  }
};

}  // namespace

// ===========================================================================
// A venue with no implementation must say so, not blame the session
// ===========================================================================

// The bug this replaces: an unknown venue fell through the builder chain and
// was reported by order_entry_unavailable(), which -- if a session happened to
// be registered and live -- answered "order entry is unavailable: session is
// live". A message that contradicts itself sends an operator to the wrong
// place.
TEST(EmsDispatch, UnknownVenueIsNamedExplicitlyOnPlace) {
  EmsService ems(Config{}, nullptr);
  Capture c;
  ems.place_order("kraken", limit_buy(), c.order_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  EXPECT_NE(c.result.error.find("no order-entry implementation"), std::string::npos)
      << c.result.error;
  EXPECT_NE(c.result.error.find("kraken"), std::string::npos) << c.result.error;
}

TEST(EmsDispatch, UnknownVenueIsNamedExplicitlyOnCancel) {
  EmsService ems(Config{}, nullptr);
  Capture c;
  ems.cancel_order("kraken", "id-1", c.bool_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  EXPECT_NE(c.result.error.find("no order-entry implementation"), std::string::npos)
      << c.result.error;
}

TEST(EmsDispatch, UnknownVenueIsNamedExplicitlyOnModify) {
  EmsService ems(Config{}, nullptr);
  Capture c;
  ems.modify_order("kraken", "id-1", std::nullopt, std::nullopt, c.order_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  EXPECT_NE(c.result.error.find("no order-entry implementation"), std::string::npos)
      << c.result.error;
}

// ===========================================================================
// Which connection an order leaves on
// ===========================================================================

// Binance and Bybit need their own trade connection. Registering one as an
// ordinary session must NOT make order entry think it has a way out -- this is
// the pair of facts that used to be two hand-copied `exchange == "binance" ||
// exchange == "bybit"` expressions, one in the selector and one in the error
// message.
TEST(EmsDispatch, BinanceIgnoresThePlainSessionForOrderEntry) {
  EmsService ems(config_with("binance"), nullptr);
  StubSession session("binance");
  ems.register_session("binance", &session);

  Capture c;
  ems.place_order("binance", limit_buy(), c.order_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  // "no ... session" proves it looked in the trade map, which is empty. If it
  // had looked at the plain map it would have found this session and named its
  // state instead.
  EXPECT_NE(c.result.error.find("no order-entry session"), std::string::npos)
      << c.result.error;
}

// Deribit and OKX send orders on the private stream they already have, so a
// plain session IS the order-entry session -- and the error names its state.
TEST(EmsDispatch, DeribitUsesThePlainSessionForOrderEntry) {
  EmsService ems(config_with("deribit"), nullptr);
  StubSession session("deribit");
  ems.register_session("deribit", &session);

  Capture c;
  ems.place_order("deribit", limit_buy(), c.order_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  EXPECT_NE(c.result.error.find("session is"), std::string::npos) << c.result.error;
  EXPECT_EQ(c.result.error.find("no order-entry session"), std::string::npos)
      << c.result.error;
}

TEST(EmsDispatch, BybitUsesTheTradeSessionForOrderEntry) {
  EmsService ems(config_with("bybit"), nullptr);
  StubSession trade("bybit");
  ems.register_trade_session("bybit", &trade);

  Capture c;
  ems.place_order("bybit", limit_buy(), c.order_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  EXPECT_NE(c.result.error.find("session is"), std::string::npos) << c.result.error;
}

// ===========================================================================
// Whether a cancel needs the order to be known locally
// ===========================================================================

// Deribit cancels by order id alone, so a cancel must reach the session even
// with no order lookup wired up at all.
TEST(EmsDispatch, DeribitCancelDoesNotNeedTheOrderLocally) {
  EmsService ems(config_with("deribit"), nullptr);
  StubSession session("deribit");
  ems.register_session("deribit", &session);

  Capture c;
  ems.cancel_order("deribit", "unknown-id", c.bool_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  // It failed on the session, not on the lookup.
  EXPECT_EQ(c.result.error.find("not known locally"), std::string::npos)
      << c.result.error;
}

// Every other venue needs the symbol, which only the order store has. Failing
// here is better than sending a request the venue will reject: it costs no
// round trip and it names the real problem.
TEST(EmsDispatch, OkxCancelRequiresTheOrderLocally) {
  EmsService ems(config_with("okx"), nullptr);
  StubSession session("okx");
  ems.register_session("okx", &session);

  Capture c;
  ems.cancel_order("okx", "unknown-id", c.bool_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  EXPECT_NE(c.result.error.find("not known locally"), std::string::npos)
      << c.result.error;
}

TEST(EmsDispatch, BinanceCancelRequiresTheOrderLocally) {
  EmsService ems(config_with("binance"), nullptr);
  Capture c;
  ems.cancel_order("binance", "unknown-id", c.bool_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_NE(c.result.error.find("not known locally"), std::string::npos)
      << c.result.error;
}

// ===========================================================================
// The contract every entry point owes its caller
// ===========================================================================

// A request that cannot be represented is rejected before it costs anything,
// and still answers exactly once.
TEST(EmsDispatch, InvalidRequestIsRejectedLocallyAndAnsweredOnce) {
  EmsService ems(config_with("deribit"), nullptr);
  Capture c;
  OrderRequest bad;  // no instrument, no amount
  ems.place_order("deribit", bad, c.order_cb());

  EXPECT_EQ(c.calls, 1);
  EXPECT_FALSE(c.result.success);
  EXPECT_FALSE(c.result.error.empty());
}

// Nothing above should have left a request waiting for a reply that will never
// come.
TEST(EmsDispatch, FailedEntryLeavesNothingInFlight) {
  EmsService ems(config_with("okx"), nullptr);
  StubSession session("okx");
  ems.register_session("okx", &session);

  Capture c;
  ems.place_order("okx", limit_buy(), c.order_cb());
  ems.cancel_order("okx", "unknown-id", c.bool_cb());

  EXPECT_EQ(c.calls, 2);
  EXPECT_EQ(ems.in_flight(), 0u);
}
