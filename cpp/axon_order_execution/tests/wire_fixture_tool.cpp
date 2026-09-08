// Emits canonical wire fixtures for the Python cross-implementation check.
//
// One JSON object per line:
//   {"kind": "<dataclass name>", "bytes": "<exactly what wire.cpp produced>"}
//
// The payload is carried as an escaped STRING rather than inlined, so the
// C++ serialiser's exact output survives the trip -- inlining it would let
// Python's own encoder rewrite it and the test would compare Python to
// Python.
//
// Every value here is fixed. Nothing may depend on the wall clock or on a
// generated id, or the fixtures would not be reproducible.

#include <cstdio>
#include <string>
#include <vector>

#include "axon/transport/wire.h"

using namespace axon::transport;
using axon::core::Price;
using axon::core::Qty;
using axon::core::Timestamp;
using axon::models::Liquidity;
using axon::models::OrderSide;
using axon::models::OrderStatus;
using axon::models::OrderType;

namespace {

Price D(const char* s) { return Price::from_string(s).value_or(Price{}); }
Timestamp T(const char* s) {
  return Timestamp::from_iso8601(s).value_or(Timestamp{});
}

void emit(const char* kind, const std::string& bytes) {
  Json line = Json::object();
  line["kind"] = kind;
  line["bytes"] = bytes;
  std::printf("%s\n", line.dump().c_str());
}

axon::models::Order full_order() {
  axon::models::Order o;
  o.order_id = "DERIBIT-12345";
  o.exchange = "deribit";
  o.instrument = "BTC-27JUN25-100000-C";
  o.side = OrderSide::kBuy;
  o.order_type = OrderType::kLimit;
  o.amount = D("2.5");
  o.status = OrderStatus::kPartiallyFilled;
  o.internal_order_id = "0123456789abcdef0123456789abcdef";
  o.price = D("0.0345");
  o.filled_amount = D("1.25");
  o.average_price = D("0.0344");
  o.client_order_id = "client-1";
  o.label = "chase-maker";
  o.liquidity = Liquidity::kMaker;
  o.post_only = true;
  o.reject_post_only = false;
  o.strategy_id = "alpha";
  o.created_at = T("2026-08-07T12:34:56");
  o.updated_at = T("2026-08-07T12:35:01.123456");
  return o;
}

}  // namespace

int main() {
  // --- Order: every field populated -----------------------------------------
  emit("Order", to_json(full_order()).dump());

  // --- Order: every optional empty, so nulls are exercised -------------------
  {
    axon::models::Order o = full_order();
    o.internal_order_id.reset();
    o.price.reset();
    o.average_price.reset();
    o.client_order_id.reset();
    o.label.reset();
    o.strategy_id.reset();
    o.order_type = OrderType::kMarket;
    o.status = OrderStatus::kRejected;
    o.liquidity = Liquidity::kTaker;
    o.post_only = false;
    o.filled_amount = Price{};
    emit("Order", to_json(o).dump());
  }

  // --- Order: numeric edges that stress float formatting on both sides ------
  for (const char* amount : {"0.000000001", "1", "100000.5", "123456.789",
                             "0.1", "0.0345", "9999999.999999999"}) {
    axon::models::Order o = full_order();
    o.amount = D(amount);
    o.price = D(amount);
    o.filled_amount = Price{};
    emit("Order", to_json(o).dump());
  }

  // --- Order: negative and zero ---------------------------------------------
  {
    axon::models::Order o = full_order();
    o.amount = D("0");
    o.price = D("-1.5");
    o.filled_amount = D("0");
    emit("Order", to_json(o).dump());
  }

  // --- Order: every status / side / type / liquidity spelling ----------------
  for (int s = 0; s <= 5; ++s) {
    axon::models::Order o = full_order();
    o.status = static_cast<OrderStatus>(s);
    o.side = (s % 2 == 0) ? OrderSide::kBuy : OrderSide::kSell;
    o.order_type = (s % 2 == 0) ? OrderType::kLimit : OrderType::kMarket;
    o.liquidity = (s % 2 == 0) ? Liquidity::kMaker : Liquidity::kTaker;
    emit("Order", to_json(o).dump());
  }

  // --- Order: timestamps with and without a fractional part ------------------
  for (const char* ts : {"2026-08-07T12:34:56", "2026-08-07T12:34:56.000001",
                         "2026-08-07T12:34:56.100000", "2026-08-07T12:34:56.999999",
                         "1970-01-01T00:00:00", "1999-12-31T23:59:59.123456",
                         "2024-02-29T00:00:00"}) {
    axon::models::Order o = full_order();
    o.created_at = T(ts);
    o.updated_at = T(ts);
    emit("Order", to_json(o).dump());
  }

  // --- OrderRequest ----------------------------------------------------------
  {
    axon::models::OrderRequest r;
    r.instrument = "BTC-PERPETUAL";
    r.side = OrderSide::kSell;
    r.amount = D("10");
    r.order_type = OrderType::kLimit;
    r.price = D("64000.5");
    r.client_order_id = "client-2";
    r.label = "hedge";
    r.post_only = true;
    r.reject_post_only = true;
    r.internal_order_id = "fedcba9876543210fedcba9876543210";
    r.strategy_id = "beta";
    emit("OrderRequest", to_json(r).dump());

    r.price.reset();
    r.order_type = OrderType::kMarket;
    r.client_order_id.reset();
    r.label.reset();
    r.strategy_id.reset();
    r.post_only = false;
    r.reject_post_only = false;
    emit("OrderRequest", to_json(r).dump());
  }

  // --- Ticker ----------------------------------------------------------------
  {
    axon::models::Ticker t;
    t.instrument = "BTC-PERPETUAL";
    t.best_bid_price = D("64000.5");
    t.best_bid_amount = D("12.5");
    t.best_ask_price = D("64001");
    t.best_ask_amount = D("8.25");
    t.last_price = D("64000.75");
    t.mark_price = D("64000.6");
    t.timestamp = T("2026-08-07T12:34:56.500000");
    emit("Ticker", to_json(t).dump());

    t.last_price.reset();
    t.mark_price.reset();
    t.timestamp.reset();
    emit("Ticker", to_json(t).dump());
  }

  // --- Fill ------------------------------------------------------------------
  {
    axon::models::Fill f;
    f.trade_id = "TRADE-9876";
    f.order_id = "DERIBIT-12345";
    f.exchange = "deribit";
    f.instrument = "BTC-27JUN25-100000-C";
    f.side = OrderSide::kBuy;
    f.amount = D("1.5");
    f.price = D("0.0345");
    f.fee = D("0.0000123");
    f.fee_currency = "BTC";
    f.liquidity = Liquidity::kMaker;
    f.timestamp = T("2026-08-07T12:34:56.123456");
    f.index_price = D("64000");
    f.mark_price = D("64000.6");
    f.iv = D("68.5");
    f.profit_loss = D("-0.0012");
    f.label = "chase-maker";
    f.strategy_id = "alpha";
    emit("Fill", to_json(f).dump());

    f.index_price.reset();
    f.mark_price.reset();
    f.iv.reset();
    f.profit_loss.reset();
    f.label.reset();
    f.strategy_id.reset();
    f.side = OrderSide::kSell;
    f.liquidity = Liquidity::kTaker;
    emit("Fill", to_json(f).dump());
  }

  // --- AccountSummary --------------------------------------------------------
  {
    axon::models::AccountSummary a;
    a.currency = "BTC";
    a.exchange = "deribit";
    a.equity = D("12.3456789");
    a.balance = D("12");
    a.available_funds = D("10.5");
    a.initial_margin = D("1.5");
    a.maintenance_margin = D("0.75");
    a.margin_balance = D("12.1");
    a.delta_total = D("0.4523");
    a.options_delta = D("0.35");
    a.options_gamma = D("0.0001");
    a.options_vega = D("-0.005");
    a.options_theta = D("-0.02");
    a.futures_pl = D("0.1");
    a.options_pl = D("-0.05");
    a.total_pl = D("0.05");
    a.timestamp = T("2026-08-07T12:34:56");
    emit("AccountSummary", to_json(a).dump());
  }

  // --- Position --------------------------------------------------------------
  {
    axon::models::Position p;
    p.instrument = "BTC-27JUN25-100000-C";
    p.exchange = "deribit";
    p.kind = "option";
    p.direction = "buy";
    p.size = D("2.5");
    p.average_price = D("0.0344");
    p.mark_price = D("0.0345");
    p.index_price = D("64000");
    p.initial_margin = D("0.5");
    p.maintenance_margin = D("0.25");
    p.delta = D("0.4523");
    p.gamma = D("0.0001");
    p.vega = D("0.005");
    p.theta = D("-0.02");
    p.total_profit_loss = D("0.0025");
    p.floating_profit_loss = D("0.0025");
    p.realized_profit_loss = D("0");
    p.timestamp = T("2026-08-07T12:34:56");
    emit("Position", to_json(p).dump());

    p.direction = "zero";
    p.size = D("0");
    emit("Position", to_json(p).dump());
  }

  // --- Command ---------------------------------------------------------------
  {
    Command c;
    c.command_type = "place_order";
    c.payload = Json::object();
    c.payload["exchange"] = "deribit";
    c.payload["order_request"] = to_json(axon::models::OrderRequest{
        .instrument = "BTC-PERPETUAL",
        .side = OrderSide::kBuy,
        .amount = D("1"),
        .order_type = OrderType::kLimit,
        .price = D("64000"),
        .internal_order_id = "0123456789abcdef0123456789abcdef"});
    c.request_id = "req-0001";
    c.strategy_id = "alpha";
    emit("Command", to_json(c).dump());

    Command empty;
    empty.command_type = "get_positions";
    empty.payload = Json::object();
    empty.request_id = "req-0002";
    empty.strategy_id = "";
    emit("Command", to_json(empty).dump());
  }

  // --- Response --------------------------------------------------------------
  {
    emit("Response", serialize_response(Response::ok("req-0001",
                                                     to_json(full_order()))));
    emit("Response",
         serialize_response(Response::fail("req-0002", "instrument not found")));

    Response scalars = Response::ok("req-0003", Json(true));
    emit("Response", serialize_response(scalars));

    Json list = Json::array();
    list.push_back(to_json(full_order()));
    list.push_back(to_json(full_order()));
    emit("Response", serialize_response(Response::ok("req-0004", list)));

    emit("Response", serialize_response(Response::ok("req-0005", Json())));
  }

  // --- Event -----------------------------------------------------------------
  {
    Event e;
    e.event_type = "order_update";
    e.data = to_json(full_order());
    e.strategy_id = "alpha";
    emit("Event", to_json(e).dump());

    Event empty;
    empty.event_type = "fill_update";
    empty.data = Json::object();
    empty.strategy_id = "";
    emit("Event", to_json(empty).dump());
  }

  return 0;
}
