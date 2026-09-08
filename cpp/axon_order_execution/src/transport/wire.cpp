#include "axon/transport/wire.h"

namespace axon::transport {
namespace {

using core::Price;
using core::Timestamp;

// ---------------------------------------------------------------------------
// Emit helpers
//
// Every field is emitted unconditionally, including nulls for empty optionals.
// Python's serialize() walks dataclasses.fields() and writes every one, so
// omitting a null here would produce a smaller object than Python does and
// break byte-identity.
// ---------------------------------------------------------------------------

void put(Json& j, const char* key, const std::string& v) { j[key] = v; }

void put(Json& j, const char* key, const std::optional<std::string>& v) {
  if (v.has_value()) {
    j[key] = *v;
  } else {
    j[key] = nullptr;
  }
}

void put(Json& j, const char* key, bool v) { j[key] = v; }

// Decimal -> JSON number.
//
// This is the one place the exact fixed-point value degrades to a double, and
// it is unavoidable: the peer is Python, whose json module has nothing but
// float to receive it into. The loss is bounded -- a 9-decimal value below
// ~9e6 round-trips exactly through a double -- and the binary hot path never
// comes through here.
void put(Json& j, const char* key, Price v) { j[key] = v.to_double(); }

void put(Json& j, const char* key, const std::optional<Price>& v) {
  if (v.has_value()) {
    j[key] = v->to_double();
  } else {
    j[key] = nullptr;
  }
}

void put(Json& j, const char* key, Timestamp v) { j[key] = v.to_iso8601(); }

void put(Json& j, const char* key, const std::optional<Timestamp>& v) {
  if (v.has_value()) {
    j[key] = v->to_iso8601();
  } else {
    j[key] = nullptr;
  }
}

template <typename E>
void put_enum(Json& j, const char* key, E v) {
  j[key] = std::string(models::to_string(v));
}

// ---------------------------------------------------------------------------
// Parse helpers
//
// Convention:
//   req_*  the key must be present, non-null, and of the right type
//   opt_*  absent or null leaves the destination at its default; a present
//          value of the wrong type is still an error
//
// Both return false on violation so callers can `if (!req_str(...)) return
// std::nullopt;` without exceptions. Unknown keys are ignored by construction.
// ---------------------------------------------------------------------------

const Json* find(const Json& j, const char* key) {
  if (!j.is_object()) {
    return nullptr;
  }
  const auto it = j.find(key);
  return it == j.end() ? nullptr : &(*it);
}

bool req_str(const Json& j, const char* key, std::string& out) {
  const Json* v = find(j, key);
  if (v == nullptr || !v->is_string()) {
    return false;
  }
  out = v->get<std::string>();
  return true;
}

bool opt_str(const Json& j, const char* key, std::optional<std::string>& out) {
  const Json* v = find(j, key);
  if (v == nullptr || v->is_null()) {
    return true;
  }
  if (!v->is_string()) {
    return false;
  }
  out = v->get<std::string>();
  return true;
}

bool opt_str_default(const Json& j, const char* key, std::string& out) {
  const Json* v = find(j, key);
  if (v == nullptr || v->is_null()) {
    return true;
  }
  if (!v->is_string()) {
    return false;
  }
  out = v->get<std::string>();
  return true;
}

bool opt_bool(const Json& j, const char* key, bool& out) {
  const Json* v = find(j, key);
  if (v == nullptr || v->is_null()) {
    return true;
  }
  if (!v->is_boolean()) {
    return false;
  }
  out = v->get<bool>();
  return true;
}

bool req_bool(const Json& j, const char* key, bool& out) {
  const Json* v = find(j, key);
  if (v == nullptr || !v->is_boolean()) {
    return false;
  }
  out = v->get<bool>();
  return true;
}

// Accepts a JSON number (what Python sends) or a JSON string (what several
// exchange REST APIs send, and what a caller preserving exactness would send).
// The string path is the exact one -- it never touches a double.
bool decimal_from(const Json& v, Price& out) {
  if (v.is_string()) {
    const auto parsed = Price::from_string(v.get<std::string>());
    if (!parsed.has_value()) {
      return false;
    }
    out = *parsed;
    return true;
  }
  if (v.is_number()) {
    out = Price::from_double(v.get<double>());
    return true;
  }
  return false;
}

bool req_dec(const Json& j, const char* key, Price& out) {
  const Json* v = find(j, key);
  if (v == nullptr || v->is_null()) {
    return false;
  }
  return decimal_from(*v, out);
}

bool opt_dec(const Json& j, const char* key, std::optional<Price>& out) {
  const Json* v = find(j, key);
  if (v == nullptr || v->is_null()) {
    return true;
  }
  Price p;
  if (!decimal_from(*v, p)) {
    return false;
  }
  out = p;
  return true;
}

bool opt_dec_default(const Json& j, const char* key, Price& out) {
  const Json* v = find(j, key);
  if (v == nullptr || v->is_null()) {
    return true;
  }
  return decimal_from(*v, out);
}

bool req_ts(const Json& j, const char* key, Timestamp& out) {
  const Json* v = find(j, key);
  if (v == nullptr) {
    return false;
  }
  if (v->is_string()) {
    const auto parsed = Timestamp::from_iso8601(v->get<std::string>());
    if (!parsed.has_value()) {
      return false;
    }
    out = *parsed;
    return true;
  }
  // Tolerated: a numeric epoch. Exchange payloads use milliseconds; Python's
  // serialize() never produces this, so it can only come from a hand-built
  // message or a direct exchange passthrough.
  if (v->is_number_integer()) {
    out = Timestamp::from_millis(v->get<std::int64_t>());
    return true;
  }
  return false;
}

bool opt_ts(const Json& j, const char* key, std::optional<Timestamp>& out) {
  const Json* v = find(j, key);
  if (v == nullptr || v->is_null()) {
    return true;
  }
  Timestamp t;
  Json wrapper = Json::object();
  wrapper[key] = *v;
  if (!req_ts(wrapper, key, t)) {
    return false;
  }
  out = t;
  return true;
}

template <typename E, typename Fn>
bool req_enum(const Json& j, const char* key, Fn parser, E& out) {
  const Json* v = find(j, key);
  if (v == nullptr || !v->is_string()) {
    return false;
  }
  const auto parsed = parser(v->get<std::string>());
  if (!parsed.has_value()) {
    return false;
  }
  out = *parsed;
  return true;
}

template <typename E, typename Fn>
bool opt_enum(const Json& j, const char* key, Fn parser, E& out) {
  const Json* v = find(j, key);
  if (v == nullptr || v->is_null()) {
    return true;
  }
  if (!v->is_string()) {
    return false;
  }
  const auto parsed = parser(v->get<std::string>());
  if (!parsed.has_value()) {
    return false;
  }
  out = *parsed;
  return true;
}

}  // namespace

// ===========================================================================
// Ticker
// ===========================================================================
Json to_json(const models::Ticker& v) {
  Json j = Json::object();
  put(j, "instrument", v.instrument);
  put(j, "best_bid_price", v.best_bid_price);
  put(j, "best_bid_amount", v.best_bid_amount);
  put(j, "best_ask_price", v.best_ask_price);
  put(j, "best_ask_amount", v.best_ask_amount);
  put(j, "last_price", v.last_price);
  put(j, "mark_price", v.mark_price);
  put(j, "timestamp", v.timestamp);
  return j;
}

std::optional<models::Ticker> ticker_from_json(const Json& j) {
  models::Ticker t;
  if (!req_str(j, "instrument", t.instrument) ||
      !req_dec(j, "best_bid_price", t.best_bid_price) ||
      !req_dec(j, "best_bid_amount", t.best_bid_amount) ||
      !req_dec(j, "best_ask_price", t.best_ask_price) ||
      !req_dec(j, "best_ask_amount", t.best_ask_amount) ||
      !opt_dec(j, "last_price", t.last_price) ||
      !opt_dec(j, "mark_price", t.mark_price) ||
      !opt_ts(j, "timestamp", t.timestamp)) {
    return std::nullopt;
  }
  return t;
}

// ===========================================================================
// OrderRequest
// ===========================================================================
Json to_json(const models::OrderRequest& v) {
  Json j = Json::object();
  put(j, "instrument", v.instrument);
  put_enum(j, "side", v.side);
  put(j, "amount", v.amount);
  put_enum(j, "order_type", v.order_type);
  put(j, "price", v.price);
  put(j, "client_order_id", v.client_order_id);
  put(j, "label", v.label);
  put(j, "post_only", v.post_only);
  put(j, "reject_post_only", v.reject_post_only);
  put(j, "internal_order_id", v.internal_order_id);
  put(j, "strategy_id", v.strategy_id);
  return j;
}

std::optional<models::OrderRequest> order_request_from_json(const Json& j) {
  models::OrderRequest r;
  if (!req_str(j, "instrument", r.instrument) ||
      !req_enum(j, "side", models::order_side_from_string, r.side) ||
      !req_dec(j, "amount", r.amount) ||
      !opt_enum(j, "order_type", models::order_type_from_string, r.order_type) ||
      !opt_dec(j, "price", r.price) ||
      !opt_str(j, "client_order_id", r.client_order_id) ||
      !opt_str(j, "label", r.label) ||
      !opt_bool(j, "post_only", r.post_only) ||
      !opt_bool(j, "reject_post_only", r.reject_post_only) ||
      !opt_str_default(j, "internal_order_id", r.internal_order_id) ||
      !opt_str(j, "strategy_id", r.strategy_id)) {
    return std::nullopt;
  }
  return r;
}

// ===========================================================================
// Order
// ===========================================================================
Json to_json(const models::Order& v) {
  Json j = Json::object();
  put(j, "order_id", v.order_id);
  put(j, "exchange", v.exchange);
  put(j, "instrument", v.instrument);
  put_enum(j, "side", v.side);
  put_enum(j, "order_type", v.order_type);
  put(j, "amount", v.amount);
  put_enum(j, "status", v.status);
  put(j, "internal_order_id", v.internal_order_id);
  put(j, "price", v.price);
  put(j, "filled_amount", v.filled_amount);
  put(j, "average_price", v.average_price);
  put(j, "client_order_id", v.client_order_id);
  put(j, "label", v.label);
  put_enum(j, "liquidity", v.liquidity);
  put(j, "post_only", v.post_only);
  put(j, "reject_post_only", v.reject_post_only);
  put(j, "strategy_id", v.strategy_id);
  put(j, "created_at", v.created_at);
  put(j, "updated_at", v.updated_at);
  return j;
}

std::optional<models::Order> order_from_json(const Json& j) {
  models::Order o;
  if (!req_str(j, "order_id", o.order_id) ||
      !req_str(j, "exchange", o.exchange) ||
      !req_str(j, "instrument", o.instrument) ||
      !req_enum(j, "side", models::order_side_from_string, o.side) ||
      !req_enum(j, "order_type", models::order_type_from_string, o.order_type) ||
      !req_dec(j, "amount", o.amount) ||
      !req_enum(j, "status", models::order_status_from_string, o.status) ||
      !opt_str(j, "internal_order_id", o.internal_order_id) ||
      !opt_dec(j, "price", o.price) ||
      !opt_dec_default(j, "filled_amount", o.filled_amount) ||
      !opt_dec(j, "average_price", o.average_price) ||
      !opt_str(j, "client_order_id", o.client_order_id) ||
      !opt_str(j, "label", o.label) ||
      !opt_enum(j, "liquidity", models::liquidity_from_string, o.liquidity) ||
      !opt_bool(j, "post_only", o.post_only) ||
      !opt_bool(j, "reject_post_only", o.reject_post_only) ||
      !opt_str(j, "strategy_id", o.strategy_id) ||
      !req_ts(j, "created_at", o.created_at) ||
      !req_ts(j, "updated_at", o.updated_at)) {
    return std::nullopt;
  }
  return o;
}

// ===========================================================================
// Fill
// ===========================================================================
Json to_json(const models::Fill& v) {
  Json j = Json::object();
  put(j, "trade_id", v.trade_id);
  put(j, "order_id", v.order_id);
  put(j, "exchange", v.exchange);
  put(j, "instrument", v.instrument);
  put_enum(j, "side", v.side);
  put(j, "amount", v.amount);
  put(j, "price", v.price);
  put(j, "fee", v.fee);
  put(j, "fee_currency", v.fee_currency);
  put_enum(j, "liquidity", v.liquidity);
  put(j, "timestamp", v.timestamp);
  put(j, "index_price", v.index_price);
  put(j, "mark_price", v.mark_price);
  put(j, "iv", v.iv);
  put(j, "profit_loss", v.profit_loss);
  put(j, "label", v.label);
  put(j, "strategy_id", v.strategy_id);
  return j;
}

std::optional<models::Fill> fill_from_json(const Json& j) {
  models::Fill f;
  if (!req_str(j, "trade_id", f.trade_id) ||
      !req_str(j, "order_id", f.order_id) ||
      !req_str(j, "exchange", f.exchange) ||
      !req_str(j, "instrument", f.instrument) ||
      !req_enum(j, "side", models::order_side_from_string, f.side) ||
      !req_dec(j, "amount", f.amount) || !req_dec(j, "price", f.price) ||
      !req_dec(j, "fee", f.fee) || !req_str(j, "fee_currency", f.fee_currency) ||
      !req_enum(j, "liquidity", models::liquidity_from_string, f.liquidity) ||
      !req_ts(j, "timestamp", f.timestamp) ||
      !opt_dec(j, "index_price", f.index_price) ||
      !opt_dec(j, "mark_price", f.mark_price) || !opt_dec(j, "iv", f.iv) ||
      !opt_dec(j, "profit_loss", f.profit_loss) || !opt_str(j, "label", f.label) ||
      !opt_str(j, "strategy_id", f.strategy_id)) {
    return std::nullopt;
  }
  return f;
}

// ===========================================================================
// AccountSummary
// ===========================================================================
Json to_json(const models::AccountSummary& v) {
  Json j = Json::object();
  put(j, "currency", v.currency);
  put(j, "exchange", v.exchange);
  put(j, "equity", v.equity);
  put(j, "balance", v.balance);
  put(j, "available_funds", v.available_funds);
  put(j, "initial_margin", v.initial_margin);
  put(j, "maintenance_margin", v.maintenance_margin);
  put(j, "margin_balance", v.margin_balance);
  put(j, "delta_total", v.delta_total);
  put(j, "options_delta", v.options_delta);
  put(j, "options_gamma", v.options_gamma);
  put(j, "options_vega", v.options_vega);
  put(j, "options_theta", v.options_theta);
  put(j, "futures_pl", v.futures_pl);
  put(j, "options_pl", v.options_pl);
  put(j, "total_pl", v.total_pl);
  put(j, "timestamp", v.timestamp);
  return j;
}

std::optional<models::AccountSummary> account_summary_from_json(const Json& j) {
  models::AccountSummary a;
  if (!req_str(j, "currency", a.currency) || !req_str(j, "exchange", a.exchange) ||
      !req_dec(j, "equity", a.equity) || !req_dec(j, "balance", a.balance) ||
      !req_dec(j, "available_funds", a.available_funds) ||
      !req_dec(j, "initial_margin", a.initial_margin) ||
      !req_dec(j, "maintenance_margin", a.maintenance_margin) ||
      !req_dec(j, "margin_balance", a.margin_balance) ||
      !req_dec(j, "delta_total", a.delta_total) ||
      !req_dec(j, "options_delta", a.options_delta) ||
      !req_dec(j, "options_gamma", a.options_gamma) ||
      !req_dec(j, "options_vega", a.options_vega) ||
      !req_dec(j, "options_theta", a.options_theta) ||
      !req_dec(j, "futures_pl", a.futures_pl) ||
      !req_dec(j, "options_pl", a.options_pl) ||
      !req_dec(j, "total_pl", a.total_pl) ||
      !req_ts(j, "timestamp", a.timestamp)) {
    return std::nullopt;
  }
  return a;
}

// ===========================================================================
// Position
// ===========================================================================
Json to_json(const models::Position& v) {
  Json j = Json::object();
  put(j, "instrument", v.instrument);
  put(j, "exchange", v.exchange);
  put(j, "kind", v.kind);
  put(j, "direction", v.direction);
  put(j, "size", v.size);
  put(j, "average_price", v.average_price);
  put(j, "mark_price", v.mark_price);
  put(j, "index_price", v.index_price);
  put(j, "initial_margin", v.initial_margin);
  put(j, "maintenance_margin", v.maintenance_margin);
  put(j, "delta", v.delta);
  put(j, "gamma", v.gamma);
  put(j, "vega", v.vega);
  put(j, "theta", v.theta);
  put(j, "total_profit_loss", v.total_profit_loss);
  put(j, "floating_profit_loss", v.floating_profit_loss);
  put(j, "realized_profit_loss", v.realized_profit_loss);
  put(j, "timestamp", v.timestamp);
  return j;
}

std::optional<models::Position> position_from_json(const Json& j) {
  models::Position p;
  if (!req_str(j, "instrument", p.instrument) ||
      !req_str(j, "exchange", p.exchange) || !req_str(j, "kind", p.kind) ||
      !req_str(j, "direction", p.direction) || !req_dec(j, "size", p.size) ||
      !req_dec(j, "average_price", p.average_price) ||
      !req_dec(j, "mark_price", p.mark_price) ||
      !req_dec(j, "index_price", p.index_price) ||
      !req_dec(j, "initial_margin", p.initial_margin) ||
      !req_dec(j, "maintenance_margin", p.maintenance_margin) ||
      !req_dec(j, "delta", p.delta) || !req_dec(j, "gamma", p.gamma) ||
      !req_dec(j, "vega", p.vega) || !req_dec(j, "theta", p.theta) ||
      !req_dec(j, "total_profit_loss", p.total_profit_loss) ||
      !req_dec(j, "floating_profit_loss", p.floating_profit_loss) ||
      !req_dec(j, "realized_profit_loss", p.realized_profit_loss) ||
      !req_ts(j, "timestamp", p.timestamp)) {
    return std::nullopt;
  }
  return p;
}

// ===========================================================================
// Protocol messages
// ===========================================================================
Json to_json(const Command& v) {
  Json j = Json::object();
  j["command_type"] = v.command_type;
  j["payload"] = v.payload;
  j["request_id"] = v.request_id;
  j["strategy_id"] = v.strategy_id;
  return j;
}

std::optional<Command> command_from_json(const Json& j) {
  Command c;
  if (!req_str(j, "command_type", c.command_type)) {
    return std::nullopt;
  }
  const Json* payload = find(j, "payload");
  if (payload == nullptr || payload->is_null()) {
    c.payload = Json::object();
  } else {
    c.payload = *payload;
  }
  if (!req_str(j, "request_id", c.request_id) ||
      !opt_str_default(j, "strategy_id", c.strategy_id)) {
    return std::nullopt;
  }
  return c;
}

Json to_json(const Response& v) {
  Json j = Json::object();
  j["request_id"] = v.request_id;
  j["success"] = v.success;
  j["data"] = v.data;
  if (v.error.has_value()) {
    j["error"] = *v.error;
  } else {
    j["error"] = nullptr;
  }
  return j;
}

std::optional<Response> response_from_json(const Json& j) {
  Response r;
  if (!req_str(j, "request_id", r.request_id) ||
      !req_bool(j, "success", r.success)) {
    return std::nullopt;
  }
  const Json* data = find(j, "data");
  r.data = (data == nullptr) ? Json() : *data;
  if (!opt_str(j, "error", r.error)) {
    return std::nullopt;
  }
  return r;
}

Json to_json(const Event& v) {
  Json j = Json::object();
  j["event_type"] = v.event_type;
  j["data"] = v.data;
  j["strategy_id"] = v.strategy_id;
  return j;
}

std::optional<Event> event_from_json(const Json& j) {
  Event e;
  if (!req_str(j, "event_type", e.event_type)) {
    return std::nullopt;
  }
  const Json* data = find(j, "data");
  e.data = (data == nullptr || data->is_null()) ? Json::object() : *data;
  if (!opt_str_default(j, "strategy_id", e.strategy_id)) {
    return std::nullopt;
  }
  return e;
}

// ===========================================================================
// Byte-level entry points
// ===========================================================================
std::string serialize_command(const Command& v) { return to_json(v).dump(); }
std::string serialize_response(const Response& v) { return to_json(v).dump(); }
std::string serialize_event(const Event& v) { return to_json(v).dump(); }

namespace {

// A malformed frame is an ordinary event on a network socket, not an
// exceptional one. Parsing with exceptions disabled keeps the receive loop
// free of try/catch and makes "bad frame" a return value like any other.
std::optional<Json> parse_bytes(std::string_view bytes) {
  Json j = Json::parse(bytes.begin(), bytes.end(), /*cb=*/nullptr,
                       /*allow_exceptions=*/false);
  if (j.is_discarded()) {
    return std::nullopt;
  }
  return j;
}

}  // namespace

std::optional<Command> deserialize_command(std::string_view bytes) {
  const auto j = parse_bytes(bytes);
  return j.has_value() ? command_from_json(*j) : std::nullopt;
}

std::optional<Response> deserialize_response(std::string_view bytes) {
  const auto j = parse_bytes(bytes);
  return j.has_value() ? response_from_json(*j) : std::nullopt;
}

std::optional<Event> deserialize_event(std::string_view bytes) {
  const auto j = parse_bytes(bytes);
  return j.has_value() ? event_from_json(*j) : std::nullopt;
}

}  // namespace axon::transport
