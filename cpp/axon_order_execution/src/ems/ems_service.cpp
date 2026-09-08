#include "axon/ems/ems_service.h"

#include "axon/core/clock.h"
#include "axon/net/crypto_lite.h"
#include "axon/util/logging.h"
#include "axon/util/metrics.h"
#include "axon/venue/binance/binance_builder.h"
#include "axon/venue/bybit/bybit_builder.h"
#include "axon/venue/deribit/deribit_builder.h"
#include "axon/venue/json_view.h"
#include "axon/venue/okx/okx_builder.h"

namespace axon::ems {
namespace {

auto& log() {
  static auto logger = util::get_logger("ems");
  return logger;
}

double now_seconds() {
  return static_cast<double>(core::monotonic_ns()) / 1e9;
}

std::string pending_key(const std::string& exchange, std::int64_t id) {
  return exchange + ":" + std::to_string(id);
}

// Order-entry ids start far above anything a session handshake consumes.
//
// This is not cosmetic. A session claims its own auth and subscribe replies by
// id before passing anything on, so an order that happened to draw the same id
// as the auth request would be read as a second authentication acknowledgement
// -- which on Deribit re-runs the subscription. Both counters used to start at
// 1, so the FIRST order on a connection could do exactly that.
constexpr std::int64_t kEmsFirstRequestId = 1'000'000;

// One set per thread. The id has to be unique per CONNECTION; the venues
// tolerate a counter that keeps climbing across reconnects, and a monotonic
// one makes a stale reply from a dropped connection impossible to mistake for
// a live request.
struct WsBuilders {
  venue::deribit::DeribitBuilder deribit{kEmsFirstRequestId};
  venue::bybit::BybitBuilder bybit{kEmsFirstRequestId};
  venue::okx::OkxBuilder okx;
  venue::binance::BinanceBuilder binance;
};

WsBuilders& builders() {
  static thread_local WsBuilders b;
  return b;
}

// Deribit answers private/buy with {"result":{"order":{...}}}. Parsed with the
// same on-demand parser the feed uses.
std::optional<models::Order> parse_deribit_order_reply(std::string_view payload) {
  static thread_local venue::Document doc(64 * 1024);
  auto root = doc.parse_copy(payload);
  if (!root.has_value()) {
    return std::nullopt;
  }
  auto result = (*root)["result"].as_object();
  if (!result.has_value()) {
    return std::nullopt;
  }
  auto order_obj = (*result)["order"].as_object();
  if (!order_obj.has_value()) {
    return std::nullopt;
  }

  models::Order order;
  order.exchange = "deribit";
  const auto id = (*order_obj)["order_id"].as_string();
  const auto instrument = (*order_obj)["instrument_name"].as_string();
  if (!id.has_value() || !instrument.has_value()) {
    return std::nullopt;
  }
  order.order_id = std::string(*id);
  order.instrument = std::string(*instrument);

  const auto direction = (*order_obj)["direction"].as_string();
  order.side = venue::deribit::map_direction(direction.value_or("buy"));
  const auto order_type = (*order_obj)["order_type"].as_string();
  order.order_type = venue::deribit::map_order_type(order_type.value_or("limit"));

  const auto amount = (*order_obj)["amount"].as_decimal();
  order.amount = amount.value_or(core::Qty{});
  const auto filled = (*order_obj)["filled_amount"].as_decimal();
  order.filled_amount = filled.value_or(core::Qty{});
  order.price = (*order_obj)["price"].as_decimal();
  order.average_price = (*order_obj)["average_price"].as_decimal();

  const auto state = (*order_obj)["order_state"].as_string();
  const auto mapping = venue::deribit::map_order_state(state.value_or("open"));
  order.status = venue::deribit::refine_with_fill(
      mapping.status, order.filled_amount.raw() > 0);

  const auto created = (*order_obj)["creation_timestamp"].as_int();
  const auto updated = (*order_obj)["last_update_timestamp"].as_int();
  order.created_at = core::Timestamp::from_millis(created.value_or(0));
  order.updated_at = core::Timestamp::from_millis(updated.value_or(created.value_or(0)));
  return order;
}

// OKX reports TWO outcomes per request: a top-level `code` for whether the
// request was processed, and an `sCode` PER ORDER for whether that order was
// accepted. The session checks the first; this checks the second.
//
// Missing this is how a rejected order reads as a successful one -- top-level
// code "0" with sCode "51008" (insufficient balance) is a perfectly normal
// shape.
std::optional<std::string> okx_order_rejection(std::string_view payload) {
  static thread_local venue::Document doc(64 * 1024);
  auto root = doc.parse_copy(payload);
  if (!root.has_value()) {
    return std::nullopt;
  }
  auto data = (*root)["data"].as_array();
  if (!data.has_value()) {
    return std::nullopt;
  }
  std::optional<std::string> rejection;
  data->for_each_object([&](venue::Object& entry) {
    if (rejection.has_value()) {
      return;
    }
    const auto s_code = entry["sCode"].as_string().value_or("0");
    if (s_code == "0") {
      return;
    }
    const auto s_msg = entry["sMsg"].as_string().value_or("");
    rejection = "okx rejected the order (sCode " + std::string(s_code) + "): " +
                std::string(s_msg);
  });
  return rejection;
}

}  // namespace

EmsService::EmsService(const Config& config, net::HttpClient* http)
    : config_(config), http_(http) {}

EmsService::~EmsService() = default;

void EmsService::register_session(const std::string& exchange,
                                  oms::VenueSession* session) {
  sessions_[exchange] = session;
}

void EmsService::register_trade_session(const std::string& exchange,
                                        oms::VenueSession* session) {
  trade_sessions_[exchange] = session;
}

oms::VenueSession* EmsService::ws_entry_session(const std::string& exchange) const {
  // Deribit and OKX send orders on the private stream they already have.
  // Binance and Bybit each need their own trade connection.
  const bool own_connection = exchange == "binance" || exchange == "bybit";
  const auto& from = own_connection ? trade_sessions_ : sessions_;
  const auto it = from.find(exchange);
  return (it != from.end() && it->second != nullptr && it->second->live())
             ? it->second
             : nullptr;
}

std::string EmsService::order_entry_unavailable(const std::string& exchange) const {
  const bool own_connection = exchange == "binance" || exchange == "bybit";
  const auto& from = own_connection ? trade_sessions_ : sessions_;
  const auto it = from.find(exchange);
  if (it == from.end() || it->second == nullptr) {
    return "no order-entry session for " + exchange;
  }
  // Naming the state is the difference between "the venue is down" and "this
  // build never connected", which are diagnosed completely differently.
  return exchange + " order entry is unavailable: session is " +
         oms::to_string(it->second->state());
}

const ExchangeConfig* EmsService::exchange_config(const std::string& name) const {
  const auto it = config_.exchanges.find(name);
  return it == config_.exchanges.end() ? nullptr : &it->second;
}

// ---------------------------------------------------------------------------
void EmsService::place_order(const std::string& exchange,
                             const models::OrderRequest& request,
                             OrderCallback callback) {
  if (const auto invalid = request.validate(); invalid.has_value()) {
    // Rejected locally, before it costs a round trip.
    callback(OrderResult{false, std::nullopt, *invalid});
    return;
  }

  if (place_via_websocket(exchange, request, callback)) {
    return;
  }
  // There is no second way to place an order. A venue whose session is not
  // live cannot trade, and saying so immediately beats queueing an order
  // against a connection that may be minutes from coming back -- by which
  // point the price the strategy decided on is long gone.
  callback(OrderResult{false, std::nullopt, order_entry_unavailable(exchange)});
}

bool EmsService::place_via_websocket(const std::string& exchange,
                                     const models::OrderRequest& request,
                                     const OrderCallback& callback) {
  oms::VenueSession* session = ws_entry_session(exchange);
  if (session == nullptr) {
    util::get_metrics().inc_ems_request_error(exchange, "place_order",
                                              "session_down");
    return false;
  }

  const std::int64_t now_ms = core::wall_clock_ns() / 1'000'000LL;
  char buf[venue::bybit::kMaxRequestBytes];
  static_assert(sizeof(buf) >= venue::deribit::kMaxRequestBytes);
  static_assert(sizeof(buf) >= venue::okx::kMaxRequestBytes);
  static_assert(sizeof(buf) >= venue::binance::kMaxRequestBytes);

  std::int64_t id = 0;
  std::size_t n = 0;
  if (exchange == "deribit") {
    n = builders().deribit.place_order(buf, sizeof(buf), request, id);
  } else if (exchange == "okx") {
    n = builders().okx.ws_place_order(buf, sizeof(buf), request, id);
  } else if (exchange == "bybit") {
    n = builders().bybit.ws_place_order(buf, sizeof(buf), request, now_ms, id);
  } else if (exchange == "binance") {
    const ExchangeConfig* cfg = exchange_config(exchange);
    if (cfg == nullptr) {
      return false;
    }
    n = builders().binance.ws_place_order(buf, sizeof(buf), request,
                                          cfg->api_key, cfg->api_secret,
                                          now_ms, id);
  } else {
    return false;
  }
  if (n == 0) {
    // Could not be built -- too long, or a label Binance cannot sign as sent.
    // There is no other encoding to fall back to, so this becomes a rejection.
    return false;
  }

  Pending pending;
  pending.exchange = exchange;
  pending.order_callback = callback;
  pending.request = request;
  pending.deadline =
      now_seconds() + static_cast<double>(config_.websocket.request_timeout_seconds);
  pending_[pending_key(exchange, id)] = std::move(pending);

  if (!session->send_raw(std::string_view(buf, n))) {
    // The send buffer is full. Drop the pending entry WITHOUT calling its
    // callback -- the caller still owns it and reports the failure.
    pending_.erase(pending_key(exchange, id));
    return false;
  }
  return true;
}

bool EmsService::cancel_via_websocket(const std::string& exchange,
                                      const std::string& order_id,
                                      const std::string& symbol,
                                      const BoolCallback& callback) {
  oms::VenueSession* session = ws_entry_session(exchange);
  if (session == nullptr) {
    return false;
  }
  // Every venue but Deribit needs the symbol, and only the order store has it.
  if (exchange != "deribit" && symbol.empty()) {
    return false;
  }

  const std::int64_t now_ms = core::wall_clock_ns() / 1'000'000LL;
  char buf[venue::bybit::kMaxRequestBytes];
  std::int64_t id = 0;
  std::size_t n = 0;
  if (exchange == "deribit") {
    n = builders().deribit.cancel_order(buf, sizeof(buf), order_id, id);
  } else if (exchange == "okx") {
    n = builders().okx.ws_cancel_order(buf, sizeof(buf), symbol, order_id, id);
  } else if (exchange == "bybit") {
    n = builders().bybit.ws_cancel_order(buf, sizeof(buf), symbol, order_id,
                                         now_ms, id);
  } else if (exchange == "binance") {
    const ExchangeConfig* cfg = exchange_config(exchange);
    if (cfg == nullptr) {
      return false;
    }
    n = builders().binance.ws_cancel_order(buf, sizeof(buf), symbol, order_id,
                                           cfg->api_key, cfg->api_secret,
                                           now_ms, id);
  } else {
    return false;
  }
  if (n == 0) {
    return false;
  }

  Pending pending;
  pending.exchange = exchange;
  pending.bool_callback = callback;
  pending.deadline =
      now_seconds() + static_cast<double>(config_.websocket.request_timeout_seconds);
  pending_[pending_key(exchange, id)] = std::move(pending);

  if (!session->send_raw(std::string_view(buf, n))) {
    pending_.erase(pending_key(exchange, id));
    return false;
  }
  return true;
}

bool EmsService::modify_via_websocket(const std::string& exchange,
                                      const models::Order& existing,
                                      std::optional<core::Qty> amount,
                                      std::optional<core::Price> price,
                                      const OrderCallback& callback) {
  oms::VenueSession* session = ws_entry_session(exchange);
  if (session == nullptr) {
    return false;
  }

  const std::int64_t now_ms = core::wall_clock_ns() / 1'000'000LL;
  char buf[venue::bybit::kMaxRequestBytes];
  std::int64_t id = 0;
  std::size_t n = 0;
  if (exchange == "deribit") {
    n = builders().deribit.modify_order(buf, sizeof(buf), existing.order_id,
                                        amount, price, id);
  } else if (exchange == "okx") {
    n = builders().okx.ws_amend_order(buf, sizeof(buf), existing.instrument,
                                      existing.order_id, amount, price, id);
  } else if (exchange == "bybit") {
    n = builders().bybit.ws_amend_order(buf, sizeof(buf), existing.instrument,
                                        existing.order_id, amount, price,
                                        now_ms, id);
  } else if (exchange == "binance") {
    const ExchangeConfig* cfg = exchange_config(exchange);
    if (cfg == nullptr) {
      return false;
    }
    // Binance will not accept a partial amend, so whatever the caller left out
    // comes from the order we already have -- same rule as the REST path.
    const core::Qty final_amount = amount.value_or(existing.amount);
    if (!price.has_value() && !existing.price.has_value()) {
      return false;
    }
    const core::Price final_price = price.value_or(existing.price.value_or(core::Price{}));
    n = builders().binance.ws_modify_order(
        buf, sizeof(buf), existing.instrument, existing.order_id, existing.side,
        final_amount, final_price, cfg->api_key, cfg->api_secret, now_ms, id);
  } else {
    return false;
  }
  if (n == 0) {
    return false;
  }

  Pending pending;
  pending.exchange = exchange;
  pending.order_callback = callback;
  pending.deadline =
      now_seconds() + static_cast<double>(config_.websocket.request_timeout_seconds);
  pending_[pending_key(exchange, id)] = std::move(pending);

  if (!session->send_raw(std::string_view(buf, n))) {
    pending_.erase(pending_key(exchange, id));
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------------
void EmsService::cancel_order(const std::string& exchange,
                              const std::string& order_id, BoolCallback callback) {
  // Every venue but Deribit needs the symbol, and a strategy only holds an id.
  std::string symbol;
  if (exchange != "deribit" && lookup_) {
    if (const auto order = lookup_(order_id); order.has_value()) {
      symbol = order->instrument;
    }
  }
  if (exchange != "deribit" && symbol.empty()) {
    callback(false, "order " + order_id +
                        " is not known locally, so its symbol cannot be "
                        "resolved for cancellation");
    return;
  }

  if (cancel_via_websocket(exchange, order_id, symbol, callback)) {
    return;
  }
  callback(false, order_entry_unavailable(exchange));
}

void EmsService::modify_order(const std::string& exchange,
                              const std::string& order_id,
                              std::optional<core::Qty> amount,
                              std::optional<core::Price> price,
                              OrderCallback callback) {
  models::Order existing;
  if (exchange == "deribit") {
    // Deribit amends by order id alone, so nothing has to be looked up.
    existing.order_id = order_id;
  } else {
    std::optional<models::Order> found;
    if (lookup_) {
      found = lookup_(order_id);
    }
    if (!found.has_value()) {
      callback(OrderResult{false, std::nullopt,
                           "order " + order_id +
                               " is not known locally, so it cannot be amended"});
      return;
    }
    existing = std::move(*found);
  }

  if (modify_via_websocket(exchange, existing, amount, price, callback)) {
    return;
  }
  callback(OrderResult{false, std::nullopt, order_entry_unavailable(exchange)});
}

// ---------------------------------------------------------------------------
void EmsService::on_rpc_reply(const std::string& exchange, std::int64_t id,
                              bool success, std::string_view payload,
                              std::string_view error) {
  auto node = pending_.extract(pending_key(exchange, id));
  if (node.empty()) {
    return;  // not ours
  }
  Pending& pending = node.mapped();

  if (pending.bool_callback) {
    pending.bool_callback(success, std::string(error));
    return;
  }
  if (!pending.order_callback) {
    return;
  }

  if (!success) {
    util::get_metrics().inc_order_place_failure(exchange, "venue_error");
    pending.order_callback(OrderResult{false, std::nullopt, std::string(error)});
    return;
  }

  if (exchange == "deribit") {
    auto order = parse_deribit_order_reply(payload);
    if (!order.has_value()) {
      pending.order_callback(
          OrderResult{false, std::nullopt, "could not parse the venue's reply"});
      return;
    }
    // The internal id is ours, not the venue's; carry it onto the order so the
    // OMS can correlate what a strategy asked for with what came back.
    order->internal_order_id = pending.request.internal_order_id;
    order->strategy_id = pending.request.strategy_id;
    pending.order_callback(OrderResult{true, std::move(order), {}});
    return;
  }

  // OKX accepts the REQUEST and rejects the ORDER independently, so a
  // successful reply still has to be inspected.
  if (exchange == "okx") {
    if (const auto rejection = okx_order_rejection(payload); rejection.has_value()) {
      util::get_metrics().inc_order_rejected(exchange, "venue");
      pending.order_callback(OrderResult{false, std::nullopt, *rejection});
      return;
    }
  }

  // The perpetual venues: acceptance only. The authoritative order state
  // arrives on the feed a moment later, exactly as it does on the REST path,
  // so no Order is constructed here. Keeping the two transports identical in
  // what they return is what lets the fallback be invisible to a strategy.
  pending.order_callback(OrderResult{true, std::nullopt, {}});
}

void EmsService::poll() {
  if (pending_.empty()) {
    return;
  }
  const double now = now_seconds();
  for (auto it = pending_.begin(); it != pending_.end();) {
    if (now <= it->second.deadline) {
      ++it;
      continue;
    }
    // A reply that never came. Answering with a timeout beats leaving a
    // strategy blocked forever on a request the venue silently dropped.
    AXON_LOG_WARN(log(), "request {} timed out", it->first);
    util::get_metrics().inc_order_place_failure(it->second.exchange, "timeout");
    if (it->second.order_callback) {
      it->second.order_callback(
          OrderResult{false, std::nullopt, "the venue did not reply in time"});
    } else if (it->second.bool_callback) {
      it->second.bool_callback(false, "the venue did not reply in time");
    }
    it = pending_.erase(it);
  }
}

}  // namespace axon::ems
