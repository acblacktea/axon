#include "calais/ems/ems_service.h"

#include "calais/core/clock.h"
#include "calais/net/crypto_lite.h"
#include "calais/util/logging.h"
#include "calais/util/metrics.h"
#include "calais/venue/binance/binance_builder.h"
#include "calais/venue/bybit/bybit_builder.h"
#include "calais/venue/deribit/deribit_builder.h"
#include "calais/venue/json_view.h"
#include "calais/venue/okx/okx_builder.h"

namespace calais::ems {
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

}  // namespace

EmsService::EmsService(const Config& config, net::HttpClient* http)
    : config_(config), http_(http) {}

EmsService::~EmsService() = default;

void EmsService::register_session(const std::string& exchange,
                                  oms::VenueSession* session) {
  sessions_[exchange] = session;
}

void EmsService::register_rest(const std::string& exchange, oms::VenueRest* rest) {
  rest_[exchange] = rest;
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

  if (exchange == "deribit") {
    if (!place_via_websocket(exchange, request, std::move(callback))) {
      return;
    }
    return;
  }
  place_via_rest(exchange, request, std::move(callback));
}

bool EmsService::place_via_websocket(const std::string& exchange,
                                     const models::OrderRequest& request,
                                     OrderCallback callback) {
  const auto it = sessions_.find(exchange);
  if (it == sessions_.end() || it->second == nullptr || !it->second->live()) {
    callback(OrderResult{false, std::nullopt,
                         "no live session for " + exchange});
    return false;
  }

  // One builder per session would be cleaner, but the id has to be unique per
  // CONNECTION and the session owns that lifetime, so it lives here keyed by
  // venue and is reset when a session reconnects.
  static thread_local std::map<std::string, venue::deribit::DeribitBuilder> builders;
  auto& builder = builders[exchange];

  char buf[venue::deribit::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = builder.place_order(buf, sizeof(buf), request, id);
  if (n == 0) {
    callback(OrderResult{false, std::nullopt, "could not build the order message"});
    return false;
  }

  Pending pending;
  pending.exchange = exchange;
  pending.order_callback = std::move(callback);
  pending.request = request;
  pending.deadline =
      now_seconds() + static_cast<double>(config_.websocket.request_timeout_seconds);
  pending_[pending_key(exchange, id)] = std::move(pending);

  if (!it->second->send_raw(std::string_view(buf, n))) {
    auto node = pending_.extract(pending_key(exchange, id));
    if (!node.empty() && node.mapped().order_callback) {
      node.mapped().order_callback(
          OrderResult{false, std::nullopt, "send buffer full"});
    }
    return false;
  }
  return true;
}

void EmsService::place_via_rest(const std::string& exchange,
                                const models::OrderRequest& request,
                                OrderCallback callback) {
  const ExchangeConfig* cfg = exchange_config(exchange);
  if (cfg == nullptr) {
    callback(OrderResult{false, std::nullopt, "exchange not configured: " + exchange});
    return;
  }
  if (http_ == nullptr) {
    callback(OrderResult{false, std::nullopt, "no HTTP client available"});
    return;
  }

  const std::int64_t now_ms = core::wall_clock_ns() / 1'000'000LL;
  net::HttpRequest http;
  http.method = "POST";
  http.timeout_seconds =
      static_cast<double>(config_.websocket.request_timeout_seconds);

  if (exchange == "binance") {
    char buf[venue::binance::kMaxRequestBytes];
    std::size_t n = venue::binance::BinanceBuilder::place_order_query(
        buf, sizeof(buf), request, now_ms);
    n = venue::binance::BinanceBuilder::sign_query(buf, n, sizeof(buf),
                                                   cfg->api_secret);
    if (n == 0) {
      callback(OrderResult{false, std::nullopt, "could not build the request"});
      return;
    }
    http.host = std::string(cfg->is_testnet() ? venue::binance::kTestnetRestHost
                                              : venue::binance::kProductionRestHost);
    // Binance takes the signed parameters in the query string, not the body.
    http.path = std::string(venue::binance::kOrderPath) + "?" + std::string(buf, n);
    http.headers.push_back({"X-MBX-APIKEY", cfg->api_key});
  } else if (exchange == "bybit") {
    char buf[venue::bybit::kMaxRequestBytes];
    const std::size_t n =
        venue::bybit::BybitBuilder::place_order_body(buf, sizeof(buf), request);
    if (n == 0) {
      callback(OrderResult{false, std::nullopt, "could not build the request"});
      return;
    }
    http.body.assign(buf, n);
    constexpr int kRecvWindow = 5000;
    const std::string signature = venue::bybit::BybitBuilder::rest_signature(
        cfg->api_secret, now_ms, cfg->api_key, kRecvWindow, http.body);
    http.host = std::string(cfg->is_testnet() ? venue::bybit::kTestnetRestHost
                                              : venue::bybit::kProductionRestHost);
    http.path = std::string(venue::bybit::kCreateOrderPath);
    http.headers.push_back({"X-BAPI-API-KEY", cfg->api_key});
    http.headers.push_back({"X-BAPI-TIMESTAMP", std::to_string(now_ms)});
    http.headers.push_back({"X-BAPI-RECV-WINDOW", std::to_string(kRecvWindow)});
    http.headers.push_back({"X-BAPI-SIGN", signature});
    http.headers.push_back({"Content-Type", "application/json"});
  } else if (exchange == "okx") {
    char buf[venue::okx::kMaxRequestBytes];
    const std::size_t n =
        venue::okx::OkxBuilder::place_order_body(buf, sizeof(buf), request);
    if (n == 0) {
      callback(OrderResult{false, std::nullopt, "could not build the request"});
      return;
    }
    http.body.assign(buf, n);
    // OKX signs an ISO-8601 timestamp with milliseconds, and the SAME string
    // must go in the header. Generating it twice would risk a mismatch.
    const std::string timestamp =
        core::Timestamp::from_millis(now_ms).to_iso8601() + "Z";
    const std::string signature = venue::okx::OkxBuilder::rest_signature(
        cfg->api_secret, timestamp, "POST", venue::okx::kPlaceOrderPath, http.body);
    http.host = std::string(venue::okx::kProductionRestHost);
    http.path = std::string(venue::okx::kPlaceOrderPath);
    http.headers.push_back({"OK-ACCESS-KEY", cfg->api_key});
    http.headers.push_back({"OK-ACCESS-SIGN", signature});
    http.headers.push_back({"OK-ACCESS-TIMESTAMP", timestamp});
    http.headers.push_back({"OK-ACCESS-PASSPHRASE", cfg->passphrase});
    http.headers.push_back({"Content-Type", "application/json"});
    if (cfg->is_testnet()) {
      // OKX's demo trading is the same host with a header, not a separate one.
      http.headers.push_back({"x-simulated-trading", "1"});
    }
  } else {
    callback(OrderResult{false, std::nullopt, "unsupported exchange: " + exchange});
    return;
  }

  const double started = now_seconds();
  const std::string venue_name = exchange;
  http_->submit(std::move(http), [callback = std::move(callback), venue_name,
                                  started](const net::HttpResponse& response) {
    util::get_metrics().observe_ems_request(venue_name, "place_order",
                                            now_seconds() - started);
    if (!response.error.empty()) {
      util::get_metrics().inc_ems_request_error(venue_name, "place_order",
                                                "transport");
      callback(OrderResult{false, std::nullopt, response.error});
      return;
    }
    if (!response.ok()) {
      util::get_metrics().inc_ems_request_error(venue_name, "place_order",
                                                "http_" + std::to_string(response.status));
      callback(OrderResult{false, std::nullopt,
                           "HTTP " + std::to_string(response.status) + ": " +
                               response.body});
      return;
    }
    // The venue accepted it. The authoritative order state arrives on the
    // WebSocket feed a moment later; this only confirms acceptance, which is
    // why no Order is constructed from the REST body here.
    CALAIS_LOG_INFO(log(), "[{}] order accepted: {}", venue_name, response.body);
    callback(OrderResult{true, std::nullopt, {}});
  });
}

// ---------------------------------------------------------------------------
void EmsService::cancel_order(const std::string& exchange,
                              const std::string& order_id, BoolCallback callback) {
  if (exchange != "deribit") {
    const auto it = rest_.find(exchange);
    if (it == rest_.end() || it->second == nullptr) {
      callback(false, "no REST client for " + exchange);
      return;
    }
    // The perp venues cancel by symbol + order id. A strategy only has the id,
    // so it comes from the order store; failing here beats sending a request
    // the venue will reject for a missing symbol.
    std::string symbol;
    if (lookup_) {
      if (const auto order = lookup_(order_id); order.has_value()) {
        symbol = order->instrument;
      }
    }
    if (symbol.empty()) {
      callback(false, "order " + order_id +
                          " is not known locally, so its symbol cannot be "
                          "resolved for cancellation");
      return;
    }
    it->second->cancel_order(symbol, order_id,
                             [callback](std::string, const std::string& error) {
                               callback(error.empty(), error);
                             });
    return;
  }

  const auto it = sessions_.find(exchange);
  if (it == sessions_.end() || it->second == nullptr || !it->second->live()) {
    callback(false, "no live session for " + exchange);
    return;
  }

  static thread_local venue::deribit::DeribitBuilder builder;
  char buf[venue::deribit::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = builder.cancel_order(buf, sizeof(buf), order_id, id);
  if (n == 0) {
    callback(false, "could not build the cancel message");
    return;
  }

  Pending pending;
  pending.exchange = exchange;
  pending.bool_callback = std::move(callback);
  pending.deadline =
      now_seconds() + static_cast<double>(config_.websocket.request_timeout_seconds);
  pending_[pending_key(exchange, id)] = std::move(pending);

  if (!it->second->send_raw(std::string_view(buf, n))) {
    auto node = pending_.extract(pending_key(exchange, id));
    if (!node.empty() && node.mapped().bool_callback) {
      node.mapped().bool_callback(false, "send buffer full");
    }
  }
}

void EmsService::modify_order(const std::string& exchange,
                              const std::string& order_id,
                              std::optional<core::Qty> amount,
                              std::optional<core::Price> price,
                              OrderCallback callback) {
  if (exchange != "deribit") {
    const auto it = rest_.find(exchange);
    if (it == rest_.end() || it->second == nullptr) {
      callback(OrderResult{false, std::nullopt, "no REST client for " + exchange});
      return;
    }
    std::optional<models::Order> existing;
    if (lookup_) {
      existing = lookup_(order_id);
    }
    if (!existing.has_value()) {
      callback(OrderResult{false, std::nullopt,
                           "order " + order_id +
                               " is not known locally, so it cannot be amended"});
      return;
    }
    // Binance will not accept a partial amend -- it needs both quantity and
    // price -- so fill whichever the caller left out from what we already know.
    const auto final_amount = amount.has_value()
                                  ? amount
                                  : std::optional<core::Qty>(existing->amount);
    const auto final_price = price.has_value() ? price : existing->price;
    it->second->amend_order(existing->instrument, order_id, final_amount,
                            final_price,
                            [callback](std::string, const std::string& error) {
                              if (!error.empty()) {
                                callback(OrderResult{false, std::nullopt, error});
                                return;
                              }
                              // The authoritative state arrives on the feed.
                              callback(OrderResult{true, std::nullopt, {}});
                            });
    return;
  }

  const auto it = sessions_.find(exchange);
  if (it == sessions_.end() || it->second == nullptr || !it->second->live()) {
    callback(OrderResult{false, std::nullopt, "no live session for " + exchange});
    return;
  }

  static thread_local venue::deribit::DeribitBuilder builder;
  char buf[venue::deribit::kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n =
      builder.modify_order(buf, sizeof(buf), order_id, amount, price, id);
  if (n == 0) {
    callback(OrderResult{false, std::nullopt, "could not build the modify message"});
    return;
  }

  Pending pending;
  pending.exchange = exchange;
  pending.order_callback = std::move(callback);
  pending.deadline =
      now_seconds() + static_cast<double>(config_.websocket.request_timeout_seconds);
  pending_[pending_key(exchange, id)] = std::move(pending);

  if (!it->second->send_raw(std::string_view(buf, n))) {
    auto node = pending_.extract(pending_key(exchange, id));
    if (!node.empty() && node.mapped().order_callback) {
      node.mapped().order_callback(
          OrderResult{false, std::nullopt, "send buffer full"});
    }
  }
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
    CALAIS_LOG_WARN(log(), "request {} timed out", it->first);
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

}  // namespace calais::ems
