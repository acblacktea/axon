#include "axon/ems/ems_service.h"

#include "axon/core/clock.h"
#include "axon/ems/venue_ops.h"
#include "axon/util/logging.h"
#include "axon/util/metrics.h"

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

}  // namespace


EmsService::EmsService(const Config& config, net::HttpClient* http)
    : config_(config), http_(http) {}

EmsService::~EmsService() = default;

void EmsService::register_session(const std::string& exchange,
                                  oms::VenueSession* session) {
  // Loud at startup rather than at the first order. A session can be perfectly
  // live for a venue this build cannot construct a request for, and that
  // combination used to surface as "order entry is unavailable: session is
  // live" -- a message that contradicts itself.
  if (ops_for(exchange) == nullptr) {
    AXON_LOG_ERROR(log(), "no order-entry implementation for exchange '{}'",
                   exchange);
  }
  sessions_[exchange] = session;
}

void EmsService::register_trade_session(const std::string& exchange,
                                        oms::VenueSession* session) {
  trade_sessions_[exchange] = session;
}

oms::VenueSession* EmsService::ws_entry_session(const std::string& exchange,
                                               const VenueOps& ops) const {
  const auto& from = ops.own_trade_connection ? trade_sessions_ : sessions_;
  const auto it = from.find(exchange);
  return (it != from.end() && it->second != nullptr && it->second->live())
             ? it->second
             : nullptr;
}

std::string EmsService::order_entry_unavailable(const std::string& exchange,
                                                const VenueOps& ops) const {
  // Reads the same map ws_entry_session just tried, because both are told
  // which one by the same field rather than each working it out.
  const auto& from = ops.own_trade_connection ? trade_sessions_ : sessions_;
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

  const VenueOps* ops = ops_for(exchange);
  if (ops == nullptr) {
    callback(OrderResult{false, std::nullopt,
                         "no order-entry implementation for " + exchange});
    return;
  }

  // Idempotency. A strategy that resubmits after a timeout carries the same
  // internal_order_id, and the first attempt may be live; refusing here is
  // what stops one intent becoming two orders.
  const std::string key = exchange + ":" + request.internal_order_id;
  if (submitted_.count(key) != 0) {
    util::get_metrics().inc_order_place_failure(exchange, "duplicate");
    callback(OrderResult{false, std::nullopt,
                         "duplicate internal_order_id " + request.internal_order_id +
                             ": an order with this id was already sent; confirm "
                             "its state before resubmitting"});
    return;
  }

  if (risk_ != nullptr) {
    if (const auto refused = risk_->check_new_order(exchange, request, now_seconds())) {
      util::get_metrics().inc_order_place_failure(exchange, "risk_" + refused->code);
      AXON_LOG_WARN(log(), "[{}] risk refused {} {}: {}", exchange,
                    request.internal_order_id, request.instrument, refused->message);
      callback(OrderResult{false, std::nullopt, "risk: " + refused->message});
      return;
    }
  }

  if (place_via_websocket(exchange, *ops, request, callback)) {
    remember_submission(key);
    if (risk_ != nullptr) {
      // Counted from the moment it leaves, not from the ack: a burst sent
      // before the first ack must still add up against the position limit.
      risk_->on_order_sent(exchange, request, now_seconds());
    }
    return;
  }
  // There is no second way to place an order. A venue whose session is not
  // live cannot trade, and saying so immediately beats queueing an order
  // against a connection that may be minutes from coming back -- by which
  // point the price the strategy decided on is long gone.
  callback(
      OrderResult{false, std::nullopt, order_entry_unavailable(exchange, *ops)});
}

bool EmsService::place_via_websocket(const std::string& exchange,
                                     const VenueOps& ops,
                                     const models::OrderRequest& request,
                                     const OrderCallback& callback) {
  oms::VenueSession* session = ws_entry_session(exchange, ops);
  if (session == nullptr) {
    util::get_metrics().inc_ems_request_error(exchange, "place_order",
                                              "session_down");
    return false;
  }

  char buf[kMaxRequestBytes];

  const BuildContext ctx{exchange_config(exchange),
                         core::wall_clock_ns() / 1'000'000LL};
  std::int64_t id = 0;
  const std::size_t n = ops.build_place(buf, sizeof(buf), request, ctx, id);
  if (n == 0) {
    // Could not be built -- too long, or a label Binance cannot sign as sent.
    // There is no other encoding to fall back to, so this becomes a rejection.
    return false;
  }

  Pending pending;
  pending.is_place = true;
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
                                      const VenueOps& ops,
                                      const std::string& order_id,
                                      const std::string& symbol,
                                      const BoolCallback& callback) {
  oms::VenueSession* session = ws_entry_session(exchange, ops);
  if (session == nullptr) {
    return false;
  }
  if (ops.needs_symbol && symbol.empty()) {
    return false;
  }

  char buf[kMaxRequestBytes];
  const BuildContext ctx{exchange_config(exchange),
                         core::wall_clock_ns() / 1'000'000LL};
  std::int64_t id = 0;
  const std::size_t n =
      ops.build_cancel(buf, sizeof(buf), symbol, order_id, ctx, id);
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
                                      const VenueOps& ops,
                                      const models::Order& existing,
                                      std::optional<core::Qty> amount,
                                      std::optional<core::Price> price,
                                      const OrderCallback& callback) {
  oms::VenueSession* session = ws_entry_session(exchange, ops);
  if (session == nullptr) {
    return false;
  }

  char buf[kMaxRequestBytes];
  const BuildContext ctx{exchange_config(exchange),
                         core::wall_clock_ns() / 1'000'000LL};
  std::int64_t id = 0;
  const std::size_t n =
      ops.build_modify(buf, sizeof(buf), existing, amount, price, ctx, id);
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
  const VenueOps* ops = ops_for(exchange);
  if (ops == nullptr) {
    callback(false, "no order-entry implementation for " + exchange);
    return;
  }

  // Most venues need the symbol, and a strategy only holds an id.
  std::string symbol;
  if (ops->needs_symbol && lookup_) {
    if (const auto order = lookup_(order_id); order.has_value()) {
      symbol = order->instrument;
    }
  }
  if (ops->needs_symbol && symbol.empty()) {
    callback(false, "order " + order_id +
                        " is not known locally, so its symbol cannot be "
                        "resolved for cancellation");
    return;
  }

  if (cancel_via_websocket(exchange, *ops, order_id, symbol, callback)) {
    return;
  }
  callback(false, order_entry_unavailable(exchange, *ops));
}

void EmsService::modify_order(const std::string& exchange,
                              const std::string& order_id,
                              std::optional<core::Qty> amount,
                              std::optional<core::Price> price,
                              OrderCallback callback) {
  const VenueOps* ops = ops_for(exchange);
  if (ops == nullptr) {
    callback(OrderResult{false, std::nullopt,
                         "no order-entry implementation for " + exchange});
    return;
  }

  models::Order existing;
  if (!ops->needs_symbol) {
    // Deribit amends by order id alone, so nothing HAS to be looked up -- but
    // the risk check needs the order's instrument, side and size, so use the
    // local copy when there is one.
    existing.order_id = order_id;
    if (lookup_) {
      if (auto found = lookup_(order_id); found.has_value()) {
        existing = std::move(*found);
      }
    }
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

  if (risk_ != nullptr) {
    if (const auto refused =
            risk_->check_modify(exchange, existing, amount, price, now_seconds())) {
      util::get_metrics().inc_order_place_failure(exchange, "risk_" + refused->code);
      AXON_LOG_WARN(log(), "[{}] risk refused amend of {}: {}", exchange, order_id,
                    refused->message);
      callback(OrderResult{false, std::nullopt, "risk: " + refused->message});
      return;
    }
  }

  if (modify_via_websocket(exchange, *ops, existing, amount, price, callback)) {
    return;
  }
  callback(
      OrderResult{false, std::nullopt, order_entry_unavailable(exchange, *ops)});
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
    if (pending.is_place) {
      // A definite no: nothing is live, so the same id may be retried.
      const std::string key = exchange + ":" + pending.request.internal_order_id;
      submitted_.erase(key);
      if (risk_ != nullptr) {
        risk_->on_order_rejected(exchange, pending.request);
      }
    }
    pending.order_callback(OrderResult{false, std::nullopt, std::string(error)});
    return;
  }

  const VenueOps* ops = ops_for(exchange);
  if (ops == nullptr) {
    // Unreachable in practice -- nothing can be in flight for a venue with no
    // ops -- but a reply that matched a pending entry must always answer it.
    pending.order_callback(
        OrderResult{false, std::nullopt, "no order-entry implementation for " + exchange});
    return;
  }
  pending.order_callback(ops->interpret_reply(payload, pending.request));
}

void EmsService::remember_submission(const std::string& key) {
  if (!submitted_.insert(key).second) {
    return;
  }
  submitted_order_.push_back(key);
  while (submitted_order_.size() > kMaxRememberedSubmissions) {
    submitted_.erase(submitted_order_.front());
    submitted_order_.pop_front();
  }
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
      // Sent, unanswered: the venue may well have acted on it. Unknown, not
      // failed -- and the id stays remembered so a blind retry is refused.
      OrderResult unknown{false, std::nullopt, "the venue did not reply in time"};
      unknown.outcome_unknown = true;
      it->second.order_callback(unknown);
    } else if (it->second.bool_callback) {
      it->second.bool_callback(false, "the venue did not reply in time");
    }
    it = pending_.erase(it);
  }
}

}  // namespace axon::ems
