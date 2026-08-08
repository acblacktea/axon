#include "calais/oms/order_store.h"

#include "calais/core/clock.h"
#include "calais/util/logging.h"
#include "calais/util/metrics.h"

namespace calais::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.orders");
  return logger;
}

double monotonic_seconds() {
  return static_cast<double>(core::monotonic_ns()) / 1e9;
}

// models::is_terminal() is the same predicate; this exists only so the three
// statuses that trigger eviction and once-only notification are spelled out
// where the logic that depends on them lives.
bool terminal_status(models::OrderStatus s) {
  return s == models::OrderStatus::kFilled || s == models::OrderStatus::kCancelled ||
         s == models::OrderStatus::kRejected;
}

}  // namespace

OrderStore::OrderStore(std::shared_ptr<OrderRepository> repository)
    : repository_(repository ? std::move(repository)
                             : std::make_shared<InMemoryOrderRepository>()) {}

void OrderStore::add_order(models::Order order) {
  // Force PENDING with zero filled regardless of what the placement response
  // said. See (2) in the header: a market order that comes back already FILLED
  // would otherwise make the first venue push look like "no change", and the
  // strategy would never be told it filled.
  order.status = models::OrderStatus::kPending;
  order.filled_amount = core::Qty{};

  const std::string id = order.order_id;
  cache_[id] = order;
  submit_monotonic_[id] = monotonic_seconds();

  try {
    repository_->save(order);
  } catch (const std::exception& e) {
    CALAIS_LOG_ERROR(log(), "failed to persist new order {}: {}", id, e.what());
    util::get_metrics().inc_db_write_failure("orders");
  }

  CALAIS_LOG_INFO(log(), "added order {}: {} {} {}", id, order.instrument,
                  models::to_string(order.side), order.amount.to_string());
  // Deliberately no notification here.
}

void OrderStore::update_order(models::Order order) {
  const std::string id = order.order_id;

  std::optional<models::Order> existing;
  if (const auto it = cache_.find(id); it != cache_.end()) {
    existing = it->second;
  } else {
    existing = repository_->get(id);
  }

  // (1) Stale-update guard. A REST reconciliation response can arrive after a
  // newer WebSocket push; applying it would roll the order backwards.
  if (existing.has_value() && existing->updated_at > order.updated_at) {
    CALAIS_LOG_WARN(log(), "dropping stale update for {}: existing={} incoming={}",
                    id, existing->updated_at.to_iso8601(),
                    order.updated_at.to_iso8601());
    return;
  }

  const bool state_changed = !existing.has_value() ||
                             existing->status != order.status ||
                             existing->filled_amount != order.filled_amount;

  // The venue does not know about our internal ids; carry them forward.
  if (existing.has_value()) {
    if (!order.strategy_id.has_value() && existing->strategy_id.has_value()) {
      order.strategy_id = existing->strategy_id;
    }
    if (!order.internal_order_id.has_value() &&
        existing->internal_order_id.has_value()) {
      order.internal_order_id = existing->internal_order_id;
    }
  }

  cache_[id] = order;
  try {
    repository_->update(order);
  } catch (const std::exception& e) {
    CALAIS_LOG_ERROR(log(), "failed to persist order {}: {}", id, e.what());
    util::get_metrics().inc_db_write_failure("orders");
  }

  const bool terminal = terminal_status(order.status);
  const bool already_notified_terminal =
      terminal && notified_terminal_.count(id) > 0;

  if (!state_changed || already_notified_terminal) {
    if (already_notified_terminal) {
      // (3) A stale read slipped past the state-changed check. Reporting the
      // same fill twice to a strategy is worse than reporting it late.
      CALAIS_LOG_WARN(log(), "suppressing duplicate terminal notification for {}",
                      id);
    }
    if (terminal) {
      cache_.erase(id);  // (4)
    }
    return;
  }

  if (terminal) {
    const double now = monotonic_seconds();
    notified_terminal_[id] = now;
    purge_expired_terminals(now);
    record_terminal_latency(order);
  }
  if (order.status == models::OrderStatus::kRejected) {
    util::get_metrics().inc_order_rejected(order.exchange, "exchange");
  }

  CALAIS_LOG_INFO(log(), "updated order {}: status={} filled={}/{}", id,
                  models::to_string(order.status), order.filled_amount.to_string(),
                  order.amount.to_string());

  if (terminal) {
    cache_.erase(id);  // (4)
  }

  notify(order);
}

void OrderStore::update_from_ws(models::Order order) {
  const std::string id = order.order_id;

  // The gap between placement and the venue's first push is the ack latency --
  // how long the feed took to confirm an order we already sent.
  if (first_ws_seen_.insert(id).second) {
    const auto it = submit_monotonic_.find(id);
    if (it != submit_monotonic_.end()) {
      util::get_metrics().observe_order_ack_latency(
          order.exchange, monotonic_seconds() - it->second);
    }
  }
  update_order(std::move(order));
}

std::optional<models::Order> OrderStore::get_order(const std::string& order_id) {
  if (const auto it = cache_.find(order_id); it != cache_.end()) {
    return it->second;
  }
  auto stored = repository_->get(order_id);
  if (stored.has_value()) {
    cache_[order_id] = *stored;
  }
  return stored;
}

std::vector<models::Order> OrderStore::all_orders() const {
  std::vector<models::Order> out;
  out.reserve(cache_.size());
  for (const auto& [_, order] : cache_) {
    out.push_back(order);
  }
  return out;
}

std::vector<models::Order> OrderStore::active_orders() const {
  std::vector<models::Order> out;
  for (const auto& [_, order] : cache_) {
    if (order.is_active()) {
      out.push_back(order);
    }
  }
  return out;
}

void OrderStore::reconcile(const std::vector<models::Order>& orders) {
  CALAIS_LOG_INFO(log(), "reconciling {} orders from the venue", orders.size());
  for (const auto& order : orders) {
    if (const auto it = cache_.find(order.order_id); it != cache_.end()) {
      const auto& existing = it->second;
      if (existing.status != order.status ||
          existing.filled_amount != order.filled_amount) {
        // Loud on purpose: a correction here means the WebSocket feed lost
        // something, and reconciliation quietly papering over it is how a
        // broken feed goes unnoticed for weeks.
        CALAIS_LOG_WARN(log(),
                        "reconciliation fix for {}: status {} -> {}, filled {} -> {}",
                        order.order_id, models::to_string(existing.status),
                        models::to_string(order.status),
                        existing.filled_amount.to_string(),
                        order.filled_amount.to_string());
      }
    }
    update_order(order);
  }
}

void OrderStore::register_update_callback(UpdateCallback callback) {
  callbacks_.push_back(std::move(callback));
}

void OrderStore::clear_callbacks() { callbacks_.clear(); }

void OrderStore::notify(const models::Order& order) {
  for (const auto& callback : callbacks_) {
    try {
      callback(order);
    } catch (const std::exception& e) {
      // One misbehaving subscriber must not stop the others from being told.
      CALAIS_LOG_ERROR(log(), "order update callback threw: {}", e.what());
    }
  }
}

void OrderStore::record_terminal_latency(const models::Order& order) {
  const auto it = submit_monotonic_.find(order.order_id);
  first_ws_seen_.erase(order.order_id);
  if (it == submit_monotonic_.end()) {
    return;  // not an order we placed; nothing to measure against
  }
  const double elapsed = monotonic_seconds() - it->second;
  submit_monotonic_.erase(it);
  util::get_metrics().observe_order_fill_latency(
      order.exchange, std::string(models::to_string(order.status)), elapsed);
}

void OrderStore::purge_expired_terminals(double now_seconds) {
  for (auto it = notified_terminal_.begin(); it != notified_terminal_.end();) {
    if (now_seconds - it->second > kTerminalTtlSeconds) {
      it = notified_terminal_.erase(it);
    } else {
      ++it;
    }
  }
}

}  // namespace calais::oms
