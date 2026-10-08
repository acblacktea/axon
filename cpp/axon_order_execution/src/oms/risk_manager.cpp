#include "axon/oms/risk_manager.h"

#include <cmath>
#include <cstdio>
#include <limits>

#include "axon/util/logging.h"

namespace axon::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.risk");
  return logger;
}

core::Qty signed_qty(models::OrderSide side, core::Qty q) {
  return side == models::OrderSide::kBuy ? q : -q;
}

std::string fmt_pct(double v) {
  char buf[32];
  std::snprintf(buf, sizeof buf, "%.2f%%", v * 100.0);
  return buf;
}

}  // namespace

RiskManager::RiskManager(RiskConfig config) : config_(std::move(config)) {}

std::string RiskManager::key(std::string_view exchange, std::string_view instrument) {
  std::string k(exchange);
  k += ':';
  k.append(instrument);
  return k;
}

std::string RiskManager::order_key(std::string_view exchange, const models::Order& order) {
  const auto& id = order.internal_order_id.has_value() && !order.internal_order_id->empty()
                       ? *order.internal_order_id
                       : order.order_id;
  return key(exchange, id);
}

RiskLimits RiskManager::limits_for(const std::string& exchange,
                                   const std::string& instrument) const {
  RiskLimits merged = config_.defaults;
  auto overlay = [&](const RiskLimits& l) {
    if (l.max_order_qty) merged.max_order_qty = l.max_order_qty;
    if (l.max_order_notional) merged.max_order_notional = l.max_order_notional;
    if (l.max_position) merged.max_position = l.max_position;
    if (l.max_price_deviation) merged.max_price_deviation = l.max_price_deviation;
  };
  // The bare instrument first, then the exchange-qualified entry over it.
  if (const auto it = config_.instruments.find(instrument); it != config_.instruments.end()) {
    overlay(it->second);
  }
  if (const auto it = config_.instruments.find(key(exchange, instrument));
      it != config_.instruments.end()) {
    overlay(it->second);
  }
  return merged;
}

std::optional<core::Price> RiskManager::fresh_reference(const std::string& exchange,
                                                        const std::string& instrument,
                                                        double now) const {
  const auto it = references_.find(key(exchange, instrument));
  if (it == references_.end()) {
    return std::nullopt;
  }
  if (now - it->second.at > config_.reference_price_max_age_seconds) {
    return std::nullopt;
  }
  return it->second.price;
}

core::Qty RiskManager::position(const std::string& exchange,
                                const std::string& instrument) const {
  const auto it = positions_.find(key(exchange, instrument));
  return it == positions_.end() ? core::Qty{} : it->second;
}

RiskManager::Exposure RiskManager::exposure(const std::string& exchange,
                                            const std::string& instrument) const {
  const core::Qty pos = position(exchange, instrument);
  Exposure e{pos, pos};
  for (const auto& [_, w] : working_) {
    if (w.exchange != exchange || w.instrument != instrument) continue;
    if (w.side == models::OrderSide::kBuy) {
      e.long_side = e.long_side + w.remaining;
    } else {
      e.short_side = e.short_side - w.remaining;
    }
  }
  return e;
}

// ---------------------------------------------------------------------------
std::optional<RiskRejection> RiskManager::check_new_order(const std::string& exchange,
                                                          const models::OrderRequest& request,
                                                          double now_seconds) {
  return check(exchange, request.instrument, request.side, request.order_type,
               request.amount, request.price, request.strategy_id.value_or(std::string()),
               /*count_rate=*/true, /*replacing_key=*/{}, now_seconds);
}

std::optional<RiskRejection> RiskManager::check_modify(const std::string& exchange,
                                                       const models::Order& existing,
                                                       std::optional<core::Qty> amount,
                                                       std::optional<core::Price> price,
                                                       double now_seconds) {
  // The order as it would stand after the amend. What has already filled is
  // in the position; what remains is the new amount less those fills.
  const core::Qty new_amount = amount.value_or(existing.amount);
  const core::Qty remaining = new_amount - existing.filled_amount;
  const std::optional<core::Price> new_price = price.has_value() ? price : existing.price;
  return check(exchange, existing.instrument, existing.side, existing.order_type,
               remaining.raw() > 0 ? remaining : core::Qty{}, new_price,
               existing.strategy_id.value_or(std::string()), /*count_rate=*/false,
               order_key(exchange, existing), now_seconds);
}

std::optional<RiskRejection> RiskManager::check(
    const std::string& exchange, const std::string& instrument, models::OrderSide side,
    models::OrderType type, core::Qty quantity, std::optional<core::Price> price,
    const std::string& strategy, bool count_rate, const std::string& replacing_key,
    double now) {
  // The kill switch is not a limit and is not covered by `enabled`.
  if (kill_engaged_) {
    return RiskRejection{"kill_switch", "kill switch engaged: " + kill_reason_};
  }
  if (!config_.enabled) {
    return std::nullopt;
  }

  // --- rate ------------------------------------------------------------------
  if (count_rate && config_.max_orders_per_strategy_per_second > 0) {
    auto& sends = sends_[strategy];
    while (!sends.empty() && now - sends.front() >= 1.0) sends.pop_front();
    if (sends.size() >= static_cast<std::size_t>(config_.max_orders_per_strategy_per_second)) {
      return RiskRejection{
          "rate", "strategy '" + strategy + "' exceeded " +
                      std::to_string(config_.max_orders_per_strategy_per_second) +
                      " orders/second"};
    }
  }

  const RiskLimits limits = limits_for(exchange, instrument);

  // --- order size --------------------------------------------------------------
  if (limits.max_order_qty && quantity > *limits.max_order_qty) {
    return RiskRejection{"order_qty", "quantity " + quantity.to_string() + " exceeds " +
                                          limits.max_order_qty->to_string() + " for " +
                                          instrument};
  }

  const bool is_limit = type == models::OrderType::kLimit && price.has_value();
  const auto reference = fresh_reference(exchange, instrument, now);

  if (limits.max_order_notional) {
    // A limit order is valued at its own price; a market order at the
    // reference, since its fill price is not known until it fills.
    const std::optional<core::Price> value_at = is_limit ? price : reference;
    if (!value_at.has_value()) {
      return RiskRejection{"no_reference_price",
                           "no fresh reference price for " + instrument +
                               " to value a market order against max_order_notional"};
    }
    const auto notional = value_at->checked_mul(quantity);
    if (!notional.has_value() || notional->abs() > *limits.max_order_notional) {
      return RiskRejection{"order_notional",
                           "notional " +
                               (notional ? notional->abs().to_string() : std::string("overflow")) +
                               " exceeds " + limits.max_order_notional->to_string() + " for " +
                               instrument};
    }
  }

  // --- price deviation ---------------------------------------------------------
  if (limits.max_price_deviation && is_limit) {
    if (!reference.has_value()) {
      return RiskRejection{"no_reference_price",
                           "no fresh reference price for " + instrument +
                               " to check the limit price against"};
    }
    const double ref = reference->to_double();
    const double deviation = ref > 0.0 ? std::fabs(price->to_double() - ref) / ref
                                       : std::numeric_limits<double>::infinity();
    if (deviation > *limits.max_price_deviation) {
      return RiskRejection{"price_deviation",
                           "price " + price->to_string() + " is " + fmt_pct(deviation) +
                               " from reference " + reference->to_string() + " (limit " +
                               fmt_pct(*limits.max_price_deviation) + ")"};
    }
  }

  // --- position --------------------------------------------------------------
  if (limits.max_position) {
    Exposure e = exposure(exchange, instrument);
    if (!replacing_key.empty()) {
      // An amend replaces this order's remaining quantity rather than adding.
      if (const auto it = working_.find(replacing_key); it != working_.end()) {
        if (it->second.side == models::OrderSide::kBuy) {
          e.long_side = e.long_side - it->second.remaining;
        } else {
          e.short_side = e.short_side + it->second.remaining;
        }
      }
    }
    // Only the side this order grows can be pushed over the limit; an order
    // that reduces the position is always allowed through this check.
    const bool buy = side == models::OrderSide::kBuy;
    const core::Qty after = buy ? e.long_side + quantity : e.short_side - quantity;
    const bool breached = buy ? after > *limits.max_position : after < -*limits.max_position;
    if (breached) {
      return RiskRejection{"position",
                           "worst-case position " + after.to_string() + " would exceed " +
                               limits.max_position->to_string() + " for " + instrument +
                               " (held " + position(exchange, instrument).to_string() + ")"};
    }
  }

  return std::nullopt;
}

// ---------------------------------------------------------------------------
void RiskManager::on_order_sent(const std::string& exchange,
                                const models::OrderRequest& request, double now_seconds) {
  WorkingOrder w;
  w.exchange = exchange;
  w.instrument = request.instrument;
  w.side = request.side;
  w.remaining = request.amount;
  working_[key(exchange, request.internal_order_id)] = std::move(w);
  if (config_.max_orders_per_strategy_per_second > 0) {
    sends_[request.strategy_id.value_or(std::string())].push_back(now_seconds);
  }
}

void RiskManager::on_order_rejected(const std::string& exchange,
                                    const models::OrderRequest& request) {
  working_.erase(key(exchange, request.internal_order_id));
}

void RiskManager::on_order_update(const models::Order& order) {
  const auto k = order_key(order.exchange, order);
  const core::Qty remaining = order.amount - order.filled_amount;
  if (!order.is_active() || remaining.raw() <= 0) {
    working_.erase(k);
    return;
  }
  WorkingOrder w;
  w.exchange = order.exchange;
  w.instrument = order.instrument;
  w.side = order.side;
  w.remaining = remaining;
  working_[k] = std::move(w);
}

void RiskManager::on_fill(const models::Fill& fill) {
  auto& pos = positions_[key(fill.exchange, fill.instrument)];
  pos = pos + signed_qty(fill.side, fill.amount);
}

void RiskManager::on_positions(const std::string& exchange,
                               const std::vector<models::Position>& positions) {
  const std::string prefix = exchange + ":";
  for (auto it = positions_.begin(); it != positions_.end();) {
    it = it->first.rfind(prefix, 0) == 0 ? positions_.erase(it) : std::next(it);
  }
  for (const auto& p : positions) {
    positions_[key(exchange, p.instrument)] = p.direction == "sell" ? -p.size : p.size;
  }
}

void RiskManager::update_reference_price(const std::string& exchange,
                                         const std::string& instrument, core::Price price,
                                         double now_seconds) {
  if (price.raw() <= 0) return;
  references_[key(exchange, instrument)] = Reference{price, now_seconds};
}

// ---------------------------------------------------------------------------
void RiskManager::engage_kill_switch(std::string reason) {
  if (kill_engaged_) return;
  kill_engaged_ = true;
  kill_reason_ = std::move(reason);
  AXON_LOG_ERROR(log(), "KILL SWITCH ENGAGED: {} -- no new orders will be sent",
                 kill_reason_);
}

void RiskManager::release_kill_switch() {
  if (!kill_engaged_) return;
  kill_engaged_ = false;
  AXON_LOG_WARN(log(), "kill switch released (was: {})", kill_reason_);
  kill_reason_.clear();
}

}  // namespace axon::oms
