#include "axon/risk/risk_manager.h"

#include <cmath>
#include <cstdio>
#include <limits>

#include "axon/util/logging.h"

namespace axon::risk {
namespace {

auto& log() {
  static auto logger = util::get_logger("risk");
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

const char* to_string(KillSource source) noexcept {
  switch (source) {
    case KillSource::kOperator: return "operator";
    case KillSource::kSignal: return "signal";
    case KillSource::kFile: return "file";
    case KillSource::kLoss: return "loss";
  }
  return "?";
}

RiskManager::RiskManager(RiskConfig config) : config_(std::move(config)) {
  for (const auto& [k, _] : config_.instruments) {
    const auto colon = k.find(':');
    if (colon == std::string::npos || colon == 0 || colon + 1 == k.size()) {
      continue;
    }
    reference_targets_.push_back({k.substr(0, colon), k.substr(colon + 1)});
  }
}

bool RiskManager::reference_refresh_due(double now_seconds) {
  if (config_.reference_refresh_seconds <= 0 || reference_targets_.empty()) {
    return false;
  }
  if (last_reference_refresh_ >= 0.0 &&
      now_seconds - last_reference_refresh_ < config_.reference_refresh_seconds) {
    return false;
  }
  last_reference_refresh_ = now_seconds;
  return true;
}

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
               request.amount, request.price, request.post_only,
               request.strategy_id.value_or(std::string()),
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
               remaining.raw() > 0 ? remaining : core::Qty{}, new_price, existing.post_only,
               existing.strategy_id.value_or(std::string()), /*count_rate=*/false,
               order_key(exchange, existing), now_seconds);
}

std::optional<RiskRejection> RiskManager::check(
    const std::string& exchange, const std::string& instrument, models::OrderSide side,
    models::OrderType type, core::Qty quantity, std::optional<core::Price> price,
    bool post_only, const std::string& strategy, bool count_rate,
    const std::string& replacing_key, double now) {
  // The kill switch is not a limit and is not covered by `enabled`.
  if (kill_engaged_) {
    return RiskRejection{"kill_switch", "kill switch engaged: " + kill_reason_};
  }
  // Nor is a blind exchange: without its feed, no limit below has a true
  // position to work from.
  if (const auto it = halted_.find(exchange); it != halted_.end()) {
    return RiskRejection{"exchange_down", it->second};
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

  // --- self-trade --------------------------------------------------------------
  // A post-only order never takes, so it can never trade with our own.
  if (config_.self_trade_prevention && !post_only) {
    if (auto refused =
            check_self_trade(exchange, instrument, side, is_limit ? price : std::nullopt, now)) {
      return refused;
    }
  }

  return std::nullopt;
}

// Would this order trade against one of our own resting on the other side?
// A limit order does if its price reaches ours. A market order takes the best
// price on the book, which we cannot see from here: ours is treated as the
// best when it is at or through the reference price -- and when there is no
// fresh reference to compare with, any of ours counts. Fail closed.
std::optional<RiskRejection> RiskManager::check_self_trade(
    const std::string& exchange, const std::string& instrument, models::OrderSide side,
    std::optional<core::Price> limit_price, double now) const {
  const bool buy = side == models::OrderSide::kBuy;
  const auto reference = limit_price.has_value() ? std::nullopt
                                                 : fresh_reference(exchange, instrument, now);
  // Only the other side can be traded against -- which also leaves out the
  // order an amend is changing.
  for (const auto& [_, w] : working_) {
    if (w.side == side || w.exchange != exchange || w.instrument != instrument ||
        !w.price.has_value()) {
      continue;
    }
    bool crosses = false;
    if (limit_price.has_value()) {
      crosses = buy ? *limit_price >= *w.price : *limit_price <= *w.price;
    } else if (reference.has_value()) {
      crosses = buy ? *w.price <= *reference : *w.price >= *reference;
    } else {
      crosses = true;
    }
    if (crosses) {
      return RiskRejection{
          "self_trade", std::string(buy ? "buy" : "sell") +
                            (limit_price ? " at " + limit_price->to_string() : std::string(" at market")) +
                            " would trade against our own " + (buy ? "sell" : "buy") + " at " +
                            w.price->to_string() + " on " + instrument};
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
  if (request.order_type == models::OrderType::kLimit) {
    w.price = request.price;
  }
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
  if (order.order_type == models::OrderType::kLimit) {
    w.price = order.price;
  }
  working_[k] = std::move(w);
}

void RiskManager::on_fill(const models::Fill& fill) {
  const auto k = key(fill.exchange, fill.instrument);
  auto& pos = positions_[k];
  pos = pos + signed_qty(fill.side, fill.amount);

  // PnL counts this session's fills only. A recovered fill from before the
  // engine started is already in the position it started with.
  if (!pnl_tracked(fill.exchange) || fill.timestamp.millis() < pnl_session_start_ms_) {
    return;
  }
  auto& book = pnl_[k];
  const double qty = signed_qty(fill.side, fill.amount).to_double();
  book.fill_qty += qty;
  book.cash -= qty * fill.price.to_double();
  // A fee in a dollar stablecoin is a cost in the quote currency. One charged
  // in anything else (BNB) cannot be valued here and is left out.
  if (fill.fee_currency == "USDT" || fill.fee_currency == "USDC" ||
      fill.fee_currency == "USD") {
    book.cash -= fill.fee.to_double();
  }
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
  const auto k = key(exchange, instrument);
  references_[k] = Reference{price, now_seconds};
  if (pnl_tracked(exchange)) {
    auto& book = pnl_[k];
    if (!book.start_mark.has_value()) {
      book.start_mark = price.to_double();
    }
  }
}

// ---------------------------------------------------------------------------
bool RiskManager::pnl_tracked(const std::string& exchange) const {
  for (const auto& e : config_.pnl_exclude_exchanges) {
    if (e == exchange) return false;
  }
  return true;
}

void RiskManager::start_pnl_session(std::int64_t wall_ms) {
  pnl_session_start_ms_ = wall_ms;
  pnl_day_ = wall_ms / 86'400'000;
  loss_tripped_ = false;
  pnl_.clear();
}

void RiskManager::on_wall_clock(std::int64_t wall_ms) {
  const std::int64_t day = wall_ms / 86'400'000;
  if (pnl_day_ < 0 || day == pnl_day_) {
    return;
  }
  // A new day: what is held now is the new starting position, valued from
  // the latest reference price.
  pnl_session_start_ms_ = day * 86'400'000;
  pnl_day_ = day;
  loss_tripped_ = false;
  for (auto& [k, book] : pnl_) {
    book.cash = 0.0;
    book.fill_qty = 0.0;
    const auto ref = references_.find(k);
    book.start_mark = ref == references_.end() ? std::nullopt
                                               : std::optional<double>(ref->second.price.to_double());
  }
  AXON_LOG_INFO(log(), "new PnL session for UTC day {}", day);
}

RiskManager::Pnl RiskManager::pnl(double now_seconds) const {
  Pnl out;
  for (const auto& [k, book] : pnl_) {
    const auto colon = k.find(':');
    const std::string exchange = k.substr(0, colon);
    const std::string instrument = k.substr(colon + 1);
    const double held = position(exchange, instrument).to_double();
    // What was held when the session started: today's fills taken back out.
    const double held_at_start = held - book.fill_qty;
    const auto mark = fresh_reference(exchange, instrument, now_seconds);
    const bool needs_mark = held != 0.0;
    const bool needs_start_mark = held_at_start != 0.0;
    if ((needs_mark && !mark.has_value()) || (needs_start_mark && !book.start_mark.has_value())) {
      ++out.unvalued;
      continue;
    }
    double value = book.cash;
    if (needs_mark) value += held * mark->to_double();
    if (needs_start_mark) value -= held_at_start * *book.start_mark;
    out.total += value;
  }
  return out;
}

void RiskManager::check_loss(double now_seconds) {
  if (!config_.max_loss.has_value() || loss_tripped_) {
    return;
  }
  const Pnl p = pnl(now_seconds);
  if (p.total >= -*config_.max_loss) {
    return;
  }
  loss_tripped_ = true;
  char reason[160];
  std::snprintf(reason, sizeof reason, "PnL %.2f breached max_loss %.2f%s", p.total,
                *config_.max_loss,
                p.unvalued > 0 ? " (some positions could not be valued)" : "");
  engage_kill_switch(KillSource::kLoss, reason);
}

// ---------------------------------------------------------------------------
void RiskManager::observe_exchange_link(const std::string& exchange, bool feed_live,
                                        double now_seconds) {
  if (feed_live) {
    down_since_.erase(exchange);
    if (const auto it = halted_.find(exchange); it != halted_.end()) {
      AXON_LOG_WARN(log(), "[{}] private feed is back; new orders allowed again", exchange);
      halted_.erase(it);
    }
    return;
  }
  if (config_.exchange_down_halt_seconds <= 0.0 || halted_.count(exchange) != 0) {
    return;
  }
  const double since = down_since_.emplace(exchange, now_seconds).first->second;
  if (now_seconds - since < config_.exchange_down_halt_seconds) {
    return;
  }
  char reason[160];
  std::snprintf(reason, sizeof reason,
                "%s private feed down for %.0fs: fills cannot be seen, new orders halted",
                exchange.c_str(), now_seconds - since);
  AXON_LOG_ERROR(log(), "[{}] {}", exchange, reason);
  halted_[exchange] = reason;
}

// ---------------------------------------------------------------------------
void RiskManager::engage_kill_switch(KillSource source, std::string reason) {
  if (kill_engaged_) return;
  kill_engaged_ = true;
  kill_source_ = source;
  kill_reason_ = std::move(reason);
  AXON_LOG_ERROR(log(), "KILL SWITCH ENGAGED ({}): {} -- no new orders will be sent",
                 to_string(source), kill_reason_);
  if (on_kill_engaged_) {
    on_kill_engaged_(kill_reason_);
  }
}

void RiskManager::release_kill_switch(KillSource source) {
  if (!kill_engaged_) return;
  // The file may only release what the file engaged.
  if (source == KillSource::kFile && kill_source_ != KillSource::kFile) return;
  kill_engaged_ = false;
  AXON_LOG_WARN(log(), "kill switch released by {} (was {}: {})", to_string(source),
                to_string(kill_source_), kill_reason_);
  kill_reason_.clear();
}

void RiskManager::engage_kill_switch(std::string reason) {
  engage_kill_switch(KillSource::kOperator, std::move(reason));
}

void RiskManager::release_kill_switch() { release_kill_switch(KillSource::kOperator); }

void RiskManager::observe_kill_file(bool present, const std::string& path) {
  if (present) {
    engage_kill_switch(KillSource::kFile, "kill switch file " + path);
  } else {
    release_kill_switch(KillSource::kFile);
  }
}

}  // namespace axon::risk
