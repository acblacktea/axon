#include "calais/oms/reconciler.h"

#include "calais/core/clock.h"
#include "calais/util/logging.h"
#include "calais/util/metrics.h"

namespace calais::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.reconciler");
  return logger;
}

double monotonic_seconds() {
  return static_cast<double>(core::monotonic_ns()) / 1e9;
}

}  // namespace

// ---------------------------------------------------------------------------
Reconciler::Reconciler(const Config& config, ExchangeConfig exchange,
                       VenueRest* rest, OrderStore* orders,
                       ReconcilerCallbacks callbacks)
    : config_(config),
      exchange_(std::move(exchange)),
      rest_(rest),
      orders_(orders),
      callbacks_(std::move(callbacks)) {
  const double now = monotonic_seconds();
  next_order_pass_ = now + config_.reconciliation.interval_seconds;
  next_fill_pass_ = now + config_.fill_reconciliation.interval_seconds;
  next_position_pass_ = now + config_.portfolio.position_refresh_interval_seconds;

  // First pull reaches back far enough to recover anything missed while the
  // process was down.
  fill_cursor_ms_ = core::wall_clock_ns() / 1'000'000LL -
                    static_cast<std::int64_t>(config_.fill_reconciliation.lookback_seconds) *
                        1000LL;
}

double Reconciler::now_seconds() const { return monotonic_seconds(); }

bool Reconciler::have_seen_fill(const std::string& trade_id) const {
  return seen_fills_.count(trade_id) > 0;
}

void Reconciler::note_fill(const std::string& trade_id) {
  seen_fills_.insert(trade_id);
}

void Reconciler::on_session_live() {
  // Everything that happened while disconnected was missed, so do not wait for
  // the next tick.
  const double now = now_seconds();
  next_order_pass_ = now;
  next_fill_pass_ = now;
  next_position_pass_ = now;
}

void Reconciler::poll() {
  const double now = now_seconds();

  if (config_.reconciliation.enabled && now >= next_order_pass_ &&
      !order_pass_in_flight_) {
    next_order_pass_ = now + config_.reconciliation.interval_seconds;
    run_order_pass();
  }
  if (config_.fill_reconciliation.enabled && now >= next_fill_pass_ &&
      !fill_pass_in_flight_) {
    next_fill_pass_ = now + config_.fill_reconciliation.interval_seconds;
    run_fill_pass();
  }
  if (now >= next_position_pass_ && !position_pass_in_flight_) {
    next_position_pass_ = now + config_.portfolio.position_refresh_interval_seconds;
    run_position_pass();
  }
}

void Reconciler::run_order_pass() {
  order_pass_in_flight_ = true;
  ++stats_.order_passes;

  for (const auto& currency : config_.portfolio.currencies) {
    rest_->get_open_orders(
        currency,
        [this, currency](std::vector<models::Order> orders, const std::string& error) {
          order_pass_in_flight_ = false;
          if (!error.empty()) {
            ++stats_.failures;
            util::get_metrics().inc_reconciler_failure("order", exchange_.name);
            CALAIS_LOG_WARN(log(), "[{}] order reconciliation failed: {}",
                            exchange_.name, error);
            return;
          }

          // What the venue says is open but we think is closed, and vice
          // versa. OrderStore::reconcile logs each correction; every one is a
          // message the feed lost.
          const auto before = orders_->active_orders().size();
          orders_->reconcile(orders);
          const auto after = orders_->active_orders().size();
          if (before != after) {
            ++stats_.orders_recovered;
            util::get_metrics().inc_reconciler_recovered("order", exchange_.name);
          }
          CALAIS_LOG_DEBUG(log(), "[{}] reconciled {} open orders for {}",
                           exchange_.name, orders.size(), currency);
        });
  }
}

void Reconciler::run_fill_pass() {
  fill_pass_in_flight_ = true;
  ++stats_.fill_passes;

  const std::int64_t now_ms = core::wall_clock_ns() / 1'000'000LL;
  // Re-fetch a little before the cursor: a trade stamped slightly out of order
  // at the boundary would otherwise fall through the gap between two pulls.
  const std::int64_t start =
      fill_cursor_ms_ -
      static_cast<std::int64_t>(config_.fill_reconciliation.overlap_seconds) * 1000LL;

  for (const auto& currency : config_.portfolio.currencies) {
    rest_->get_user_trades(
        currency, start, now_ms,
        [this, now_ms](std::vector<models::Fill> fills, const std::string& error) {
          fill_pass_in_flight_ = false;
          if (!error.empty()) {
            ++stats_.failures;
            util::get_metrics().inc_reconciler_failure("fill", exchange_.name);
            CALAIS_LOG_WARN(log(), "[{}] fill reconciliation failed: {}",
                            exchange_.name, error);
            return;
          }

          std::size_t recovered = 0;
          for (auto& fill : fills) {
            // trade_id is the venue's idempotency key: the same fill arrives
            // on the feed AND here, and counting it twice would double the
            // position.
            if (have_seen_fill(fill.trade_id)) {
              continue;
            }
            note_fill(fill.trade_id);
            ++recovered;
            if (callbacks_.on_recovered_fill) {
              callbacks_.on_recovered_fill(fill);
            }
          }
          if (recovered > 0) {
            // Loud: a recovered fill means the WebSocket feed dropped one.
            stats_.fills_recovered += recovered;
            util::get_metrics().inc_reconciler_recovered("fill", exchange_.name);
            CALAIS_LOG_WARN(log(),
                            "[{}] recovered {} fill(s) the feed did not deliver",
                            exchange_.name, recovered);
          }
          fill_cursor_ms_ = now_ms;
        });
  }
}

void Reconciler::run_position_pass() {
  if (config_.portfolio.currencies.empty()) {
    return;
  }
  position_pass_in_flight_ = true;
  ++stats_.position_passes;
  position_accumulator_.clear();
  position_pass_failed_ = false;
  // Counted UP FRONT, before any call is issued: an immediate synchronous
  // failure would otherwise drive the count to zero mid-fanout and publish a
  // half-built snapshot.
  position_replies_outstanding_ = config_.portfolio.currencies.size();

  for (const auto& currency : config_.portfolio.currencies) {
    rest_->get_positions(
        currency,
        [this](std::vector<models::Position> positions, const std::string& error) {
          if (!error.empty()) {
            ++stats_.failures;
            position_pass_failed_ = true;
            util::get_metrics().inc_reconciler_failure("position", exchange_.name);
          } else {
            position_accumulator_.insert(position_accumulator_.end(),
                                         positions.begin(), positions.end());
          }

          if (--position_replies_outstanding_ > 0) {
            return;
          }
          position_pass_in_flight_ = false;
          // A pass where SOME currency failed is not published. The snapshot
          // replaces what the engine holds, so a partial one would report the
          // failed currency's positions as closed.
          if (position_pass_failed_) {
            CALAIS_LOG_WARN(log(),
                            "[{}] position pass incomplete; keeping the previous "
                            "snapshot",
                            exchange_.name);
            return;
          }
          if (callbacks_.on_positions) {
            callbacks_.on_positions(position_accumulator_);
          }
        });
  }
}

}  // namespace calais::oms
