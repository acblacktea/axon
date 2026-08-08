// REST snapshots and the three reconcilers. Ports oms/order_reconciler.py,
// oms/fill_reconciler.py and oms/position_refresher.py.
//
// WHAT RECONCILIATION IS FOR, and it is not what it looks like. The WebSocket
// feed is the fast path and is supposed to be complete. Reconciliation is the
// backstop for when it is not: a message lost during a reconnect, a fill that
// arrived while the socket was down, an order the venue closed without telling
// us. Every correction it makes is EVIDENCE THE FEED DROPPED SOMETHING, which
// is why each one is logged loudly and counted -- a reconciler quietly fixing
// things every cycle means the feed is broken, not that reconciliation works.
//
// It runs on the engine thread, off timers, using the async HTTP client. A
// snapshot in flight never blocks the feed.
//
// SCOPE: all four venues, through the VenueRest interface. Each signs its own
// requests differently; see venue_rest.h.
//
// ONE CAVEAT that is real and not papered over: Binance's userTrades endpoint
// is SYMBOL-scoped, not currency-scoped, so its fill reconciliation only
// covers the symbols named in portfolio.currencies. Configuring "BTC" there
// gets nothing on Binance; it needs "BTCUSDT".

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>
#include <vector>

#include "calais/config.h"
#include "calais/models/fill.h"
#include "calais/models/order.h"
#include "calais/models/portfolio.h"
#include "calais/net/http_client.h"
#include "calais/oms/order_store.h"
#include "calais/oms/venue_rest.h"

namespace calais::oms {

struct ReconcilerCallbacks {
  std::function<void(const models::Fill&)> on_recovered_fill;
  std::function<void(const std::vector<models::Position>&)> on_positions;
};

class Reconciler {
 public:
  // `rest` must outlive the reconciler; the engine owns one per venue and
  // shares it with the EMS so cancel/amend and the snapshots use the same
  // connections.
  Reconciler(const Config& config, ExchangeConfig exchange, VenueRest* rest,
             OrderStore* orders, ReconcilerCallbacks callbacks);

  // Fires whichever timers are due. Never blocks.
  void poll();

  // Called when a session goes live. Everything that happened while we were
  // disconnected was missed, so a full pass runs immediately rather than
  // waiting for the next tick.
  void on_session_live();

  // Trade ids already seen, so a recovered fill is not double-counted. The
  // venue's trade_id is the idempotency key; this is the same dedup the fill
  // repository does, kept in memory so the check costs nothing.
  bool have_seen_fill(const std::string& trade_id) const;
  void note_fill(const std::string& trade_id);

  struct Stats {
    std::uint64_t order_passes = 0;
    std::uint64_t fill_passes = 0;
    std::uint64_t position_passes = 0;
    std::uint64_t orders_recovered = 0;
    std::uint64_t fills_recovered = 0;
    std::uint64_t failures = 0;
  };
  const Stats& stats() const noexcept { return stats_; }

 private:
  void run_order_pass();
  void run_fill_pass();
  void run_position_pass();
  double now_seconds() const;

  Config config_;
  ExchangeConfig exchange_;
  VenueRest* rest_ = nullptr;
  OrderStore* orders_ = nullptr;
  ReconcilerCallbacks callbacks_;

  double next_order_pass_ = 0.0;
  double next_fill_pass_ = 0.0;
  double next_position_pass_ = 0.0;
  // Only one pass of each kind in flight; a slow venue must not queue up
  // overlapping snapshots that then apply out of order.
  bool order_pass_in_flight_ = false;
  bool fill_pass_in_flight_ = false;
  bool position_pass_in_flight_ = false;

  // A position pass fans out one REST call per configured currency and the
  // replies land separately. They are accumulated here and reported ONCE,
  // because on_positions replaces the whole snapshot for the exchange --
  // reporting per currency would let the ETH reply erase the BTC positions.
  std::vector<models::Position> position_accumulator_;
  std::size_t position_replies_outstanding_ = 0;
  bool position_pass_failed_ = false;

  // Cursor for incremental fill pulls, with the configured overlap subtracted
  // so a trade stamped slightly out of order at the boundary is not missed.
  std::int64_t fill_cursor_ms_ = 0;
  std::unordered_set<std::string> seen_fills_;

  Stats stats_;
};

}  // namespace calais::oms
