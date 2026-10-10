// Pre-trade risk: the checks every order passes before it reaches a venue.
//
// WHY IT LIVES IN THE ENGINE. A strategy with a bug -- a loop that never
// stops placing, a price off by a factor of ten, a sign error on the side --
// will send exactly what its bug says. The engine is the last place anything
// can still say no, and the only place every order path goes through, so the
// limits are enforced here and nowhere else is trusted to.
//
// WHAT IS CHECKED, in order, for each new order (and for an amend, as the
// order it would become):
//
//   kill switch        while engaged, nothing new goes out (cancels still do)
//   rate               orders per strategy per second
//   order size         quantity, and notional = |price x quantity|
//   price deviation    a limit price too far from the reference price
//   position           worst-case net position if every working order filled
//
// POSITION is the one that needs state, and it is computed from three sources
// so that no single lagging one can let an order through:
//
//   * the venue's position, refreshed by the reconciler's position pass;
//   * fills since then, applied as they arrive;
//   * every working order -- including ones just sent that the venue has not
//     acknowledged yet. Counting only acknowledged orders would let a burst of
//     orders through before the first ack arrives, which is exactly the burst
//     a runaway strategy produces.
//
// FAIL CLOSED. A check that needs a reference price and has no fresh one
// refuses the order rather than skipping the check.
//
// Pure logic: no I/O, and time is passed in, so every rule is testable. The
// EMS asks it before every order; it is kept current through RiskFeed
// (risk_feed.h), which listens to the OMS.
//
// ITS OWN MODULE. Risk is neither order management nor order entry: it
// depends on configuration and the domain models and on nothing in oms/ or
// ems/. The one file that knows the OMS is risk_feed.h.

#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "axon/config.h"
#include "axon/models/fill.h"
#include "axon/models/order.h"
#include "axon/models/portfolio.h"

namespace axon::risk {

struct RiskRejection {
  // A fixed set, used as a metric label: kill_switch, rate, order_qty,
  // order_notional, price_deviation, no_reference_price, position.
  std::string code;
  // Human-readable, with the numbers that tripped it.
  std::string message;
};

// Who engaged the kill switch. It matters for release: removing the kill file
// releases only a switch the FILE engaged, so deleting a stale file cannot
// undo a stop someone ordered another way.
enum class KillSource : std::uint8_t {
  kOperator,  // a direct call: an operator, a test, a future control command
  kSignal,    // SIGUSR1 / SIGUSR2
  kFile,      // cpp.risk.kill_switch_file
};

const char* to_string(KillSource source) noexcept;

class RiskManager {
 public:
  explicit RiskManager(RiskConfig config = {});

  // --- the checks -----------------------------------------------------------
  // nullopt means allowed. Neither records anything: call on_order_sent once
  // the order has actually gone out.
  std::optional<RiskRejection> check_new_order(const std::string& exchange,
                                               const models::OrderRequest& request,
                                               double now_seconds);
  // An amend, judged as the order it would become: its old remaining quantity
  // is taken out of the position calculation and the new one put in.
  std::optional<RiskRejection> check_modify(const std::string& exchange,
                                            const models::Order& existing,
                                            std::optional<core::Qty> amount,
                                            std::optional<core::Price> price,
                                            double now_seconds);

  // --- what the EMS reports ---------------------------------------------------
  // Sent: counts toward position and rate from now, before any ack.
  void on_order_sent(const std::string& exchange, const models::OrderRequest& request,
                     double now_seconds);
  // Definitively refused by the venue: nothing is live, release it.
  void on_order_rejected(const std::string& exchange, const models::OrderRequest& request);

  // --- what the engine feeds -------------------------------------------------
  void on_order_update(const models::Order& order);
  void on_fill(const models::Fill& fill);
  // The venue's positions for one exchange, replacing whatever was held.
  void on_positions(const std::string& exchange,
                    const std::vector<models::Position>& positions);
  void update_reference_price(const std::string& exchange, const std::string& instrument,
                              core::Price price, double now_seconds);

  // --- kill switch -------------------------------------------------------------
  //
  // Engaging is idempotent: an engaged switch stays with whoever engaged it
  // first. An explicit release (operator or signal) releases it whatever the
  // source -- that is a person deciding -- and the file, if still present,
  // engages it again on its next observation, because present means stopped.
  void engage_kill_switch(KillSource source, std::string reason);
  void release_kill_switch(KillSource source);
  // Shorthands for KillSource::kOperator.
  void engage_kill_switch(std::string reason);
  void release_kill_switch();

  // The kill file is level-triggered: present engages, absent releases a
  // switch the file engaged and nothing else. The caller does the filesystem
  // check and must NOT call this when the check itself failed -- an
  // unreadable directory is not evidence that the file is gone.
  void observe_kill_file(bool present, const std::string& path);

  // Runs once on each transition to engaged -- the engine cancels every
  // working order here. Not on repeated engages of an engaged switch.
  void set_on_kill_engaged(std::function<void(const std::string& reason)> callback) {
    on_kill_engaged_ = std::move(callback);
  }

  bool kill_switch_engaged() const noexcept { return kill_engaged_; }
  const std::string& kill_switch_reason() const noexcept { return kill_reason_; }
  KillSource kill_switch_source() const noexcept { return kill_source_; }

  // --- reference prices ---------------------------------------------------------
  //
  // The instruments whose reference price is refreshed over REST: every
  // "exchange:instrument" key under cpp.risk.instruments. Bare "instrument"
  // keys name no venue to ask, so they are not targets.
  struct ReferenceTarget {
    std::string exchange;
    std::string instrument;
  };
  const std::vector<ReferenceTarget>& reference_targets() const noexcept {
    return reference_targets_;
  }
  // True when a refresh is due, and starts the next interval. Never true
  // when reference_refresh_seconds is 0 or there are no targets.
  bool reference_refresh_due(double now_seconds);

  // --- introspection -------------------------------------------------------------
  // Signed net position as last known (venue snapshot plus fills since).
  core::Qty position(const std::string& exchange, const std::string& instrument) const;
  // Signed worst-case position on each side if every working order filled.
  struct Exposure {
    core::Qty long_side;   // position + all working buys
    core::Qty short_side;  // position - all working sells
  };
  Exposure exposure(const std::string& exchange, const std::string& instrument) const;
  std::size_t working_order_count() const noexcept { return working_.size(); }
  // The limits that apply to an instrument: its own entry over the defaults.
  RiskLimits limits_for(const std::string& exchange, const std::string& instrument) const;
  const RiskConfig& config() const noexcept { return config_; }

 private:
  struct WorkingOrder {
    std::string exchange;
    std::string instrument;
    models::OrderSide side = models::OrderSide::kBuy;
    core::Qty remaining;
  };
  struct Reference {
    core::Price price;
    double at = 0.0;
  };

  static std::string key(std::string_view exchange, std::string_view instrument);
  // Working orders are keyed by internal_order_id where there is one -- so an
  // order counted when sent is the same entry its first update replaces --
  // and by venue order id otherwise.
  static std::string order_key(std::string_view exchange, const models::Order& order);

  std::optional<RiskRejection> check(const std::string& exchange,
                                     const std::string& instrument, models::OrderSide side,
                                     models::OrderType type, core::Qty quantity,
                                     std::optional<core::Price> price,
                                     const std::string& strategy, bool count_rate,
                                     const std::string& replacing_key, double now);
  std::optional<core::Price> fresh_reference(const std::string& exchange,
                                             const std::string& instrument,
                                             double now) const;

  RiskConfig config_;
  bool kill_engaged_ = false;
  std::string kill_reason_;
  KillSource kill_source_ = KillSource::kOperator;
  std::function<void(const std::string&)> on_kill_engaged_;

  std::vector<ReferenceTarget> reference_targets_;
  double last_reference_refresh_ = -1.0;

  std::unordered_map<std::string, core::Qty> positions_;        // exchange:instrument
  std::unordered_map<std::string, WorkingOrder> working_;       // order_key
  std::unordered_map<std::string, Reference> references_;       // exchange:instrument
  std::unordered_map<std::string, std::deque<double>> sends_;   // strategy -> send times
};

}  // namespace axon::risk
