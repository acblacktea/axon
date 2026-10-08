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
// engine feeds it; the EMS asks it.

#pragma once

#include <cstdint>
#include <deque>
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

namespace axon::oms {

struct RiskRejection {
  // A fixed set, used as a metric label: kill_switch, rate, order_qty,
  // order_notional, price_deviation, no_reference_price, position.
  std::string code;
  // Human-readable, with the numbers that tripped it.
  std::string message;
};

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
  void engage_kill_switch(std::string reason);
  void release_kill_switch();
  bool kill_switch_engaged() const noexcept { return kill_engaged_; }
  const std::string& kill_switch_reason() const noexcept { return kill_reason_; }

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

  std::unordered_map<std::string, core::Qty> positions_;        // exchange:instrument
  std::unordered_map<std::string, WorkingOrder> working_;       // order_key
  std::unordered_map<std::string, Reference> references_;       // exchange:instrument
  std::unordered_map<std::string, std::deque<double>> sends_;   // strategy -> send times
};

}  // namespace axon::oms
