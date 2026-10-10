// The bridge from order management to risk: an OmsListener that keeps a
// RiskManager current with every order update, fill and position snapshot.
//
// The only file in risk/ that knows the OMS exists. RiskManager itself depends
// on nothing in oms/, so the dependency runs one way: whoever wires the engine
// registers this with the OMS.

#pragma once

#include <functional>
#include <string>
#include <utility>
#include <vector>

#include "axon/oms/oms_listener.h"
#include "axon/risk/risk_manager.h"

namespace axon::risk {

// Keeps a RiskManager current from what the OMS publishes. Order changes
// from reconciliation count too -- an order the venue closed while the feed
// was down must leave the working-order book, or its exposure lingers.
class RiskFeed final : public oms::OmsListener {
 public:
  // `clock` gives the time reference prices are stamped with.
  RiskFeed(RiskManager& risk, std::function<double()> clock)
      : risk_(risk), clock_(std::move(clock)) {}

  void on_order_update(const models::Order& order) override { risk_.on_order_update(order); }
  void on_order_changed(const models::Order& order) override { risk_.on_order_update(order); }

  void on_fill(const transport::FillMsg*, const models::Fill& fill, bool recovered) override {
    risk_.on_fill(fill);
    // A recovered fill can be hours old; its price is no reference for now.
    if (!recovered) {
      risk_.update_reference_price(fill.exchange, fill.instrument, fill.price, clock_());
    }
  }

  void on_positions(const std::string& exchange,
                    const std::vector<models::Position>& positions) override {
    risk_.on_positions(exchange, positions);
    const double now = clock_();
    for (const auto& p : positions) {
      risk_.update_reference_price(exchange, p.instrument, p.mark_price, now);
    }
  }

 private:
  RiskManager& risk_;
  std::function<double()> clock_;
};

}  // namespace axon::risk
