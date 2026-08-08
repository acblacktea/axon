// Fill (trade execution) model. Mirrors models/fill.py.
//
// Fills are append-only facts. `trade_id` is the exchange-assigned identifier
// and the idempotency key: the same fill legitimately arrives twice, once over
// the WebSocket feed and once via REST reconciliation, and deduplicating on
// trade_id is what keeps the position from being double-counted.
//
// This is the strongest argument for fixed-point arithmetic in this codebase.
// Position is the running sum of fill amounts; with doubles that sum drifts,
// and a residual of 1e-12 contracts is enough to make the reconciler believe
// there is a position to flatten. With int64 the sum is exact by construction.

#pragma once

#include <optional>
#include <string>

#include "calais/core/decimal.h"
#include "calais/core/timestamp.h"
#include "calais/models/enums.h"

namespace calais::models {

using core::Price;
using core::Qty;
using core::Timestamp;

struct Fill {
  std::string trade_id;
  std::string order_id;
  std::string exchange;
  std::string instrument;
  OrderSide side = OrderSide::kBuy;
  Qty amount;
  Price price;
  Price fee;
  std::string fee_currency;
  Liquidity liquidity = Liquidity::kMaker;
  Timestamp timestamp;
  std::optional<Price> index_price;
  std::optional<Price> mark_price;
  std::optional<Price> iv;
  std::optional<Price> profit_loss;
  std::optional<std::string> label;
  std::optional<std::string> strategy_id;

  // Signed contribution to position: buys add, sells subtract.
  Qty signed_amount() const noexcept {
    return side == OrderSide::kBuy ? amount : -amount;
  }

  core::Notional notional() const noexcept { return price.mul(amount); }
};

}  // namespace calais::models
