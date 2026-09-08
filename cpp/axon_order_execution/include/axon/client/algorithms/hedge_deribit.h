// Deribit options hedge. Port of client/algorithms/hedge_deribit.py.
//
// Works one leg against the other in batches: acquire symbol1 as a MAKER via
// the chase algorithm, then immediately cross symbol2 as a TAKER for exactly
// what the maker leg filled. Repeat until the requested size is done.
//
// THE BATCHING IS THE RISK CONTROL. Between the two legs the book is one-sided
// and unhedged, so the exposure window is bounded by batch_amount rather than
// by the whole order. A larger batch means fewer round trips and more delta
// risk; that trade is the caller's to make.
//
// THE TAKER LEG HEDGES WHAT ACTUALLY FILLED, never what was requested. If the
// chase only gets half a batch, only that half is crossed -- hedging the full
// batch would leave a naked position in the opposite direction.
//
// Client-side and blocking, for the same reason chase_maker is: it belongs in
// the strategy process, not in the engine loop.

#pragma once

#include <cstdint>
#include <string>

#include "axon/client/algorithms/chase_maker.h"
#include "axon/core/decimal.h"
#include "axon/models/order.h"

namespace axon::client::algorithms {

struct HedgeDeribitParams {
  std::string symbol1;  // acquired as maker
  std::string symbol2;  // hedged as taker
  models::OrderSide side1 = models::OrderSide::kBuy;
  models::OrderSide side2 = models::OrderSide::kSell;

  core::Qty amount;
  core::Qty batch_amount;

  double chase_max_attempt_seconds = 60.0;
  // Empty means one is generated, matching the Python's uuid4().hex.
  std::string internal_order_id;
};

struct HedgeDeribitResult {
  // Both legs carry this, so the whole hedge is one queryable unit afterwards.
  std::string internal_order_id;
  core::Qty maker_filled;
  core::Qty taker_sent;
  std::uint32_t batches = 0;
  // Set when the loop stopped early. The legs already done stay done: this
  // reports what happened rather than unwinding it.
  std::string error;
};

template <typename Client>
HedgeDeribitResult hedge_deribit_options(Client& client,
                                         const HedgeDeribitParams& params);

}  // namespace axon::client::algorithms

#include "axon/client/algorithms/hedge_deribit.inl"
