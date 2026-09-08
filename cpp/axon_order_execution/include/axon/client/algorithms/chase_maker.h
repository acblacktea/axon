// Chase-maker fill. Port of client/algorithms/chase_maker.py.
//
// Keeps a limit order pinned to the far touch, re-pricing as the book moves,
// and falls back to a market order for whatever is left when time runs out.
// The point is to pay the maker rebate rather than cross the spread, while
// still guaranteeing a fill by the deadline.
//
// DELIBERATELY SYNCHRONOUS AND BLOCKING, unlike everything else in this
// codebase. It is an algorithm a STRATEGY runs, not something the engine loop
// does: it lives in the strategy process, where blocking on a round trip only
// stalls that strategy. Running it inside the engine would stall every venue
// feed, which is why it takes a StrategyClient rather than a service pointer.
//
// The Python version accepts any object exposing place/cancel/get/modify, so
// it works with either the client or the in-process service. Here it is a
// template on the client type for the same reason -- and so the calls inline.

#pragma once

#include <cstdint>
#include <string>

#include "axon/core/decimal.h"
#include "axon/models/order.h"

namespace axon::client::algorithms {

struct ChaseMakerParams {
  std::string exchange;
  std::string instrument;
  models::OrderSide side = models::OrderSide::kBuy;
  core::Qty amount;
  std::string internal_order_id;

  double max_attempt_seconds = 60.0;
  double poll_interval_seconds = 0.2;

  // Cross with a market order for the remainder when the deadline passes.
  // Turning this off makes the algorithm best-effort rather than guaranteed.
  bool taker_fallback = true;
};

struct ChaseMakerResult {
  core::Qty filled;
  // How many times the resting order was re-priced. A number that climbs with
  // no fill means the quote is chasing a moving book and losing the race --
  // worth alerting on rather than only reporting at the end.
  std::uint32_t reprices = 0;
  bool used_taker = false;
  std::string error;
};

// `Client` must provide get_ticker, place_order, modify_order, cancel_order and
// get_order with the StrategyClient signatures.
template <typename Client>
ChaseMakerResult chase_maker_fill(Client& client, const ChaseMakerParams& params);

}  // namespace axon::client::algorithms

#include "axon/client/algorithms/chase_maker.inl"
