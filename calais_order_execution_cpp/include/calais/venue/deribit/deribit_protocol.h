// Deribit protocol constants and field mappings.
//
// EVERY MAPPING HERE IS TRANSCRIBED FROM THE PYTHON IMPLEMENTATION
// (oms/deribit/deribit_ws.py and ems/deribit/deribit.py), not from Deribit's
// documentation. That is deliberate. The Python code has been reconciling
// against the live venue for a long time; the documentation has not. Where the
// two disagree, the running code is the specification, and any divergence
// introduced here is a bug that will only show up in production.
//
// Divergences that ARE intentional are called out individually below. There
// are two, and both are marked DIVERGENCE.

#pragma once

#include <cstdint>
#include <string_view>

#include "calais/models/enums.h"

namespace calais::venue::deribit {

inline constexpr std::string_view kExchangeName = "deribit";

inline constexpr std::string_view kProductionUrl = "wss://www.deribit.com/ws/api/v2";
inline constexpr std::string_view kTestnetUrl = "wss://test.deribit.com/ws/api/v2";
inline constexpr std::string_view kProductionHost = "www.deribit.com";
inline constexpr std::string_view kTestnetHost = "test.deribit.com";
inline constexpr std::string_view kPath = "/ws/api/v2";

// Subscription channels, exactly as the Python client builds them.
inline constexpr std::string_view kAllOrdersChannel = "user.orders.any.any.raw";
inline constexpr std::string_view kAllTradesChannel = "user.trades.any.any.raw";

// The id the Python client uses for its own heartbeat probe, and the one it
// answers a test_request with. Kept identical so a captured session from
// either implementation reads the same.
inline constexpr std::int64_t kHeartbeatRequestId = 9999;
inline constexpr std::int64_t kTestResponseId = 9998;

// ---------------------------------------------------------------------------
// order_state -> OrderStatus
//
// From _parse_order in deribit_ws.py. Note the default: an UNRECOGNISED state
// maps to kOpen rather than failing.
//
// That is the opposite of the policy in models/enums.h, where an unknown enum
// string is a parse error, and the tension is real. Treating an unknown state
// as open can leave a closed order looking live. Rejecting the message loses
// the update entirely, which is worse: the order then has no state at all
// until the reconciler catches up.
//
// So this keeps the Python behaviour, and makes the event observable instead
// of silent -- the parser reports `unknown_order_state` so it can be counted
// and alerted on rather than discovered during an incident.
// ---------------------------------------------------------------------------
struct StatusMapping {
  models::OrderStatus status;
  bool recognised;
};

constexpr StatusMapping map_order_state(std::string_view state) noexcept {
  if (state == "open") {
    return {models::OrderStatus::kOpen, true};
  }
  if (state == "filled") {
    return {models::OrderStatus::kFilled, true};
  }
  if (state == "cancelled") {
    return {models::OrderStatus::kCancelled, true};
  }
  if (state == "rejected") {
    return {models::OrderStatus::kRejected, true};
  }
  if (state == "untriggered") {
    return {models::OrderStatus::kPending, true};
  }
  return {models::OrderStatus::kOpen, false};
}

// From _parse_order: Deribit has no "partially filled" state of its own, so it
// is derived. Applied only to kOpen -- a filled or cancelled order with a
// non-zero fill is already terminal.
constexpr models::OrderStatus refine_with_fill(models::OrderStatus status,
                                               bool has_partial_fill) noexcept {
  if (status == models::OrderStatus::kOpen && has_partial_fill) {
    return models::OrderStatus::kPartiallyFilled;
  }
  return status;
}

// From _parse_order: anything that is not exactly "buy" is a sell.
constexpr models::OrderSide map_direction(std::string_view direction) noexcept {
  return direction == "buy" ? models::OrderSide::kBuy : models::OrderSide::kSell;
}

// From _parse_order: anything that is not exactly "limit" is a market order.
constexpr models::OrderType map_order_type(std::string_view type) noexcept {
  return type == "limit" ? models::OrderType::kLimit : models::OrderType::kMarket;
}

// From _parse_fill: Deribit reports liquidity as a single character, "M" or
// "T". Anything other than "T" is treated as maker.
constexpr models::Liquidity map_liquidity(std::string_view liquidity) noexcept {
  return liquidity == "T" ? models::Liquidity::kTaker : models::Liquidity::kMaker;
}

// Order-entry method, from place_order in ems/deribit/deribit.py.
constexpr std::string_view order_entry_method(models::OrderSide side) noexcept {
  return side == models::OrderSide::kBuy ? "private/buy" : "private/sell";
}

}  // namespace calais::venue::deribit
