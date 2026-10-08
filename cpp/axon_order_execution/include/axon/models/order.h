// Order domain models.
//
// Field ORDER matters: the JSON codec emits keys in declaration order so the
// output is byte-identical to the Python implementation's, whose dataclass
// fields serialise in declaration order. Reordering a member here changes the
// wire bytes. It stays valid JSON either way, but byte-identity is what lets
// the cross-implementation test diff two strings instead of comparing parsed
// trees, which catches far more.
//
// Numeric fields use core::Decimal, not double. See core/decimal.h for why.
// The JSON control plane still degrades them to doubles on the way out,
// because Python has nothing better to receive them into; the binary hot path
// keeps them exact.

#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "axon/core/decimal.h"
#include "axon/core/timestamp.h"
#include "axon/models/enums.h"
#include "axon/models/ids.h"

namespace axon::models {

using core::Price;
using core::Qty;
using core::Timestamp;

// Mirrors models/order.py :: Ticker
struct Ticker {
  std::string instrument;
  Price best_bid_price;
  Qty best_bid_amount;
  Price best_ask_price;
  Qty best_ask_amount;
  std::optional<Price> last_price;
  std::optional<Price> mark_price;
  std::optional<Timestamp> timestamp;

  Price mid() const noexcept {
    return core::Price::from_raw(
        (best_bid_price.raw() + best_ask_price.raw()) / 2);
  }
  Price spread() const noexcept { return best_ask_price - best_bid_price; }
};

// Mirrors models/order.py :: OrderRequest
//
// The Python dataclass raises in __post_init__ when a limit order has no
// price. We do not throw from a constructor here -- validation returns a
// reason instead, because on the hot path a rejected order should produce a
// metric and a log line, not stack unwinding.
struct OrderRequest {
  std::string instrument;
  OrderSide side = OrderSide::kBuy;
  Qty amount;
  OrderType order_type = OrderType::kLimit;
  std::optional<Price> price;
  std::optional<std::string> client_order_id;
  std::optional<std::string> label;
  bool post_only = false;
  bool reject_post_only = false;
  std::string internal_order_id = generate_internal_id();
  std::optional<std::string> strategy_id;

  // Returns nullopt when valid, or a human-readable reason when not.
  std::optional<std::string> validate() const {
    if (instrument.empty()) {
      return "instrument is required";
    }
    if (amount.raw() <= 0) {
      return "amount must be positive";
    }
    if (order_type == OrderType::kLimit && !price.has_value()) {
      return "price is required for limit orders";
    }
    if (price.has_value() && price->raw() <= 0) {
      return "price must be positive";
    }
    // internal_order_id is sent to the venue as its client order id (see
    // venue_client_id), so it must be something all four accept: OKX's
    // clOrdId is the strictest, 1-32 alphanumerics. The generated default is
    // a 32-character hex uuid, which always passes.
    if (internal_order_id.empty() || internal_order_id.size() > 32) {
      return "internal_order_id must be 1-32 characters";
    }
    for (const char c : internal_order_id) {
      const bool alnum = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') ||
                         (c >= 'A' && c <= 'Z');
      if (!alnum) {
        return "internal_order_id must be alphanumeric";
      }
    }
    return std::nullopt;
  }

  // The client order id the venue is given for this request -- the strategy's
  // label when it set one, otherwise internal_order_id.
  //
  // Sending one on EVERY order is what makes an order findable after a
  // timeout: the venue echoes it on every update, so a reply that arrives
  // late, or a request whose outcome is unknown, can still be tied back to
  // the request that produced it. It is also the venue-side idempotency key --
  // a resubmission carrying the same id is refused while the first is live.
  std::string_view venue_client_id() const noexcept {
    if (label.has_value() && !label->empty()) {
      return *label;
    }
    return internal_order_id;
  }
};

// Mirrors models/order.py :: Order
struct Order {
  std::string order_id;
  std::string exchange;
  std::string instrument;
  OrderSide side = OrderSide::kBuy;
  OrderType order_type = OrderType::kLimit;
  Qty amount;
  OrderStatus status = OrderStatus::kPending;
  std::optional<std::string> internal_order_id;
  std::optional<Price> price;
  Qty filled_amount;
  std::optional<Price> average_price;
  std::optional<std::string> client_order_id;
  std::optional<std::string> label;
  Liquidity liquidity = Liquidity::kMaker;
  bool post_only = false;
  bool reject_post_only = false;
  std::optional<std::string> strategy_id;
  Timestamp created_at;
  Timestamp updated_at;

  Qty remaining_amount() const noexcept { return amount - filled_amount; }
  bool is_active() const noexcept { return models::is_active(status); }
  bool is_terminal() const noexcept { return models::is_terminal(status); }
};

}  // namespace axon::models
