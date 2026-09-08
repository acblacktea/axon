#include "axon/models/enums.h"

#include <array>
#include <utility>

namespace axon::models {
namespace {

// Linear scan over a handful of entries beats a hash map here: these tables are
// 2-11 entries, they fit in one or two cache lines, and from_string is only
// ever called on the JSON control plane. The hot path carries the enum value
// itself and never sees the text.
template <typename E, std::size_t N>
std::string_view lookup(const std::array<std::pair<E, std::string_view>, N>& table,
                        E value) noexcept {
  for (const auto& [k, v] : table) {
    if (k == value) {
      return v;
    }
  }
  return {};
}

template <typename E, std::size_t N>
std::optional<E> reverse_lookup(
    const std::array<std::pair<E, std::string_view>, N>& table,
    std::string_view s) noexcept {
  for (const auto& [k, v] : table) {
    if (v == s) {
      return k;
    }
  }
  return std::nullopt;
}

constexpr std::array<std::pair<OrderStatus, std::string_view>, 6> kOrderStatus{{
    {OrderStatus::kPending, "pending"},
    {OrderStatus::kOpen, "open"},
    {OrderStatus::kPartiallyFilled, "partially_filled"},
    {OrderStatus::kFilled, "filled"},
    {OrderStatus::kCancelled, "cancelled"},
    {OrderStatus::kRejected, "rejected"},
}};

constexpr std::array<std::pair<OrderSide, std::string_view>, 2> kOrderSide{{
    {OrderSide::kBuy, "buy"},
    {OrderSide::kSell, "sell"},
}};

constexpr std::array<std::pair<OrderType, std::string_view>, 2> kOrderType{{
    {OrderType::kLimit, "limit"},
    {OrderType::kMarket, "market"},
}};

constexpr std::array<std::pair<Liquidity, std::string_view>, 2> kLiquidity{{
    {Liquidity::kMaker, "maker"},
    {Liquidity::kTaker, "taker"},
}};

constexpr std::array<std::pair<CommandType, std::string_view>, 11> kCommandType{{
    {CommandType::kPlaceOrder, "place_order"},
    {CommandType::kCancelOrder, "cancel_order"},
    {CommandType::kModifyOrder, "modify_order"},
    {CommandType::kGetOrder, "get_order"},
    {CommandType::kGetAllOrders, "get_all_orders"},
    {CommandType::kGetActiveOrders, "get_active_orders"},
    {CommandType::kGetTicker, "get_ticker"},
    {CommandType::kGetAccountSummary, "get_account_summary"},
    {CommandType::kGetPositions, "get_positions"},
    {CommandType::kGetFillsByOrder, "get_fills_by_order"},
    {CommandType::kGetFillsByStrategy, "get_fills_by_strategy"},
}};

constexpr std::array<std::pair<EventType, std::string_view>, 4> kEventType{{
    {EventType::kOrderUpdate, "order_update"},
    {EventType::kAccountUpdate, "account_update"},
    {EventType::kPositionUpdate, "position_update"},
    {EventType::kFillUpdate, "fill_update"},
}};

}  // namespace

std::string_view to_string(OrderStatus v) noexcept { return lookup(kOrderStatus, v); }
std::string_view to_string(OrderSide v) noexcept { return lookup(kOrderSide, v); }
std::string_view to_string(OrderType v) noexcept { return lookup(kOrderType, v); }
std::string_view to_string(Liquidity v) noexcept { return lookup(kLiquidity, v); }
std::string_view to_string(CommandType v) noexcept { return lookup(kCommandType, v); }
std::string_view to_string(EventType v) noexcept { return lookup(kEventType, v); }

std::optional<OrderStatus> order_status_from_string(std::string_view s) noexcept {
  return reverse_lookup(kOrderStatus, s);
}
std::optional<OrderSide> order_side_from_string(std::string_view s) noexcept {
  return reverse_lookup(kOrderSide, s);
}
std::optional<OrderType> order_type_from_string(std::string_view s) noexcept {
  return reverse_lookup(kOrderType, s);
}
std::optional<Liquidity> liquidity_from_string(std::string_view s) noexcept {
  return reverse_lookup(kLiquidity, s);
}
std::optional<CommandType> command_type_from_string(std::string_view s) noexcept {
  return reverse_lookup(kCommandType, s);
}
std::optional<EventType> event_type_from_string(std::string_view s) noexcept {
  return reverse_lookup(kEventType, s);
}

}  // namespace axon::models
