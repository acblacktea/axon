// Domain enums.
//
// WIRE CONTRACT: the string spellings below are copied verbatim from the
// Python implementation (axon_order_execution/models/order.py and
// transport/types.py). They travel over ZMQ to live Python strategies, so they
// are frozen -- changing one silently breaks every deployed strategy client.
// Any test that touches serialisation should assert against the literal
// strings, not against round-tripping through this header, so that a typo here
// fails loudly.
//
// Backed by uint8_t so they can be embedded directly in the fixed-layout hot
// messages without padding surprises.

#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

namespace axon::models {

enum class OrderStatus : std::uint8_t {
  kPending = 0,
  kOpen = 1,
  kPartiallyFilled = 2,
  kFilled = 3,
  kCancelled = 4,
  kRejected = 5,
};

enum class OrderSide : std::uint8_t {
  kBuy = 0,
  kSell = 1,
};

enum class OrderType : std::uint8_t {
  kLimit = 0,
  kMarket = 1,
};

enum class Liquidity : std::uint8_t {
  kMaker = 0,
  kTaker = 1,
};

// Strategy -> engine, over the ZMQ ROUTER socket.
enum class CommandType : std::uint8_t {
  kPlaceOrder = 0,
  kCancelOrder = 1,
  kModifyOrder = 2,
  kGetOrder = 3,
  kGetAllOrders = 4,
  kGetActiveOrders = 5,
  kGetTicker = 6,
  kGetAccountSummary = 7,
  kGetPositions = 8,
  kGetFillsByOrder = 9,
  kGetFillsByStrategy = 10,
};

// Engine -> strategy, over the ZMQ PUB socket.
enum class EventType : std::uint8_t {
  kOrderUpdate = 0,
  kAccountUpdate = 1,
  kPositionUpdate = 2,
  kFillUpdate = 3,
};

// ---------------------------------------------------------------------------
// to_string returns the exact wire spelling. from_string returns nullopt for
// anything unrecognised -- an unknown status from an exchange must surface as
// a parse failure, never as a silent default of kPending.
// ---------------------------------------------------------------------------

std::string_view to_string(OrderStatus v) noexcept;
std::string_view to_string(OrderSide v) noexcept;
std::string_view to_string(OrderType v) noexcept;
std::string_view to_string(Liquidity v) noexcept;
std::string_view to_string(CommandType v) noexcept;
std::string_view to_string(EventType v) noexcept;

std::optional<OrderStatus> order_status_from_string(std::string_view s) noexcept;
std::optional<OrderSide> order_side_from_string(std::string_view s) noexcept;
std::optional<OrderType> order_type_from_string(std::string_view s) noexcept;
std::optional<Liquidity> liquidity_from_string(std::string_view s) noexcept;
std::optional<CommandType> command_type_from_string(std::string_view s) noexcept;
std::optional<EventType> event_type_from_string(std::string_view s) noexcept;

// Mirrors Order.is_active in the Python model: an order the exchange may still
// act on. Anything else is terminal.
constexpr bool is_active(OrderStatus s) noexcept {
  return s == OrderStatus::kPending || s == OrderStatus::kOpen ||
         s == OrderStatus::kPartiallyFilled;
}

constexpr bool is_terminal(OrderStatus s) noexcept { return !is_active(s); }

constexpr OrderSide opposite(OrderSide s) noexcept {
  return s == OrderSide::kBuy ? OrderSide::kSell : OrderSide::kBuy;
}

}  // namespace axon::models
