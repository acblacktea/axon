// Enum wire-spelling tests.
//
// These assert against LITERAL strings rather than round-tripping through the
// header. Round-tripping would happily pass with a typo'd spelling on both
// sides; the literals here are transcribed from the Python source and are the
// actual contract with deployed strategy clients.

#include "axon/models/enums.h"

#include <gtest/gtest.h>

#include <set>
#include <string>

using namespace axon::models;

TEST(Enums, OrderStatusSpellings) {
  EXPECT_EQ(to_string(OrderStatus::kPending), "pending");
  EXPECT_EQ(to_string(OrderStatus::kOpen), "open");
  EXPECT_EQ(to_string(OrderStatus::kPartiallyFilled), "partially_filled");
  EXPECT_EQ(to_string(OrderStatus::kFilled), "filled");
  EXPECT_EQ(to_string(OrderStatus::kCancelled), "cancelled");
  EXPECT_EQ(to_string(OrderStatus::kRejected), "rejected");
}

TEST(Enums, OrderSideSpellings) {
  EXPECT_EQ(to_string(OrderSide::kBuy), "buy");
  EXPECT_EQ(to_string(OrderSide::kSell), "sell");
}

TEST(Enums, OrderTypeSpellings) {
  EXPECT_EQ(to_string(OrderType::kLimit), "limit");
  EXPECT_EQ(to_string(OrderType::kMarket), "market");
}

TEST(Enums, LiquiditySpellings) {
  EXPECT_EQ(to_string(Liquidity::kMaker), "maker");
  EXPECT_EQ(to_string(Liquidity::kTaker), "taker");
}

TEST(Enums, CommandTypeSpellings) {
  EXPECT_EQ(to_string(CommandType::kPlaceOrder), "place_order");
  EXPECT_EQ(to_string(CommandType::kCancelOrder), "cancel_order");
  EXPECT_EQ(to_string(CommandType::kModifyOrder), "modify_order");
  EXPECT_EQ(to_string(CommandType::kGetOrder), "get_order");
  EXPECT_EQ(to_string(CommandType::kGetAllOrders), "get_all_orders");
  EXPECT_EQ(to_string(CommandType::kGetActiveOrders), "get_active_orders");
  EXPECT_EQ(to_string(CommandType::kGetTicker), "get_ticker");
  EXPECT_EQ(to_string(CommandType::kGetAccountSummary), "get_account_summary");
  EXPECT_EQ(to_string(CommandType::kGetPositions), "get_positions");
  EXPECT_EQ(to_string(CommandType::kGetFillsByOrder), "get_fills_by_order");
  EXPECT_EQ(to_string(CommandType::kGetFillsByStrategy), "get_fills_by_strategy");
}

TEST(Enums, EventTypeSpellings) {
  EXPECT_EQ(to_string(EventType::kOrderUpdate), "order_update");
  EXPECT_EQ(to_string(EventType::kAccountUpdate), "account_update");
  EXPECT_EQ(to_string(EventType::kPositionUpdate), "position_update");
  EXPECT_EQ(to_string(EventType::kFillUpdate), "fill_update");
}

TEST(Enums, RoundTrip) {
  for (auto v : {OrderStatus::kPending, OrderStatus::kOpen,
                 OrderStatus::kPartiallyFilled, OrderStatus::kFilled,
                 OrderStatus::kCancelled, OrderStatus::kRejected}) {
    EXPECT_EQ(order_status_from_string(to_string(v)), v);
  }
  for (auto v : {OrderSide::kBuy, OrderSide::kSell}) {
    EXPECT_EQ(order_side_from_string(to_string(v)), v);
  }
  for (auto v : {OrderType::kLimit, OrderType::kMarket}) {
    EXPECT_EQ(order_type_from_string(to_string(v)), v);
  }
  for (auto v : {Liquidity::kMaker, Liquidity::kTaker}) {
    EXPECT_EQ(liquidity_from_string(to_string(v)), v);
  }
  for (int i = 0; i <= 10; ++i) {
    const auto v = static_cast<CommandType>(i);
    EXPECT_EQ(command_type_from_string(to_string(v)), v);
  }
  for (int i = 0; i <= 3; ++i) {
    const auto v = static_cast<EventType>(i);
    EXPECT_EQ(event_type_from_string(to_string(v)), v);
  }
}

TEST(Enums, UnknownStringsFailRatherThanDefault) {
  // An unrecognised status from an exchange must surface as a parse failure.
  // Silently defaulting to kPending would leave a filled order looking live.
  EXPECT_FALSE(order_status_from_string("").has_value());
  EXPECT_FALSE(order_status_from_string("PENDING").has_value());
  EXPECT_FALSE(order_status_from_string("untriggered").has_value());
  EXPECT_FALSE(order_side_from_string("BUY").has_value());
  EXPECT_FALSE(order_side_from_string("long").has_value());
  EXPECT_FALSE(order_type_from_string("stop_limit").has_value());
  EXPECT_FALSE(liquidity_from_string("both").has_value());
  EXPECT_FALSE(command_type_from_string("delete_everything").has_value());
  EXPECT_FALSE(event_type_from_string("trade_update").has_value());
}

TEST(Enums, SpellingsAreUnique) {
  std::set<std::string> seen;
  for (int i = 0; i <= 10; ++i) {
    const auto s = std::string(to_string(static_cast<CommandType>(i)));
    EXPECT_FALSE(s.empty()) << "CommandType " << i << " has no spelling";
    EXPECT_TRUE(seen.insert(s).second) << "duplicate spelling: " << s;
  }
}

TEST(Enums, IsActiveMatchesPythonSemantics) {
  EXPECT_TRUE(is_active(OrderStatus::kPending));
  EXPECT_TRUE(is_active(OrderStatus::kOpen));
  EXPECT_TRUE(is_active(OrderStatus::kPartiallyFilled));
  EXPECT_FALSE(is_active(OrderStatus::kFilled));
  EXPECT_FALSE(is_active(OrderStatus::kCancelled));
  EXPECT_FALSE(is_active(OrderStatus::kRejected));

  for (int i = 0; i <= 5; ++i) {
    const auto s = static_cast<OrderStatus>(i);
    EXPECT_NE(is_active(s), is_terminal(s));
  }
}

TEST(Enums, Opposite) {
  EXPECT_EQ(opposite(OrderSide::kBuy), OrderSide::kSell);
  EXPECT_EQ(opposite(OrderSide::kSell), OrderSide::kBuy);
}

TEST(Enums, AreOneByteForHotMessageEmbedding) {
  EXPECT_EQ(sizeof(OrderStatus), 1u);
  EXPECT_EQ(sizeof(OrderSide), 1u);
  EXPECT_EQ(sizeof(OrderType), 1u);
  EXPECT_EQ(sizeof(Liquidity), 1u);
}
