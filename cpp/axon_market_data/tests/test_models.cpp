// The shared data model: the local orderbook every adapter maintains, the
// venue-timestamp rule the age metric relies on, and the JSON shape published
// to subscribers.

#include "test_support.hpp"

#include <glaze/glaze.hpp>

namespace mds_test {
namespace {

TEST(Orderbook, BidsDescendAndAsksAscend) {
    Orderbook b;
    b.apply_bid(99, 1);
    b.apply_bid(101, 2);
    b.apply_bid(100, 3);
    b.apply_ask(103, 1);
    b.apply_ask(102, 1);

    auto bids = b.top_bids(10);
    ASSERT_EQ(bids.size(), 3u);
    EXPECT_DOUBLE_EQ(bids[0].price, 101);
    EXPECT_DOUBLE_EQ(bids[2].price, 99);
    EXPECT_DOUBLE_EQ(b.best_ask()->price, 102);
    EXPECT_DOUBLE_EQ(b.best_bid()->quantity, 2);
}

// Every supported venue encodes "level removed" as quantity zero.
TEST(Orderbook, ZeroQuantityRemovesTheLevel) {
    Orderbook b;
    b.apply_bid(100, 1);
    b.apply_bid(100, 0);
    b.apply_ask(101, 0);  // removing an absent level is harmless
    EXPECT_TRUE(b.empty());
}

TEST(Orderbook, SamePriceReplacesRatherThanAccumulates) {
    Orderbook b;
    b.apply_ask(101, 1);
    b.apply_ask(101, 7);
    EXPECT_EQ(b.asks.size(), 1u);
    EXPECT_DOUBLE_EQ(b.best_ask()->quantity, 7);
}

TEST(Orderbook, MidAndSpreadNeedBothSides) {
    Orderbook b;
    b.apply_bid(100, 1);
    EXPECT_FALSE(b.mid_price().has_value());
    EXPECT_FALSE(b.spread().has_value());
    b.apply_ask(102, 1);
    EXPECT_DOUBLE_EQ(*b.mid_price(), 101);
    EXPECT_DOUBLE_EQ(*b.spread(), 2);
}

TEST(Orderbook, SnapshotCopiesTheTopLevelsAndMetadata) {
    Orderbook b;
    b.symbol = "ETH_USDT_SPOT";
    b.exchange = "binance_spot";
    b.sequence = 42;
    b.timestamp = 1000;
    b.local_timestamp = 1005;
    for (int i = 0; i < 10; ++i) {
        b.apply_bid(100 - i, 1);
        b.apply_ask(101 + i, 1);
    }
    auto s = b.snapshot(3);
    EXPECT_EQ(s.symbol, "ETH_USDT_SPOT");
    EXPECT_EQ(s.exchange, "binance_spot");
    EXPECT_EQ(s.sequence, 42);
    EXPECT_EQ(s.timestamp, 1000);
    EXPECT_EQ(s.local_timestamp, 1005);
    ASSERT_EQ(s.bids.size(), 3u);
    EXPECT_DOUBLE_EQ(s.bids[2].price, 98);
    EXPECT_DOUBLE_EQ(s.asks[2].price, 103);
}

TEST(Orderbook, ClearResetsLevelsAndSequence) {
    Orderbook b;
    b.apply_bid(1, 1);
    b.sequence = 9;
    b.clear();
    EXPECT_TRUE(b.empty());
    EXPECT_EQ(b.sequence, 0);
}

// The age metric must skip streams that carry no venue time. Adapters mark
// those by stamping both fields from one now_ms() call.
TEST(VenueTimestamp, EqualStampsMeanTheVenueSentNone) {
    TickerData t;
    t.timestamp = t.local_timestamp = 1700000000000;
    EXPECT_EQ(venue_timestamp(t), 0);

    t.local_timestamp = t.timestamp + 12;
    EXPECT_EQ(venue_timestamp(t), 1700000000000);

    KlineData k;
    k.timestamp = 5;
    k.local_timestamp = 9;
    EXPECT_EQ(venue_timestamp(EventData{k}), 5);
}

TEST(Enums, MarketTypeRoundTripsAndDefaultsToSpot) {
    for (auto mt : {MarketType::Spot, MarketType::UsdtFutures, MarketType::CoinFutures})
        EXPECT_EQ(parse_market_type(to_string(mt)), mt);
    EXPECT_EQ(parse_market_type("nonsense"), MarketType::Spot);
    EXPECT_EQ(to_string(DataType::Depth), "depth");
    EXPECT_EQ(to_string(DataType::Ticker), "ticker");
    EXPECT_EQ(to_string(DataType::Kline), "kline");
}

// The published JSON is the subscriber contract (example/subscriber.py reads
// these keys). Renaming a field here breaks every consumer silently.
TEST(Serialization, OrderbookJsonShape) {
    OrderbookSnapshot s{"ETH_USDT_SPOT", "okx", {{2000.5, 1.25}}, {{2001, 3}}, 7, 100, 101};
    EXPECT_EQ(glz::write_json(s).value(),
              R"({"symbol":"ETH_USDT_SPOT","exchange":"okx","bids":[{"price":2000.5,"quantity":1.25}],)"
              R"("asks":[{"price":2001,"quantity":3}],"sequence":7,"timestamp":100,"local_timestamp":101})");
}

TEST(Serialization, TickerJsonShape) {
    TickerData t{"BTC_USDT_PERP", "bybit_linear", 1, 2, 3, 4, 5, 6};
    EXPECT_EQ(glz::write_json(t).value(),
              R"({"symbol":"BTC_USDT_PERP","exchange":"bybit_linear","best_bid_price":1,)"
              R"("best_bid_qty":2,"best_ask_price":3,"best_ask_qty":4,"timestamp":5,"local_timestamp":6})");
}

TEST(Serialization, KlineJsonShape) {
    KlineData k{"ETH_USD_PERP", "hyperliquid", "1m", 0, 59999, 1, 2, 0.5, 1.5, 10, 20, 3, true, 7, 8};
    EXPECT_EQ(glz::write_json(k).value(),
              R"({"symbol":"ETH_USD_PERP","exchange":"hyperliquid","interval":"1m","open_time":0,)"
              R"("close_time":59999,"open":1,"high":2,"low":0.5,"close":1.5,"volume":10,)"
              R"("quote_volume":20,"num_trades":3,"is_closed":true,"timestamp":7,"local_timestamp":8})");
}

}  // namespace
}  // namespace mds_test
