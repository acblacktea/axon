// Hyperliquid: l2Book (a full book every message), bbo and candle.

#include "test_support.h"

namespace mds_test {
namespace {

using A = AdapterTestAccess;

std::string levels(int n, double start, double step) {
    std::string s = "[";
    for (int i = 0; i < n; ++i) {
        if (i) s += ',';
        s += R"({"px":")" + std::to_string(start + step * i) + R"(","sz":"1","n":1})";
    }
    return s + "]";
}

std::string l2book(const std::string& coin, int bids, int asks, int64_t time = 0) {
    if (time == 0) time = now_ms() - 60;
    return R"({"channel":"l2Book","data":{"coin":")" + coin + R"(","time":)" +
           std::to_string(time) + R"(,"levels":[)" + levels(bids, 2000, -0.1) + "," +
           levels(asks, 2000.1, 0.1) + "]}}";
}

class HyperliquidTest : public MetricsTest {
protected:
    std::shared_ptr<HyperliquidAdapter> make(std::vector<std::string> depth,
                                             std::vector<std::string> ticker = {},
                                             std::vector<std::string> kline  = {},
                                             size_t depth_levels = 400) {
        return std::make_shared<HyperliquidAdapter>(
            ioc, exchange("hyperliquid", MarketType::Spot, depth, ticker, kline, depth_levels),
            sink.callback(), log.logger);
    }

    net::io_context ioc;
    EventSink       sink;
    CapturedLogger  log;
};

// ===========================================================================
// Configuration
// ===========================================================================

// Perps are named by their base asset; spot keeps both sides.
TEST_F(HyperliquidTest, CoinNamesFollowTheMarket) {
    auto a = make({"ETH_USD_PERP", "PURR_USDC_SPOT"}, {"BTC_USD_PERP"}, {"SOL_USD_PERP"});
    EXPECT_EQ(A::subscriptions(*a), (std::vector<std::string>{
                                        "l2Book:ETH", "l2Book:PURR/USDC", "bbo:BTC",
                                        "candle:SOL"}));
}

// l2Book serves 20 levels, or 5 in "fast" mode at ~8x the rate. Fast is only
// worth it when 5 levels is all that was asked for.
TEST_F(HyperliquidTest, DepthLevelsSelectsFastModeAndCaps) {
    struct Case { size_t want; bool fast; size_t published; };
    for (auto c : {Case{1, true, 1}, Case{5, true, 5}, Case{6, false, 6},
                   Case{20, false, 20}, Case{400, false, 20}}) {
        auto a = make({"ETH_USD_PERP"}, {}, {}, c.want);
        EXPECT_EQ(A::fast_book(*a), c.fast) << c.want;
        EXPECT_EQ(A::depth_levels(*a), c.published) << c.want;
    }
}

TEST_F(HyperliquidTest, CappingIsLogged) {
    make({"ETH_USD_PERP"}, {}, {}, 400);
    EXPECT_EQ(log.count_containing("capped to 20"), 1u);
}

// ===========================================================================
// l2Book
// ===========================================================================

TEST_F(HyperliquidTest, EveryL2BookIsAFullSnapshot) {
    auto a = make({"ETH_USD_PERP"});
    A::feed(*a, l2book("ETH", 3, 2));
    A::feed(*a, l2book("ETH", 1, 1));

    ASSERT_EQ(sink.size(), 2u);
    EXPECT_EQ(sink.last().event_type, "snapshot");
    EXPECT_EQ(sink.last().exchange, "hyperliquid");
    EXPECT_EQ(sink.last().symbol, "ETH_USD_PERP");
    EXPECT_EQ(sink.book(0).bids.size(), 3u);
    EXPECT_EQ(sink.book(0).asks.size(), 2u);
    EXPECT_EQ(sink.book(1).bids.size(), 1u);  // replaced, not merged
    // No sequence numbers exist on this venue.
    EXPECT_EQ(sink.book(1).sequence, 0);
}

TEST_F(HyperliquidTest, BookLevelsKeepTheVenuesOrdering) {
    auto a = make({"ETH_USD_PERP"});
    A::feed(*a, l2book("ETH", 2, 2));
    const auto& b = sink.last_book();
    EXPECT_GT(b.bids[0].price, b.bids[1].price);
    EXPECT_LT(b.asks[0].price, b.asks[1].price);
    EXPECT_NEAR(b.bids[0].price, 2000.0, 1e-9);
    EXPECT_NEAR(b.asks[0].price, 2000.1, 1e-9);
    EXPECT_DOUBLE_EQ(b.bids[0].quantity, 1.0);
}

TEST_F(HyperliquidTest, BookIsTruncatedToDepthLevels) {
    auto a = make({"ETH_USD_PERP"}, {}, {}, 3);
    A::feed(*a, l2book("ETH", 20, 20));
    EXPECT_EQ(sink.last_book().bids.size(), 3u);
    EXPECT_EQ(sink.last_book().asks.size(), 3u);
}

TEST_F(HyperliquidTest, SpotCoinMapsBackToItsUnifiedSymbol) {
    auto a = make({"PURR_USDC_SPOT"});
    A::feed(*a, l2book("PURR/USDC", 1, 1));
    EXPECT_EQ(sink.last().symbol, "PURR_USDC_SPOT");
    EXPECT_EQ(metric("axon_mds_events_total", {{"symbol", "PURR_USDC_SPOT"}}), 1.0);
}

TEST_F(HyperliquidTest, BookAgeIsMeasuredFromTheVenueTime) {
    auto a = make({"ETH_USD_PERP"});
    A::feed(*a, l2book("ETH", 1, 1, now_ms() - 200));
    const std::map<std::string, std::string> l{{"exchange", "hyperliquid"}, {"data_type", "depth"}};
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count", l), 1.0);
    EXPECT_GE(*metric("axon_mds_message_age_seconds_sum", l), 0.2);
}

// ===========================================================================
// bbo
// ===========================================================================

TEST_F(HyperliquidTest, BboBecomesATicker) {
    auto a = make({}, {"ETH_USD_PERP"});
    A::feed(*a, R"({"channel":"bbo","data":{"coin":"ETH","time":1700000000000,"bbo":[)"
                R"({"px":"2000.1","sz":"1.5","n":2},{"px":"2000.2","sz":"2.5","n":1}]}})");
    ASSERT_EQ(sink.size(), 1u);
    const auto& t = sink.ticker(0);
    EXPECT_EQ(sink.last().symbol, "ETH_USD_PERP");
    EXPECT_DOUBLE_EQ(t.best_bid_price, 2000.1);
    EXPECT_DOUBLE_EQ(t.best_bid_qty, 1.5);
    EXPECT_DOUBLE_EQ(t.best_ask_price, 2000.2);
    EXPECT_DOUBLE_EQ(t.best_ask_qty, 2.5);
    EXPECT_EQ(t.timestamp, 1700000000000);
}

// A side is null when that side of the book is empty; the last known value
// carries forward.
TEST_F(HyperliquidTest, NullBboSideKeepsTheLastValue) {
    auto a = make({}, {"ETH_USD_PERP"});
    A::feed(*a, R"({"channel":"bbo","data":{"coin":"ETH","time":1,"bbo":[)"
                R"({"px":"1","sz":"1","n":1},{"px":"2","sz":"1","n":1}]}})");
    A::feed(*a, R"({"channel":"bbo","data":{"coin":"ETH","time":2,"bbo":[null,)"
                R"({"px":"3","sz":"4","n":1}]}})");
    ASSERT_EQ(sink.size(), 2u);
    EXPECT_DOUBLE_EQ(sink.ticker(1).best_bid_price, 1.0);
    EXPECT_DOUBLE_EQ(sink.ticker(1).best_ask_price, 3.0);
    EXPECT_DOUBLE_EQ(sink.ticker(1).best_ask_qty, 4.0);
}

TEST_F(HyperliquidTest, DisconnectForgetsCarriedForwardBbo) {
    auto a = make({}, {"ETH_USD_PERP"});
    A::feed(*a, R"({"channel":"bbo","data":{"coin":"ETH","time":1,"bbo":[)"
                R"({"px":"1","sz":"1","n":1},{"px":"2","sz":"1","n":1}]}})");
    A::connection_state(*a, false);
    A::feed(*a, R"({"channel":"bbo","data":{"coin":"ETH","time":2,"bbo":[null,)"
                R"({"px":"3","sz":"4","n":1}]}})");
    EXPECT_DOUBLE_EQ(sink.ticker(1).best_bid_price, 0.0);
}

// ===========================================================================
// candle
// ===========================================================================

TEST_F(HyperliquidTest, CandleMapsEveryField) {
    auto a = make({}, {}, {"ETH_USD_PERP"});
    A::feed(*a, R"({"channel":"candle","data":{"t":1700000000000,"T":1700000059999,"s":"ETH",)"
                R"("i":"1m","o":"2000","c":"2005","h":"2010","l":"1990","v":"100","n":42}})");
    ASSERT_EQ(sink.size(), 1u);
    const auto& k = sink.kline(0);
    EXPECT_EQ(sink.last().symbol, "ETH_USD_PERP");
    EXPECT_EQ(k.interval, "1m");
    EXPECT_EQ(k.open_time, 1700000000000);
    EXPECT_EQ(k.close_time, 1700000059999);
    EXPECT_DOUBLE_EQ(k.open, 2000);
    EXPECT_DOUBLE_EQ(k.close, 2005);
    EXPECT_DOUBLE_EQ(k.high, 2010);
    EXPECT_DOUBLE_EQ(k.low, 1990);
    EXPECT_DOUBLE_EQ(k.volume, 100);
    EXPECT_DOUBLE_EQ(k.quote_volume, 0);  // not published
    EXPECT_EQ(k.num_trades, 42);
}

// The venue sends no "closed" flag; a candle is closed once its window ends.
TEST_F(HyperliquidTest, CandleIsClosedOnceItsWindowHasPassed) {
    auto a = make({}, {}, {"ETH_USD_PERP"});
    const int64_t now = now_ms();
    auto candle = [&](int64_t close) {
        return R"({"channel":"candle","data":{"t":0,"T":)" + std::to_string(close) +
               R"(,"s":"ETH","i":"1m","o":"1","c":"1","h":"1","l":"1","v":"1","n":1}})";
    };
    A::feed(*a, candle(now - 1000));
    A::feed(*a, candle(now + 60000));
    EXPECT_TRUE(sink.kline(0).is_closed);
    EXPECT_FALSE(sink.kline(1).is_closed);
}

TEST_F(HyperliquidTest, CandleRecordsNoMessageAge) {
    auto a = make({}, {}, {"ETH_USD_PERP"});
    A::feed(*a, R"({"channel":"candle","data":{"t":0,"T":1,"s":"ETH","i":"1m","o":"1",)"
                R"("c":"1","h":"1","l":"1","v":"1","n":1}})");
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count",
                     {{"exchange", "hyperliquid"}, {"data_type", "kline"}}), 0.0);
}

// ===========================================================================
// Control frames
// ===========================================================================

TEST_F(HyperliquidTest, PongAndSubscriptionResponseAreNotData) {
    auto a = make({"ETH_USD_PERP"});
    A::feed(*a, R"({"channel":"pong"})");
    A::feed(*a, R"({"channel":"subscriptionResponse","data":{"method":"subscribe",)"
                R"("subscription":{"type":"l2Book","coin":"ETH"}}})");
    EXPECT_EQ(sink.size(), 0u);
}

TEST_F(HyperliquidTest, ServerErrorIsLogged) {
    auto a = make({"ETH_USD_PERP"});
    A::feed(*a, R"({"channel":"error","data":"Invalid subscription"})");
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(log.count_containing("Invalid subscription"), 1u);
}

TEST_F(HyperliquidTest, EmptyFrameIsCountedAsAParseError) {
    auto a = make({"ETH_USD_PERP"});
    A::feed(*a, "");
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "hyperliquid"}}), 1.0);
}

// BUG: see BinanceTest.DISABLED_MalformedJsonIsCountedAsAParseError.
TEST_F(HyperliquidTest, DISABLED_MalformedJsonIsCountedAsAParseError) {
    auto a = make({"ETH_USD_PERP"});
    A::feed(*a, R"({"channel":"l2Book","data":{"coin":"ETH")");
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "hyperliquid"}}), 1.0);
}

// BUG: see BinanceTest.DISABLED_FrameMissingAFieldIsDroppedNotThrown.
TEST_F(HyperliquidTest, DISABLED_FrameMissingAFieldIsDroppedNotThrown) {
    auto a = make({"ETH_USD_PERP"});
    EXPECT_NO_THROW(A::feed(*a, R"({"channel":"l2Book","data":{"coin":"ETH"}})"));
}

}  // namespace
}  // namespace mds_test
