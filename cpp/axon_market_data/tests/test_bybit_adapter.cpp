// Bybit V5 public streams: orderbook.{50,200,1000} depth, orderbook.1 as the
// ticker source, and kline.1, for spot / linear / inverse.

#include "test_support.hpp"

namespace mds_test {
namespace {

using A = AdapterTestAccess;

std::string orderbook(const std::string& topic, const std::string& type, const std::string& bids,
                      const std::string& asks, int64_t u, int64_t ts = 0) {
    if (ts == 0) ts = now_ms() - 35;
    auto sym = topic.substr(topic.rfind('.') + 1);
    return R"({"topic":")" + topic + R"(","type":")" + type + R"(","ts":)" +
           std::to_string(ts) + R"(,"data":{"s":")" + sym + R"(","b":)" + bids +
           R"(,"a":)" + asks + R"(,"u":)" + std::to_string(u) +
           R"(,"seq":7961638724},"cts":1700000000000})";
}

class BybitTest : public MetricsTest {
protected:
    std::shared_ptr<BybitAdapter> make(MarketType mt, std::vector<std::string> depth,
                                       std::vector<std::string> ticker = {},
                                       std::vector<std::string> kline  = {},
                                       size_t depth_levels = 400) {
        return std::make_shared<BybitAdapter>(
            ioc, exchange("bybit", mt, depth, ticker, kline, depth_levels), sink.callback(),
            log.logger);
    }

    net::io_context ioc;
    EventSink       sink;
    CapturedLogger  log;
};

// ===========================================================================
// Configuration
// ===========================================================================

TEST_F(BybitTest, ExchangeNameIsTheV5Category) {
    EXPECT_EQ(make(MarketType::Spot, {})->exchange_name(), "bybit_spot");
    EXPECT_EQ(make(MarketType::UsdtFutures, {})->exchange_name(), "bybit_linear");
    EXPECT_EQ(make(MarketType::CoinFutures, {})->exchange_name(), "bybit_inverse");
}

// The level-1 book is the ticker source for every category: spot's `tickers`
// topic carries no bid/ask at all.
TEST_F(BybitTest, TopicsUseOrderbookOneForTickers) {
    auto a = make(MarketType::UsdtFutures, {"ETH_USDT_PERP"}, {"BTC_USDT_PERP"}, {"SOL_USDT_PERP"});
    EXPECT_EQ(A::topics(*a), (std::vector<std::string>{"orderbook.1000.ETHUSDT",
                                                       "orderbook.1.BTCUSDT", "kline.1.SOLUSDT"}));
}

// There is no orderbook.400: requests round up to 50 / 200 / 1000.
TEST_F(BybitTest, DepthRoundsUpToAnOfferedTopic) {
    struct Case { size_t want; const char* topic; size_t published; };
    for (auto c : {Case{1, "orderbook.50.ETHUSDT", 1}, Case{50, "orderbook.50.ETHUSDT", 50},
                   Case{51, "orderbook.200.ETHUSDT", 51}, Case{400, "orderbook.1000.ETHUSDT", 400},
                   Case{5000, "orderbook.1000.ETHUSDT", 1000}}) {
        auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"}, {}, {}, c.want);
        EXPECT_EQ(A::topics(*a).at(0), c.topic) << c.want;
        EXPECT_EQ(A::depth_levels(*a), c.published) << c.want;
    }
}

// ===========================================================================
// Depth
// ===========================================================================

TEST_F(BybitTest, SnapshotThenDeltasMaintainTheBook) {
    auto a = make(MarketType::UsdtFutures, {"ETH_USDT_PERP"});
    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "snapshot",
                          R"([["2000.1","1"],["2000.0","2"]])", R"([["2000.2","3"]])", 10));
    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().event_type, "snapshot");
    EXPECT_EQ(sink.last().exchange, "bybit_linear");
    EXPECT_EQ(sink.last().symbol, "ETH_USDT_PERP");
    EXPECT_EQ(sink.last_book().sequence, 10);

    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "delta", R"([["2000.1","0"]])",
                          R"([["2000.15","5"]])", 11));
    ASSERT_EQ(sink.size(), 2u);
    EXPECT_EQ(sink.last().event_type, "update");
    const auto& b = sink.last_book();
    EXPECT_EQ(b.sequence, 11);
    ASSERT_EQ(b.bids.size(), 1u);
    EXPECT_DOUBLE_EQ(b.bids[0].price, 2000.0);
    ASSERT_EQ(b.asks.size(), 2u);
    EXPECT_DOUBLE_EQ(b.asks[0].price, 2000.15);
}

TEST_F(BybitTest, SnapshotReplacesTheWholeBook) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "snapshot", R"([["1","1"],["2","1"]])", "[]", 1));
    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "snapshot", R"([["3","1"]])", "[]", 50));
    ASSERT_EQ(sink.last_book().bids.size(), 1u);
    EXPECT_DOUBLE_EQ(sink.last_book().bids[0].price, 3.0);
}

TEST_F(BybitTest, DeltaBeforeTheFirstSnapshotIsIgnored) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "delta", R"([["1","1"]])", "[]", 5));
    EXPECT_EQ(sink.size(), 0u);
}

// u == 1 on a delta means Bybit restarted the stream; its state is gone and
// a new snapshot follows.
TEST_F(BybitTest, StreamRestartDropsTheBookUntilTheNextSnapshot) {
    auto a = make(MarketType::UsdtFutures, {"ETH_USDT_PERP"});
    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "snapshot", R"([["1","1"]])", "[]", 10));
    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "delta", R"([["2","1"]])", "[]", 1));

    EXPECT_EQ(sink.size(), 1u);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "bybit_linear"}, {"reason", "stream_restart"}}), 1.0);

    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "delta", R"([["3","1"]])", "[]", 2));
    EXPECT_EQ(sink.size(), 1u);

    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "snapshot", R"([["4","1"]])", "[]", 1));
    EXPECT_EQ(sink.size(), 2u);
    EXPECT_DOUBLE_EQ(sink.last_book().bids[0].price, 4.0);
}

TEST_F(BybitTest, PublishedDepthIsCapped) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"}, {}, {}, 2);
    A::feed(*a, orderbook("orderbook.50.ETHUSDT", "snapshot", R"([["5","1"],["4","1"],["3","1"]])",
                          R"([["6","1"],["7","1"],["8","1"]])", 1));
    EXPECT_EQ(sink.last_book().bids.size(), 2u);
    EXPECT_EQ(sink.last_book().asks.size(), 2u);
}

TEST_F(BybitTest, DepthAgeIsMeasuredFromTheVenueTs) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "snapshot", "[]", "[]", 1, now_ms() - 300));
    const std::map<std::string, std::string> l{{"exchange", "bybit_spot"}, {"data_type", "depth"}};
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count", l), 1.0);
    EXPECT_GE(*metric("axon_mds_message_age_seconds_sum", l), 0.3);
}

TEST_F(BybitTest, DisconnectCountsOneResyncPerBookAndForgetsThem) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT", "BTC_USDT_SPOT"});
    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "snapshot", "[]", "[]", 1));
    A::feed(*a, orderbook("orderbook.1000.BTCUSDT", "snapshot", "[]", "[]", 1));

    A::connection_state(*a, false);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "bybit_spot"}, {"reason", "disconnect"}}), 2.0);

    A::feed(*a, orderbook("orderbook.1000.ETHUSDT", "delta", R"([["1","1"]])", "[]", 2));
    EXPECT_EQ(sink.size(), 2u);  // no book to apply the delta to
}

// ===========================================================================
// Ticker (orderbook.1)
// ===========================================================================

TEST_F(BybitTest, LevelOneBookIsPublishedAsATicker) {
    auto a = make(MarketType::UsdtFutures, {}, {"ETH_USDT_PERP"});
    A::feed(*a, orderbook("orderbook.1.ETHUSDT", "snapshot", R"([["2000.1","1.5"]])",
                          R"([["2000.2","2.5"]])", 100));
    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().data_type, DataType::Ticker);
    EXPECT_EQ(sink.last().symbol, "ETH_USDT_PERP");
    const auto& t = sink.ticker(0);
    EXPECT_DOUBLE_EQ(t.best_bid_price, 2000.1);
    EXPECT_DOUBLE_EQ(t.best_bid_qty, 1.5);
    EXPECT_DOUBLE_EQ(t.best_ask_price, 2000.2);
    EXPECT_DOUBLE_EQ(t.best_ask_qty, 2.5);
}

// A delta omits the side that did not change; the last known value of that
// side has to be carried forward rather than reset to zero.
TEST_F(BybitTest, TickerDeltaCarriesTheUnchangedSideForward) {
    auto a = make(MarketType::UsdtFutures, {}, {"ETH_USDT_PERP"});
    A::feed(*a, orderbook("orderbook.1.ETHUSDT", "snapshot", R"([["2000.1","1.5"]])",
                          R"([["2000.2","2.5"]])", 100));
    A::feed(*a, orderbook("orderbook.1.ETHUSDT", "delta", "[]", R"([["2000.3","9"]])", 101));

    ASSERT_EQ(sink.size(), 2u);
    const auto& t = sink.ticker(1);
    EXPECT_DOUBLE_EQ(t.best_bid_price, 2000.1);
    EXPECT_DOUBLE_EQ(t.best_bid_qty, 1.5);
    EXPECT_DOUBLE_EQ(t.best_ask_price, 2000.3);
    EXPECT_DOUBLE_EQ(t.best_ask_qty, 9);
}

TEST_F(BybitTest, TickerSnapshotResetsBothSides) {
    auto a = make(MarketType::UsdtFutures, {}, {"ETH_USDT_PERP"});
    A::feed(*a, orderbook("orderbook.1.ETHUSDT", "snapshot", R"([["1","1"]])", R"([["2","1"]])", 1));
    A::feed(*a, orderbook("orderbook.1.ETHUSDT", "snapshot", R"([["3","1"]])", "[]", 2));
    EXPECT_DOUBLE_EQ(sink.ticker(1).best_bid_price, 3.0);
    EXPECT_DOUBLE_EQ(sink.ticker(1).best_ask_price, 0.0);
}

// ===========================================================================
// Kline
// ===========================================================================

TEST_F(BybitTest, KlineMapsEveryField) {
    auto a = make(MarketType::CoinFutures, {}, {}, {"BTC_USD_PERP"});
    A::feed(*a, R"({"topic":"kline.1.BTCUSD","data":[{"start":1700000000000,)"
                R"("end":1700000059999,"interval":"1","open":"2000","close":"2005","high":"2010",)"
                R"("low":"1990","volume":"100","turnover":"200500","confirm":true,)"
                R"("timestamp":1700000059999}],"ts":1700000059999,"type":"snapshot"})");

    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().exchange, "bybit_inverse");
    EXPECT_EQ(sink.last().symbol, "BTC_USD_PERP");
    const auto& k = sink.kline(0);
    EXPECT_EQ(k.interval, "1m");
    EXPECT_EQ(k.open_time, 1700000000000);
    EXPECT_EQ(k.close_time, 1700000059999);
    EXPECT_DOUBLE_EQ(k.open, 2000);
    EXPECT_DOUBLE_EQ(k.close, 2005);
    EXPECT_DOUBLE_EQ(k.high, 2010);
    EXPECT_DOUBLE_EQ(k.low, 1990);
    EXPECT_DOUBLE_EQ(k.volume, 100);
    EXPECT_DOUBLE_EQ(k.quote_volume, 200500);
    EXPECT_EQ(k.num_trades, 0);  // not published by Bybit
    EXPECT_TRUE(k.is_closed);
    EXPECT_EQ(k.timestamp, 1700000059999);
}

TEST_F(BybitTest, KlineArrayMayCarrySeveralCandles) {
    auto a = make(MarketType::Spot, {}, {}, {"ETH_USDT_SPOT"});
    std::string c = R"({"start":1,"end":2,"open":"1","close":"1","high":"1","low":"1",)"
                    R"("volume":"1","turnover":"1","confirm":false,"timestamp":2})";
    A::feed(*a, R"({"topic":"kline.1.ETHUSDT","data":[)" + c + "," + c + "]}");
    EXPECT_EQ(sink.size(), 2u);
}

// ===========================================================================
// Control frames
// ===========================================================================

TEST_F(BybitTest, PongAndSubscribeAckAreNotData) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"success":true,"ret_msg":"pong","conn_id":"x","op":"ping"})");
    A::feed(*a, R"({"success":true,"ret_msg":"","conn_id":"x","op":"subscribe"})");
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(log.count_containing("subscription failed"), 0u);
}

TEST_F(BybitTest, RejectedSubscriptionIsLogged) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"success":false,"ret_msg":"error:handler not found","conn_id":"x","op":"subscribe"})");
    EXPECT_EQ(log.count_containing("error:handler not found"), 1u);
}

TEST_F(BybitTest, UnknownTopicShapesAreIgnored) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"topic":"tickers","data":{}})");
    A::feed(*a, R"({"topic":"publicTrade.ETHUSDT","data":[]})");
    EXPECT_EQ(sink.size(), 0u);
}

TEST_F(BybitTest, EmptyFrameIsCountedAsAParseError) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::feed(*a, "");
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "bybit_spot"}}), 1.0);
}

// BUG: see BinanceTest.DISABLED_MalformedJsonIsCountedAsAParseError.
TEST_F(BybitTest, DISABLED_MalformedJsonIsCountedAsAParseError) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"topic":"orderbook.1000.ETHUSDT","type":"snapshot")");
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "bybit_spot"}}), 1.0);
}

// BUG: see BinanceTest.DISABLED_FrameMissingAFieldIsDroppedNotThrown.
TEST_F(BybitTest, DISABLED_FrameMissingAFieldIsDroppedNotThrown) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    EXPECT_NO_THROW(A::feed(*a, R"({"topic":"orderbook.1000.ETHUSDT","type":"snapshot","data":{}})"));
}

}  // namespace
}  // namespace mds_test
