// BaseAdapter behaviour shared by every venue: the one-shot "subscribed but
// silent" check, and the per-topic bookkeeping behind it.

#include "test_support.h"

namespace mds_test {
namespace {

using A = AdapterTestAccess;

class SilentTopicTest : public MetricsTest {
protected:
    net::io_context ioc;
    EventSink       sink;
    CapturedLogger  log;
};

// A venue's subscribe acknowledgement proves nothing -- Binance acks stream
// names that do not exist. A topic that never delivers must be called out.
TEST_F(SilentTopicTest, NamesEveryTopicThatNeverDelivered) {
    BybitAdapter a(ioc, exchange("bybit", MarketType::Spot, {"ETH_USDT_SPOT"}, {"BTC_USDT_SPOT"},
                                 {"SOL_USDT_SPOT"}),
                   sink.callback(), log.logger);
    A::feed(a, R"({"topic":"orderbook.1.BTCUSDT","type":"snapshot","ts":1,)"
               R"("data":{"b":[["1","1"]],"a":[["2","1"]],"u":1}})");

    a.warn_if_silent();

    EXPECT_EQ(log.count_containing("SILENT"), 2u);
    EXPECT_EQ(log.count_containing("depth.ETH_USDT_SPOT"), 1u);
    EXPECT_EQ(log.count_containing("kline.SOL_USDT_SPOT"), 1u);
    EXPECT_EQ(log.count_containing("ticker.BTC_USDT_SPOT"), 0u);
    EXPECT_EQ(log.count_containing("2 of 3 subscribed topics are silent"), 1u);
}

TEST_F(SilentTopicTest, SaysNothingWhenEveryTopicDelivered) {
    HyperliquidAdapter a(ioc, exchange("hyperliquid", MarketType::Spot, {}, {"ETH_USD_PERP"}),
                         sink.callback(), log.logger);
    A::feed(a, R"({"channel":"bbo","data":{"coin":"ETH","time":1,"bbo":[null,null]}})");

    a.warn_if_silent();
    EXPECT_EQ(log.count_containing("SILENT"), 0u);
    EXPECT_EQ(log.count_containing("silent"), 0u);
}

// The same symbol subscribed for two data types is two topics: data on one
// must not mark the other as alive.
TEST_F(SilentTopicTest, TopicsAreTrackedPerDataTypeNotPerSymbol) {
    OkxAdapter a(ioc, exchange("okx", MarketType::Spot, {"ETH_USDT_SPOT"}, {"ETH_USDT_SPOT"}),
                 sink.callback(), log.logger);
    A::feed(a, R"({"arg":{"channel":"bbo-tbt","instId":"ETH-USDT"},"data":[{"asks":[["2","1"]],)"
               R"("bids":[["1","1"]],"ts":"1"}]})");

    a.warn_if_silent();
    EXPECT_EQ(log.count_containing("depth.ETH_USDT_SPOT"), 1u);
    EXPECT_EQ(log.count_containing("1 of 2 subscribed topics are silent"), 1u);
}

// Undeclared data is counted in aggregate; it neither satisfies a declared
// topic nor shows up as one.
TEST_F(SilentTopicTest, UndeclaredDataDoesNotSatisfyADeclaredTopic) {
    BybitAdapter a(ioc, exchange("bybit", MarketType::Spot, {}, {"ETH_USDT_SPOT"}),
                   sink.callback(), log.logger);
    A::feed(a, R"({"topic":"orderbook.1.DOGEUSDT","type":"snapshot","ts":1,)"
               R"("data":{"b":[],"a":[],"u":1}})");

    a.warn_if_silent();
    EXPECT_EQ(log.count_containing("ticker.ETH_USDT_SPOT"), 1u);
    EXPECT_EQ(metric("axon_mds_undeclared_event_total", {{"exchange", "bybit_spot"}}), 1.0);
}

}  // namespace
}  // namespace mds_test
