// The ZMQ PUB output: the [topic, json] framing subscribers depend on, topic
// prefix filtering, and behaviour when nobody is draining.

#include "test_support.h"

#include <atomic>

#include <simdjson.h>
#include <zmq.hpp>
#include <zmq_addon.hpp>

#include "axon/market_data/zmq_publisher.h"

namespace mds_test {
namespace {

std::string unique_ipc() {
    static std::atomic<int> n{0};
    return "ipc:///tmp/mds_test_" + std::to_string(::getpid()) + "_" + std::to_string(n++) + ".ipc";
}

MarketDataEvent ticker_event(const std::string& exchange, const std::string& symbol,
                             double bid = 1, double ask = 2) {
    TickerData t;
    t.symbol = symbol;
    t.exchange = exchange;
    t.best_bid_price = bid;
    t.best_ask_price = ask;
    MarketDataEvent e;
    e.data_type = DataType::Ticker;
    e.event_type = "update";
    e.symbol = symbol;
    e.exchange = exchange;
    e.data = t;
    return e;
}

class ZmqPublisherTest : public MetricsTest {
protected:
    void SetUp() override {
        MetricsTest::SetUp();
        pub = std::make_unique<ZmqPublisher>(address, log.logger);
        pub->start();
        sub.set(zmq::sockopt::rcvtimeo, 50);
    }

    // PUB/SUB has the slow-joiner problem: messages published before the
    // subscription propagates are dropped. Keep publishing until one lands.
    std::vector<std::string> publish_until_received(const MarketDataEvent& e) {
        for (int i = 0; i < 200; ++i) {
            pub->publish(e);
            std::vector<zmq::message_t> parts;
            if (zmq::recv_multipart(sub, std::back_inserter(parts))) {
                std::vector<std::string> out;
                for (auto& p : parts) out.push_back(p.to_string());
                return out;
            }
        }
        return {};
    }

    std::string                   address = unique_ipc();
    CapturedLogger                log;
    std::unique_ptr<ZmqPublisher> pub;
    zmq::context_t                ctx;
    zmq::socket_t                 sub{ctx, zmq::socket_type::sub};
};

TEST_F(ZmqPublisherTest, MessageIsTopicFrameThenJsonFrame) {
    sub.connect(address);
    sub.set(zmq::sockopt::subscribe, "");

    auto parts = publish_until_received(ticker_event("okx", "ETH_USDT_SPOT", 2000.5, 2000.6));
    ASSERT_EQ(parts.size(), 2u);
    EXPECT_EQ(parts[0], "okx.ticker.ETH_USDT_SPOT");

    simdjson::dom::parser p;
    auto doc = p.parse(parts[1]);
    ASSERT_FALSE(doc.error()) << parts[1];
    EXPECT_EQ(std::string_view(doc["symbol"].get_string().value()), "ETH_USDT_SPOT");
    EXPECT_DOUBLE_EQ(double(doc["best_bid_price"]), 2000.5);
    EXPECT_DOUBLE_EQ(double(doc["best_ask_price"]), 2000.6);
}

TEST_F(ZmqPublisherTest, EveryDataTypeHasItsOwnTopic) {
    sub.connect(address);
    sub.set(zmq::sockopt::subscribe, "");

    MarketDataEvent depth;
    depth.data_type = DataType::Depth;
    depth.exchange = "binance_spot";
    depth.symbol = "BTC_USDT_SPOT";
    depth.data = OrderbookSnapshot{"BTC_USDT_SPOT", "binance_spot", {{1, 2}}, {{3, 4}}, 9, 0, 0};
    auto parts = publish_until_received(depth);
    ASSERT_EQ(parts.size(), 2u);
    EXPECT_EQ(parts[0], "binance_spot.depth.BTC_USDT_SPOT");
    EXPECT_NE(parts[1].find(R"("bids":[{"price":1,"quantity":2}])"), std::string::npos);

    MarketDataEvent kline;
    kline.data_type = DataType::Kline;
    kline.exchange = "bybit_linear";
    kline.symbol = "ETH_USDT_PERP";
    KlineData k;
    k.interval = "1m";
    kline.data = k;
    parts = publish_until_received(kline);
    ASSERT_EQ(parts.size(), 2u);
    EXPECT_EQ(parts[0], "bybit_linear.kline.ETH_USDT_PERP");
    EXPECT_NE(parts[1].find(R"("interval":"1m")"), std::string::npos);
}

// Subscribers filter on topic prefix, which is why the topic leads with the
// exchange: "binance_spot.depth" must select exactly that exchange's books.
TEST_F(ZmqPublisherTest, SubscribersFilterByTopicPrefix) {
    sub.connect(address);
    sub.set(zmq::sockopt::subscribe, "bybit_linear.ticker");

    // Warm up the subscription with a matching message first.
    ASSERT_EQ(publish_until_received(ticker_event("bybit_linear", "X")).size(), 2u);

    pub->publish(ticker_event("okx", "ETH_USDT_SPOT"));
    pub->publish(ticker_event("bybit_spot", "ETH_USDT_SPOT"));
    pub->publish(ticker_event("bybit_linear", "ETH_USDT_PERP"));

    std::vector<zmq::message_t> parts;
    ASSERT_TRUE(zmq::recv_multipart(sub, std::back_inserter(parts)));
    EXPECT_EQ(parts[0].to_string(), "bybit_linear.ticker.ETH_USDT_PERP");
    parts.clear();
    EXPECT_FALSE(zmq::recv_multipart(sub, std::back_inserter(parts)));
}

TEST_F(ZmqPublisherTest, PublishingWhileStoppedIsANoOp) {
    pub->stop();
    pub->publish(ticker_event("okx", "ETH_USDT_SPOT"));
    EXPECT_EQ(metric_or_zero("axon_mds_publish_failure_total"), 0.0);
}

TEST_F(ZmqPublisherTest, StartAndStopAreIdempotent) {
    pub->start();
    pub->stop();
    pub->stop();
    SUCCEED();
}

TEST(ZmqPublisherBind, BindFailureThrows) {
    ZmqPublisher pub("tcp://256.0.0.1:1");
    EXPECT_ANY_THROW(pub.start());
}

// A PUB socket with no subscribers discards; that is not a failure.
TEST_F(ZmqPublisherTest, NoSubscribersIsNotAFailure) {
    for (int i = 0; i < 1000; ++i) pub->publish(ticker_event("okx", "ETH_USDT_SPOT"));
    EXPECT_EQ(metric_or_zero("axon_mds_publish_failure_total"), 0.0);
}

// A plain PUB socket drops at the high-water mark and reports success, so a
// subscriber that stopped draining -- the one case
// axon_mds_publish_failure_total exists for -- used to be invisible. The
// socket now runs with ZMQ_XPUB_NODROP, which turns a full subscriber queue
// into EAGAIN; the publisher counts it and resends lossy.
TEST_F(ZmqPublisherTest, StalledSubscriberIsCountedAsDropped) {
    sub.set(zmq::sockopt::rcvhwm, 10);
    sub.connect(address);
    sub.set(zmq::sockopt::subscribe, "");
    ASSERT_EQ(publish_until_received(ticker_event("okx", "X")).size(), 2u);

    // Never read again: well past sndhwm (100k) + rcvhwm + socket buffers.
    for (int i = 0; i < 400000; ++i) pub->publish(ticker_event("okx", "ETH_USDT_SPOT"));
    EXPECT_GT(metric_or_zero("axon_mds_publish_failure_total", {{"exchange", "okx"}}), 0.0);
}

// What NODROP must not cost: under NODROP alone a frame that would overflow
// one subscriber is sent to nobody, so one stalled consumer would black out
// every other. A subscriber that keeps reading must miss nothing.
TEST_F(ZmqPublisherTest, HealthySubscriberMissesNothingWhileAnotherStalls) {
    sub.set(zmq::sockopt::rcvhwm, 10);
    sub.connect(address);
    sub.set(zmq::sockopt::subscribe, "");
    ASSERT_EQ(publish_until_received(ticker_event("okx", "X")).size(), 2u);

    zmq::socket_t healthy{ctx, zmq::socket_type::sub};
    healthy.connect(address);
    healthy.set(zmq::sockopt::subscribe, "okx.ticker.ETH_USDT_SPOT");
    // Slow joiner: publish until `healthy` has its subscription in place,
    // then discard whatever arrived.
    healthy.set(zmq::sockopt::rcvtimeo, 10);
    for (int i = 0; i < 200; ++i) {
        pub->publish(ticker_event("okx", "ETH_USDT_SPOT"));
        std::vector<zmq::message_t> parts;
        if (zmq::recv_multipart(healthy, std::back_inserter(parts))) break;
    }
    for (std::vector<zmq::message_t> parts;
         zmq::recv_multipart(healthy, std::back_inserter(parts)); parts.clear()) {}
    healthy.set(zmq::sockopt::rcvtimeo, 1000);

    constexpr int n = 300000;
    int received = 0;
    auto take = [&](zmq::recv_flags f) {
        std::vector<zmq::message_t> parts;
        if (!zmq::recv_multipart(healthy, std::back_inserter(parts), f)) return false;
        EXPECT_EQ(parts.size(), 2u);
        EXPECT_EQ(parts[0].to_string(), "okx.ticker.ETH_USDT_SPOT");
        ++received;
        return true;
    };
    for (int i = 0; i < n; ++i) {
        pub->publish(ticker_event("okx", "ETH_USDT_SPOT"));
        while (take(zmq::recv_flags::dontwait)) {}
    }
    while (received < n && take(zmq::recv_flags::none)) {}

    EXPECT_GT(metric_or_zero("axon_mds_publish_failure_total", {{"exchange", "okx"}}), 0.0);
    EXPECT_EQ(received, n);
}

}  // namespace
}  // namespace mds_test
