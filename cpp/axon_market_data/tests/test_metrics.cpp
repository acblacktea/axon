// The metrics client: the hot-path topic handles, the age-sample rules, label
// cardinality, the disabled no-op mode, and the endpoint's failure behaviour.

#include "test_support.hpp"

#include <boost/asio.hpp>
#include <boost/beast.hpp>

namespace mds_test {
namespace {

using Metrics = MetricsTest;

TEST_F(Metrics, DeclaringATopicPublishesItsSubscriptionGauge) {
    get_metrics().declare_topic("okx", DataType::Depth, "ETH_USDT_SPOT");
    EXPECT_EQ(metric("axon_mds_subscription",
                     {{"exchange", "okx"}, {"data_type", "depth"}, {"symbol", "ETH_USDT_SPOT"}}),
              1.0);
    // Declared but silent is the state the alert looks for: the event counter
    // exists at zero, rather than being absent.
    EXPECT_EQ(metric("axon_mds_events_total", {{"symbol", "ETH_USDT_SPOT"}}), 0.0);
}

TEST_F(Metrics, EventsCountAndStampTheTopic) {
    auto t = get_metrics().declare_topic("okx", DataType::Ticker, "ETH_USDT_SPOT");
    t.on_event(1000, 1050);
    t.on_event(2000, 2010);
    EXPECT_EQ(metric("axon_mds_events_total", {{"symbol", "ETH_USDT_SPOT"}}), 2.0);
    EXPECT_DOUBLE_EQ(*metric("axon_mds_last_event_timestamp_seconds", {{"symbol", "ETH_USDT_SPOT"}}),
                     2.010);
    const std::map<std::string, std::string> l{{"exchange", "okx"}, {"data_type", "ticker"}};
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count", l), 2.0);
    EXPECT_NEAR(*metric("axon_mds_message_age_seconds_sum", l), 0.060, 1e-9);
}

// A missing stamp, or a venue clock ahead of ours, would record an age of
// zero -- a perfect-looking feed. The sample is skipped instead.
TEST_F(Metrics, AgeIsNotSampledWithoutAUsableVenueStamp) {
    auto t = get_metrics().declare_topic("okx", DataType::Ticker, "X");
    t.on_event(0, 5000);     // no venue stamp
    t.on_event(9000, 5000);  // venue stamp in our future
    EXPECT_EQ(metric("axon_mds_events_total", {{"symbol", "X"}}), 2.0);
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count", {{"exchange", "okx"}}), 0.0);
}

TEST_F(Metrics, AgeBucketsSpanMillisecondsToSeconds) {
    auto t = get_metrics().declare_topic("x", DataType::Depth, "S");
    t.on_event(1000, 1003);   // 3 ms
    t.on_event(1000, 1200);   // 200 ms
    t.on_event(1000, 4000);   // 3 s
    const std::map<std::string, std::string> l{{"exchange", "x"}};
    EXPECT_EQ(bucket("axon_mds_message_age_seconds", l, 0.001), 0.0);
    EXPECT_EQ(bucket("axon_mds_message_age_seconds", l, 0.005), 1.0);
    EXPECT_EQ(bucket("axon_mds_message_age_seconds", l, 0.25), 2.0);
    EXPECT_EQ(bucket("axon_mds_message_age_seconds", l, 1.0), 2.0);
    EXPECT_EQ(bucket("axon_mds_message_age_seconds", l, 5.0), 3.0);
}

// Healthy internal work takes tens of microseconds. With Prometheus' default
// buckets (starting at 5 ms) every sample would land in the first bucket.
TEST_F(Metrics, InternalLatencyBucketsResolveMicroseconds) {
    get_metrics().observe_handler("x", 0.000015);  // 15 us
    get_metrics().observe_handler("x", 0.000300);  // 300 us
    const std::map<std::string, std::string> l{{"exchange", "x"}};
    EXPECT_EQ(bucket("axon_mds_handler_duration_seconds", l, 0.00001), 0.0);
    EXPECT_EQ(bucket("axon_mds_handler_duration_seconds", l, 0.000025), 1.0);
    EXPECT_EQ(bucket("axon_mds_handler_duration_seconds", l, 0.0005), 2.0);

    auto t = get_metrics().declare_topic("x", DataType::Depth, "S");
    t.on_published(0.00004);
    EXPECT_EQ(bucket("axon_mds_publish_duration_seconds", l, 0.00005), 1.0);
}

// A histogram is a dozen series per label set; carrying the symbol would
// multiply that by the whole symbol table.
TEST_F(Metrics, HistogramsAreNotLabelledBySymbol) {
    for (auto sym : {"A", "B", "C"}) {
        auto t = get_metrics().declare_topic("x", DataType::Depth, sym);
        t.on_event(1, 2);
        t.on_published(0.001);
    }
    for (auto& s : parse_exposition(get_metrics().serialize())) {
        if (s.name.starts_with("axon_mds_message_age") ||
            s.name.starts_with("axon_mds_publish_duration"))
            EXPECT_EQ(s.labels.count("symbol"), 0u) << s.name;
    }
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count", {{"exchange", "x"}}), 3.0);
}

TEST_F(Metrics, ResyncReasonsAreAClosedSet) {
    EXPECT_EQ(to_string(ResyncReason::SequenceGap), "sequence_gap");
    EXPECT_EQ(to_string(ResyncReason::SnapshotTooOld), "snapshot_too_old");
    EXPECT_EQ(to_string(ResyncReason::ChecksumMismatch), "checksum_mismatch");
    EXPECT_EQ(to_string(ResyncReason::StreamRestart), "stream_restart");
    EXPECT_EQ(to_string(ResyncReason::Disconnect), "disconnect");
    EXPECT_EQ(to_string(ResyncReason::SnapshotFailed), "snapshot_failed");

    get_metrics().inc_resync("x", ResyncReason::SequenceGap);
    get_metrics().inc_resync("x", ResyncReason::SequenceGap);
    get_metrics().inc_resync("x", ResyncReason::Disconnect);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total", {{"reason", "sequence_gap"}}), 2.0);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total", {{"exchange", "x"}}), 3.0);
}

TEST_F(Metrics, ConnectionHealthSeries) {
    auto& m = get_metrics();
    m.set_ws_connected("bybit_linear", true);
    m.observe_ws_connect("bybit_linear", 0.3);
    m.inc_ws_connect_failure("bybit_linear");
    m.inc_ws_reconnect("bybit_linear");
    m.observe_ws_session("bybit_linear", 120);
    m.observe_rest_snapshot("bybit_linear", 0.2);
    m.inc_rest_snapshot_failure("bybit_linear");
    m.inc_checksum_failure("bybit_linear");
    m.inc_publish_failure("bybit_linear");
    m.inc_parse_error("bybit_linear");
    m.inc_undeclared_event("bybit_linear");

    const std::map<std::string, std::string> l{{"exchange", "bybit_linear"}};
    EXPECT_EQ(metric("axon_mds_ws_connected", l), 1.0);
    EXPECT_EQ(metric("axon_mds_ws_connect_duration_seconds_count", l), 1.0);
    EXPECT_EQ(metric("axon_mds_ws_connect_failure_total", l), 1.0);
    EXPECT_EQ(metric("axon_mds_ws_reconnect_total", l), 1.0);
    EXPECT_EQ(bucket("axon_mds_ws_session_duration_seconds", l, 60), 0.0);
    EXPECT_EQ(bucket("axon_mds_ws_session_duration_seconds", l, 300), 1.0);
    EXPECT_EQ(metric("axon_mds_rest_snapshot_duration_seconds_count", l), 1.0);
    EXPECT_EQ(metric("axon_mds_rest_snapshot_failure_total", l), 1.0);
    EXPECT_EQ(metric("axon_mds_checksum_failure_total", l), 1.0);
    EXPECT_EQ(metric("axon_mds_publish_failure_total", l), 1.0);
    EXPECT_EQ(metric("axon_mds_parse_error_total", l), 1.0);
    EXPECT_EQ(metric("axon_mds_undeclared_event_total", l), 1.0);

    m.set_ws_connected("bybit_linear", false);
    EXPECT_EQ(metric("axon_mds_ws_connected", l), 0.0);
}

// Business code never guards with `if (metrics_enabled)`, so every call must
// be safe on a disabled client.
TEST(MetricsDisabled, EveryCallIsANoOp) {
    MetricsClient m(false);
    auto t = m.declare_topic("x", DataType::Depth, "S");
    t.on_event(1, 2);
    t.on_published(0.1);
    m.set_ws_connected("x", true);
    m.observe_ws_connect("x", 1);
    m.inc_ws_connect_failure("x");
    m.inc_ws_reconnect("x");
    m.observe_ws_session("x", 1);
    m.observe_handler("x", 1);
    m.inc_parse_error("x");
    m.inc_resync("x", ResyncReason::Disconnect);
    m.inc_checksum_failure("x");
    m.observe_rest_snapshot("x", 1);
    m.inc_rest_snapshot_failure("x");
    m.inc_publish_failure("x");
    m.inc_undeclared_event("x");
    m.start_server("127.0.0.1", 0);
    m.stop_server();
    EXPECT_FALSE(m.enabled());
    EXPECT_EQ(m.serialize(), "");
}

// An unregistered handle (a topic never declared) must be inert too.
TEST(MetricsDisabled, DefaultTopicHandleIsInert) {
    TopicMetrics t;
    t.on_event(1, 2);
    t.on_published(1);
    SUCCEED();
}

// A metrics endpoint that silently failed to start looks exactly like a
// process that is down. Failing to bind must stop startup.
TEST(MetricsServer, BindFailureThrows) {
    boost::asio::io_context ioc;
    boost::asio::ip::tcp::acceptor taken(
        ioc, {boost::asio::ip::make_address("127.0.0.1"), 0});
    const int port = taken.local_endpoint().port();

    MetricsClient m(true);
    EXPECT_THROW(m.start_server("127.0.0.1", port), std::runtime_error);
}

TEST(MetricsServer, ServesTheRegistryOverHttp) {
    namespace beast = boost::beast;
    namespace http  = beast::http;
    using tcp       = boost::asio::ip::tcp;

    // The client does not report the port it bound, so find a free one first.
    boost::asio::io_context ioc;
    int port = 0;
    {
        tcp::acceptor probe(ioc, {boost::asio::ip::make_address("127.0.0.1"), 0});
        port = probe.local_endpoint().port();
    }

    MetricsClient m(true);
    m.inc_parse_error("served");
    m.start_server("127.0.0.1", port);

    beast::tcp_stream stream(ioc);
    stream.connect(tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"),
                                 static_cast<unsigned short>(port)));
    http::request<http::empty_body> req(http::verb::get, "/metrics", 11);
    req.set(http::field::host, "127.0.0.1");
    http::write(stream, req);
    beast::flat_buffer buf;
    http::response<http::string_body> res;
    http::read(stream, buf, res);

    EXPECT_EQ(res.result_int(), 200);
    EXPECT_NE(res.body().find(R"(axon_mds_parse_error_total{exchange="served"} 1)"),
              std::string::npos);
    m.stop_server();
}

}  // namespace
}  // namespace mds_test
