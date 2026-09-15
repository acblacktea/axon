#include "axon_market_data/metrics.hpp"

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <map>
#include <stdexcept>

namespace axon_market_data {
namespace {

// Internal work: JSON parse, book update, serialize, ZMQ send. Tens of
// microseconds when healthy, so the buckets have to start well below a
// millisecond -- the usual Prometheus default ladder starts at 5ms and would
// drop every healthy sample into the first bucket, producing a histogram that
// says nothing except "fast".
const prometheus::Histogram::BucketBoundaries kInternal{
    0.00001, 0.000025, 0.00005, 0.0001, 0.00025, 0.0005,
    0.001,   0.0025,   0.005,   0.01,   0.025,   0.05};

// Venue stamp -> our publish. Dominated by the WAN leg and the venue's own
// queueing, so this ladder lives in milliseconds. This is the one metric that
// separates "we are slow" from "the feed is slow".
const prometheus::Histogram::BucketBoundaries kAge{
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 5.0};

// TCP + TLS + WebSocket handshake, or one REST snapshot round trip.
const prometheus::Histogram::BucketBoundaries kConnect{
    0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0};

// Seconds a connection stayed up before it dropped.
const prometheus::Histogram::BucketBoundaries kSession{
    30, 60, 300, 900, 3600, 10800, 43200, 86400};

} // namespace

std::string_view to_string(ResyncReason reason) {
    switch (reason) {
    case ResyncReason::SequenceGap:      return "sequence_gap";
    case ResyncReason::SnapshotTooOld:   return "snapshot_too_old";
    case ResyncReason::ChecksumMismatch: return "checksum_mismatch";
    case ResyncReason::StreamRestart:    return "stream_restart";
    case ResyncReason::Disconnect:       return "disconnect";
    case ResyncReason::SnapshotFailed:   return "snapshot_failed";
    }
    return "unknown";
}

// ---------------------------------------------------------------------------
// TopicMetrics -- the hot path
// ---------------------------------------------------------------------------

void TopicMetrics::on_event(int64_t venue_ts_ms, int64_t local_ts_ms) noexcept {
    if (events_ == nullptr) return; // disabled, or never declared

    events_->Increment();
    last_event_->Set(static_cast<double>(local_ts_ms) / 1000.0);

    // A venue that sends no timestamp, or a clock skewed far enough that its
    // stamp is in our future, would both record as 0 and make the feed look
    // instantaneous. Skip the sample instead: a missing observation is honest,
    // a zero one is a lie that drags every quantile down with it.
    if (venue_ts_ms > 0 && local_ts_ms >= venue_ts_ms) {
        age_->Observe(static_cast<double>(local_ts_ms - venue_ts_ms) / 1000.0);
    }
}

void TopicMetrics::on_published(double seconds) noexcept {
    if (publish_ == nullptr) return;
    publish_->Observe(seconds);
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

struct MetricsClient::Impl {
    std::shared_ptr<prometheus::Registry> registry =
        std::make_shared<prometheus::Registry>();
    std::unique_ptr<prometheus::Exposer> exposer;

    prometheus::Family<prometheus::Gauge>*     subscription      = nullptr;
    prometheus::Family<prometheus::Counter>*   events            = nullptr;
    prometheus::Family<prometheus::Gauge>*     last_event        = nullptr;
    prometheus::Family<prometheus::Histogram>* message_age       = nullptr;
    prometheus::Family<prometheus::Histogram>* publish_duration  = nullptr;
    prometheus::Family<prometheus::Counter>*   undeclared_event  = nullptr;

    prometheus::Family<prometheus::Gauge>*     ws_connected      = nullptr;
    prometheus::Family<prometheus::Histogram>* ws_connect        = nullptr;
    prometheus::Family<prometheus::Counter>*   ws_connect_failure = nullptr;
    prometheus::Family<prometheus::Counter>*   ws_reconnect      = nullptr;
    prometheus::Family<prometheus::Histogram>* ws_session        = nullptr;

    prometheus::Family<prometheus::Histogram>* handler_duration  = nullptr;
    prometheus::Family<prometheus::Counter>*   parse_error       = nullptr;

    prometheus::Family<prometheus::Counter>*   resync            = nullptr;
    prometheus::Family<prometheus::Counter>*   checksum_failure  = nullptr;
    prometheus::Family<prometheus::Histogram>* rest_snapshot     = nullptr;
    prometheus::Family<prometheus::Counter>*   rest_failure      = nullptr;

    prometheus::Family<prometheus::Counter>*   publish_failure   = nullptr;

    void build() {
        subscription = &prometheus::BuildGauge()
                            .Name("axon_mds_subscription")
                            .Help("1 for every topic this service is configured to receive")
                            .Register(*registry);
        events = &prometheus::BuildCounter()
                      .Name("axon_mds_events_total")
                      .Help("Market data events published, per topic")
                      .Register(*registry);
        last_event = &prometheus::BuildGauge()
                          .Name("axon_mds_last_event_timestamp_seconds")
                          .Help("Unix time of the last event published on a topic")
                          .Register(*registry);
        message_age = &prometheus::BuildHistogram()
                           .Name("axon_mds_message_age_seconds")
                           .Help("Venue timestamp to local publish")
                           .Register(*registry);
        publish_duration = &prometheus::BuildHistogram()
                                .Name("axon_mds_publish_duration_seconds")
                                .Help("JSON serialization plus ZMQ send, per event")
                                .Register(*registry);
        undeclared_event = &prometheus::BuildCounter()
                                .Name("axon_mds_undeclared_event_total")
                                .Help("Events for a symbol that was never subscribed")
                                .Register(*registry);

        ws_connected = &prometheus::BuildGauge()
                            .Name("axon_mds_ws_connected")
                            .Help("1 while the exchange WebSocket is connected")
                            .Register(*registry);
        ws_connect = &prometheus::BuildHistogram()
                          .Name("axon_mds_ws_connect_duration_seconds")
                          .Help("DNS, TCP, TLS and WebSocket handshake")
                          .Register(*registry);
        ws_connect_failure = &prometheus::BuildCounter()
                                  .Name("axon_mds_ws_connect_failure_total")
                                  .Help("Connection attempts that failed")
                                  .Register(*registry);
        ws_reconnect = &prometheus::BuildCounter()
                            .Name("axon_mds_ws_reconnect_total")
                            .Help("Reconnection attempts")
                            .Register(*registry);
        ws_session = &prometheus::BuildHistogram()
                          .Name("axon_mds_ws_session_duration_seconds")
                          .Help("How long a connection lasted before it dropped")
                          .Register(*registry);

        handler_duration = &prometheus::BuildHistogram()
                                .Name("axon_mds_handler_duration_seconds")
                                .Help("Parse, book update and publish for one raw frame")
                                .Register(*registry);
        parse_error = &prometheus::BuildCounter()
                           .Name("axon_mds_parse_error_total")
                           .Help("Frames that failed to parse as JSON")
                           .Register(*registry);

        resync = &prometheus::BuildCounter()
                      .Name("axon_mds_orderbook_resync_total")
                      .Help("Local orderbook rebuilds, by what forced them")
                      .Register(*registry);
        checksum_failure = &prometheus::BuildCounter()
                                .Name("axon_mds_checksum_failure_total")
                                .Help("Venue checksum disagreed with the local book")
                                .Register(*registry);
        rest_snapshot = &prometheus::BuildHistogram()
                             .Name("axon_mds_rest_snapshot_duration_seconds")
                             .Help("REST orderbook snapshot round trip")
                             .Register(*registry);
        rest_failure = &prometheus::BuildCounter()
                            .Name("axon_mds_rest_snapshot_failure_total")
                            .Help("REST snapshot calls that failed")
                            .Register(*registry);

        publish_failure = &prometheus::BuildCounter()
                               .Name("axon_mds_publish_failure_total")
                               .Help("ZMQ sends that failed or were dropped")
                               .Register(*registry);
    }
};

MetricsClient::MetricsClient(bool enabled) : enabled_(enabled) {
    if (enabled_) {
        impl_ = std::make_unique<Impl>();
        impl_->build();
    }
}

MetricsClient::~MetricsClient() = default;

void MetricsClient::start_server(const std::string& host, int port) {
    if (!enabled_ || !impl_) return;

    const std::string bind = host + ":" + std::to_string(port);
    try {
        impl_->exposer = std::make_unique<prometheus::Exposer>(bind);
        impl_->exposer->RegisterCollectable(impl_->registry);
    } catch (const std::exception& e) {
        // Loud rather than silent: an endpoint that failed to bind looks
        // exactly like a process that is down.
        throw std::runtime_error("could not start the metrics endpoint on " +
                                 bind + ": " + e.what());
    }
}

void MetricsClient::stop_server() {
    if (impl_) impl_->exposer.reset();
}

// ---------------------------------------------------------------------------
#define AXON_GUARD()          \
    if (!enabled_ || !impl_) { \
        return;                \
    }

TopicMetrics MetricsClient::declare_topic(const std::string& exchange,
                                          DataType data_type,
                                          const std::string& symbol) {
    TopicMetrics handle;
    if (!enabled_ || !impl_) return handle;

    const std::string dt{to_string(data_type)};
    const std::map<std::string, std::string> full{
        {"exchange", exchange}, {"data_type", dt}, {"symbol", symbol}};
    // The two histograms are labelled without the symbol on purpose. A
    // histogram is a dozen series per label set, so carrying the symbol would
    // multiply the series count by the symbol table -- affordable at ten
    // symbols, not at a thousand. The per-symbol signal stays on the counter
    // and the gauge, which are one series each.
    const std::map<std::string, std::string> coarse{{"exchange", exchange},
                                                    {"data_type", dt}};

    impl_->subscription->Add(full).Set(1.0);
    handle.events_     = &impl_->events->Add(full);
    handle.last_event_ = &impl_->last_event->Add(full);
    handle.age_        = &impl_->message_age->Add(coarse, kAge);
    handle.publish_    = &impl_->publish_duration->Add(coarse, kInternal);
    return handle;
}

void MetricsClient::inc_undeclared_event(const std::string& exchange) {
    AXON_GUARD();
    impl_->undeclared_event->Add({{"exchange", exchange}}).Increment();
}

void MetricsClient::set_ws_connected(const std::string& tag, bool connected) {
    AXON_GUARD();
    impl_->ws_connected->Add({{"exchange", tag}}).Set(connected ? 1.0 : 0.0);
}

void MetricsClient::observe_ws_connect(const std::string& tag, double seconds) {
    AXON_GUARD();
    impl_->ws_connect->Add({{"exchange", tag}}, kConnect).Observe(seconds);
}

void MetricsClient::inc_ws_connect_failure(const std::string& tag) {
    AXON_GUARD();
    impl_->ws_connect_failure->Add({{"exchange", tag}}).Increment();
}

void MetricsClient::inc_ws_reconnect(const std::string& tag) {
    AXON_GUARD();
    impl_->ws_reconnect->Add({{"exchange", tag}}).Increment();
}

void MetricsClient::observe_ws_session(const std::string& tag, double seconds) {
    AXON_GUARD();
    impl_->ws_session->Add({{"exchange", tag}}, kSession).Observe(seconds);
}

void MetricsClient::observe_handler(const std::string& tag, double seconds) {
    AXON_GUARD();
    impl_->handler_duration->Add({{"exchange", tag}}, kInternal).Observe(seconds);
}

void MetricsClient::inc_parse_error(const std::string& exchange) {
    AXON_GUARD();
    impl_->parse_error->Add({{"exchange", exchange}}).Increment();
}

void MetricsClient::inc_resync(const std::string& exchange, ResyncReason reason) {
    AXON_GUARD();
    impl_->resync
        ->Add({{"exchange", exchange}, {"reason", std::string(to_string(reason))}})
        .Increment();
}

void MetricsClient::inc_checksum_failure(const std::string& exchange) {
    AXON_GUARD();
    impl_->checksum_failure->Add({{"exchange", exchange}}).Increment();
}

void MetricsClient::observe_rest_snapshot(const std::string& exchange,
                                          double seconds) {
    AXON_GUARD();
    impl_->rest_snapshot->Add({{"exchange", exchange}}, kConnect).Observe(seconds);
}

void MetricsClient::inc_rest_snapshot_failure(const std::string& exchange) {
    AXON_GUARD();
    impl_->rest_failure->Add({{"exchange", exchange}}).Increment();
}

void MetricsClient::inc_publish_failure(const std::string& exchange) {
    AXON_GUARD();
    impl_->publish_failure->Add({{"exchange", exchange}}).Increment();
}

#undef AXON_GUARD

// ---------------------------------------------------------------------------
namespace {
std::unique_ptr<MetricsClient>& metrics_slot() {
    static std::unique_ptr<MetricsClient> instance;
    return instance;
}
} // namespace

void init_metrics(const MetricsConfig& config) {
    metrics_slot() = std::make_unique<MetricsClient>(config.enabled);
    if (config.enabled)
        metrics_slot()->start_server(config.host, config.port);
}

MetricsClient& get_metrics() {
    auto& slot = metrics_slot();
    if (!slot) {
        // A disabled no-op rather than null: reporting before startup has
        // finished should do nothing, not crash.
        slot = std::make_unique<MetricsClient>(false);
    }
    return *slot;
}

} // namespace axon_market_data
