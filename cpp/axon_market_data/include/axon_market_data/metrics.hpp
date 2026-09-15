#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "config.hpp"
#include "models.hpp"

// Only pointers to these are held here, so prometheus-cpp stays out of every
// translation unit that reports. The backend can be swapped without touching
// any call site.
namespace prometheus {
class Counter;
class Gauge;
class Histogram;
} // namespace prometheus

namespace axon_market_data {

// Monotonic seconds. Prometheus histograms take seconds by convention, and a
// steady clock is the only one that cannot jump backwards mid-measurement and
// report a negative latency.
inline double steady_seconds() {
    return std::chrono::duration<double>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

// Why this is an enum and not a free string: every distinct label value
// creates a new Prometheus series. A typo in a string literal would silently
// split a dashboard panel in two rather than fail.
enum class ResyncReason {
    SequenceGap,       // the diff chain broke
    SnapshotTooOld,    // the REST snapshot fell behind the buffered diffs
    ChecksumMismatch,  // OKX CRC32 disagreed with the local book
    StreamRestart,     // the venue restarted its own sequence
    Disconnect,        // the socket dropped, so the local book is gone
    SnapshotFailed,    // the REST snapshot call itself failed
};

std::string_view to_string(ResyncReason reason);

// ---------------------------------------------------------------------------
// Per-topic counters, resolved ONCE at subscription time.
//
// prometheus-cpp resolves labels by hashing a vector of string pairs on every
// single observation. At one lookup per market data message that cost lands on
// the receive path, where it is comparable to the JSON parse it is supposed to
// be measuring. So the label lookup happens once, here, and what is left on
// the hot path is a pointer dereference plus an atomic add.
//
// A default-constructed handle (metrics disabled, or a topic that was never
// declared) holds null pointers and every method is a no-op, so call sites
// never guard with `if (metrics_enabled)`.
// ---------------------------------------------------------------------------
class TopicMetrics {
public:
    TopicMetrics() = default;

    // `venue_ts_ms` is the exchange's own stamp. Pass 0 when the venue sends
    // none: the age sample is then skipped rather than recorded as zero, which
    // would read as a perfectly fast feed and pull the quantiles down.
    void on_event(int64_t venue_ts_ms, int64_t local_ts_ms) noexcept;

    // Seconds spent in serialize + ZMQ send for this one event.
    void on_published(double seconds) noexcept;

private:
    friend class MetricsClient;

    prometheus::Counter*   events_     = nullptr;
    prometheus::Gauge*     last_event_ = nullptr;
    prometheus::Histogram* age_        = nullptr;
    prometheus::Histogram* publish_    = nullptr;
};

// ---------------------------------------------------------------------------
// Single point of contact for all observability. Business code calls domain
// methods; nothing below includes a prometheus header.
//
// Every method is safe to call when metrics are disabled -- it is a no-op.
// ---------------------------------------------------------------------------
class MetricsClient {
public:
    explicit MetricsClient(bool enabled = true);
    ~MetricsClient();

    MetricsClient(const MetricsClient&)            = delete;
    MetricsClient& operator=(const MetricsClient&) = delete;

    bool enabled() const noexcept { return enabled_; }

    // Exposes /metrics. Throws if the port cannot be bound: an endpoint that
    // silently failed to start shows up as a gap in the dashboard, which reads
    // like the process was down.
    void start_server(const std::string& host, int port);
    void stop_server();

    // --- subscriptions -----------------------------------------------------
    // Declares what this service intends to receive, and hands back the handle
    // the receive path uses. Declaring is what makes "subscribed but silent"
    // visible: the subscription gauge stays pinned at 1 while the event
    // counter for that topic stops moving. Without it, a dead topic is
    // indistinguishable from one that was never configured.
    TopicMetrics declare_topic(const std::string& exchange, DataType data_type,
                               const std::string& symbol);

    // Events for a symbol that was never declared -- a venue sending something
    // we did not ask for. Counted per exchange rather than per symbol on
    // purpose: an unbounded label would let a misbehaving feed blow up the
    // series count, which is a real outage and not just noise.
    void inc_undeclared_event(const std::string& exchange);

    // --- connection health --------------------------------------------------
    void set_ws_connected(const std::string& tag, bool connected);
    void observe_ws_connect(const std::string& tag, double seconds);
    void inc_ws_connect_failure(const std::string& tag);
    void inc_ws_reconnect(const std::string& tag);
    // How long a connection lasted before it dropped. A feed that reconnects
    // once a day and one that flaps every 30 seconds have the same reconnect
    // rate over a week; only this separates them.
    void observe_ws_session(const std::string& tag, double seconds);

    // --- receive path -------------------------------------------------------
    // The whole handler for one raw frame: JSON parse + book update + publish.
    // This is the number that says whether the service itself is the problem,
    // as opposed to the venue or the link.
    void observe_handler(const std::string& tag, double seconds);
    void inc_parse_error(const std::string& exchange);

    // --- orderbook health ---------------------------------------------------
    void inc_resync(const std::string& exchange, ResyncReason reason);
    void inc_checksum_failure(const std::string& exchange);
    void observe_rest_snapshot(const std::string& exchange, double seconds);
    void inc_rest_snapshot_failure(const std::string& exchange);

    // --- publishing ---------------------------------------------------------
    void inc_publish_failure(const std::string& exchange);

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    bool                  enabled_;
};

// Process-wide instance. init_metrics() once at startup, get_metrics()
// anywhere. Before init, get_metrics() returns a disabled no-op client rather
// than null, so a path that reports before startup finishes cannot crash.
void init_metrics(const MetricsConfig& config);
MetricsClient& get_metrics();

} // namespace axon_market_data
