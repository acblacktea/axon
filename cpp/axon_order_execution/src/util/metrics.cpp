#include "axon/util/metrics.h"

#include <prometheus/counter.h>
#include <prometheus/exposer.h>
#include <prometheus/family.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>

#include <array>
#include <stdexcept>

namespace axon::util {
namespace {

// Bucket sets shared across several histograms. Copied from metrics.py so the
// same dashboard queries work against either engine.
const prometheus::Histogram::BucketBoundaries kRestLatency{
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10};
const prometheus::Histogram::BucketBoundaries kAckLatency{
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5};
const prometheus::Histogram::BucketBoundaries kFillLatency{
    0.05, 0.25, 1, 5, 30, 60, 300, 1800, 7200};
const prometheus::Histogram::BucketBoundaries kMessageAge{
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5};

constexpr std::array<const char*, 3> kReconcilerKinds{"order", "fill", "position"};

bool valid_reconciler_kind(const std::string& kind) {
  for (const char* k : kReconcilerKinds) {
    if (kind == k) {
      return true;
    }
  }
  return false;
}

}  // namespace

struct MetricsClient::Impl {
  std::shared_ptr<prometheus::Registry> registry =
      std::make_shared<prometheus::Registry>();
  std::unique_ptr<prometheus::Exposer> exposer;

  prometheus::Family<prometheus::Gauge>* ws_connected = nullptr;
  prometheus::Family<prometheus::Counter>* ws_reconnect = nullptr;
  prometheus::Family<prometheus::Counter>* ws_heartbeat_miss = nullptr;
  prometheus::Family<prometheus::Histogram>* ws_message_age = nullptr;

  prometheus::Family<prometheus::Counter>* reconciler_recovered = nullptr;
  prometheus::Family<prometheus::Counter>* reconciler_failure = nullptr;

  prometheus::Family<prometheus::Histogram>* order_submit_latency = nullptr;
  prometheus::Family<prometheus::Histogram>* order_cancel_latency = nullptr;
  prometheus::Family<prometheus::Histogram>* order_modify_latency = nullptr;
  prometheus::Family<prometheus::Histogram>* order_ack_latency = nullptr;
  prometheus::Family<prometheus::Histogram>* order_fill_latency = nullptr;

  prometheus::Family<prometheus::Counter>* order_rejected = nullptr;
  prometheus::Family<prometheus::Counter>* order_place_failure = nullptr;
  prometheus::Family<prometheus::Counter>* order_cancel_failure = nullptr;
  prometheus::Family<prometheus::Counter>* order_modify_failure = nullptr;

  prometheus::Family<prometheus::Counter>* ems_request_error = nullptr;
  prometheus::Family<prometheus::Histogram>* ems_request_latency = nullptr;

  prometheus::Family<prometheus::Counter>* db_write_failure = nullptr;
  prometheus::Family<prometheus::Gauge>* account_margin_ratio = nullptr;

  void build() {
    ws_connected = &prometheus::BuildGauge()
                        .Name("axon_ws_connected")
                        .Help("1 when the exchange WebSocket is connected")
                        .Register(*registry);
    ws_reconnect = &prometheus::BuildCounter()
                        .Name("axon_ws_reconnect_total")
                        .Help("WebSocket reconnection attempts")
                        .Register(*registry);
    ws_heartbeat_miss = &prometheus::BuildCounter()
                             .Name("axon_ws_heartbeat_miss_total")
                             .Help("Heartbeats that detected a broken connection")
                             .Register(*registry);
    ws_message_age = &prometheus::BuildHistogram()
                          .Name("axon_ws_message_age_seconds")
                          .Help("Age of a venue-stamped message on arrival")
                          .Register(*registry);

    reconciler_recovered =
        &prometheus::BuildCounter()
             .Name("axon_reconciler_recovered_total")
             .Help("Records the reconciler recovered that the feed lost")
             .Register(*registry);
    reconciler_failure = &prometheus::BuildCounter()
                              .Name("axon_reconciler_failure_total")
                              .Help("Reconciliation passes that failed")
                              .Register(*registry);

    order_submit_latency = &prometheus::BuildHistogram()
                                .Name("axon_order_submit_latency_seconds")
                                .Help("Order placement round trip")
                                .Register(*registry);
    order_cancel_latency = &prometheus::BuildHistogram()
                                .Name("axon_order_cancel_latency_seconds")
                                .Help("Order cancellation round trip")
                                .Register(*registry);
    order_modify_latency = &prometheus::BuildHistogram()
                                .Name("axon_order_modify_latency_seconds")
                                .Help("Order modification round trip")
                                .Register(*registry);
    order_ack_latency = &prometheus::BuildHistogram()
                             .Name("axon_order_ack_latency_seconds")
                             .Help("Submit to first acknowledgement on the feed")
                             .Register(*registry);
    order_fill_latency = &prometheus::BuildHistogram()
                              .Name("axon_order_fill_latency_seconds")
                              .Help("Submit to terminal state")
                              .Register(*registry);

    order_rejected = &prometheus::BuildCounter()
                          .Name("axon_order_rejected_total")
                          .Help("Orders rejected")
                          .Register(*registry);
    order_place_failure = &prometheus::BuildCounter()
                               .Name("axon_order_place_failure_total")
                               .Help("Order placement failures")
                               .Register(*registry);
    order_cancel_failure = &prometheus::BuildCounter()
                                .Name("axon_order_cancel_failure_total")
                                .Help("Order cancellation failures")
                                .Register(*registry);
    order_modify_failure = &prometheus::BuildCounter()
                                .Name("axon_order_modify_failure_total")
                                .Help("Order modification failures")
                                .Register(*registry);

    ems_request_error = &prometheus::BuildCounter()
                             .Name("axon_ems_request_error_total")
                             .Help("EMS request errors")
                             .Register(*registry);
    ems_request_latency = &prometheus::BuildHistogram()
                               .Name("axon_ems_request_latency_seconds")
                               .Help("EMS request latency")
                               .Register(*registry);

    db_write_failure = &prometheus::BuildCounter()
                            .Name("axon_db_write_failure_total")
                            .Help("Persistence write failures")
                            .Register(*registry);
    account_margin_ratio = &prometheus::BuildGauge()
                                .Name("axon_account_margin_ratio")
                                .Help("Maintenance margin over equity")
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
  if (!enabled_ || !impl_) {
    return;
  }
  const std::string bind = host + ":" + std::to_string(port);
  try {
    impl_->exposer = std::make_unique<prometheus::Exposer>(bind);
    impl_->exposer->RegisterCollectable(impl_->registry);
  } catch (const std::exception& e) {
    // Loud rather than silent: a metrics endpoint that failed to bind shows up
    // as a gap in the dashboard, which reads like the process was down.
    throw std::runtime_error("could not start the metrics endpoint on " + bind +
                             ": " + e.what());
  }
}

void MetricsClient::stop_server() {
  if (impl_) {
    impl_->exposer.reset();
  }
}

// ---------------------------------------------------------------------------
#define AXON_GUARD()      \
  if (!enabled_ || !impl_) { \
    return;                  \
  }

void MetricsClient::set_ws_connected(const std::string& exchange, bool connected) {
  AXON_GUARD();
  impl_->ws_connected->Add({{"exchange", exchange}}).Set(connected ? 1.0 : 0.0);
}

void MetricsClient::inc_ws_reconnect(const std::string& exchange) {
  AXON_GUARD();
  impl_->ws_reconnect->Add({{"exchange", exchange}}).Increment();
}

void MetricsClient::inc_ws_heartbeat_miss(const std::string& exchange) {
  AXON_GUARD();
  impl_->ws_heartbeat_miss->Add({{"exchange", exchange}}).Increment();
}

void MetricsClient::observe_ws_message_age(const std::string& exchange,
                                           const std::string& category,
                                           double seconds) {
  AXON_GUARD();
  impl_->ws_message_age
      ->Add({{"exchange", exchange}, {"category", category}}, kMessageAge)
      .Observe(seconds);
}

void MetricsClient::inc_reconciler_recovered(const std::string& kind,
                                             const std::string& exchange) {
  AXON_GUARD();
  // Label sanity, as in the Python: an unexpected kind would create a new
  // series and quietly split the dashboard.
  if (!valid_reconciler_kind(kind)) {
    return;
  }
  impl_->reconciler_recovered->Add({{"kind", kind}, {"exchange", exchange}})
      .Increment();
}

void MetricsClient::inc_reconciler_failure(const std::string& kind,
                                           const std::string& exchange) {
  AXON_GUARD();
  if (!valid_reconciler_kind(kind)) {
    return;
  }
  impl_->reconciler_failure->Add({{"kind", kind}, {"exchange", exchange}})
      .Increment();
}

void MetricsClient::observe_order_submit_latency(const std::string& exchange,
                                                 double seconds) {
  AXON_GUARD();
  impl_->order_submit_latency->Add({{"exchange", exchange}}, kRestLatency)
      .Observe(seconds);
}

void MetricsClient::observe_order_cancel_latency(const std::string& exchange,
                                                 double seconds) {
  AXON_GUARD();
  impl_->order_cancel_latency->Add({{"exchange", exchange}}, kRestLatency)
      .Observe(seconds);
}

void MetricsClient::observe_order_modify_latency(const std::string& exchange,
                                                 double seconds) {
  AXON_GUARD();
  impl_->order_modify_latency->Add({{"exchange", exchange}}, kRestLatency)
      .Observe(seconds);
}

void MetricsClient::observe_order_ack_latency(const std::string& exchange,
                                              double seconds) {
  AXON_GUARD();
  impl_->order_ack_latency->Add({{"exchange", exchange}}, kAckLatency)
      .Observe(seconds);
}

void MetricsClient::observe_order_fill_latency(const std::string& exchange,
                                               const std::string& terminal_status,
                                               double seconds) {
  AXON_GUARD();
  impl_->order_fill_latency
      ->Add({{"exchange", exchange}, {"terminal_status", terminal_status}},
            kFillLatency)
      .Observe(seconds);
}

void MetricsClient::inc_order_rejected(const std::string& exchange,
                                       const std::string& reason) {
  AXON_GUARD();
  impl_->order_rejected->Add({{"exchange", exchange}, {"reason", reason}})
      .Increment();
}

void MetricsClient::inc_order_place_failure(const std::string& exchange,
                                            const std::string& error_type) {
  AXON_GUARD();
  impl_->order_place_failure
      ->Add({{"exchange", exchange}, {"error_type", error_type}})
      .Increment();
}

void MetricsClient::inc_order_cancel_failure(const std::string& exchange,
                                             const std::string& error_type) {
  AXON_GUARD();
  impl_->order_cancel_failure
      ->Add({{"exchange", exchange}, {"error_type", error_type}})
      .Increment();
}

void MetricsClient::inc_order_modify_failure(const std::string& exchange,
                                             const std::string& error_type) {
  AXON_GUARD();
  impl_->order_modify_failure
      ->Add({{"exchange", exchange}, {"error_type", error_type}})
      .Increment();
}

void MetricsClient::inc_ems_request_error(const std::string& exchange,
                                          const std::string& path,
                                          const std::string& error_type) {
  AXON_GUARD();
  impl_->ems_request_error
      ->Add({{"exchange", exchange}, {"path", path}, {"error_type", error_type}})
      .Increment();
}

void MetricsClient::observe_ems_request(const std::string& exchange,
                                        const std::string& path, double seconds) {
  AXON_GUARD();
  impl_->ems_request_latency
      ->Add({{"exchange", exchange}, {"path", path}}, kRestLatency)
      .Observe(seconds);
}

void MetricsClient::inc_db_write_failure(const std::string& repository) {
  AXON_GUARD();
  impl_->db_write_failure->Add({{"repository", repository}}).Increment();
}

void MetricsClient::set_account_margin_ratio(const std::string& exchange,
                                             const std::string& currency,
                                             double ratio) {
  AXON_GUARD();
  impl_->account_margin_ratio
      ->Add({{"exchange", exchange}, {"currency", currency}})
      .Set(ratio);
}

#undef AXON_GUARD

// ---------------------------------------------------------------------------
namespace {
std::unique_ptr<MetricsClient>& metrics_slot() {
  static std::unique_ptr<MetricsClient> instance;
  return instance;
}
}  // namespace

void init_metrics(const MetricsConfig& config) {
  metrics_slot() = std::make_unique<MetricsClient>(config.enabled);
  if (config.enabled) {
    metrics_slot()->start_server(config.host, config.port);
  }
}

MetricsClient& get_metrics() {
  auto& slot = metrics_slot();
  if (!slot) {
    // A disabled no-op rather than null: reporting before startup finishes
    // should be a no-op, not a crash.
    slot = std::make_unique<MetricsClient>(false);
  }
  return *slot;
}

}  // namespace axon::util
