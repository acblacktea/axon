// Metrics client.
//
// Single point of contact for all observability, exactly as in util/metrics.py.
// Business code calls domain methods here; prometheus-cpp is encapsulated so
// callers never include it and the backend can be swapped without touching
// anything that reports.
//
// The metric NAMES and LABELS match the Python implementation, so one
// dashboard and one alert set work against either engine. That is the point of
// keeping the surface identical rather than designing a nicer one.
//
// HOT PATH NOTE: prometheus-cpp resolves labels through a hash map on every
// observation, which is fine at the rates these are called (per order, per
// reconnect) and far too slow per market-data message. Nothing here may be
// called from inside the receive loop. When per-message counters are needed
// they go in a per-thread POD struct that a background thread folds in --
// see the note in README.
//
// When metrics are disabled every call is a cheap no-op, so production code
// never guards with `if (metrics_enabled)`.

#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include "axon/config.h"

namespace axon::util {

class MetricsClient {
 public:
  explicit MetricsClient(bool enabled = true);
  ~MetricsClient();

  MetricsClient(const MetricsClient&) = delete;
  MetricsClient& operator=(const MetricsClient&) = delete;

  bool enabled() const noexcept { return enabled_; }

  // Exposes /metrics. Throws if the port cannot be bound -- a metrics endpoint
  // that silently failed to start is worse than no metrics, because the
  // dashboard just shows a gap.
  void start_server(const std::string& host, int port);
  void stop_server();

  // --- websocket ---------------------------------------------------------
  void set_ws_connected(const std::string& exchange, bool connected);
  void inc_ws_reconnect(const std::string& exchange);
  void inc_ws_heartbeat_miss(const std::string& exchange);
  // Age of a venue-stamped message on arrival. The one metric that separates
  // "we are slow" from "the venue is slow".
  void observe_ws_message_age(const std::string& exchange,
                              const std::string& category, double seconds);

  // --- reconciliation ----------------------------------------------------
  // A non-zero recovered count means the WebSocket feed lost something. It is
  // supposed to be zero; treat any sustained rate as a feed problem, not as
  // reconciliation working.
  void inc_reconciler_recovered(const std::string& kind,
                                const std::string& exchange);
  void inc_reconciler_failure(const std::string& kind,
                              const std::string& exchange);

  // --- order lifecycle ---------------------------------------------------
  void observe_order_submit_latency(const std::string& exchange, double seconds);
  void observe_order_cancel_latency(const std::string& exchange, double seconds);
  void observe_order_modify_latency(const std::string& exchange, double seconds);
  // Time from submit to the venue's first acknowledgement on the feed.
  void observe_order_ack_latency(const std::string& exchange, double seconds);
  void observe_order_fill_latency(const std::string& exchange,
                                  const std::string& terminal_status,
                                  double seconds);

  void inc_order_rejected(const std::string& exchange,
                          const std::string& reason = "unknown");
  void inc_order_place_failure(const std::string& exchange,
                               const std::string& error_type);
  void inc_order_cancel_failure(const std::string& exchange,
                                const std::string& error_type);
  void inc_order_modify_failure(const std::string& exchange,
                                const std::string& error_type);

  // --- REST / EMS --------------------------------------------------------
  void inc_ems_request_error(const std::string& exchange,
                             const std::string& path,
                             const std::string& error_type);
  void observe_ems_request(const std::string& exchange, const std::string& path,
                           double seconds);

  // --- persistence -------------------------------------------------------
  void inc_db_write_failure(const std::string& repository);

  // --- risk --------------------------------------------------------------
  void set_account_margin_ratio(const std::string& exchange,
                                const std::string& currency, double ratio);

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  bool enabled_;
};

// Process-wide instance. init_metrics() once at startup; get_metrics()
// anywhere. Before init, get_metrics() returns a disabled no-op client rather
// than null, so a code path that reports before startup finishes does not
// crash.
void init_metrics(const MetricsConfig& config);
MetricsClient& get_metrics();

}  // namespace axon::util
