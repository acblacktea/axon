"""Metrics client.

Single point of contact for all observability. Business code calls domain
methods on `MetricsClient`; the underlying backend (Prometheus today, possibly
OpenTelemetry/statsd later) is encapsulated here so callers never import the
backend directly.

Usage:
    # Once at engine startup:
    init_metrics(MetricsConfig(enabled=True, host="0.0.0.0", port=9100))

    # Anywhere in the codebase:
    from calais_order_execution.util.metrics import get_metrics
    metrics = get_metrics()
    metrics.inc_order_rejected(exchange="deribit", reason="post_only_violation")

When metrics are disabled (`enabled=False`) every call is a cheap no-op, so
production code paths never have to guard with `if metrics_enabled:`.
"""

from __future__ import annotations

import threading
from typing import Optional

from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from calais_order_execution.config import MetricsConfig

# Bucket sets shared across multiple histograms.
_REST_LATENCY_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
)
_ACK_LATENCY_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5,
)
_FILL_LATENCY_BUCKETS = (
    0.05, 0.25, 1, 5, 30, 60, 300, 1800, 7200,
)
_WS_MESSAGE_AGE_BUCKETS = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5,
)
_RECONCILER_TYPES = ("order", "fill", "position")  # for label sanity


class MetricsClient:
    """Thin wrapper around Prometheus metrics for this project.

    Each instance owns its own `CollectorRegistry`, so tests can construct
    isolated clients. Production goes through `init_metrics()` /
    `get_metrics()` to share a single configured instance.
    """

    def __init__(self, enabled: bool = True):
        self._enabled = enabled
        self._registry = CollectorRegistry()
        self._http_server = None
        self._http_thread: Optional[threading.Thread] = None

        if enabled:
            self._build_metrics()

    # ============================================================
    # Lifecycle
    # ============================================================

    @property
    def enabled(self) -> bool:
        return self._enabled

    @property
    def registry(self) -> CollectorRegistry:
        """Exposed for tests / scrape helpers — not for direct emission."""
        return self._registry

    def start_server(self, host: str, port: int) -> None:
        """Bind a Prometheus scrape endpoint at /metrics.

        Safe to call when metrics are disabled (no-op).
        """
        if not self._enabled:
            return
        if self._http_server is not None:
            return
        # Returns (server, thread) on prometheus_client >= 0.17.
        result = start_http_server(port=port, addr=host, registry=self._registry)
        if isinstance(result, tuple):
            self._http_server, self._http_thread = result

    def stop_server(self) -> None:
        if self._http_server is not None:
            try:
                self._http_server.shutdown()
            except Exception:
                pass
            self._http_server = None
            self._http_thread = None

    # ============================================================
    # Metric construction (private)
    # ============================================================

    def _build_metrics(self) -> None:
        r = self._registry

        # ---- WebSocket health ----
        self._ws_connected = Gauge(
            "calais_ws_connected",
            "1 if the exchange WebSocket is currently connected, 0 otherwise.",
            labelnames=("exchange",),
            registry=r,
        )
        self._ws_reconnect_total = Counter(
            "calais_ws_reconnect_total",
            "Number of WebSocket reconnect attempts.",
            labelnames=("exchange",),
            registry=r,
        )
        self._ws_heartbeat_miss_total = Counter(
            "calais_ws_heartbeat_miss_total",
            "Number of times a WebSocket heartbeat detected a broken connection.",
            labelnames=("exchange",),
            registry=r,
        )
        self._ws_message_age_seconds = Histogram(
            "calais_ws_message_age_seconds",
            "Age of WebSocket messages (now - exchange_timestamp), per channel.",
            labelnames=("exchange", "channel"),
            buckets=_WS_MESSAGE_AGE_BUCKETS,
            registry=r,
        )

        # ---- Reconciliation health ----
        self._reconciler_recovered_total = Counter(
            "calais_reconciler_recovered_total",
            "Number of records that the reconciler had to recover (= records "
            "missed by the WebSocket primary path).",
            labelnames=("kind", "exchange"),  # kind in {order, fill, position}
            registry=r,
        )
        self._reconciler_failure_total = Counter(
            "calais_reconciler_failure_total",
            "Number of failed reconciliation cycles.",
            labelnames=("kind", "exchange"),
            registry=r,
        )

        # ---- Order lifecycle latency ----
        self._order_submit_latency_seconds = Histogram(
            "calais_order_submit_latency_seconds",
            "Latency of place_order REST round-trip.",
            labelnames=("exchange",),
            buckets=_REST_LATENCY_BUCKETS,
            registry=r,
        )
        self._order_cancel_latency_seconds = Histogram(
            "calais_order_cancel_latency_seconds",
            "Latency of cancel_order REST round-trip.",
            labelnames=("exchange",),
            buckets=_REST_LATENCY_BUCKETS,
            registry=r,
        )
        self._order_modify_latency_seconds = Histogram(
            "calais_order_modify_latency_seconds",
            "Latency of modify_order REST round-trip.",
            labelnames=("exchange",),
            buckets=_REST_LATENCY_BUCKETS,
            registry=r,
        )
        self._order_ack_latency_seconds = Histogram(
            "calais_order_ack_latency_seconds",
            "Time from add_order to first WebSocket update for that order.",
            labelnames=("exchange",),
            buckets=_ACK_LATENCY_BUCKETS,
            registry=r,
        )
        self._order_fill_latency_seconds = Histogram(
            "calais_order_fill_latency_seconds",
            "Time from add_order to terminal status (filled/cancelled/rejected).",
            labelnames=("exchange", "terminal_status"),
            buckets=_FILL_LATENCY_BUCKETS,
            registry=r,
        )

        # ---- Error counters ----
        self._order_rejected_total = Counter(
            "calais_order_rejected_total",
            "Number of orders that landed in REJECTED status.",
            labelnames=("exchange", "reason"),
            registry=r,
        )
        self._order_place_failure_total = Counter(
            "calais_order_place_failure_total",
            "Number of place_order calls that raised an exception.",
            labelnames=("exchange", "error_type"),
            registry=r,
        )
        self._order_cancel_failure_total = Counter(
            "calais_order_cancel_failure_total",
            "Number of cancel_order calls that failed.",
            labelnames=("exchange", "error_type"),
            registry=r,
        )
        self._order_modify_failure_total = Counter(
            "calais_order_modify_failure_total",
            "Number of modify_order calls that failed.",
            labelnames=("exchange", "error_type"),
            registry=r,
        )
        self._ems_request_error_total = Counter(
            "calais_ems_request_error_total",
            "Number of EMS REST request errors.",
            labelnames=("exchange", "endpoint", "error_type"),
            registry=r,
        )
        self._ems_request_latency_seconds = Histogram(
            "calais_ems_request_latency_seconds",
            "Latency of EMS REST requests.",
            labelnames=("exchange", "endpoint"),
            buckets=_REST_LATENCY_BUCKETS,
            registry=r,
        )
        self._db_write_failure_total = Counter(
            "calais_db_write_failure_total",
            "Number of DB write failures, by repository.",
            labelnames=("repository",),  # orders, fills, accounts, positions
            registry=r,
        )

        # ---- Risk gauge ----
        self._account_margin_ratio = Gauge(
            "calais_account_margin_ratio",
            "maintenance_margin / equity. Approaches 1.0 near liquidation.",
            labelnames=("exchange", "currency"),
            registry=r,
        )

    # ============================================================
    # WS health
    # ============================================================

    def set_ws_connected(self, exchange: str, connected: bool) -> None:
        if not self._enabled:
            return
        self._ws_connected.labels(exchange=exchange).set(1 if connected else 0)

    def inc_ws_reconnect(self, exchange: str) -> None:
        if not self._enabled:
            return
        self._ws_reconnect_total.labels(exchange=exchange).inc()

    def inc_ws_heartbeat_miss(self, exchange: str) -> None:
        if not self._enabled:
            return
        self._ws_heartbeat_miss_total.labels(exchange=exchange).inc()

    def observe_ws_message_age(
        self, exchange: str, channel: str, age_seconds: float
    ) -> None:
        if not self._enabled:
            return
        # Negative values mean clock skew; clamp to 0 so the histogram stays valid.
        self._ws_message_age_seconds.labels(
            exchange=exchange, channel=channel
        ).observe(max(0.0, age_seconds))

    # ============================================================
    # Reconciliation
    # ============================================================

    def inc_reconciler_recovered(
        self, kind: str, exchange: str, count: int = 1
    ) -> None:
        if not self._enabled or count <= 0:
            return
        if kind not in _RECONCILER_TYPES:
            kind = "other"
        self._reconciler_recovered_total.labels(
            kind=kind, exchange=exchange
        ).inc(count)

    def inc_reconciler_failure(self, kind: str, exchange: str) -> None:
        if not self._enabled:
            return
        if kind not in _RECONCILER_TYPES:
            kind = "other"
        self._reconciler_failure_total.labels(kind=kind, exchange=exchange).inc()

    # ============================================================
    # Order lifecycle latency
    # ============================================================

    def observe_order_submit_latency(self, exchange: str, seconds: float) -> None:
        if not self._enabled:
            return
        self._order_submit_latency_seconds.labels(exchange=exchange).observe(
            max(0.0, seconds)
        )

    def observe_order_cancel_latency(self, exchange: str, seconds: float) -> None:
        if not self._enabled:
            return
        self._order_cancel_latency_seconds.labels(exchange=exchange).observe(
            max(0.0, seconds)
        )

    def observe_order_modify_latency(self, exchange: str, seconds: float) -> None:
        if not self._enabled:
            return
        self._order_modify_latency_seconds.labels(exchange=exchange).observe(
            max(0.0, seconds)
        )

    def observe_order_ack_latency(self, exchange: str, seconds: float) -> None:
        if not self._enabled:
            return
        self._order_ack_latency_seconds.labels(exchange=exchange).observe(
            max(0.0, seconds)
        )

    def observe_order_fill_latency(
        self, exchange: str, terminal_status: str, seconds: float
    ) -> None:
        if not self._enabled:
            return
        self._order_fill_latency_seconds.labels(
            exchange=exchange, terminal_status=terminal_status
        ).observe(max(0.0, seconds))

    # ============================================================
    # Errors
    # ============================================================

    def inc_order_rejected(self, exchange: str, reason: str = "unknown") -> None:
        if not self._enabled:
            return
        self._order_rejected_total.labels(exchange=exchange, reason=reason).inc()

    def inc_order_place_failure(self, exchange: str, error_type: str) -> None:
        if not self._enabled:
            return
        self._order_place_failure_total.labels(
            exchange=exchange, error_type=error_type
        ).inc()

    def inc_order_cancel_failure(self, exchange: str, error_type: str) -> None:
        if not self._enabled:
            return
        self._order_cancel_failure_total.labels(
            exchange=exchange, error_type=error_type
        ).inc()

    def inc_order_modify_failure(self, exchange: str, error_type: str) -> None:
        if not self._enabled:
            return
        self._order_modify_failure_total.labels(
            exchange=exchange, error_type=error_type
        ).inc()

    def inc_ems_request_error(
        self, exchange: str, endpoint: str, error_type: str
    ) -> None:
        if not self._enabled:
            return
        self._ems_request_error_total.labels(
            exchange=exchange, endpoint=endpoint, error_type=error_type
        ).inc()

    def observe_ems_request(
        self, exchange: str, endpoint: str, seconds: float
    ) -> None:
        if not self._enabled:
            return
        self._ems_request_latency_seconds.labels(
            exchange=exchange, endpoint=endpoint
        ).observe(max(0.0, seconds))

    def inc_db_write_failure(self, repository: str) -> None:
        if not self._enabled:
            return
        self._db_write_failure_total.labels(repository=repository).inc()

    # ============================================================
    # Risk
    # ============================================================

    def set_account_margin_ratio(
        self, exchange: str, currency: str, ratio: float
    ) -> None:
        if not self._enabled:
            return
        self._account_margin_ratio.labels(
            exchange=exchange, currency=currency
        ).set(ratio)


# ---------------------------------------------------------------
# Singleton accessor
# ---------------------------------------------------------------

_default_client: MetricsClient = MetricsClient(enabled=False)


def init_metrics(config: MetricsConfig) -> MetricsClient:
    """Configure the process-wide metrics client.

    Idempotent: calling twice replaces the client. The previous server
    (if any) is shut down first.
    """
    global _default_client
    _default_client.stop_server()
    _default_client = MetricsClient(enabled=config.enabled)
    if config.enabled:
        _default_client.start_server(config.host, config.port)
    return _default_client


def get_metrics() -> MetricsClient:
    """Return the process-wide metrics client.

    If `init_metrics()` has not been called, returns a disabled stub whose
    methods are no-ops. This means library code can call metrics methods
    unconditionally — including from tests that never set up monitoring.
    """
    return _default_client
