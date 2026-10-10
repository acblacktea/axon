"""Prometheus metrics for axon_market_data."""
import asyncio
import os

import psutil
from prometheus_client import Counter, Gauge, Histogram, start_http_server


# ── Counters ─────────────────────────────────────────────────────────────

messages_total = Counter(
    "mds_messages_total",
    "Total market data messages received",
    ["exchange", "data_type"],
)

reconnect_total = Counter(
    "mds_reconnect_total",
    "Total WebSocket reconnect attempts",
    ["exchange", "conn_idx", "result"],  # success | failure
)

heartbeat_failures_total = Counter(
    "mds_heartbeat_failures_total",
    "Total heartbeat failures",
    ["exchange", "conn_idx"],
)

errors_total = Counter(
    "mds_errors_total",
    "Total errors by category",
    ["exchange", "category"],  # parse | callback | ws | rpc
)

# ── Gauges ───────────────────────────────────────────────────────────────

connections_active = Gauge(
    "mds_connections_active",
    "Number of active WebSocket connections",
    ["exchange"],
)

subscribed_symbols = Gauge(
    "mds_subscribed_symbols",
    "Number of subscribed symbols",
    ["exchange", "data_type"],
)

# ── Histograms ───────────────────────────────────────────────────────────

_LATENCY_BUCKETS = (1, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000)

e2e_latency_ms = Histogram(
    "mds_e2e_latency_ms",
    "End-to-end latency from exchange timestamp to local receive (ms)",
    ["exchange", "data_type"],
    buckets=_LATENCY_BUCKETS,
)

network_latency_ms = Histogram(
    "mds_network_latency_ms",
    "Network latency from exchange to adapter (ms)",
    ["exchange", "data_type"],
    buckets=_LATENCY_BUCKETS,
)


# ── Process resource gauges (cross-platform via psutil) ──────────────────

process_cpu_percent = Gauge(
    "mds_process_cpu_percent",
    "Process CPU usage percent (100% = 1 full core)",
)

process_memory_rss_bytes = Gauge(
    "mds_process_memory_rss_bytes",
    "Process resident set size (RSS) in bytes",
)

process_num_threads = Gauge(
    "mds_process_num_threads",
    "Number of threads in the process",
)


def start_metrics_server(port: int = 9090, addr: str = "0.0.0.0") -> None:
    """Start the Prometheus HTTP metrics server."""
    start_http_server(port, addr=addr)


async def process_metrics_loop(interval: float = 5.0) -> None:
    """Sample process CPU/memory periodically and update gauges."""
    proc = psutil.Process(os.getpid())
    proc.cpu_percent(interval=None)  # prime; first call returns 0.0
    while True:
        try:
            await asyncio.sleep(interval)
            with proc.oneshot():
                process_cpu_percent.set(proc.cpu_percent(interval=None))
                process_memory_rss_bytes.set(proc.memory_info().rss)
                process_num_threads.set(proc.num_threads())
        except asyncio.CancelledError:
            break
        except Exception:
            pass
