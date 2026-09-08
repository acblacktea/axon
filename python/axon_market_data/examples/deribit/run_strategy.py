"""
Deribit Options Strategy (ZMQ Client)

Connects to DeribitOptionsDataService via ZMQ, receives
orderbook/ticker/index_price and logs statistics periodically.

Usage:
    # First start the service:
    python examples/deribit/run_service.py

    # Then run this strategy:
    python examples/deribit/run_strategy.py
"""
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, Dict

from axon_market_data.client import DeribitOptionsClient
from axon_market_data.core.models import (
    MarketDataEvent, OrderbookSnapshot, TickerData, IndexPriceData,
)
from axon_market_data.utils import setup_colored_logger


@dataclass
class SymbolStats:
    orderbook_count: int = 0
    ticker_count: int = 0
    spot_count: int = 0
    # receive timestamps
    first_time: Optional[datetime] = None
    last_time: Optional[datetime] = None
    # consecutive-message interval tracking
    prev_ob_time: Optional[datetime] = None
    prev_ticker_time: Optional[datetime] = None
    max_ob_interval_ms: float = 0.0
    max_ticker_interval_ms: float = 0.0
    # e2e latency (now - exchange timestamp)
    max_ob_e2e_ms: float = 0.0
    total_ob_e2e_ms: float = 0.0
    max_ticker_e2e_ms: float = 0.0
    total_ticker_e2e_ms: float = 0.0
    # network latency (local_timestamp - exchange timestamp)
    max_ob_net_ms: float = 0.0
    total_ob_net_ms: float = 0.0
    max_ticker_net_ms: float = 0.0
    total_ticker_net_ms: float = 0.0
    # processing latency (now - local_timestamp)
    max_ob_proc_ms: float = 0.0
    total_ob_proc_ms: float = 0.0
    max_ticker_proc_ms: float = 0.0
    total_ticker_proc_ms: float = 0.0


class OptionsStrategy:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("strategy")
        self.stats_logger = setup_colored_logger(
            "stats", level=logging.INFO, log_file="logs/deribit_strategy_stats.log"
        )
        self._stats: Dict[str, SymbolStats] = {}
        self._stats_task: Optional[asyncio.Task] = None

    # ── callbacks ────────────────────────────────────────────────

    async def on_orderbook(self, event: MarketDataEvent) -> None:
        if event.event_type == "error":
            self.logger.error(f"Orderbook error: {event.error}")
            return

        ob: Optional[OrderbookSnapshot] = event.data
        if not ob:
            return

        s = self._ensure(event.symbol)
        s.orderbook_count += 1
        now = datetime.now()
        now_ms = time.time() * 1000

        if s.first_time is None:
            s.first_time = now
        s.last_time = now

        if s.prev_ob_time is not None:
            interval = (now - s.prev_ob_time).total_seconds() * 1000
            s.max_ob_interval_ms = max(s.max_ob_interval_ms, interval)
        s.prev_ob_time = now

        if ob.timestamp:
            e2e = now_ms - ob.timestamp
            s.total_ob_e2e_ms += e2e
            s.max_ob_e2e_ms = max(s.max_ob_e2e_ms, e2e)
            if ob.local_timestamp:
                net = ob.local_timestamp - ob.timestamp
                s.total_ob_net_ms += net
                s.max_ob_net_ms = max(s.max_ob_net_ms, net)
                proc = now_ms - ob.local_timestamp
                s.total_ob_proc_ms += proc
                s.max_ob_proc_ms = max(s.max_ob_proc_ms, proc)

        self.logger.info(
            f"OB {event.symbol}: "
            f"bids={len(ob.bids)} asks={len(ob.asks)} ts={ob.timestamp}"
        )

    async def on_ticker(self, event: MarketDataEvent) -> None:
        if event.event_type == "error":
            self.logger.error(f"Ticker error: {event.error}")
            return

        ticker: Optional[TickerData] = event.data
        if not ticker:
            return

        s = self._ensure(event.symbol)
        s.ticker_count += 1
        now = datetime.now()
        now_ms = time.time() * 1000

        if s.first_time is None:
            s.first_time = now
        s.last_time = now

        if s.prev_ticker_time is not None:
            interval = (now - s.prev_ticker_time).total_seconds() * 1000
            s.max_ticker_interval_ms = max(s.max_ticker_interval_ms, interval)
        s.prev_ticker_time = now

        if ticker.timestamp:
            e2e = now_ms - ticker.timestamp
            s.total_ticker_e2e_ms += e2e
            s.max_ticker_e2e_ms = max(s.max_ticker_e2e_ms, e2e)
            if ticker.local_timestamp:
                net = ticker.local_timestamp - ticker.timestamp
                s.total_ticker_net_ms += net
                s.max_ticker_net_ms = max(s.max_ticker_net_ms, net)
                proc = now_ms - ticker.local_timestamp
                s.total_ticker_proc_ms += proc
                s.max_ticker_proc_ms = max(s.max_ticker_proc_ms, proc)

        self.logger.info(
            f"Ticker {event.symbol}: "
            f"mark={ticker.mark_price} delta={ticker.delta} iv={ticker.mark_iv}"
        )

    async def on_spot_price(self, event: MarketDataEvent) -> None:
        index_data: IndexPriceData = event.data
        s = self._ensure(event.symbol)
        s.spot_count += 1
        self.logger.info(
            f"Spot: {index_data.index_name} = ${index_data.price}"
        )

    # ── stats ────────────────────────────────────────────────────

    def start_stats_logging(self, interval_seconds: int = 180) -> None:
        if self._stats_task is None or self._stats_task.done():
            self._stats_task = asyncio.create_task(self._stats_loop(interval_seconds))
            self.stats_logger.info(f"Stats logging started (interval: {interval_seconds}s)")

    def stop_stats_logging(self) -> None:
        if self._stats_task and not self._stats_task.done():
            self._stats_task.cancel()

    async def _stats_loop(self, interval: int) -> None:
        while True:
            await asyncio.sleep(interval)
            self._log_stats()

    def _log_stats(self) -> None:
        self.stats_logger.info("=" * 90)
        self.stats_logger.info(f"Symbol Statistics  (symbols: {len(self._stats)})")
        self.stats_logger.info("-" * 90)

        total_ob = total_ticker = total_spot = 0
        for symbol, s in sorted(self._stats.items()):
            total_ob += s.orderbook_count
            total_ticker += s.ticker_count
            total_spot += s.spot_count

            avg_ob_e2e = f"{s.total_ob_e2e_ms / s.orderbook_count:.1f}" if s.orderbook_count else "N/A"
            max_ob_e2e = f"{s.max_ob_e2e_ms:.1f}" if s.max_ob_e2e_ms > 0 else "N/A"
            avg_ob_net = f"{s.total_ob_net_ms / s.orderbook_count:.1f}" if s.orderbook_count and s.total_ob_net_ms > 0 else "N/A"
            avg_ob_proc = f"{s.total_ob_proc_ms / s.orderbook_count:.1f}" if s.orderbook_count and s.total_ob_proc_ms > 0 else "N/A"

            avg_tk_e2e = f"{s.total_ticker_e2e_ms / s.ticker_count:.1f}" if s.ticker_count else "N/A"
            max_tk_e2e = f"{s.max_ticker_e2e_ms:.1f}" if s.max_ticker_e2e_ms > 0 else "N/A"
            avg_tk_net = f"{s.total_ticker_net_ms / s.ticker_count:.1f}" if s.ticker_count and s.total_ticker_net_ms > 0 else "N/A"
            avg_tk_proc = f"{s.total_ticker_proc_ms / s.ticker_count:.1f}" if s.ticker_count and s.total_ticker_proc_ms > 0 else "N/A"

            self.stats_logger.info(
                f"{symbol}: ob={s.orderbook_count} ticker={s.ticker_count} | "
                f"ob_e2e={avg_ob_e2e}/{max_ob_e2e}ms ob_net={avg_ob_net}ms ob_proc={avg_ob_proc}ms | "
                f"tk_e2e={avg_tk_e2e}/{max_tk_e2e}ms tk_net={avg_tk_net}ms tk_proc={avg_tk_proc}ms"
            )

        self.stats_logger.info("-" * 90)
        self.stats_logger.info(
            f"TOTAL: ob={total_ob} ticker={total_ticker} spot={total_spot}"
        )
        self.stats_logger.info("=" * 90)

    def _ensure(self, symbol: str) -> SymbolStats:
        if symbol not in self._stats:
            self._stats[symbol] = SymbolStats()
        return self._stats[symbol]


async def main():
    logger = setup_colored_logger(
        "strategy", level=logging.INFO, log_file="logs/deribit_strategy.log"
    )

    strategy = OptionsStrategy(logger=logger)

    client = DeribitOptionsClient(
        server_address="tcp://localhost:5557",
        logger=logger,
    )

    try:
        await client.connect()
        logger.info("Connected to DeribitOptionsDataService")

        client.on_orderbook(strategy.on_orderbook)
        client.on_ticker(strategy.on_ticker)
        client.on_index_price(strategy.on_spot_price)

        strategy.start_stats_logging(interval_seconds=180)

        logger.info("=" * 50)
        logger.info("Strategy running. Press Ctrl+C to stop...")
        logger.info("=" * 50)

        await client.run()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    finally:
        strategy.stop_stats_logging()
        await client.disconnect()
        logger.info("Client stopped")


if __name__ == "__main__":
    asyncio.run(main())
