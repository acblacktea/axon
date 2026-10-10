import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Optional, Dict
from dataclasses import dataclass

from axon_market_data import (
    MarketDataService, DeribitOptionsDataClient, DeribitAdapter,
    MarketDataEvent, IndexPriceData, OrderbookSnapshot, TickerData
)
from axon_market_data.utils import setup_colored_logger


@dataclass
class SymbolStats:
    """Statistics for a single symbol"""
    orderbook_count: int = 0
    ticker_count: int = 0
    spot_price_count: int = 0
    first_orderbook_time: Optional[datetime] = None
    last_orderbook_time: Optional[datetime] = None
    first_ticker_time: Optional[datetime] = None
    last_ticker_time: Optional[datetime] = None
    # Track the previous receive time for interval calculation
    prev_orderbook_time: Optional[datetime] = None
    prev_ticker_time: Optional[datetime] = None
    # Track the maximum interval between consecutive messages (per symbol)
    max_orderbook_interval_ms: float = 0.0
    max_ticker_interval_ms: float = 0.0
    # Track e2e latency (time.now() - timestamp)
    max_orderbook_e2e_latency_ms: float = 0.0
    total_orderbook_e2e_latency_ms: float = 0.0
    max_ticker_e2e_latency_ms: float = 0.0
    total_ticker_e2e_latency_ms: float = 0.0
    # Track network latency (local_timestamp - timestamp) and processing latency (now - local_timestamp)
    max_ob_network_latency_ms: float = 0.0
    total_ob_network_latency_ms: float = 0.0
    max_ob_process_latency_ms: float = 0.0
    total_ob_process_latency_ms: float = 0.0
    max_ticker_network_latency_ms: float = 0.0
    total_ticker_network_latency_ms: float = 0.0
    max_ticker_process_latency_ms: float = 0.0
    total_ticker_process_latency_ms: float = 0.0


class OptionsStrategy:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("mds")
        self.stats_logger = setup_colored_logger("stats", level=20, log_file="logs/stats.log")
        self._spot_price: Optional[IndexPriceData] = None
        self._stats: Dict[str, SymbolStats] = {}
        self._stats_task: Optional[asyncio.Task] = None

        self._orderbooks: Dict[str, OrderbookSnapshot] = {}
        self._tickers: Dict[str, TickerData] = {}

    async def on_orderbook(self, event: MarketDataEvent) -> None:
        if event.event_type == "error":
            self.logger.error(f"Orderbook error: {event.error}")
            return

        ob: Optional[OrderbookSnapshot] = event.data
        if ob:
            self._orderbooks[event.symbol] = ob
            self._update_orderbook_stats(event.symbol, ob)
            self.logger.info(f"Symbol: {event.symbol}, Orderbook: {json.dumps(ob.to_dict(), separators=(',', ':'))}")
        else:
            self.logger.warning(f"Symbol: {event.symbol}, Event: {event.event_type}, No orderbook data")

    async def on_ticker(self, event: MarketDataEvent) -> None:
        if event.event_type == "error":
            self.logger.error(f"Ticker error: {event.error}")
            return

        ticker: Optional[TickerData] = event.data
        if ticker:
            self._tickers[event.symbol] = ticker
            self._update_ticker_stats(event.symbol, ticker)
            self.logger.info(f"Symbol: {event.symbol}, Ticker: {json.dumps(ticker.to_dict(), separators=(',', ':'))}")
        else:
            self.logger.warning(f"Symbol: {event.symbol}, Event: {event.event_type}, No ticker data")

    async def on_spot_price(self, event: MarketDataEvent) -> None:
        index_data: IndexPriceData = event.data
        self._spot_price = index_data

        # Update stats
        stats = self._get_or_create_stats(event.symbol)
        stats.spot_price_count += 1

        self.logger.info(f"Spot Price: {json.dumps({'index_name': index_data.index_name, 'price': str(index_data.price), 'timestamp': index_data.timestamp}, separators=(',', ':'))}")

    @property
    def spot_price(self) -> Optional[IndexPriceData]:
        return self._spot_price

    def log_stats(self) -> None:
        self.stats_logger.info("=" * 80)
        self.stats_logger.info("=== Symbol Statistics ===")
        self.stats_logger.info(f"Total symbols tracked: {len(self._stats)}")
        self.stats_logger.info("-" * 80)

        for symbol, stats in sorted(self._stats.items()):
            first_ob = stats.first_orderbook_time.strftime('%Y-%m-%d %H:%M:%S') if stats.first_orderbook_time else "N/A"
            last_ob = stats.last_orderbook_time.strftime('%Y-%m-%d %H:%M:%S') if stats.last_orderbook_time else "N/A"
            first_ticker = stats.first_ticker_time.strftime('%Y-%m-%d %H:%M:%S') if stats.first_ticker_time else "N/A"
            last_ticker = stats.last_ticker_time.strftime('%Y-%m-%d %H:%M:%S') if stats.last_ticker_time else "N/A"
            max_ob_interval = f"{stats.max_orderbook_interval_ms:.1f}ms" if stats.max_orderbook_interval_ms > 0 else "N/A"
            max_ticker_interval = f"{stats.max_ticker_interval_ms:.1f}ms" if stats.max_ticker_interval_ms > 0 else "N/A"
            # e2e latency stats
            max_ob_e2e = f"{stats.max_orderbook_e2e_latency_ms:.1f}" if stats.max_orderbook_e2e_latency_ms > 0 else "N/A"
            avg_ob_e2e = f"{stats.total_orderbook_e2e_latency_ms / stats.orderbook_count:.1f}" if stats.orderbook_count > 0 else "N/A"
            max_ticker_e2e = f"{stats.max_ticker_e2e_latency_ms:.1f}" if stats.max_ticker_e2e_latency_ms > 0 else "N/A"
            avg_ticker_e2e = f"{stats.total_ticker_e2e_latency_ms / stats.ticker_count:.1f}" if stats.ticker_count > 0 else "N/A"
            # network/process latency stats
            avg_ob_net = f"{stats.total_ob_network_latency_ms / stats.orderbook_count:.1f}" if stats.orderbook_count > 0 and stats.total_ob_network_latency_ms > 0 else "N/A"
            avg_ob_proc = f"{stats.total_ob_process_latency_ms / stats.orderbook_count:.1f}" if stats.orderbook_count > 0 and stats.total_ob_process_latency_ms > 0 else "N/A"
            avg_ticker_net = f"{stats.total_ticker_network_latency_ms / stats.ticker_count:.1f}" if stats.ticker_count > 0 and stats.total_ticker_network_latency_ms > 0 else "N/A"
            avg_ticker_proc = f"{stats.total_ticker_process_latency_ms / stats.ticker_count:.1f}" if stats.ticker_count > 0 and stats.total_ticker_process_latency_ms > 0 else "N/A"
            self.stats_logger.info(
                f"{symbol}: ob={stats.orderbook_count}, ticker={stats.ticker_count}, "
                f"ob_e2e={avg_ob_e2e}/{max_ob_e2e}ms(avg/max), ob_net={avg_ob_net}ms, ob_proc={avg_ob_proc}ms, "
                f"ticker_e2e={avg_ticker_e2e}/{max_ticker_e2e}ms, ticker_net={avg_ticker_net}ms, ticker_proc={avg_ticker_proc}ms"
            )

        self.stats_logger.info("=" * 80)

    async def _stats_logging_task(self, interval_seconds: int = 180) -> None:
        while True:
            await asyncio.sleep(interval_seconds)
            self.log_stats()

    def start_stats_logging(self, interval_seconds: int = 180) -> None:
        if self._stats_task is None or self._stats_task.done():
            self._stats_task = asyncio.create_task(self._stats_logging_task(interval_seconds))
            self.stats_logger.info(f"Started stats logging task (interval: {interval_seconds}s)")

    def stop_stats_logging(self) -> None:
        if self._stats_task and not self._stats_task.done():
            self._stats_task.cancel()
            self.stats_logger.info("Stopped stats logging task")

    # ==================== Private Methods ====================

    def _get_or_create_stats(self, symbol: str) -> SymbolStats:
        if symbol not in self._stats:
            self._stats[symbol] = SymbolStats()
        return self._stats[symbol]

    def _update_orderbook_stats(self, symbol: str, ob: OrderbookSnapshot) -> None:
        """Update orderbook statistics for a symbol"""
        stats = self._get_or_create_stats(symbol)
        stats.orderbook_count += 1
        now = datetime.now()
        now_ms = time.time() * 1000

        if stats.first_orderbook_time is None:
            stats.first_orderbook_time = now
        if stats.prev_orderbook_time is not None:
            interval_ms = (now - stats.prev_orderbook_time).total_seconds() * 1000
            stats.max_orderbook_interval_ms = max(stats.max_orderbook_interval_ms, interval_ms)
        stats.prev_orderbook_time = now
        stats.last_orderbook_time = now

        if ob.timestamp:
            e2e_latency_ms = now_ms - ob.timestamp
            stats.total_orderbook_e2e_latency_ms += e2e_latency_ms
            stats.max_orderbook_e2e_latency_ms = max(stats.max_orderbook_e2e_latency_ms, e2e_latency_ms)
            if ob.local_timestamp:
                network_latency_ms = ob.local_timestamp - ob.timestamp
                stats.total_ob_network_latency_ms += network_latency_ms
                stats.max_ob_network_latency_ms = max(stats.max_ob_network_latency_ms, network_latency_ms)
                process_latency_ms = now_ms - ob.local_timestamp
                stats.total_ob_process_latency_ms += process_latency_ms
                stats.max_ob_process_latency_ms = max(stats.max_ob_process_latency_ms, process_latency_ms)

    def _update_ticker_stats(self, symbol: str, ticker: TickerData) -> None:
        """Update ticker statistics for a symbol"""
        stats = self._get_or_create_stats(symbol)
        stats.ticker_count += 1
        now = datetime.now()
        now_ms = time.time() * 1000

        if stats.first_ticker_time is None:
            stats.first_ticker_time = now
        if stats.prev_ticker_time is not None:
            interval_ms = (now - stats.prev_ticker_time).total_seconds() * 1000
            stats.max_ticker_interval_ms = max(stats.max_ticker_interval_ms, interval_ms)
        stats.prev_ticker_time = now
        stats.last_ticker_time = now

        if ticker.timestamp:
            e2e_latency_ms = now_ms - ticker.timestamp
            stats.total_ticker_e2e_latency_ms += e2e_latency_ms
            stats.max_ticker_e2e_latency_ms = max(stats.max_ticker_e2e_latency_ms, e2e_latency_ms)
            if ticker.local_timestamp:
                network_latency_ms = ticker.local_timestamp - ticker.timestamp
                stats.total_ticker_network_latency_ms += network_latency_ms
                stats.max_ticker_network_latency_ms = max(stats.max_ticker_network_latency_ms, network_latency_ms)
                process_latency_ms = now_ms - ticker.local_timestamp
                stats.total_ticker_process_latency_ms += process_latency_ms
                stats.max_ticker_process_latency_ms = max(stats.max_ticker_process_latency_ms, process_latency_ms)


async def main():
    # Setup logger
    logger = setup_colored_logger("example", level=20)

    # Create strategy instance
    strategy = OptionsStrategy(logger=logger)

    # Create MDS
    mds = MarketDataService(logger)

    try:
        # Add Deribit adapter with multi-connection support
        adapter = DeribitAdapter(
            testnet=False,
            logger=logger,
            max_symbols_per_connection=200,
            orderbook_interval="agg2"
        )
        await mds.add_exchange(adapter)

        # Connect
        await mds.connect()

        # Create options data client
        client = DeribitOptionsDataClient(mds, logger=logger)

        # Subscribe to BTC options orderbooks and tickers - bind strategy callbacks
        await client.subscribe_btc_options(
            months=3,
            callback=strategy.on_orderbook,
            ticker_callback=strategy.on_ticker
        )

        # Subscribe to BTC spot price - bind strategy.on_spot_price
        await client.subscribe_spot_price(
            currency="BTC",
            callback=strategy.on_spot_price
        )

        # Start stats logging task (every 3 minutes)
        strategy.start_stats_logging(interval_seconds=180)

        # Print connection stats
        stats = adapter.get_connection_stats()
        if stats['multi_connection_enabled']:
            logger.info("=== connection summary ===")
            logger.info(f"total connections: {stats['total_connections']}")
            logger.info(f"active connections: {stats['active_connections']}")
            logger.info(f"total subscriptions: {stats['total_subscriptions']}")
            logger.info(f"rate limit per connections: {stats['max_symbols_per_connection']}")
            logger.info("connections distribution:")
            for conn in stats['connections']:
                logger.info(f"connection #{conn['id']}: {conn['subscriptions']}/{conn['capacity']} "
                           f"({conn['utilization']:.1f}% usage)")
            logger.info("=" * 50)

        await client.run()

    finally:
        strategy.stop_stats_logging()
        await mds.disconnect()
        await asyncio.sleep(0.2)

if __name__ == "__main__":
    asyncio.run(main())