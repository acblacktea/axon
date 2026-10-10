import asyncio
import json
import logging
from datetime import datetime
from typing import Optional, Dict, List

from axon_market_data import (
    MarketDataService, DeribitOptionsDataClient, DeribitAdapter,
    MarketDataEvent, IndexPriceData, OrderbookSnapshot, TickerData
)
from axon_market_data.utils import setup_colored_logger


class OptionsStrategy:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("mds")
        self._spot_price: Optional[IndexPriceData] = None
        self._orderbooks: Dict[str, OrderbookSnapshot] = {}
        self._tickers: Dict[str, TickerData] = {}

    async def on_orderbook(self, event: MarketDataEvent) -> None:
        if event.event_type == "error":
            self.logger.error(f"Orderbook error: {event.error}")
            return

        ob: Optional[OrderbookSnapshot] = event.data
        if ob:
            self._orderbooks[event.symbol] = ob
            ob_dict = ob.to_dict()
            ob_json = json.dumps(ob_dict, ensure_ascii=False)
            self.logger.info(f"Symbol: {event.symbol}, Event: {event.event_type}, Orderbook: {ob_json}")
        else:
            self.logger.warning(f"Symbol: {event.symbol}, Event: {event.event_type}, No orderbook data")

    async def on_ticker(self, event: MarketDataEvent) -> None:
        if event.event_type == "error":
            self.logger.error(f"Ticker error: {event.error}")
            return

        ticker: Optional[TickerData] = event.data
        if ticker:
            self._tickers[event.symbol] = ticker
            ticker_dict = ticker.to_dict()
            ticker_json = json.dumps(ticker_dict, ensure_ascii=False)
            self.logger.info(f"Symbol: {event.symbol}, Event: {event.event_type}, Ticker: {ticker_json}")
        else:
            self.logger.warning(f"Symbol: {event.symbol}, Event: {event.event_type}, No ticker data")

    async def on_spot_price(self, event: MarketDataEvent) -> None:
        index_data: IndexPriceData = event.data
        self._spot_price = index_data
        self.logger.info(f"Spot Price: {index_data.index_name} = ${index_data.price} (ts: {index_data.timestamp})")

    @property
    def spot_price(self) -> Optional[IndexPriceData]:
        return self._spot_price

    def symbol_filter(self, expiry_start: datetime, expiry_end: datetime) -> List[str]:
        all_symbols = set(self._orderbooks.keys()) | set(self._tickers.keys())

        result = []
        for symbol in all_symbols:
            expiration = self._parse_expiration_from_symbol(symbol)
            if expiration and expiry_start <= expiration <= expiry_end:
                result.append(symbol)

        return sorted(result)

    def _parse_expiration_from_symbol(self, symbol: str) -> Optional[datetime]:
        try:
            parts = symbol.split('-')
            if len(parts) < 4:
                return None

            date_str = parts[1]  # e.g., "21MAR25"
            # Parse format: DDMMMYY
            return datetime.strptime(date_str, "%d%b%y")
        except (ValueError, IndexError):
            return None

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
        await mds.disconnect()
        await asyncio.sleep(0.2)

if __name__ == "__main__":
    asyncio.run(main())