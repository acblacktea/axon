"""
Example: Run Strategy (ZMQ client for Deribit Options)

Connects to DeribitOptionsDataService via ZMQ, receives orderbook/ticker/spot data.

Usage:
    # First start the service:
    python examples/run_deribit_options_service.py

    # Then run this client:
    python examples/run_strategy.py
"""
import asyncio
import json
import logging
from typing import Optional
from decimal import Decimal

from axon_market_data.client import DeribitOptionsClient
from axon_market_data.core.models import (
    MarketDataEvent, OrderbookSnapshot, TickerData, IndexPriceData
)
from axon_market_data.utils import setup_colored_logger


class OptionsStrategy:
    """Example strategy that receives data from DeribitOptionsDataService via ZMQ."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("strategy")
        self._spot_price: Optional[Decimal] = None
        self._orderbook_count = 0

    async def on_orderbook(self, event: MarketDataEvent) -> None:
        if event.event_type == "error":
            self.logger.error(f"Orderbook error for {event.symbol}: {event.error}")
            return

        ob: Optional[OrderbookSnapshot] = event.data
        if ob:
            self._orderbook_count += 1
            ob_dict = ob.to_dict()
            ob_json = json.dumps(ob_dict, ensure_ascii=False)
            self.logger.info(f"[{self._orderbook_count}] {event.symbol}: {ob_json}")

    async def on_ticker(self, event: MarketDataEvent) -> None:
        if event.event_type == "error":
            self.logger.error(f"Ticker error for {event.symbol}: {event.error}")
            return

        ticker: Optional[TickerData] = event.data
        if ticker:
            self.logger.info(
                f"Ticker: {event.symbol} "
                f"mark={ticker.mark_price} delta={ticker.delta} iv={ticker.mark_iv}"
            )

    async def on_spot_price(self, event: MarketDataEvent) -> None:
        index_data: IndexPriceData = event.data
        self._spot_price = index_data.price
        self.logger.info(f"Spot: {index_data.index_name} = ${index_data.price}")


async def main():
    logger = setup_colored_logger("strategy", level=logging.INFO, log_file="logs/strategy.log")

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

        logger.info("=" * 50)
        logger.info("Strategy running. Press Ctrl+C to stop...")
        logger.info("=" * 50)

        await client.run()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    except Exception as e:
        logger.error(f"Error: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Client stopped")


if __name__ == "__main__":
    asyncio.run(main())
