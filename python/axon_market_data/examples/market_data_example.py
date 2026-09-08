"""
MarketDataService example - Test all exchanges with unified symbol format

Unified symbol format: BASE_QUOTE_TYPE
- BTC_USDT_SPOT -> spot trading
- BTC_USDT_PERP -> perpetual/swap

Deribit options keep original format (e.g., BTC-27FEB26-100000-C)
"""
import asyncio
import json
import logging

from axon_market_data import MarketDataService, MarketDataEvent, DataType
from axon_market_data.exchanges import (
    BinanceAdapter, BinanceMarketType,
    OKXAdapter,
    BybitAdapter, BybitMarketType,
    KrakenAdapter,
    UpbitAdapter,
    HyperliquidAdapter,
    CoinbaseAdapter,
)
from axon_market_data.utils import setup_colored_logger


async def on_orderbook(event: MarketDataEvent) -> None:
    """Callback for orderbook updates"""
    data_json = json.dumps(event.data.to_dict()) if event.data else None
    print(f"[{event.exchange}] {event.symbol} type={event.event_type} data={data_json}")


async def main():
    logger = setup_colored_logger("mds_example", level=logging.DEBUG)

    # Create MarketDataService
    mds = MarketDataService(logger)

    # Add all adapters
    # Binance spot
    await mds.add_exchange(BinanceAdapter(
        market_type=BinanceMarketType.SPOT,
        logger=logger
    ))
    # Binance USDT futures
    await mds.add_exchange(BinanceAdapter(
        market_type=BinanceMarketType.USDT_FUTURES,
        logger=logger
    ))
    # OKX
    await mds.add_exchange(OKXAdapter(logger=logger))
    # Bybit spot
    await mds.add_exchange(BybitAdapter(
        market_type=BybitMarketType.SPOT,
        logger=logger
    ))
    # Bybit linear (perp)
    await mds.add_exchange(BybitAdapter(
        market_type=BybitMarketType.LINEAR,
        logger=logger
    ))
    # Kraken
    await mds.add_exchange(KrakenAdapter(logger=logger))
    # Upbit
    await mds.add_exchange(UpbitAdapter(logger=logger))
    # Hyperliquid
    await mds.add_exchange(HyperliquidAdapter(logger=logger))
    # Coinbase
    await mds.add_exchange(CoinbaseAdapter(
        api_key="",
        api_secret="",
        logger=logger
    ))

    # Register orderbook callback
    mds.on_market_data(DataType.ORDERBOOK, on_orderbook)

    try:
        # Connect to all exchanges
        await mds.connect()

        # Test subscriptions - uncomment to test specific exchanges
        #await mds.subscribe("binance_spot", ["BTC_USDT_SPOT"], DataType.ORDERBOOK)
        #await mds.subscribe("binance_usdt_futures", ["BTC_USDT_PERP"], DataType.ORDERBOOK)
        #await mds.subscribe("okx", ["BTC_USDT_SPOT"], DataType.ORDERBOOK)
        #await mds.subscribe("okx", ["BTC_USDT_PERP"], DataType.ORDERBOOK)
        #await mds.subscribe("bybit_spot", ["BTC_USDT_SPOT"], DataType.ORDERBOOK)
        #await mds.subscribe("bybit_linear", ["BTC_USDT_PERP"], DataType.ORDERBOOK)
        #await mds.subscribe("kraken", ["BTC_USD_SPOT"], DataType.ORDERBOOK)
        #await mds.subscribe("upbit", ["BTC_USDT_SPOT"], DataType.ORDERBOOK)
        #await mds.subscribe("hyperliquid", ["BTC_USD_PERP"], DataType.ORDERBOOK)
        await mds.subscribe("coinbase", ["BTC_USD_SPOT"], DataType.ORDERBOOK)

        # Default: test Binance spot
        #await mds.subscribe("binance_spot", ["BTC_USDT_SPOT"], DataType.ORDERBOOK)
        #logger.info("Subscribed to Binance BTC_USDT_SPOT")

        # Keep running
        while True:
            await asyncio.sleep(1)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await mds.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
