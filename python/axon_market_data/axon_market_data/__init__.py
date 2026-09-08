"""
Market Data Service (MDS)

A modular, extensible market data service for cryptocurrency exchanges.

Supports:
- Multiple exchange adapters (Deribit, with Binance/OKX/Bybit planned)
- Real-time orderbook subscription and maintenance
- Real-time index price subscription (spot price)
- Automatic sequence validation and recovery
- Unified callback-based event handling via MarketDataEvent

Example:
    from axon_market_data import (
        MarketDataService, DeribitAdapter, DeribitOptionsDataClient,
        DataType, MarketDataEvent
    )

    async def on_orderbook(event: MarketDataEvent):
        print(f"Orderbook: {event.symbol}")

    async def on_spot(event: MarketDataEvent):
        print(f"Spot: ${event.data.price}")

    # Create service
    mds = MarketDataService()
    await mds.add_exchange(DeribitAdapter())

    # Use high-level client for options
    client = DeribitOptionsDataClient(mds)
    await client.subscribe_btc_options(months=3, callback=on_orderbook)
    await client.subscribe_spot_price("BTC", callback=on_spot)

    # Get orderbooks
    orderbooks = client.get_all_orderbooks()
"""
from .core import (
    OrderbookSide,
    PriceLevel,
    Orderbook,
    OrderbookSnapshot,
    Instrument,
    ExchangeAdapter,
    OrderbookManager,
    # Unified types
    DataType,
    MarketDataEvent,
    MarketDataCallback,
    TickerData,
    IndexPriceData,
)
from .exchanges import DeribitAdapter
from .service import MarketDataService, DeribitOptionsDataClient
from .server import MarketDataServer, DeribitOptionsDataService
from .client import MarketDataClient, DeribitOptionsClient

__version__ = "0.4.0"

__all__ = [
    # Core models
    'OrderbookSide',
    'PriceLevel',
    'Orderbook',
    'OrderbookSnapshot',
    'Instrument',
    # Unified types
    'DataType',
    'MarketDataEvent',
    'MarketDataCallback',
    'TickerData',
    'IndexPriceData',
    # Adapters
    'ExchangeAdapter',
    'OrderbookManager',
    'DeribitAdapter',
    # In-process service
    'MarketDataService',
    'DeribitOptionsDataClient',
    # ZMQ-based servers
    'MarketDataServer',
    'DeribitOptionsDataService',
    # ZMQ-based clients
    'MarketDataClient',
    'DeribitOptionsClient',
]
