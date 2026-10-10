"""
ZMQ-based Market Data Clients

MarketDataClient: General-purpose client that subscribes to topics from MarketDataServer.
DeribitOptionsClient: Specialized client for DeribitOptionsDataService.
"""
import logging
from typing import Optional

from .core.models import DataType
from .core.zmq_transport import ZmqSubscriber, MarketDataCallback


class MarketDataClient:
    """
    General-purpose ZMQ market data client.

    Connects to a MarketDataServer's PUB socket, subscribes to topics,
    and delivers deserialized MarketDataEvent objects via callbacks.

    Topic format: {data_type}.{exchange}.{symbol}

    Usage:
        client = MarketDataClient(server_address="tcp://localhost:5558")

        async def on_orderbook(event: MarketDataEvent):
            print(event.symbol, event.data)

        client.on(DataType.ORDERBOOK, on_orderbook)
        await client.connect()

        # Subscribe to specific topics
        client.subscribe("orderbook.deribit.BTC-25DEC26-100000-C")
        # Or subscribe with wildcard prefix
        client.subscribe("orderbook.deribit.")

        await client.run()
    """

    def __init__(
        self,
        server_address: str = "tcp://localhost:5558",
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger("mds.client")
        self._sub = ZmqSubscriber(server_address, logger=self.logger)

    def on(self, data_type: DataType, callback: MarketDataCallback) -> None:
        """Register a callback for a specific data type."""
        self._sub.on(data_type, callback)

    def remove_callback(self, data_type: DataType, callback: MarketDataCallback) -> None:
        """Remove a callback for a specific data type."""
        self._sub.remove_callback(data_type, callback)

    async def connect(self) -> None:
        """Connect to the server's PUB socket."""
        self._sub.connect()

    def subscribe(self, topic: str = "") -> None:
        """
        Subscribe to a ZMQ topic filter.

        Args:
            topic: Topic prefix to subscribe to.
                "" = receive all messages
                "orderbook." = all orderbook messages
                "orderbook.deribit." = all deribit orderbook messages
                "orderbook.deribit.BTC-25DEC26-100000-C" = specific symbol
        """
        self._sub.subscribe(topic)

    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a ZMQ topic filter."""
        self._sub.unsubscribe(topic)

    async def run(self) -> None:
        """Start receiving messages and dispatching to callbacks."""
        await self._sub.run()

    def stop(self) -> None:
        """Signal the run loop to stop."""
        self._sub.stop()

    async def disconnect(self) -> None:
        """Disconnect and clean up."""
        self._sub.close()
        self.logger.info("Disconnected")


class DeribitOptionsClient:
    """
    Specialized ZMQ client for DeribitOptionsDataService.

    Subscribes to all Deribit options data (orderbook, ticker, index price)
    published by DeribitOptionsDataService.

    Usage:
        client = DeribitOptionsClient(server_address="tcp://localhost:5557")

        async def on_orderbook(event):
            print(f"OB: {event.symbol}")

        async def on_ticker(event):
            print(f"Ticker: {event.symbol} delta={event.data.delta}")

        async def on_spot(event):
            print(f"Spot: {event.data.price}")

        client.on_orderbook(on_orderbook)
        client.on_ticker(on_ticker)
        client.on_index_price(on_spot)

        await client.connect()
        await client.run()
    """

    def __init__(
        self,
        server_address: str = "tcp://localhost:5557",
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger("mds.deribit_client")
        self._inner = MarketDataClient(
            server_address=server_address,
            logger=self.logger,
        )

    def on_orderbook(self, callback: MarketDataCallback) -> None:
        """Register callback for orderbook updates."""
        self._inner.on(DataType.ORDERBOOK, callback)

    def on_ticker(self, callback: MarketDataCallback) -> None:
        """Register callback for ticker updates."""
        self._inner.on(DataType.TICKER, callback)

    def on_index_price(self, callback: MarketDataCallback) -> None:
        """Register callback for index/spot price updates."""
        self._inner.on(DataType.INDEX_PRICE, callback)

    async def connect(self) -> None:
        """Connect to DeribitOptionsDataService."""
        await self._inner.connect()
        # Subscribe to all messages (the service only publishes deribit options data)
        self._inner.subscribe("")

    async def run(self) -> None:
        """Start receiving and dispatching messages."""
        await self._inner.run()

    def stop(self) -> None:
        """Stop the client."""
        self._inner.stop()

    async def disconnect(self) -> None:
        """Disconnect and clean up."""
        await self._inner.disconnect()
