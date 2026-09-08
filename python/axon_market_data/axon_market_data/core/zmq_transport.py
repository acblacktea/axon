"""
ZMQ transport layer for market data PUB/SUB.

Shared by server.py and client.py. Encapsulates:
- Message serialization / deserialization
- ZMQ PUB publisher (ZmqPublisher)
- ZMQ SUB subscriber (ZmqSubscriber)
"""
import asyncio
import json
import logging
from decimal import Decimal
from typing import Optional, List, Dict, Set, Callable, Awaitable

import zmq
import zmq.asyncio

from .models import (
    DataType,
    MarketDataEvent,
    OrderbookSnapshot,
    PriceLevel,
    TickerData,
    IndexPriceData,
)


# ── Serialization ──────────────────────────────────────────────────────────


def serialize_event(event: MarketDataEvent) -> tuple[bytes, bytes]:
    """
    Serialize a MarketDataEvent into a ZMQ multipart message.

    Returns:
        (topic_bytes, payload_bytes)

    Topic format: {data_type}.{exchange}.{symbol}
    """
    topic = f"{event.data_type.value}.{event.exchange}.{event.symbol}"
    payload = {
        "data_type": event.data_type.value,
        "event_type": event.event_type,
        "symbol": event.symbol,
        "exchange": event.exchange,
        "timestamp": event.timestamp,
        "data": event.data.to_dict() if event.data else None,
        "error": event.error,
    }
    return topic.encode(), json.dumps(payload).encode()


def _decimal_or_none(value) -> Optional[Decimal]:
    """Convert a string value to Decimal, or return None."""
    return Decimal(value) if value else None


def deserialize_event(payload_bytes: bytes) -> MarketDataEvent:
    """
    Deserialize a JSON payload back into a MarketDataEvent.

    Args:
        payload_bytes: JSON bytes from ZMQ message part[1]
    """
    payload = json.loads(payload_bytes)
    data_type = DataType(payload["data_type"])
    raw = payload.get("data")
    data = None

    if raw:
        if data_type == DataType.ORDERBOOK:
            data = OrderbookSnapshot(
                symbol=raw["symbol"],
                exchange=raw["exchange"],
                bids=[PriceLevel(Decimal(p), Decimal(q)) for p, q in raw["bids"]],
                asks=[PriceLevel(Decimal(p), Decimal(q)) for p, q in raw["asks"]],
                sequence=raw.get("sequence"),
                timestamp=raw.get("timestamp"),
                local_timestamp=raw.get("local_timestamp", 0),
            )
        elif data_type == DataType.TICKER:
            data = TickerData(
                symbol=raw["symbol"],
                exchange=raw["exchange"],
                timestamp=raw.get("timestamp"),
                local_timestamp=raw.get("local_timestamp"),
                state=raw.get("state"),
                last_price=_decimal_or_none(raw.get("last_price")),
                best_bid_price=_decimal_or_none(raw.get("best_bid_price")),
                best_bid_amount=_decimal_or_none(raw.get("best_bid_amount")),
                best_ask_price=_decimal_or_none(raw.get("best_ask_price")),
                best_ask_amount=_decimal_or_none(raw.get("best_ask_amount")),
                mark_price=_decimal_or_none(raw.get("mark_price")),
                index_price=_decimal_or_none(raw.get("index_price")),
                settlement_price=_decimal_or_none(raw.get("settlement_price")),
                underlying_price=_decimal_or_none(raw.get("underlying_price")),
                underlying_index=raw.get("underlying_index"),
                delta=_decimal_or_none(raw.get("delta")),
                gamma=_decimal_or_none(raw.get("gamma")),
                vega=_decimal_or_none(raw.get("vega")),
                theta=_decimal_or_none(raw.get("theta")),
                rho=_decimal_or_none(raw.get("rho")),
                mark_iv=_decimal_or_none(raw.get("mark_iv")),
                bid_iv=_decimal_or_none(raw.get("bid_iv")),
                ask_iv=_decimal_or_none(raw.get("ask_iv")),
                open_interest=_decimal_or_none(raw.get("open_interest")),
                volume=_decimal_or_none(raw.get("volume")),
                volume_usd=_decimal_or_none(raw.get("volume_usd")),
            )
        elif data_type == DataType.INDEX_PRICE:
            data = IndexPriceData(
                index_name=raw["index_name"],
                exchange=raw["exchange"],
                price=Decimal(raw["price"]),
                timestamp=raw.get("timestamp"),
            )

    return MarketDataEvent(
        data_type=data_type,
        event_type=payload["event_type"],
        symbol=payload["symbol"],
        exchange=payload["exchange"],
        data=data,
        error=payload.get("error"),
        timestamp=payload.get("timestamp", 0),
    )


# ── ZMQ Publisher ──────────────────────────────────────────────────────────


class ZmqPublisher:
    """
    Async ZMQ PUB socket wrapper.

    Usage:
        pub = ZmqPublisher("tcp://*:5557")
        pub.start()
        await pub.send(event)
        pub.close()
    """

    def __init__(self, address: str, logger: Optional[logging.Logger] = None):
        self._address = address
        self.logger = logger or logging.getLogger("zmq.pub")
        self._ctx: Optional[zmq.asyncio.Context] = None
        self._socket = None

    def start(self) -> None:
        """Create context and bind PUB socket."""
        self._ctx = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.PUB)
        self._socket.bind(self._address)
        self.logger.info(f"ZMQ PUB bound to {self._address}")

    async def send(self, event: MarketDataEvent) -> None:
        """Serialize and publish a MarketDataEvent."""
        if not self._socket:
            return
        topic_bytes, payload_bytes = serialize_event(event)
        await self._socket.send_multipart([topic_bytes, payload_bytes])

    def close(self) -> None:
        """Close socket and terminate context."""
        if self._socket:
            self._socket.close()
            self._socket = None
        if self._ctx:
            self._ctx.term()
            self._ctx = None


# ── ZMQ Subscriber ─────────────────────────────────────────────────────────


MarketDataCallback = Callable[[MarketDataEvent], Awaitable[None]]


class ZmqSubscriber:
    """
    Async ZMQ SUB socket wrapper with callback dispatch.

    Usage:
        sub = ZmqSubscriber("tcp://localhost:5557")
        sub.on(DataType.ORDERBOOK, my_callback)
        sub.connect()
        sub.subscribe("")   # receive all
        await sub.run()     # blocks until stop()
        sub.close()
    """

    def __init__(self, address: str, logger: Optional[logging.Logger] = None):
        self._address = address
        self.logger = logger or logging.getLogger("zmq.sub")
        self._ctx: Optional[zmq.asyncio.Context] = None
        self._socket = None
        self._stop_event = asyncio.Event()
        self._recv_task: Optional[asyncio.Task] = None
        self._callbacks: Dict[DataType, List[MarketDataCallback]] = {
            dt: [] for dt in DataType
        }
        self._subscribed_topics: Set[str] = set()

    def on(self, data_type: DataType, callback: MarketDataCallback) -> None:
        """Register a callback for a specific data type."""
        if callback not in self._callbacks[data_type]:
            self._callbacks[data_type].append(callback)

    def remove_callback(self, data_type: DataType, callback: MarketDataCallback) -> None:
        """Remove a callback."""
        if callback in self._callbacks[data_type]:
            self._callbacks[data_type].remove(callback)

    def connect(self) -> None:
        """Create context and connect SUB socket."""
        self._ctx = zmq.asyncio.Context()
        self._socket = self._ctx.socket(zmq.SUB)
        self._socket.connect(self._address)
        self.logger.info(f"ZMQ SUB connected to {self._address}")

    def subscribe(self, topic: str = "") -> None:
        """
        Subscribe to a topic prefix.

        "" = all, "orderbook." = all orderbooks,
        "orderbook.deribit." = deribit orderbooks only.
        """
        if self._socket and topic not in self._subscribed_topics:
            self._socket.setsockopt_string(zmq.SUBSCRIBE, topic)
            self._subscribed_topics.add(topic)

    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic prefix."""
        if self._socket and topic in self._subscribed_topics:
            self._socket.setsockopt_string(zmq.UNSUBSCRIBE, topic)
            self._subscribed_topics.discard(topic)

    async def run(self) -> None:
        """Start recv loop. Blocks until stop() is called."""
        self._stop_event.clear()
        self._recv_task = asyncio.create_task(self._recv_loop())
        try:
            await self._stop_event.wait()
        finally:
            if self._recv_task:
                self._recv_task.cancel()
                try:
                    await self._recv_task
                except asyncio.CancelledError:
                    pass

    async def _recv_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                parts = await self._socket.recv_multipart()
                if len(parts) != 2:
                    self.logger.debug(f"Unexpected message parts: {len(parts)}")
                    continue

                event = deserialize_event(parts[1])

                for callback in self._callbacks[event.data_type]:
                    try:
                        await callback(event)
                    except Exception as e:
                        self.logger.error(f"Callback error: {e}", exc_info=True)

            except asyncio.CancelledError:
                break
            except zmq.ZMQError as e:
                if e.errno == zmq.ETERM:
                    break
                self.logger.error(f"ZMQ error: {e}")
            except Exception as e:
                self.logger.error(f"Recv loop error: {e}", exc_info=True)

    def stop(self) -> None:
        """Signal the run loop to stop."""
        self._stop_event.set()

    def close(self) -> None:
        """Close socket and terminate context."""
        self.stop()
        if self._socket:
            self._socket.close()
            self._socket = None
        if self._ctx:
            self._ctx.term()
            self._ctx = None
