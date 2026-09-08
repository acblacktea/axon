"""Strategy Client SDK - mirrors AxonExecutionService interface over ZMQ."""

import asyncio
from typing import Any, Callable, Optional

import zmq
import zmq.asyncio

from axon_order_execution.config import ZMQConfig
from axon_order_execution.models.fill import Fill
from axon_order_execution.models.order import Order, OrderRequest, Ticker
from axon_order_execution.models.portfolio import AccountSummary, Position
from axon_order_execution.models.messages import Command
from axon_order_execution.transport.types import CommandType, EventType
from axon_order_execution.transport.serialization import (
    serialize_command,
    serialize_order_request,
    deserialize_response,
    deserialize_event,
    deserialize_fill,
    deserialize_order,
    deserialize_ticker,
    deserialize_account_summary,
    deserialize_position,
)
from axon_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class StrategyClient:
    """Client SDK that strategies use to communicate with the engine process.

    Provides the same interface as AxonExecutionService but communicates
    via ZMQ sockets.

    Uses:
    - DEALER socket to send commands and receive responses from engine ROUTER.
    - SUB socket to receive order update events from engine PUB.
    """

    def __init__(
        self,
        zmq_config: ZMQConfig,
        strategy_id: str,
        request_timeout: float = 30.0,
    ):
        if not strategy_id:
            raise ValueError("strategy_id is required")
        self._zmq_config = zmq_config
        self._strategy_id = strategy_id
        self._request_timeout = request_timeout
        self._ctx: Optional[zmq.asyncio.Context] = None
        self._dealer: Optional[zmq.asyncio.Socket] = None
        self._sub: Optional[zmq.asyncio.Socket] = None
        self._running = False
        self._pending: dict[str, asyncio.Future] = {}
        self._recv_task: Optional[asyncio.Task] = None
        self._sub_task: Optional[asyncio.Task] = None
        self._order_update_callbacks: list[Callable[[Order], None]] = []
        self._async_order_update_callbacks: list[Callable[[Order], Any]] = []
        self._account_update_callbacks: list[Callable[[AccountSummary], None]] = []
        self._position_update_callbacks: list[Callable[[list[Position]], None]] = []
        self._fill_update_callbacks: list[Callable[[Fill], None]] = []
        self._async_fill_update_callbacks: list[Callable[[Fill], Any]] = []

    @property
    def strategy_id(self) -> str:
        return self._strategy_id

    async def connect(self) -> None:
        """Connect DEALER and SUB sockets to the engine."""
        self._ctx = zmq.asyncio.Context()

        # DEALER socket for request-response
        self._dealer = self._ctx.socket(zmq.DEALER)
        self._dealer.setsockopt_string(zmq.IDENTITY, self._strategy_id)
        self._dealer.connect(self._zmq_config.router_connect)
        logger.info(f"DEALER connected to {self._zmq_config.router_connect}")

        # SUB socket for events
        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.connect(self._zmq_config.pub_connect)
        self._sub.setsockopt(zmq.SUBSCRIBE, self._strategy_id.encode("utf-8"))
        self._sub.setsockopt(zmq.SUBSCRIBE, b"__broadcast__")
        logger.info(f"SUB connected to {self._zmq_config.pub_connect}, topics=[{self._strategy_id}, __broadcast__]")

        self._running = True
        self._recv_task = asyncio.create_task(self._dealer_recv_loop())
        self._sub_task = asyncio.create_task(self._sub_recv_loop())

    async def disconnect(self) -> None:
        """Disconnect and cleanup."""
        self._running = False

        if self._recv_task:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass

        if self._sub_task:
            self._sub_task.cancel()
            try:
                await self._sub_task
            except asyncio.CancelledError:
                pass

        for future in self._pending.values():
            future.cancel()
        self._pending.clear()

        if self._dealer:
            self._dealer.close()
        if self._sub:
            self._sub.close()
        if self._ctx:
            self._ctx.term()

        logger.info("StrategyClient disconnected")

    # ============= Request-Response =============

    async def _send_command(self, command_type: CommandType, payload: dict[str, Any]) -> Any:
        """Send a command and wait for the response."""
        cmd = Command(
            command_type=command_type.value,
            payload=payload,
            strategy_id=self._strategy_id,
        )
        data = serialize_command(cmd)

        future: asyncio.Future = asyncio.get_running_loop().create_future()
        self._pending[cmd.request_id] = future

        # DEALER sends: [empty_frame, data]
        await self._dealer.send_multipart([b"", data])

        try:
            response = await asyncio.wait_for(future, timeout=self._request_timeout)
            return response
        except asyncio.TimeoutError:
            self._pending.pop(cmd.request_id, None)
            raise TimeoutError(
                f"Request {command_type.value} timed out after {self._request_timeout}s"
            )

    async def _dealer_recv_loop(self) -> None:
        """Receive responses from the engine ROUTER."""
        while self._running:
            try:
                frames = await self._dealer.recv_multipart()
                # DEALER receives: [empty_frame, data]
                if len(frames) < 2:
                    continue
                data = frames[1]
                response = deserialize_response(data)

                future = self._pending.pop(response.request_id, None)
                if future and not future.done():
                    future.set_result(response)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in dealer recv loop: {e}")

    # ============= Event Subscription =============

    async def _sub_recv_loop(self) -> None:
        """Receive events from the engine PUB."""
        while self._running:
            try:
                frames = await self._sub.recv_multipart()
                # PUB sends: [topic, event_bytes]
                if len(frames) < 2:
                    continue
                event = deserialize_event(frames[1])

                if event.event_type == EventType.ORDER_UPDATE.value:
                    order = deserialize_order(event.data)
                    await self._notify_order_update(order)
                elif event.event_type == EventType.ACCOUNT_UPDATE.value:
                    summary = deserialize_account_summary(event.data)
                    self._notify_account_update(summary)
                elif event.event_type == EventType.POSITION_UPDATE.value:
                    positions = [deserialize_position(p) for p in event.data]
                    self._notify_position_update(positions)
                elif event.event_type == EventType.FILL_UPDATE.value:
                    fill = deserialize_fill(event.data)
                    await self._notify_fill_update(fill)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in sub recv loop: {e}")

    async def _notify_order_update(self, order: Order) -> None:
        """Notify registered callbacks of an order update."""
        for callback in self._order_update_callbacks:
            try:
                callback(order)
            except Exception as e:
                logger.error(f"Order update callback error: {e}")
        for callback in self._async_order_update_callbacks:
            try:
                await callback(order)
            except Exception as e:
                logger.error(f"Async order update callback error: {e}")

    def register_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Register a synchronous callback for order updates."""
        self._order_update_callbacks.append(callback)

    def unregister_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Unregister an order update callback."""
        if callback in self._order_update_callbacks:
            self._order_update_callbacks.remove(callback)

    def register_async_order_update_callback(self, callback: Callable[[Order], Any]) -> None:
        """Register an async callback for order updates."""
        self._async_order_update_callbacks.append(callback)

    # ============= Account/Position Callbacks =============

    def _notify_account_update(self, summary: AccountSummary) -> None:
        """Notify registered callbacks of an account summary update."""
        for callback in self._account_update_callbacks:
            try:
                callback(summary)
            except Exception as e:
                logger.error(f"Account update callback error: {e}")

    def _notify_position_update(self, positions: list[Position]) -> None:
        """Notify registered callbacks of a position update."""
        for callback in self._position_update_callbacks:
            try:
                callback(positions)
            except Exception as e:
                logger.error(f"Position update callback error: {e}")

    def register_account_update_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Register a callback for account summary updates."""
        self._account_update_callbacks.append(callback)

    def unregister_account_update_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Unregister an account summary callback."""
        if callback in self._account_update_callbacks:
            self._account_update_callbacks.remove(callback)

    def register_position_update_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Register a callback for position updates."""
        self._position_update_callbacks.append(callback)

    def unregister_position_update_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Unregister a position callback."""
        if callback in self._position_update_callbacks:
            self._position_update_callbacks.remove(callback)

    # ============= Fill Callbacks =============

    async def _notify_fill_update(self, fill: Fill) -> None:
        for callback in self._fill_update_callbacks:
            try:
                callback(fill)
            except Exception as e:
                logger.error(f"Fill update callback error: {e}")
        for callback in self._async_fill_update_callbacks:
            try:
                await callback(fill)
            except Exception as e:
                logger.error(f"Async fill update callback error: {e}")

    def register_fill_update_callback(self, callback: Callable[[Fill], None]) -> None:
        """Register a synchronous callback for fills (trade executions)."""
        self._fill_update_callbacks.append(callback)

    def unregister_fill_update_callback(self, callback: Callable[[Fill], None]) -> None:
        """Unregister a fill callback."""
        if callback in self._fill_update_callbacks:
            self._fill_update_callbacks.remove(callback)

    def register_async_fill_update_callback(self, callback: Callable[[Fill], Any]) -> None:
        """Register an async callback for fills."""
        self._async_fill_update_callbacks.append(callback)

    # ============= Strategy Interface (mirrors AxonExecutionService) =============

    async def place_order(self, exchange: str, request: OrderRequest) -> Order:
        """Place an order on the specified exchange."""
        response = await self._send_command(
            CommandType.PLACE_ORDER,
            {
                "exchange": exchange,
                "request": serialize_order_request(request),
            },
        )
        if not response.success:
            raise RuntimeError(f"place_order failed: {response.error}")
        return deserialize_order(response.data)

    async def cancel_order(self, exchange: str, order_id: str) -> bool:
        """Cancel an order."""
        response = await self._send_command(
            CommandType.CANCEL_ORDER,
            {"exchange": exchange, "order_id": order_id},
        )
        if not response.success:
            raise RuntimeError(f"cancel_order failed: {response.error}")
        return response.data

    async def modify_order(
        self,
        exchange: str,
        order_id: str,
        amount: float | None = None,
        price: float | None = None,
    ) -> Order:
        """Modify an existing order."""
        response = await self._send_command(
            CommandType.MODIFY_ORDER,
            {"exchange": exchange, "order_id": order_id, "amount": amount, "price": price},
        )
        if not response.success:
            raise RuntimeError(f"modify_order failed: {response.error}")
        return deserialize_order(response.data)

    async def get_order(self, order_id: str) -> Optional[Order]:
        """Get order by ID from engine cache."""
        response = await self._send_command(
            CommandType.GET_ORDER,
            {"order_id": order_id},
        )
        if not response.success:
            raise RuntimeError(f"get_order failed: {response.error}")
        return deserialize_order(response.data) if response.data else None

    async def get_all_orders(self) -> list[Order]:
        """Get all orders for this strategy."""
        response = await self._send_command(
            CommandType.GET_ALL_ORDERS,
            {},
        )
        if not response.success:
            raise RuntimeError(f"get_all_orders failed: {response.error}")
        return [deserialize_order(o) for o in response.data]

    async def get_active_orders(self) -> list[Order]:
        """Get all active orders for this strategy."""
        response = await self._send_command(
            CommandType.GET_ACTIVE_ORDERS,
            {},
        )
        if not response.success:
            raise RuntimeError(f"get_active_orders failed: {response.error}")
        return [deserialize_order(o) for o in response.data]

    async def get_ticker(self, exchange: str, instrument: str) -> Ticker:
        """Get ticker data for an instrument."""
        response = await self._send_command(
            CommandType.GET_TICKER,
            {"exchange": exchange, "instrument": instrument},
        )
        if not response.success:
            raise RuntimeError(f"get_ticker failed: {response.error}")
        return deserialize_ticker(response.data)

    # ============= Portfolio Interface =============

    async def get_account_summary(self, exchange: str, currency: str = "BTC") -> Optional[AccountSummary]:
        """Get account summary from engine cache."""
        response = await self._send_command(
            CommandType.GET_ACCOUNT_SUMMARY,
            {"exchange": exchange, "currency": currency},
        )
        if not response.success:
            raise RuntimeError(f"get_account_summary failed: {response.error}")
        return deserialize_account_summary(response.data) if response.data else None

    async def get_positions(self, exchange: str, currency: str | None = None) -> list[Position]:
        """Get positions from engine cache."""
        response = await self._send_command(
            CommandType.GET_POSITIONS,
            {"exchange": exchange, "currency": currency},
        )
        if not response.success:
            raise RuntimeError(f"get_positions failed: {response.error}")
        return [deserialize_position(p) for p in response.data]

    # ============= Fills =============

    async def get_fills_by_order(self, order_id: str) -> list[Fill]:
        """Fetch all fills for a given order_id from the engine."""
        response = await self._send_command(
            CommandType.GET_FILLS_BY_ORDER,
            {"order_id": order_id},
        )
        if not response.success:
            raise RuntimeError(f"get_fills_by_order failed: {response.error}")
        return [deserialize_fill(f) for f in response.data]

    async def get_fills_by_strategy(self, strategy_id: str | None = None) -> list[Fill]:
        """Fetch all fills for a strategy (defaults to this client's strategy_id)."""
        response = await self._send_command(
            CommandType.GET_FILLS_BY_STRATEGY,
            {"strategy_id": strategy_id or self._strategy_id},
        )
        if not response.success:
            raise RuntimeError(f"get_fills_by_strategy failed: {response.error}")
        return [deserialize_fill(f) for f in response.data]

    # ============= Context Manager =============

    async def __aenter__(self) -> "StrategyClient":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()
