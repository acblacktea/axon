"""ZMQ Transport Server - bridges ZMQ messages with CalaisExecutionService."""

import asyncio
from typing import Optional

import zmq
import zmq.asyncio

from calais_order_execution.config import ZMQConfig
from calais_order_execution.models.order import Order
from calais_order_execution.models.portfolio import AccountSummary, Position
from calais_order_execution.models.messages import Response, Event
from calais_order_execution.transport.types import CommandType, EventType
from calais_order_execution.transport.serialization import (
    deserialize_command,
    deserialize_order_request,
    serialize_response,
    serialize_event,
    serialize_order,
    serialize_ticker,
    serialize_account_summary,
    serialize_position,
)
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class ZMQTransportServer:
    """ZMQ server that bridges strategy clients with CalaisExecutionService.

    Uses two ZMQ sockets:
    - ROUTER: receives Command messages from strategy DEALER sockets,
              dispatches them to CalaisExecutionService, returns Response.
    - PUB: publishes Event messages (order updates) to strategy SUB sockets.
              Topic = strategy_id for per-strategy filtering.
    """

    def __init__(self, config: ZMQConfig, service):
        self._config = config
        self._service = service
        self._ctx: Optional[zmq.asyncio.Context] = None
        self._router: Optional[zmq.asyncio.Socket] = None
        self._pub: Optional[zmq.asyncio.Socket] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Bind sockets and start the command processing loop."""
        self._ctx = zmq.asyncio.Context()

        self._router = self._ctx.socket(zmq.ROUTER)
        self._router.bind(self._config.router_endpoint)
        logger.info(f"ZMQ ROUTER bound on {self._config.router_endpoint}")

        self._pub = self._ctx.socket(zmq.PUB)
        self._pub.bind(self._config.pub_endpoint)
        logger.info(f"ZMQ PUB bound on {self._config.pub_endpoint}")

        # Register callbacks
        self._service.register_order_update_callback(self._on_order_update)
        self._service.register_account_update_callback(self._on_account_update)
        self._service.register_position_update_callback(self._on_position_update)

        self._running = True
        self._task = asyncio.create_task(self._command_loop())
        logger.info("ZMQ Transport Server started")

    async def stop(self) -> None:
        """Stop the server and cleanup sockets."""
        self._running = False
        self._service.unregister_order_update_callback(self._on_order_update)
        self._service.unregister_account_update_callback(self._on_account_update)
        self._service.unregister_position_update_callback(self._on_position_update)

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._router:
            self._router.close()
        if self._pub:
            self._pub.close()
        if self._ctx:
            self._ctx.term()

        logger.info("ZMQ Transport Server stopped")

    async def _command_loop(self) -> None:
        """Main loop: receive commands from ROUTER, dispatch, send responses."""
        while self._running:
            try:
                # ROUTER receives: [identity, empty_frame, data]
                frames = await self._router.recv_multipart()
                if len(frames) < 3:
                    logger.warning(f"Malformed ROUTER message: {len(frames)} frames")
                    continue

                identity = frames[0]
                data = frames[2]

                command = deserialize_command(data)
                # Dispatch in a task so we don't block the recv loop
                asyncio.create_task(self._handle_command(identity, command))

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in command loop: {e}", exc_info=True)

    async def _handle_command(self, identity: bytes, command) -> None:
        """Handle a single command and send the response back."""
        try:
            response = await self._dispatch(command)
        except Exception as e:
            logger.error(f"Dispatch error: {e}", exc_info=True)
            response = Response(
                request_id=command.request_id,
                success=False,
                error=str(e),
            )

        response_bytes = serialize_response(response)
        await self._router.send_multipart([identity, b"", response_bytes])

    async def _dispatch(self, command) -> Response:
        """Dispatch a Command to the appropriate CalaisExecutionService method."""
        try:
            cmd_type = CommandType(command.command_type)
        except ValueError:
            return Response(
                request_id=command.request_id,
                success=False,
                error=f"Unknown command type: {command.command_type}",
            )

        try:
            if cmd_type == CommandType.PLACE_ORDER:
                exchange = command.payload["exchange"]
                order_request = deserialize_order_request(command.payload["request"])
                order_request.strategy_id = command.strategy_id
                order = await self._service.place_order(exchange, order_request)
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=serialize_order(order),
                )

            elif cmd_type == CommandType.CANCEL_ORDER:
                exchange = command.payload["exchange"]
                order_id = command.payload["order_id"]
                result = await self._service.cancel_order(exchange, order_id)
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=result,
                )

            elif cmd_type == CommandType.MODIFY_ORDER:
                exchange = command.payload["exchange"]
                order_id = command.payload["order_id"]
                amount = command.payload.get("amount")
                price = command.payload.get("price")
                order = await self._service.modify_order(
                    exchange, order_id, amount=amount, price=price
                )
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=serialize_order(order),
                )

            elif cmd_type == CommandType.GET_ORDER:
                order_id = command.payload["order_id"]
                order = await self._service.get_order(order_id)
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=serialize_order(order) if order else None,
                )

            elif cmd_type == CommandType.GET_ALL_ORDERS:
                orders = await self._service.get_all_orders()
                if command.strategy_id:
                    orders = [o for o in orders if o.strategy_id == command.strategy_id]
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=[serialize_order(o) for o in orders],
                )

            elif cmd_type == CommandType.GET_ACTIVE_ORDERS:
                orders = await self._service.get_active_orders()
                if command.strategy_id:
                    orders = [o for o in orders if o.strategy_id == command.strategy_id]
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=[serialize_order(o) for o in orders],
                )

            elif cmd_type == CommandType.GET_TICKER:
                exchange = command.payload["exchange"]
                instrument = command.payload["instrument"]
                ticker = await self._service.get_ticker(exchange, instrument)
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=serialize_ticker(ticker),
                )

            elif cmd_type == CommandType.GET_ACCOUNT_SUMMARY:
                exchange = command.payload["exchange"]
                currency = command.payload.get("currency", "BTC")
                summary = self._service.get_account_summary(exchange, currency)
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=serialize_account_summary(summary) if summary else None,
                )

            elif cmd_type == CommandType.GET_POSITIONS:
                exchange = command.payload["exchange"]
                currency = command.payload.get("currency")
                positions = self._service.get_positions(exchange, currency)
                return Response(
                    request_id=command.request_id,
                    success=True,
                    data=[serialize_position(p) for p in positions],
                )

            else:
                return Response(
                    request_id=command.request_id,
                    success=False,
                    error=f"Unhandled command type: {command.command_type}",
                )

        except Exception as e:
            logger.error(f"Command dispatch error for {command.command_type}: {e}", exc_info=True)
            return Response(
                request_id=command.request_id,
                success=False,
                error=str(e),
            )

    def _on_order_update(self, order: Order) -> None:
        """Callback from OrderManager. Publishes order update via PUB socket.

        This is a synchronous callback. We schedule the async PUB send
        on the running event loop.
        """
        if self._pub is None or not self._running:
            return

        event = Event(
            event_type=EventType.ORDER_UPDATE.value,
            data=serialize_order(order),
            strategy_id=order.strategy_id or "",
        )
        event_bytes = serialize_event(event)
        topic = (order.strategy_id or "__broadcast__").encode("utf-8")

        try:
            asyncio.ensure_future(self._pub.send_multipart([topic, event_bytes]))
        except Exception as e:
            logger.error(f"Failed to publish order update event: {e}")

    def _on_account_update(self, summary: AccountSummary) -> None:
        """Publish account summary update via PUB socket (broadcast to all strategies)."""
        if self._pub is None or not self._running:
            return

        event = Event(
            event_type=EventType.ACCOUNT_UPDATE.value,
            data=serialize_account_summary(summary),
            strategy_id="",
        )
        event_bytes = serialize_event(event)
        topic = b"__broadcast__"

        try:
            asyncio.ensure_future(self._pub.send_multipart([topic, event_bytes]))
        except Exception as e:
            logger.error(f"Failed to publish account update event: {e}")

    def _on_position_update(self, positions: list[Position]) -> None:
        """Publish position update via PUB socket (broadcast to all strategies)."""
        if self._pub is None or not self._running:
            return

        event = Event(
            event_type=EventType.POSITION_UPDATE.value,
            data=[serialize_position(p) for p in positions],
            strategy_id="",
        )
        event_bytes = serialize_event(event)
        topic = b"__broadcast__"

        try:
            asyncio.ensure_future(self._pub.send_multipart([topic, event_bytes]))
        except Exception as e:
            logger.error(f"Failed to publish position update event: {e}")
