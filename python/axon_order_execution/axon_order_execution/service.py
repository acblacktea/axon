"""Axon Execution Service - ZMQ-based execution engine for strategies."""

import time
from typing import Callable

from axon_order_execution.config import Config
from axon_order_execution.ems.ems_service import EMSService
from axon_order_execution.models import Fill, Order, OrderRequest, OrderStatus, Ticker
from axon_order_execution.models.portfolio import AccountSummary, Position
from axon_order_execution.oms.oms_service import OMSService
from axon_order_execution.repository import FillRepository, OrderRepository
from axon_order_execution.repository.account_base import AccountRepository
from axon_order_execution.repository.position_base import PositionRepository
from axon_order_execution.transport.server import ZMQTransportServer
from axon_order_execution.util.logging import get_logger
from axon_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class AxonExecutionService:
    def __init__(
        self,
        config: Config,
        repository: OrderRepository | None = None,
        account_repository: AccountRepository | None = None,
        position_repository: PositionRepository | None = None,
        fill_repository: FillRepository | None = None,
    ):
        """Initialize execution service.

        Args:
            config: Service configuration (must include zmq config).
            repository: Order repository. Uses InMemoryOrderRepository if not provided.
            account_repository: Account repository. Uses InMemoryAccountRepository if not provided.
            position_repository: Position repository. Uses InMemoryPositionRepository if not provided.
            fill_repository: Fill repository. Uses InMemoryFillRepository if not provided.
        """
        self._config = config
        self._ems = EMSService(config)
        self._oms = OMSService(
            config,
            self._ems,
            repository,
            account_repository,
            position_repository,
            fill_repository,
        )
        self._transport = ZMQTransportServer(config.zmq, self)
        self._running = False

    async def start(self) -> None:
        """Start all components (EMS, OMS, ZMQ transport)."""
        logger.info("Starting Axon Execution Service")
        await self._ems.start()
        await self._oms.start()
        await self._transport.start()
        self._running = True
        logger.info("Axon Execution Service started")

    async def stop(self) -> None:
        """Stop all components and cleanup."""
        logger.info("Stopping Axon Execution Service")
        self._running = False
        await self._transport.stop()
        await self._oms.stop()
        await self._ems.stop()
        logger.info("Axon Execution Service stopped")

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._running

    # ============= Order Interface (called by ZMQ transport) =============

    async def place_order(self, exchange: str, request: OrderRequest) -> Order:
        """Place an order on the specified exchange."""
        metrics = get_metrics()
        start = time.monotonic()
        try:
            order = await self._ems.place_order(exchange, request)
        except Exception as e:
            metrics.observe_order_submit_latency(exchange, time.monotonic() - start)
            metrics.inc_order_place_failure(exchange, type(e).__name__)
            raise
        metrics.observe_order_submit_latency(exchange, time.monotonic() - start)
        if order.status == OrderStatus.REJECTED:
            metrics.inc_order_rejected(exchange, reason="exchange")
        if request.strategy_id:
            order.strategy_id = request.strategy_id
        await self._oms.add_order(order)
        return order

    async def cancel_order(self, exchange: str, order_id: str) -> bool:
        """Cancel an order."""
        metrics = get_metrics()
        start = time.monotonic()
        try:
            result = await self._ems.cancel_order(exchange, order_id)
        except Exception as e:
            metrics.observe_order_cancel_latency(exchange, time.monotonic() - start)
            metrics.inc_order_cancel_failure(exchange, type(e).__name__)
            raise
        metrics.observe_order_cancel_latency(exchange, time.monotonic() - start)
        return result

    async def get_order(self, order_id: str) -> Order | None:
        """Get order by ID from local cache."""
        return await self._oms.get_order(order_id)

    async def get_all_orders(self) -> list[Order]:
        """Get all orders from local cache."""
        return await self._oms.get_all_orders()

    async def get_active_orders(self) -> list[Order]:
        """Get all active orders from local cache."""
        return await self._oms.get_active_orders()

    async def modify_order(
        self,
        exchange: str,
        order_id: str,
        amount: float | None = None,
        price: float | None = None,
    ) -> Order:
        """Modify an existing order."""
        metrics = get_metrics()
        start = time.monotonic()
        try:
            order = await self._ems.modify_order(
                exchange, order_id, amount=amount, price=price
            )
        except Exception as e:
            metrics.observe_order_modify_latency(exchange, time.monotonic() - start)
            metrics.inc_order_modify_failure(exchange, type(e).__name__)
            raise
        metrics.observe_order_modify_latency(exchange, time.monotonic() - start)
        return order

    def register_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Register a callback for order updates."""
        self._oms.register_order_update_callback(callback)

    def unregister_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Unregister an order update callback."""
        self._oms.unregister_order_update_callback(callback)

    # ============= Portfolio Interface (called by ZMQ transport) =============

    async def get_ticker(self, exchange: str, instrument: str) -> Ticker:
        """Get ticker data for an instrument."""
        return await self._ems.get_ticker(exchange, instrument)

    def get_account_summary(self, exchange: str, currency: str = "BTC") -> AccountSummary | None:
        """Get account summary from local cache."""
        return self._oms.get_account(exchange, currency)

    def get_positions(self, exchange: str, currency: str | None = None) -> list[Position]:
        """Get positions from local cache."""
        positions = self._oms.get_positions_by_exchange(exchange)
        if currency:
            positions = [p for p in positions if p.instrument.startswith(currency)]
        return positions

    def register_account_update_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Register a callback for account summary updates."""
        self._oms.register_account_update_callback(callback)

    def unregister_account_update_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Unregister an account summary callback."""
        self._oms.unregister_account_update_callback(callback)

    def register_position_update_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Register a callback for position updates."""
        self._oms.register_position_update_callback(callback)

    def unregister_position_update_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Unregister a position callback."""
        self._oms.unregister_position_update_callback(callback)

    # ============= Fills =============

    async def get_fill(self, trade_id: str) -> Fill | None:
        """Get a fill by trade_id."""
        return await self._oms.get_fill(trade_id)

    async def get_fills_by_order(self, order_id: str) -> list[Fill]:
        """Get all fills for a given order_id, ordered by timestamp."""
        return await self._oms.get_fills_by_order(order_id)

    async def get_fills_by_strategy(self, strategy_id: str) -> list[Fill]:
        """Get all fills for a given strategy_id, ordered by timestamp."""
        return await self._oms.get_fills_by_strategy(strategy_id)

    async def get_all_fills(self) -> list[Fill]:
        """Get all known fills, ordered by timestamp."""
        return await self._oms.get_all_fills()

    def register_fill_update_callback(self, callback: Callable[[Fill], None]) -> None:
        """Register a callback for new fills."""
        self._oms.register_fill_update_callback(callback)

    def unregister_fill_update_callback(self, callback: Callable[[Fill], None]) -> None:
        """Unregister a fill callback."""
        self._oms.unregister_fill_update_callback(callback)

    # ============= Context Manager =============

    async def __aenter__(self) -> "AxonExecutionService":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
