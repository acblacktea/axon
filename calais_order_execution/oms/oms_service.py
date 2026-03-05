"""OMS Service - manages order state, WebSocket connections, and reconciliation."""

from typing import Callable

from calais_order_execution.config import Config
from calais_order_execution.ems.ems_service import EMSService
from calais_order_execution.models import Order
from calais_order_execution.models.portfolio import AccountSummary, Position
from calais_order_execution.oms.deribit import DeribitOMS
from calais_order_execution.oms.order_manager import OrderManager
from calais_order_execution.oms.portfolio_manager import PortfolioManager
from calais_order_execution.oms.position_refresher import PositionRefresher
from calais_order_execution.oms.reconciler import OrderReconciler
from calais_order_execution.repository import InMemoryOrderRepository, OrderRepository
from calais_order_execution.util import WebSocketBase
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class OMSService:
    """Manages OMS WebSocket clients, reconcilers, and order state."""

    def __init__(
        self,
        config: Config,
        ems_service: EMSService,
        repository: OrderRepository | None = None,
    ):
        """Initialize OMS service.

        Args:
            config: Service configuration.
            ems_service: EMS service for reconciliation.
            repository: Order repository. Uses InMemoryOrderRepository if not provided.
        """
        self._config = config
        self._ems_service = ems_service
        self._repository = repository or InMemoryOrderRepository()

        self._oms_ws: dict[str, WebSocketBase] = {}
        self._reconcilers: dict[str, OrderReconciler] = {}
        self._position_refreshers: dict[str, PositionRefresher] = {}
        self._order_manager = OrderManager(self._repository)
        self._portfolio_manager = PortfolioManager()

        self._init_clients()

    def _init_clients(self) -> None:
        """Initialize OMS WebSocket clients and reconcilers."""
        for name, exchange_config in self._config.exchanges.items():
            if name == "deribit":
                # OMS WebSocket
                ws = DeribitOMS(
                    self._order_manager,
                    exchange_config,
                    self._config.websocket,
                    portfolio_manager=self._portfolio_manager,
                    portfolio_config=self._config.portfolio,
                )
                self._oms_ws[name] = ws

                # Reconciler
                ems = self._ems_service.get(name)
                if ems:
                    self._reconcilers[name] = OrderReconciler(
                        ems,
                        self._order_manager,
                        self._config.reconciliation,
                    )
                    # Position refresher
                    self._position_refreshers[name] = PositionRefresher(
                        ems,
                        self._portfolio_manager,
                        self._config.portfolio,
                    )
            else:
                logger.warning(f"Unsupported exchange for OMS: {name}")

    async def start(self) -> None:
        """Start all OMS components."""
        # Connect WebSocket clients
        for name, ws in self._oms_ws.items():
            await ws.connect()
            logger.info(f"Connected OMS WebSocket: {name}")

        # Start reconcilers
        for name, reconciler in self._reconcilers.items():
            await reconciler.start()
            logger.info(f"Started reconciler: {name}")

        # Start position refreshers
        for name, refresher in self._position_refreshers.items():
            await refresher.start()
            logger.info(f"Started position refresher: {name}")

    async def stop(self) -> None:
        """Stop all OMS components."""
        # Stop position refreshers
        for name, refresher in self._position_refreshers.items():
            await refresher.stop()
            logger.info(f"Stopped position refresher: {name}")

        # Stop reconcilers
        for name, reconciler in self._reconcilers.items():
            await reconciler.stop()
            logger.info(f"Stopped reconciler: {name}")

        # Disconnect WebSocket clients
        for name, ws in self._oms_ws.items():
            await ws.disconnect()
            logger.info(f"Disconnected OMS WebSocket: {name}")

    @property
    def order_manager(self) -> OrderManager:
        """Get the order manager."""
        return self._order_manager

    async def add_order(self, order: Order) -> None:
        """Add an order to the order manager."""
        await self._order_manager.add_order(order)

    async def get_order(self, order_id: str) -> Order | None:
        """Get order by ID from local cache."""
        return await self._order_manager.get_order(order_id)

    async def get_all_orders(self) -> list[Order]:
        """Get all orders from local cache."""
        return await self._order_manager.get_all_orders()

    async def get_active_orders(self) -> list[Order]:
        """Get all active orders from local cache."""
        return await self._order_manager.get_active_orders()

    def register_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Register a callback for order updates."""
        self._order_manager.register_update_callback(callback)

    def unregister_order_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Unregister an order update callback."""
        self._order_manager.unregister_update_callback(callback)

    # ============= Portfolio =============

    @property
    def portfolio_manager(self) -> PortfolioManager:
        """Get the portfolio manager."""
        return self._portfolio_manager

    def get_account(self, currency: str) -> AccountSummary | None:
        """Get account summary by currency from cache."""
        return self._portfolio_manager.get_account(currency)

    def get_all_positions(self) -> list[Position]:
        """Get all positions from cache."""
        return self._portfolio_manager.get_all_positions()

    def register_account_update_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Register a callback for account summary updates."""
        self._portfolio_manager.register_account_callback(callback)

    def unregister_account_update_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Unregister an account summary callback."""
        self._portfolio_manager.unregister_account_callback(callback)

    def register_position_update_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Register a callback for position updates."""
        self._portfolio_manager.register_position_callback(callback)

    def unregister_position_update_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Unregister a position callback."""
        self._portfolio_manager.unregister_position_callback(callback)

    async def __aenter__(self) -> "OMSService":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
