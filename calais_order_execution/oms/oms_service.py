"""OMS Service - manages order state, WebSocket connections, and reconciliation."""

from typing import Callable

from calais_order_execution.config import Config
from calais_order_execution.ems.ems_service import EMSService
from calais_order_execution.models import Order
from calais_order_execution.oms.deribit import DeribitOMS
from calais_order_execution.oms.order_manager import OrderManager
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
        self._order_manager = OrderManager(self._repository)

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

    async def stop(self) -> None:
        """Stop all OMS components."""
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

    async def __aenter__(self) -> "OMSService":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
