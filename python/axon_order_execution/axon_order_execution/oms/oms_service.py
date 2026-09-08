"""OMS Service - manages order state, WebSocket connections, and reconciliation."""

from typing import Callable

from axon_order_execution.config import Config
from axon_order_execution.ems.ems_service import EMSService
from axon_order_execution.models import Fill, Order
from axon_order_execution.models.portfolio import AccountSummary, Position
from axon_order_execution.oms.binance import BinanceOMS
from axon_order_execution.oms.bybit import BybitOMS
from axon_order_execution.oms.deribit import DeribitOMS
from axon_order_execution.oms.fill_manager import FillManager
from axon_order_execution.oms.fill_reconciler import FillReconciler
from axon_order_execution.oms.okx import OkxOMS
from axon_order_execution.oms.order_manager import OrderManager
from axon_order_execution.oms.portfolio_manager import PortfolioManager
from axon_order_execution.oms.position_refresher import PositionRefresher
from axon_order_execution.oms.order_reconciler import OrderReconciler
from axon_order_execution.repository import (
    FillRepository,
    InMemoryOrderRepository,
    OrderRepository,
)
from axon_order_execution.repository.account_base import AccountRepository
from axon_order_execution.repository.position_base import PositionRepository
from axon_order_execution.util import WebSocketBase
from axon_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class OMSService:
    """Manages OMS WebSocket clients, reconcilers, and order state."""

    def __init__(
        self,
        config: Config,
        ems_service: EMSService,
        repository: OrderRepository | None = None,
        account_repository: AccountRepository | None = None,
        position_repository: PositionRepository | None = None,
        fill_repository: FillRepository | None = None,
    ):
        """Initialize OMS service.

        Args:
            config: Service configuration.
            ems_service: EMS service for reconciliation.
            repository: Order repository. Uses InMemoryOrderRepository if not provided.
            account_repository: Account repository. Uses InMemoryAccountRepository if not provided.
            position_repository: Position repository. Uses InMemoryPositionRepository if not provided.
            fill_repository: Fill repository. Uses InMemoryFillRepository if not provided.
        """
        self._config = config
        self._ems_service = ems_service
        self._repository = repository or InMemoryOrderRepository()

        self._oms_ws: dict[str, WebSocketBase] = {}
        self._order_reconcilers: dict[str, OrderReconciler] = {}
        self._position_refreshers: dict[str, PositionRefresher] = {}
        self._fill_reconcilers: dict[str, FillReconciler] = {}
        self._order_manager = OrderManager(self._repository)
        self._portfolio_manager = PortfolioManager(
            account_repository=account_repository,
            position_repository=position_repository,
        )
        self._fill_manager = FillManager(fill_repository)

        self._init_clients()

    def _create_ws(self, name: str, exchange_config) -> WebSocketBase | None:
        """Create an OMS WebSocket client for the given exchange."""
        common_kwargs = dict(
            order_manager=self._order_manager,
            exchange_config=exchange_config,
            ws_config=self._config.websocket,
            portfolio_manager=self._portfolio_manager,
            portfolio_config=self._config.portfolio,
            fill_manager=self._fill_manager,
        )
        if name == "deribit":
            return DeribitOMS(**common_kwargs)
        elif name == "bybit":
            return BybitOMS(**common_kwargs)
        elif name == "okx":
            return OkxOMS(**common_kwargs)
        elif name == "binance":
            ems = self._ems_service.get(name)
            return BinanceOMS(**common_kwargs, ems=ems)
        return None

    def _init_clients(self) -> None:
        """Initialize OMS WebSocket clients and reconcilers."""
        for name, exchange_config in self._config.exchanges.items():
            ws = self._create_ws(name, exchange_config)
            if ws is None:
                logger.warning(f"Unsupported exchange for OMS: {name}")
                continue

            self._oms_ws[name] = ws

            ems = self._ems_service.get(name)
            if ems:
                self._order_reconcilers[name] = OrderReconciler(
                    ems,
                    self._order_manager,
                    self._config.reconciliation,
                )
                self._position_refreshers[name] = PositionRefresher(
                    ems,
                    self._portfolio_manager,
                    self._config.portfolio,
                )
                self._fill_reconcilers[name] = FillReconciler(
                    ems,
                    self._fill_manager,
                    self._config.portfolio,
                    self._config.fill_reconciliation,
                )

    async def start(self) -> None:
        """Start all OMS components."""
        # Connect WebSocket clients
        for name, ws in self._oms_ws.items():
            await ws.connect()
            logger.info(f"Connected OMS WebSocket: {name}")

        # Start reconcilers
        for name, reconciler in self._order_reconcilers.items():
            await reconciler.start()
            logger.info(f"Started reconciler: {name}")

        # Start position refreshers
        for name, refresher in self._position_refreshers.items():
            await refresher.start()
            logger.info(f"Started position refresher: {name}")

        # Start fill reconcilers
        for name, fill_reconciler in self._fill_reconcilers.items():
            await fill_reconciler.start()
            logger.info(f"Started fill reconciler: {name}")

    async def stop(self) -> None:
        """Stop all OMS components."""
        # Stop fill reconcilers
        for name, fill_reconciler in self._fill_reconcilers.items():
            await fill_reconciler.stop()
            logger.info(f"Stopped fill reconciler: {name}")

        # Stop position refreshers
        for name, refresher in self._position_refreshers.items():
            await refresher.stop()
            logger.info(f"Stopped position refresher: {name}")

        # Stop reconcilers
        for name, reconciler in self._order_reconcilers.items():
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

    def get_account(self, exchange: str, currency: str) -> AccountSummary | None:
        """Get account summary by exchange and currency from cache."""
        return self._portfolio_manager.get_account(exchange, currency)

    def get_all_positions(self) -> list[Position]:
        """Get all positions from cache."""
        return self._portfolio_manager.get_all_positions()

    def get_positions_by_exchange(self, exchange: str) -> list[Position]:
        """Get all positions for an exchange from cache."""
        return self._portfolio_manager.get_positions_by_exchange(exchange)

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

    # ============= Fills =============

    @property
    def fill_manager(self) -> FillManager:
        return self._fill_manager

    async def get_fill(self, trade_id: str) -> Fill | None:
        return await self._fill_manager.get(trade_id)

    async def get_fills_by_order(self, order_id: str) -> list[Fill]:
        return await self._fill_manager.get_by_order_id(order_id)

    async def get_fills_by_strategy(self, strategy_id: str) -> list[Fill]:
        return await self._fill_manager.get_by_strategy_id(strategy_id)

    async def get_all_fills(self) -> list[Fill]:
        return await self._fill_manager.get_all()

    def register_fill_update_callback(self, callback: Callable[[Fill], None]) -> None:
        self._fill_manager.register_update_callback(callback)

    def unregister_fill_update_callback(self, callback: Callable[[Fill], None]) -> None:
        self._fill_manager.unregister_update_callback(callback)

    async def __aenter__(self) -> "OMSService":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
