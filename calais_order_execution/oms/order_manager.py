"""Order state management."""

import asyncio
from typing import Callable

from calais_order_execution.models import Order
from calais_order_execution.repository import InMemoryOrderRepository, OrderRepository
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class OrderManager:
    """Manages order state and notifies subscribers of updates.

    Maintains an in-memory cache of orders and persists to repository.
    Provides callbacks for order state changes.
    """

    def __init__(self, repository: OrderRepository | None = None):
        """Initialize OrderManager.

        Args:
            repository: Order repository for persistence.
                       Uses InMemoryOrderRepository if not provided.
        """
        self._repository = repository or InMemoryOrderRepository()
        self._order_cache: dict[str, Order] = {}
        self._update_callbacks: list[Callable[[Order], None]] = []
        self._async_update_callbacks: list[Callable[[Order], asyncio.Future]] = []
        self._lock = asyncio.Lock()

    async def add_order(self, order: Order) -> None:
        """Add a new order.

        Args:
            order: Order to add.
        """
        async with self._lock:
            self._order_cache[order.order_id] = order
            await self._repository.save(order)
            logger.info(f"Added order {order.order_id}: {order.instrument} {order.side.value} {order.amount}")

        await self._notify_update(order)

    async def update_order(self, order: Order) -> None:
        """Update an existing order.

        Also triggers update callbacks. Uses updated_at timestamp to prevent
        stale data from overwriting newer updates (e.g., REST response arriving
        after a more recent WebSocket update).

        Args:
            order: Order with updated state.
        """
        async with self._lock:
            existing = self._order_cache.get(order.order_id)
            if existing and existing.updated_at > order.updated_at:
                logger.warning(
                    f"Skipping stale update for order {order.order_id}: "
                    f"existing={existing.updated_at}, incoming={order.updated_at}"
                )
                return

            self._order_cache[order.order_id] = order
            await self._repository.update(order)
            logger.info(
                f"Updated order {order.order_id}: status={order.status.value}, "
                f"filled={order.filled_amount}/{order.amount}"
            )

        await self._notify_update(order)

    async def update_from_ws(self, order: Order) -> None:
        """Update order from WebSocket message.

        Same as update_order but used to distinguish source.

        Args:
            order: Order data from WebSocket.
        """
        await self.update_order(order)

    async def get_order(self, order_id: str) -> Order | None:
        """Get order by ID.

        Args:
            order_id: Order ID to look up.

        Returns:
            Order if found, None otherwise.
        """
        # Check cache first
        if order_id in self._order_cache:
            return self._order_cache[order_id]

        # Fall back to repository
        order = await self._repository.get(order_id)
        if order:
            self._order_cache[order_id] = order
        return order

    async def get_all_orders(self) -> list[Order]:
        """Get all orders."""
        return list(self._order_cache.values())

    async def get_active_orders(self) -> list[Order]:
        """Get all active (non-terminal) orders."""
        return [order for order in self._order_cache.values() if order.is_active]

    async def reconcile(self, orders: list[Order]) -> None:
        """Reconcile local state with orders from exchange.

        Updates local cache with exchange data, overwriting any discrepancies.

        Args:
            orders: List of orders from exchange (typically from REST API).
        """
        logger.info(f"Reconciling {len(orders)} orders from exchange")

        for order in orders:
            existing = self._order_cache.get(order.order_id)
            if existing:
                # Check for differences
                if (
                    existing.status != order.status
                    or existing.filled_amount != order.filled_amount
                ):
                    logger.warning(
                        f"Reconciliation fix for {order.order_id}: "
                        f"status {existing.status.value} -> {order.status.value}, "
                        f"filled {existing.filled_amount} -> {order.filled_amount}"
                    )
            await self.update_order(order)

    def register_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Register a synchronous callback for order updates.

        Args:
            callback: Function to call when order is updated.
        """
        self._update_callbacks.append(callback)

    def unregister_update_callback(self, callback: Callable[[Order], None]) -> None:
        """Unregister an order update callback."""
        if callback in self._update_callbacks:
            self._update_callbacks.remove(callback)

    def register_async_update_callback(
        self, callback: Callable[[Order], asyncio.Future]
    ) -> None:
        """Register an async callback for order updates.

        Args:
            callback: Async function to call when order is updated.
        """
        self._async_update_callbacks.append(callback)

    def unregister_async_update_callback(
        self, callback: Callable[[Order], asyncio.Future]
    ) -> None:
        """Unregister an async order update callback."""
        if callback in self._async_update_callbacks:
            self._async_update_callbacks.remove(callback)

    async def _notify_update(self, order: Order) -> None:
        """Notify all registered callbacks of order update.

        Args:
            order: Updated order.
        """
        # Sync callbacks
        for callback in self._update_callbacks:
            try:
                callback(order)
            except Exception as e:
                logger.error(f"Order update callback error: {e}")

        # Async callbacks
        for callback in self._async_update_callbacks:
            try:
                await callback(order)
            except Exception as e:
                logger.error(f"Async order update callback error: {e}")
