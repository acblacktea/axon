"""Order state management."""

import asyncio
import time
from typing import Callable

from calais_order_execution.models import Order, OrderStatus
from calais_order_execution.repository import InMemoryOrderRepository, OrderRepository
from calais_order_execution.util.logging import get_logger
from calais_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)

_TERMINAL_STATUSES = {OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED}


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
        # Side state for latency metrics. Keyed by order_id.
        self._submit_monotonic: dict[str, float] = {}
        self._first_ws_seen: set[str] = set()

    async def add_order(self, order: Order) -> None:
        """Add a new order.

        Args:
            order: Order to add.
        """
        async with self._lock:
            self._order_cache[order.order_id] = order
            self._submit_monotonic[order.order_id] = time.monotonic()
            try:
                await self._repository.save(order)
            except Exception:
                logger.exception(f"Failed to persist new order {order.order_id} to DB")
                get_metrics().inc_db_write_failure("orders")
            logger.info(f"Added order {order.order_id}: {order.instrument} {order.side.value} {order.amount}")

        # If the exchange already returned a terminal state synchronously
        # (e.g. immediate-or-cancel filled/rejected), record fill latency now.
        if order.status in _TERMINAL_STATUSES:
            self._record_terminal_latency(order)

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

            # Preserve fields from existing order if incoming doesn't have them
            if existing:
                if not order.strategy_id and existing.strategy_id:
                    order.strategy_id = existing.strategy_id
                if not order.internal_order_id and existing.internal_order_id:
                    order.internal_order_id = existing.internal_order_id

            self._order_cache[order.order_id] = order
            try:
                await self._repository.update(order)
            except Exception:
                logger.exception(f"Failed to persist order {order.order_id} to DB")
                get_metrics().inc_db_write_failure("orders")
            logger.info(
                f"Updated order {order.order_id}: status={order.status.value}, "
                f"filled={order.filled_amount}/{order.amount}"
            )

        if order.status in _TERMINAL_STATUSES:
            self._record_terminal_latency(order)
        if order.status == OrderStatus.REJECTED:
            get_metrics().inc_order_rejected(order.exchange, reason="exchange")

        await self._notify_update(order)

    async def update_from_ws(self, order: Order) -> None:
        """Update order from WebSocket message.

        Same as update_order but used to distinguish source.

        Args:
            order: Order data from WebSocket.
        """
        # First WS update for this order — the gap from add_order is the
        # ack latency, i.e. how long it took the user.orders subscription
        # to confirm the order we just placed via REST.
        if order.order_id not in self._first_ws_seen:
            self._first_ws_seen.add(order.order_id)
            submit_t = self._submit_monotonic.get(order.order_id)
            if submit_t is not None:
                get_metrics().observe_order_ack_latency(
                    order.exchange, time.monotonic() - submit_t
                )

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

    def _record_terminal_latency(self, order: Order) -> None:
        """Emit fill latency for an order's first transition to a terminal state."""
        submit_t = self._submit_monotonic.pop(order.order_id, None)
        # Drop ack-state side info too — order is done.
        self._first_ws_seen.discard(order.order_id)
        if submit_t is None:
            return
        get_metrics().observe_order_fill_latency(
            order.exchange,
            terminal_status=order.status.value,
            seconds=time.monotonic() - submit_t,
        )

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
