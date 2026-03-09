"""In-memory implementation of order repository."""

import asyncio
from typing import Optional

from calais_order_execution.models import Order
from calais_order_execution.repository.order_base import OrderRepository


class InMemoryOrderRepository(OrderRepository):
    """In-memory order storage using dictionaries.

    Thread-safe implementation using asyncio locks.
    """

    def __init__(self):
        self._orders: dict[str, Order] = {}
        self._client_order_id_index: dict[str, str] = {}  # client_order_id -> order_id
        self._strategy_id_index: dict[str, set[str]] = {}  # strategy_id -> set of order_ids
        self._lock = asyncio.Lock()

    async def save(self, order: Order) -> None:
        """Save a new order."""
        async with self._lock:
            self._orders[order.order_id] = order
            if order.client_order_id:
                self._client_order_id_index[order.client_order_id] = order.order_id
            if order.strategy_id:
                self._strategy_id_index.setdefault(order.strategy_id, set()).add(order.order_id)

    async def get(self, order_id: str) -> Order | None:
        """Get an order by ID."""
        return self._orders.get(order_id)

    async def get_by_client_order_id(self, client_order_id: str) -> Order | None:
        """Get an order by client order ID."""
        order_id = self._client_order_id_index.get(client_order_id)
        if order_id:
            return self._orders.get(order_id)
        return None

    async def get_by_strategy_id(self, strategy_id: str) -> list[Order]:
        """Get all orders for a strategy."""
        order_ids = self._strategy_id_index.get(strategy_id, set())
        return [self._orders[oid] for oid in order_ids if oid in self._orders]

    async def get_all(self) -> list[Order]:
        """Get all orders."""
        return list(self._orders.values())

    async def get_active_orders(self) -> list[Order]:
        """Get all active orders."""
        return [order for order in self._orders.values() if order.is_active]

    async def update(self, order: Order) -> None:
        """Update an existing order."""
        async with self._lock:
            if order.order_id in self._orders:
                self._orders[order.order_id] = order

    async def delete(self, order_id: str) -> bool:
        """Delete an order."""
        async with self._lock:
            if order_id in self._orders:
                order = self._orders.pop(order_id)
                if order.client_order_id:
                    self._client_order_id_index.pop(order.client_order_id, None)
                if order.strategy_id and order.strategy_id in self._strategy_id_index:
                    self._strategy_id_index[order.strategy_id].discard(order_id)
                return True
            return False

    async def clear(self) -> None:
        """Clear all orders (useful for testing)."""
        async with self._lock:
            self._orders.clear()
            self._client_order_id_index.clear()
            self._strategy_id_index.clear()
