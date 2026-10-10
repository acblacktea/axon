"""Abstract repository interface for order storage."""

from abc import ABC, abstractmethod

from axon_order_execution.models import Order


class OrderRepository(ABC):
    """Abstract base class for order storage.

    Implement this interface to add database support.
    """

    @abstractmethod
    async def save(self, order: Order) -> None:
        """Save a new order.

        Args:
            order: Order to save.
        """
        ...

    @abstractmethod
    async def get(self, order_id: str) -> Order | None:
        """Get an order by ID.

        Args:
            order_id: Order ID to look up.

        Returns:
            Order if found, None otherwise.
        """
        ...

    @abstractmethod
    async def get_by_client_order_id(self, client_order_id: str) -> Order | None:
        """Get an order by client order ID.

        Args:
            client_order_id: Client order ID to look up.

        Returns:
            Order if found, None otherwise.
        """
        ...

    @abstractmethod
    async def get_by_strategy_id(self, strategy_id: str) -> list[Order]:
        """Get all orders for a strategy.

        Args:
            strategy_id: Strategy ID to filter by.

        Returns:
            List of orders for the strategy.
        """
        ...

    @abstractmethod
    async def get_all(self) -> list[Order]:
        """Get all orders.

        Returns:
            List of all orders.
        """
        ...

    @abstractmethod
    async def get_active_orders(self) -> list[Order]:
        """Get all active (open) orders.

        Returns:
            List of active orders.
        """
        ...

    @abstractmethod
    async def update(self, order: Order) -> None:
        """Update an existing order.

        Args:
            order: Order with updated fields.
        """
        ...

    @abstractmethod
    async def delete(self, order_id: str) -> bool:
        """Delete an order.

        Args:
            order_id: Order ID to delete.

        Returns:
            True if deleted, False if not found.
        """
        ...
