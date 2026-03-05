"""Abstract base class for Execution Management System."""

from abc import ABC, abstractmethod

from calais_order_execution.models import Order, OrderRequest
from calais_order_execution.models.portfolio import AccountSummary, Position


class BaseEMS(ABC):
    """Abstract base class for exchange execution.

    Implement this interface to add support for new exchanges.
    """

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """Get the exchange name."""
        ...

    @abstractmethod
    async def start(self) -> None:
        """Start the EMS client."""
        ...

    @abstractmethod
    async def stop(self) -> None:
        """Stop the EMS client and cleanup resources."""
        ...

    @abstractmethod
    async def place_order(self, request: OrderRequest) -> Order:
        """Place a new order.

        Args:
            request: Order request with instrument, side, amount, etc.

        Returns:
            Created order with order_id and initial status.

        Raises:
            Exception: If order placement fails.
        """
        ...

    @abstractmethod
    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an existing order.

        Args:
            order_id: ID of the order to cancel.

        Returns:
            True if cancellation was successful.

        Raises:
            Exception: If cancellation fails.
        """
        ...

    @abstractmethod
    async def get_order(self, order_id: str) -> Order | None:
        """Get order by ID.

        Args:
            order_id: Order ID to look up.

        Returns:
            Order if found, None otherwise.
        """
        ...

    @abstractmethod
    async def get_open_orders(self, instrument: str | None = None) -> list[Order]:
        """Get all open orders.

        Used for reconciliation.

        Args:
            instrument: Optional instrument filter.

        Returns:
            List of open orders.
        """
        ...

    @abstractmethod
    async def modify_order(
        self,
        order_id: str,
        amount: float | None = None,
        price: float | None = None,
    ) -> Order:
        """Modify an existing order.

        Args:
            order_id: ID of the order to modify.
            amount: New amount (optional).
            price: New price (optional).

        Returns:
            Updated order.

        Raises:
            Exception: If modification fails.
        """
        ...

    async def get_account_summary(self, currency: str = "BTC") -> AccountSummary:
        """Get account summary for a currency. Override in subclass."""
        raise NotImplementedError

    async def get_positions(self, currency: str = "BTC", kind: str = "option") -> list[Position]:
        """Get positions for a currency and kind. Override in subclass."""
        raise NotImplementedError

    async def __aenter__(self) -> "BaseEMS":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
