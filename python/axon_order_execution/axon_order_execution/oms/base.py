"""Abstract base class for Order Management System WebSocket."""

from abc import abstractmethod

from axon_order_execution.oms.order_manager import OrderManager
from axon_order_execution.util import WebSocketBase


class BaseOMS(WebSocketBase):
    """Abstract base class for exchange OMS WebSocket implementations.

    Provides common functionality for:
    - Receiving order/trade updates via WebSocket
    - Delegating order state management to OrderManager
    - Subscription management

    Subclasses should implement exchange-specific logic.
    """

    def __init__(self, order_manager: OrderManager, *args, **kwargs):
        """Initialize BaseOMS.

        Args:
            order_manager: OrderManager instance for order state management.
        """
        super().__init__(*args, **kwargs)
        self._order_manager = order_manager
        self._subscribed_channels: set[str] = set()

    @property
    @abstractmethod
    def exchange_name(self) -> str:
        """Get the exchange name."""
        ...

    @abstractmethod
    async def subscribe_orders(self, instrument: str | None = None) -> None:
        """Subscribe to order updates.

        Args:
            instrument: Specific instrument to subscribe to.
                       If None, subscribes to all orders.
        """
        ...

    @abstractmethod
    async def unsubscribe_orders(self, instrument: str | None = None) -> None:
        """Unsubscribe from order updates.

        Args:
            instrument: Specific instrument to unsubscribe from.
        """
        ...

    @abstractmethod
    async def subscribe_trades(self) -> None:
        """Subscribe to user trades."""
        ...
