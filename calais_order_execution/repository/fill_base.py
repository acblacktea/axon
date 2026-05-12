"""Abstract repository interface for fill (trade execution) storage."""

from abc import ABC, abstractmethod
from datetime import datetime

from calais_order_execution.models import Fill


class FillRepository(ABC):
    """Abstract base class for fill storage.

    Fills are append-only and idempotent on `trade_id`.
    """

    @abstractmethod
    async def save(self, fill: Fill) -> bool:
        """Persist a fill. Idempotent on trade_id.

        Returns:
            True if a new fill was inserted, False if it was a duplicate.
        """
        ...

    @abstractmethod
    async def get(self, trade_id: str) -> Fill | None:
        """Get a fill by trade_id."""
        ...

    @abstractmethod
    async def get_by_order_id(self, order_id: str) -> list[Fill]:
        """Get all fills for an order, ordered by timestamp ascending."""
        ...

    @abstractmethod
    async def get_by_strategy_id(self, strategy_id: str) -> list[Fill]:
        """Get all fills for a strategy, ordered by timestamp ascending."""
        ...

    @abstractmethod
    async def get_since(
        self,
        since: datetime,
        exchange: str | None = None,
    ) -> list[Fill]:
        """Get fills with timestamp >= `since`, ordered by timestamp ascending.

        Used by reconciler to detect missed WS messages.
        """
        ...

    @abstractmethod
    async def get_all(self) -> list[Fill]:
        """Get all fills (ordered by timestamp ascending)."""
        ...
