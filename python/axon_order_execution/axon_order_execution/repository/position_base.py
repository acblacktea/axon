"""Abstract repository interface for position storage."""

from abc import ABC, abstractmethod

from axon_order_execution.models.portfolio import Position


class PositionRepository(ABC):
    """Abstract base class for position storage."""

    @abstractmethod
    async def save(self, position: Position) -> None:
        """Save or update a position (upsert by exchange+instrument)."""
        ...

    @abstractmethod
    async def get(self, exchange: str, instrument: str) -> Position | None:
        """Get position by exchange and instrument."""
        ...

    @abstractmethod
    async def get_by_exchange(self, exchange: str) -> list[Position]:
        """Get all positions for an exchange."""
        ...

    @abstractmethod
    async def get_by_kind(self, exchange: str, kind: str) -> list[Position]:
        """Get positions by exchange and kind (e.g., 'option', 'future')."""
        ...

    @abstractmethod
    async def get_all(self) -> list[Position]:
        """Get all positions."""
        ...

    @abstractmethod
    async def delete(self, exchange: str, instrument: str) -> bool:
        """Delete a position."""
        ...

    @abstractmethod
    async def delete_by_exchange(self, exchange: str) -> int:
        """Delete all positions for an exchange. Returns count deleted."""
        ...

    @abstractmethod
    async def replace_all(self, exchange: str, positions: list[Position]) -> None:
        """Atomically replace all positions for an exchange."""
        ...
