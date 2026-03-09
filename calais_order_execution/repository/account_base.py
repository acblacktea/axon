"""Abstract repository interface for account summary storage."""

from abc import ABC, abstractmethod

from calais_order_execution.models.portfolio import AccountSummary


class AccountRepository(ABC):
    """Abstract base class for account summary storage."""

    @abstractmethod
    async def save(self, summary: AccountSummary) -> None:
        """Save or update an account summary (upsert by exchange+currency)."""
        ...

    @abstractmethod
    async def get(self, exchange: str, currency: str) -> AccountSummary | None:
        """Get account summary by exchange and currency."""
        ...

    @abstractmethod
    async def get_by_exchange(self, exchange: str) -> list[AccountSummary]:
        """Get all account summaries for an exchange."""
        ...

    @abstractmethod
    async def get_all(self) -> list[AccountSummary]:
        """Get all account summaries."""
        ...

    @abstractmethod
    async def delete(self, exchange: str, currency: str) -> bool:
        """Delete an account summary."""
        ...
