"""In-memory implementation of account repository."""

import asyncio

from calais_order_execution.models.portfolio import AccountSummary
from calais_order_execution.repository.account_base import AccountRepository


class InMemoryAccountRepository(AccountRepository):
    """In-memory account summary storage using dictionaries."""

    def __init__(self):
        self._accounts: dict[tuple[str, str], AccountSummary] = {}  # (exchange, currency)
        self._lock = asyncio.Lock()

    async def save(self, summary: AccountSummary) -> None:
        async with self._lock:
            self._accounts[(summary.exchange, summary.currency)] = summary

    async def get(self, exchange: str, currency: str) -> AccountSummary | None:
        return self._accounts.get((exchange, currency))

    async def get_by_exchange(self, exchange: str) -> list[AccountSummary]:
        return [s for (ex, _), s in self._accounts.items() if ex == exchange]

    async def get_all(self) -> list[AccountSummary]:
        return list(self._accounts.values())

    async def delete(self, exchange: str, currency: str) -> bool:
        async with self._lock:
            key = (exchange, currency)
            if key in self._accounts:
                del self._accounts[key]
                return True
            return False

    async def clear(self) -> None:
        """Clear all accounts (useful for testing)."""
        async with self._lock:
            self._accounts.clear()
