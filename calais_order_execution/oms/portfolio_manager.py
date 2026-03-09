"""Portfolio state management - account summaries and positions."""

import asyncio
from typing import Callable

from calais_order_execution.models.portfolio import AccountSummary, Position
from calais_order_execution.repository.account_base import AccountRepository
from calais_order_execution.repository.account_memory import InMemoryAccountRepository
from calais_order_execution.repository.position_base import PositionRepository
from calais_order_execution.repository.position_memory import InMemoryPositionRepository
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class PortfolioManager:
    """Manages account summary and position state, notifies subscribers.

    Maintains in-memory caches for fast synchronous reads.
    Delegates persistence to AccountRepository and PositionRepository.
    """

    def __init__(
        self,
        account_repository: AccountRepository | None = None,
        position_repository: PositionRepository | None = None,
    ):
        self._account_repo = account_repository or InMemoryAccountRepository()
        self._position_repo = position_repository or InMemoryPositionRepository()
        self._account_cache: dict[tuple[str, str], AccountSummary] = {}  # (exchange, currency)
        self._position_cache: dict[tuple[str, str], Position] = {}  # (exchange, instrument)
        self._account_callbacks: list[Callable[[AccountSummary], None]] = []
        self._position_callbacks: list[Callable[[list[Position]], None]] = []
        self._lock = asyncio.Lock()

    # ============= Account Summary =============

    async def update_account(self, summary: AccountSummary) -> None:
        """Update account summary and notify callbacks."""
        async with self._lock:
            self._account_cache[(summary.exchange, summary.currency)] = summary
            await self._account_repo.save(summary)
            logger.debug(
                f"Account update [{summary.exchange}/{summary.currency}]: "
                f"equity={summary.equity}, balance={summary.balance}, "
                f"available={summary.available_funds}"
            )

        for callback in self._account_callbacks:
            try:
                callback(summary)
            except Exception as e:
                logger.error(f"Account update callback error: {e}")

    def get_account(self, exchange: str, currency: str) -> AccountSummary | None:
        """Get account summary by exchange and currency."""
        return self._account_cache.get((exchange, currency))

    def get_all_accounts(self) -> list[AccountSummary]:
        """Get all account summaries."""
        return list(self._account_cache.values())

    def get_accounts_by_exchange(self, exchange: str) -> list[AccountSummary]:
        """Get all account summaries for an exchange."""
        return [s for (ex, _), s in self._account_cache.items() if ex == exchange]

    def register_account_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Register a callback for account summary updates."""
        self._account_callbacks.append(callback)

    def unregister_account_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Unregister an account summary callback."""
        if callback in self._account_callbacks:
            self._account_callbacks.remove(callback)

    # ============= Positions =============

    async def update_positions(self, exchange: str, positions: list[Position]) -> None:
        """Bulk replace positions for an exchange and notify callbacks."""
        async with self._lock:
            # Clear cache entries for this exchange
            keys_to_remove = [k for k in self._position_cache if k[0] == exchange]
            for k in keys_to_remove:
                del self._position_cache[k]
            # Add new positions to cache
            for pos in positions:
                self._position_cache[(pos.exchange, pos.instrument)] = pos
            # Persist
            await self._position_repo.replace_all(exchange, positions)
            logger.debug(f"Positions updated [{exchange}]: {len(positions)} instruments")

        for callback in self._position_callbacks:
            try:
                callback(positions)
            except Exception as e:
                logger.error(f"Position update callback error: {e}")

    def get_position(self, exchange: str, instrument: str) -> Position | None:
        """Get position by exchange and instrument."""
        return self._position_cache.get((exchange, instrument))

    def get_all_positions(self) -> list[Position]:
        """Get all positions."""
        return list(self._position_cache.values())

    def get_positions_by_exchange(self, exchange: str) -> list[Position]:
        """Get all positions for an exchange."""
        return [p for (ex, _), p in self._position_cache.items() if ex == exchange]

    def register_position_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Register a callback for position updates."""
        self._position_callbacks.append(callback)

    def unregister_position_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Unregister a position callback."""
        if callback in self._position_callbacks:
            self._position_callbacks.remove(callback)
