"""Portfolio state management - account summaries and positions."""

import asyncio
from typing import Callable

from calais_order_execution.models.portfolio import AccountSummary, Position
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class PortfolioManager:
    """Manages account summary and position state, notifies subscribers.

    Follows the same callback pattern as OrderManager.
    """

    def __init__(self):
        self._account_cache: dict[str, AccountSummary] = {}  # key = currency
        self._position_cache: dict[str, Position] = {}  # key = instrument
        self._account_callbacks: list[Callable[[AccountSummary], None]] = []
        self._position_callbacks: list[Callable[[list[Position]], None]] = []
        self._lock = asyncio.Lock()

    # ============= Account Summary =============

    async def update_account(self, summary: AccountSummary) -> None:
        """Update account summary and notify callbacks."""
        async with self._lock:
            self._account_cache[summary.currency] = summary
            logger.debug(
                f"Account update [{summary.currency}]: "
                f"equity={summary.equity}, balance={summary.balance}, "
                f"available={summary.available_funds}"
            )

        for callback in self._account_callbacks:
            try:
                callback(summary)
            except Exception as e:
                logger.error(f"Account update callback error: {e}")

    def get_account(self, currency: str) -> AccountSummary | None:
        """Get account summary by currency."""
        return self._account_cache.get(currency)

    def get_all_accounts(self) -> list[AccountSummary]:
        """Get all account summaries."""
        return list(self._account_cache.values())

    def register_account_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Register a callback for account summary updates."""
        self._account_callbacks.append(callback)

    def unregister_account_callback(self, callback: Callable[[AccountSummary], None]) -> None:
        """Unregister an account summary callback."""
        if callback in self._account_callbacks:
            self._account_callbacks.remove(callback)

    # ============= Positions =============

    async def update_positions(self, positions: list[Position]) -> None:
        """Bulk replace position cache and notify callbacks."""
        async with self._lock:
            self._position_cache.clear()
            for pos in positions:
                self._position_cache[pos.instrument] = pos
            logger.debug(f"Positions updated: {len(positions)} instruments")

        for callback in self._position_callbacks:
            try:
                callback(positions)
            except Exception as e:
                logger.error(f"Position update callback error: {e}")

    def get_position(self, instrument: str) -> Position | None:
        """Get position by instrument."""
        return self._position_cache.get(instrument)

    def get_all_positions(self) -> list[Position]:
        """Get all positions."""
        return list(self._position_cache.values())

    def register_position_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Register a callback for position updates."""
        self._position_callbacks.append(callback)

    def unregister_position_callback(self, callback: Callable[[list[Position]], None]) -> None:
        """Unregister a position callback."""
        if callback in self._position_callbacks:
            self._position_callbacks.remove(callback)
