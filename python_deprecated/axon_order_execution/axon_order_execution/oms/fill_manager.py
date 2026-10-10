"""Fill state management."""

import asyncio
from datetime import datetime
from typing import Awaitable, Callable

from axon_order_execution.models import Fill
from axon_order_execution.repository import FillRepository, InMemoryFillRepository
from axon_order_execution.util.logging import get_logger
from axon_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class FillManager:
    """Persists fills idempotently and notifies subscribers.

    `add_fill` returns True only when the fill is new (i.e. its `trade_id`
    has not been seen before). This makes WS + REST reconciliation safe:
    duplicates from the reconciler do not re-fire callbacks.
    """

    def __init__(self, repository: FillRepository | None = None):
        self._repository = repository or InMemoryFillRepository()
        self._update_callbacks: list[Callable[[Fill], None]] = []
        self._async_update_callbacks: list[Callable[[Fill], Awaitable[None]]] = []
        self._lock = asyncio.Lock()

    async def add_fill(self, fill: Fill) -> bool:
        """Persist a fill and fire callbacks.

        Returns:
            True if the fill is new and was inserted; False if it was a
            duplicate (already seen `trade_id`).
        """
        async with self._lock:
            try:
                inserted = await self._repository.save(fill)
            except Exception:
                logger.exception(f"Failed to persist fill {fill.trade_id}")
                get_metrics().inc_db_write_failure("fills")
                return False

        if not inserted:
            logger.debug(f"Duplicate fill ignored: {fill.trade_id}")
            return False

        logger.info(
            f"Fill {fill.trade_id} order={fill.order_id} {fill.instrument} "
            f"{fill.side.value} amount={fill.amount} price={fill.price} "
            f"liquidity={fill.liquidity.value} fee={fill.fee} {fill.fee_currency}"
        )
        await self._notify_update(fill)
        return True

    async def get(self, trade_id: str) -> Fill | None:
        return await self._repository.get(trade_id)

    async def get_by_order_id(self, order_id: str) -> list[Fill]:
        return await self._repository.get_by_order_id(order_id)

    async def get_by_strategy_id(self, strategy_id: str) -> list[Fill]:
        return await self._repository.get_by_strategy_id(strategy_id)

    async def get_since(
        self,
        since: datetime,
        exchange: str | None = None,
    ) -> list[Fill]:
        return await self._repository.get_since(since, exchange=exchange)

    async def get_all(self) -> list[Fill]:
        return await self._repository.get_all()

    def register_update_callback(self, callback: Callable[[Fill], None]) -> None:
        self._update_callbacks.append(callback)

    def unregister_update_callback(self, callback: Callable[[Fill], None]) -> None:
        if callback in self._update_callbacks:
            self._update_callbacks.remove(callback)

    def register_async_update_callback(
        self, callback: Callable[[Fill], Awaitable[None]]
    ) -> None:
        self._async_update_callbacks.append(callback)

    def unregister_async_update_callback(
        self, callback: Callable[[Fill], Awaitable[None]]
    ) -> None:
        if callback in self._async_update_callbacks:
            self._async_update_callbacks.remove(callback)

    async def _notify_update(self, fill: Fill) -> None:
        for callback in self._update_callbacks:
            try:
                callback(fill)
            except Exception as e:
                logger.error(f"Fill update callback error: {e}")
        for callback in self._async_update_callbacks:
            try:
                await callback(fill)
            except Exception as e:
                logger.error(f"Async fill update callback error: {e}")
