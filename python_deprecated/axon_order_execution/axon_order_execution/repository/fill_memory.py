"""In-memory implementation of fill repository."""

import asyncio
from datetime import datetime

from axon_order_execution.models import Fill
from axon_order_execution.repository.fill_base import FillRepository


class InMemoryFillRepository(FillRepository):
    """In-memory fill storage using dictionaries.

    Idempotent on trade_id; duplicate saves are no-ops.
    """

    def __init__(self):
        self._fills: dict[str, Fill] = {}
        self._order_index: dict[str, set[str]] = {}     # order_id -> set of trade_ids
        self._strategy_index: dict[str, set[str]] = {}  # strategy_id -> set of trade_ids
        self._lock = asyncio.Lock()

    async def save(self, fill: Fill) -> bool:
        async with self._lock:
            if fill.trade_id in self._fills:
                return False
            self._fills[fill.trade_id] = fill
            self._order_index.setdefault(fill.order_id, set()).add(fill.trade_id)
            if fill.strategy_id:
                self._strategy_index.setdefault(fill.strategy_id, set()).add(fill.trade_id)
            return True

    async def get(self, trade_id: str) -> Fill | None:
        return self._fills.get(trade_id)

    async def get_by_order_id(self, order_id: str) -> list[Fill]:
        ids = self._order_index.get(order_id, set())
        fills = [self._fills[tid] for tid in ids if tid in self._fills]
        return sorted(fills, key=lambda f: f.timestamp)

    async def get_by_strategy_id(self, strategy_id: str) -> list[Fill]:
        ids = self._strategy_index.get(strategy_id, set())
        fills = [self._fills[tid] for tid in ids if tid in self._fills]
        return sorted(fills, key=lambda f: f.timestamp)

    async def get_since(
        self,
        since: datetime,
        exchange: str | None = None,
    ) -> list[Fill]:
        fills = [
            f for f in self._fills.values()
            if f.timestamp >= since and (exchange is None or f.exchange == exchange)
        ]
        return sorted(fills, key=lambda f: f.timestamp)

    async def get_all(self) -> list[Fill]:
        return sorted(self._fills.values(), key=lambda f: f.timestamp)

    async def clear(self) -> None:
        """Clear all fills (useful for testing)."""
        async with self._lock:
            self._fills.clear()
            self._order_index.clear()
            self._strategy_index.clear()
