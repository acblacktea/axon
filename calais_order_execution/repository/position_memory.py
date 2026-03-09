"""In-memory implementation of position repository."""

import asyncio

from calais_order_execution.models.portfolio import Position
from calais_order_execution.repository.position_base import PositionRepository


class InMemoryPositionRepository(PositionRepository):
    """In-memory position storage using dictionaries."""

    def __init__(self):
        self._positions: dict[tuple[str, str], Position] = {}  # (exchange, instrument)
        self._lock = asyncio.Lock()

    async def save(self, position: Position) -> None:
        async with self._lock:
            self._positions[(position.exchange, position.instrument)] = position

    async def get(self, exchange: str, instrument: str) -> Position | None:
        return self._positions.get((exchange, instrument))

    async def get_by_exchange(self, exchange: str) -> list[Position]:
        return [p for (ex, _), p in self._positions.items() if ex == exchange]

    async def get_by_kind(self, exchange: str, kind: str) -> list[Position]:
        return [
            p for (ex, _), p in self._positions.items()
            if ex == exchange and p.kind == kind
        ]

    async def get_all(self) -> list[Position]:
        return list(self._positions.values())

    async def delete(self, exchange: str, instrument: str) -> bool:
        async with self._lock:
            key = (exchange, instrument)
            if key in self._positions:
                del self._positions[key]
                return True
            return False

    async def delete_by_exchange(self, exchange: str) -> int:
        async with self._lock:
            keys_to_delete = [k for k in self._positions if k[0] == exchange]
            for k in keys_to_delete:
                del self._positions[k]
            return len(keys_to_delete)

    async def replace_all(self, exchange: str, positions: list[Position]) -> None:
        async with self._lock:
            keys_to_delete = [k for k in self._positions if k[0] == exchange]
            for k in keys_to_delete:
                del self._positions[k]
            for pos in positions:
                self._positions[(pos.exchange, pos.instrument)] = pos

    async def clear(self) -> None:
        """Clear all positions (useful for testing)."""
        async with self._lock:
            self._positions.clear()
