"""PostgreSQL implementation of position repository using asyncpg."""

from typing import Any

import asyncpg

from calais_order_execution.config import DatabaseConfig
from calais_order_execution.models.portfolio import Position
from calais_order_execution.repository.position_base import PositionRepository

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS positions (
    exchange              TEXT NOT NULL,
    instrument            TEXT NOT NULL,
    kind                  TEXT NOT NULL,
    direction             TEXT NOT NULL,
    size                  DOUBLE PRECISION NOT NULL DEFAULT 0,
    average_price         DOUBLE PRECISION NOT NULL DEFAULT 0,
    mark_price            DOUBLE PRECISION NOT NULL DEFAULT 0,
    index_price           DOUBLE PRECISION NOT NULL DEFAULT 0,
    initial_margin        DOUBLE PRECISION NOT NULL DEFAULT 0,
    maintenance_margin    DOUBLE PRECISION NOT NULL DEFAULT 0,
    delta                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    gamma                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    vega                  DOUBLE PRECISION NOT NULL DEFAULT 0,
    theta                 DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_profit_loss     DOUBLE PRECISION NOT NULL DEFAULT 0,
    floating_profit_loss  DOUBLE PRECISION NOT NULL DEFAULT 0,
    realized_profit_loss  DOUBLE PRECISION NOT NULL DEFAULT 0,
    timestamp             TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, instrument)
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_positions_exchange ON positions(exchange);",
    "CREATE INDEX IF NOT EXISTS idx_positions_kind ON positions(exchange, kind);",
]

UPSERT_SQL = """
INSERT INTO positions (
    exchange, instrument, kind, direction, size,
    average_price, mark_price, index_price,
    initial_margin, maintenance_margin,
    delta, gamma, vega, theta,
    total_profit_loss, floating_profit_loss, realized_profit_loss,
    timestamp
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18
)
ON CONFLICT (exchange, instrument) DO UPDATE SET
    kind = EXCLUDED.kind,
    direction = EXCLUDED.direction,
    size = EXCLUDED.size,
    average_price = EXCLUDED.average_price,
    mark_price = EXCLUDED.mark_price,
    index_price = EXCLUDED.index_price,
    initial_margin = EXCLUDED.initial_margin,
    maintenance_margin = EXCLUDED.maintenance_margin,
    delta = EXCLUDED.delta,
    gamma = EXCLUDED.gamma,
    vega = EXCLUDED.vega,
    theta = EXCLUDED.theta,
    total_profit_loss = EXCLUDED.total_profit_loss,
    floating_profit_loss = EXCLUDED.floating_profit_loss,
    realized_profit_loss = EXCLUDED.realized_profit_loss,
    timestamp = EXCLUDED.timestamp;
"""


def _position_to_params(position: Position) -> tuple[Any, ...]:
    """Convert a Position to a tuple of query parameters."""
    return (
        position.exchange,
        position.instrument,
        position.kind,
        position.direction,
        position.size,
        position.average_price,
        position.mark_price,
        position.index_price,
        position.initial_margin,
        position.maintenance_margin,
        position.delta,
        position.gamma,
        position.vega,
        position.theta,
        position.total_profit_loss,
        position.floating_profit_loss,
        position.realized_profit_loss,
        position.timestamp,
    )


def _row_to_position(row: asyncpg.Record) -> Position:
    """Convert a database row to a Position."""
    return Position(
        instrument=row["instrument"],
        exchange=row["exchange"],
        kind=row["kind"],
        direction=row["direction"],
        size=row["size"],
        average_price=row["average_price"],
        mark_price=row["mark_price"],
        index_price=row["index_price"],
        initial_margin=row["initial_margin"],
        maintenance_margin=row["maintenance_margin"],
        delta=row["delta"],
        gamma=row["gamma"],
        vega=row["vega"],
        theta=row["theta"],
        total_profit_loss=row["total_profit_loss"],
        floating_profit_loss=row["floating_profit_loss"],
        realized_profit_loss=row["realized_profit_loss"],
        timestamp=row["timestamp"],
    )


class PostgresPositionRepository(PositionRepository):
    """PostgreSQL position storage using asyncpg connection pool."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @staticmethod
    async def create_pool(config: DatabaseConfig) -> asyncpg.Pool:
        """Create an asyncpg connection pool from DatabaseConfig."""
        return await asyncpg.create_pool(
            dsn=config.dsn,
            min_size=config.pool_min,
            max_size=config.pool_max,
        )

    async def ensure_table(self) -> None:
        """Create the positions table and indexes if they don't exist."""
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
            for idx_sql in CREATE_INDEXES_SQL:
                await conn.execute(idx_sql)

    async def save(self, position: Position) -> None:
        await self._pool.execute(UPSERT_SQL, *_position_to_params(position))

    async def get(self, exchange: str, instrument: str) -> Position | None:
        row = await self._pool.fetchrow(
            "SELECT * FROM positions WHERE exchange = $1 AND instrument = $2",
            exchange, instrument,
        )
        return _row_to_position(row) if row else None

    async def get_by_exchange(self, exchange: str) -> list[Position]:
        rows = await self._pool.fetch(
            "SELECT * FROM positions WHERE exchange = $1", exchange,
        )
        return [_row_to_position(r) for r in rows]

    async def get_by_kind(self, exchange: str, kind: str) -> list[Position]:
        rows = await self._pool.fetch(
            "SELECT * FROM positions WHERE exchange = $1 AND kind = $2",
            exchange, kind,
        )
        return [_row_to_position(r) for r in rows]

    async def get_all(self) -> list[Position]:
        rows = await self._pool.fetch("SELECT * FROM positions")
        return [_row_to_position(r) for r in rows]

    async def delete(self, exchange: str, instrument: str) -> bool:
        result = await self._pool.execute(
            "DELETE FROM positions WHERE exchange = $1 AND instrument = $2",
            exchange, instrument,
        )
        return result == "DELETE 1"

    async def delete_by_exchange(self, exchange: str) -> int:
        result = await self._pool.execute(
            "DELETE FROM positions WHERE exchange = $1", exchange,
        )
        # result format: "DELETE N"
        return int(result.split()[-1])

    async def replace_all(self, exchange: str, positions: list[Position]) -> None:
        """Atomically replace all positions for an exchange."""
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "DELETE FROM positions WHERE exchange = $1", exchange
                )
                for pos in positions:
                    await conn.execute(UPSERT_SQL, *_position_to_params(pos))

    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()
