"""PostgreSQL implementation of fill repository using asyncpg."""

from datetime import datetime
from typing import Any

import asyncpg

from calais_order_execution.config import DatabaseConfig
from calais_order_execution.models import Fill
from calais_order_execution.models.order import Liquidity, OrderSide
from calais_order_execution.repository.fill_base import FillRepository

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS fills (
    trade_id      TEXT PRIMARY KEY,
    order_id      TEXT NOT NULL,
    exchange      TEXT NOT NULL,
    instrument    TEXT NOT NULL,
    side          TEXT NOT NULL,
    amount        DOUBLE PRECISION NOT NULL,
    price         DOUBLE PRECISION NOT NULL,
    fee           DOUBLE PRECISION NOT NULL DEFAULT 0,
    fee_currency  TEXT NOT NULL DEFAULT '',
    liquidity     TEXT NOT NULL DEFAULT 'maker',
    timestamp     TIMESTAMPTZ NOT NULL,
    index_price   DOUBLE PRECISION,
    mark_price    DOUBLE PRECISION,
    iv            DOUBLE PRECISION,
    profit_loss   DOUBLE PRECISION,
    label         TEXT,
    strategy_id   TEXT
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills(order_id);",
    "CREATE INDEX IF NOT EXISTS idx_fills_strategy_id ON fills(strategy_id);",
    "CREATE INDEX IF NOT EXISTS idx_fills_timestamp ON fills(timestamp DESC);",
    "CREATE INDEX IF NOT EXISTS idx_fills_exchange_ts ON fills(exchange, timestamp DESC);",
    "CREATE INDEX IF NOT EXISTS idx_fills_instrument ON fills(instrument);",
]

# ON CONFLICT DO NOTHING gives us idempotency; we report whether a row was
# inserted via the RETURNING clause.
INSERT_SQL = """
INSERT INTO fills (
    trade_id, order_id, exchange, instrument, side, amount, price,
    fee, fee_currency, liquidity, timestamp,
    index_price, mark_price, iv, profit_loss, label, strategy_id
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11,
    $12, $13, $14, $15, $16, $17
)
ON CONFLICT (trade_id) DO NOTHING
RETURNING trade_id;
"""


def _fill_to_params(fill: Fill) -> tuple[Any, ...]:
    return (
        fill.trade_id,
        fill.order_id,
        fill.exchange,
        fill.instrument,
        fill.side.value,
        fill.amount,
        fill.price,
        fill.fee,
        fill.fee_currency,
        fill.liquidity.value,
        fill.timestamp,
        fill.index_price,
        fill.mark_price,
        fill.iv,
        fill.profit_loss,
        fill.label,
        fill.strategy_id,
    )


def _row_to_fill(row: asyncpg.Record) -> Fill:
    return Fill(
        trade_id=row["trade_id"],
        order_id=row["order_id"],
        exchange=row["exchange"],
        instrument=row["instrument"],
        side=OrderSide(row["side"]),
        amount=row["amount"],
        price=row["price"],
        fee=row["fee"] or 0.0,
        fee_currency=row["fee_currency"] or "",
        liquidity=Liquidity(row["liquidity"]) if row["liquidity"] else Liquidity.MAKER,
        timestamp=row["timestamp"].replace(tzinfo=None) if row["timestamp"] else datetime.utcnow(),
        index_price=row["index_price"],
        mark_price=row["mark_price"],
        iv=row["iv"],
        profit_loss=row["profit_loss"],
        label=row["label"],
        strategy_id=row["strategy_id"],
    )


class PostgresFillRepository(FillRepository):
    """PostgreSQL fill storage using asyncpg connection pool."""

    def __init__(self, pool: asyncpg.Pool):
        self._pool = pool

    @staticmethod
    async def create_pool(config: DatabaseConfig) -> asyncpg.Pool:
        return await asyncpg.create_pool(
            dsn=config.dsn,
            min_size=config.pool_min,
            max_size=config.pool_max,
        )

    async def ensure_table(self) -> None:
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
            for idx_sql in CREATE_INDEXES_SQL:
                await conn.execute(idx_sql)

    async def save(self, fill: Fill) -> bool:
        row = await self._pool.fetchrow(INSERT_SQL, *_fill_to_params(fill))
        return row is not None

    async def get(self, trade_id: str) -> Fill | None:
        row = await self._pool.fetchrow(
            "SELECT * FROM fills WHERE trade_id = $1", trade_id
        )
        return _row_to_fill(row) if row else None

    async def get_by_order_id(self, order_id: str) -> list[Fill]:
        rows = await self._pool.fetch(
            "SELECT * FROM fills WHERE order_id = $1 ORDER BY timestamp ASC",
            order_id,
        )
        return [_row_to_fill(r) for r in rows]

    async def get_by_strategy_id(self, strategy_id: str) -> list[Fill]:
        rows = await self._pool.fetch(
            "SELECT * FROM fills WHERE strategy_id = $1 ORDER BY timestamp ASC",
            strategy_id,
        )
        return [_row_to_fill(r) for r in rows]

    async def get_since(
        self,
        since: datetime,
        exchange: str | None = None,
    ) -> list[Fill]:
        if exchange is not None:
            rows = await self._pool.fetch(
                "SELECT * FROM fills WHERE timestamp >= $1 AND exchange = $2 ORDER BY timestamp ASC",
                since, exchange,
            )
        else:
            rows = await self._pool.fetch(
                "SELECT * FROM fills WHERE timestamp >= $1 ORDER BY timestamp ASC",
                since,
            )
        return [_row_to_fill(r) for r in rows]

    async def get_all(self) -> list[Fill]:
        rows = await self._pool.fetch("SELECT * FROM fills ORDER BY timestamp ASC")
        return [_row_to_fill(r) for r in rows]

    async def close(self) -> None:
        await self._pool.close()
