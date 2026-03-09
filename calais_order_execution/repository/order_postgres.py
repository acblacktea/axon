"""PostgreSQL implementation of order repository using asyncpg."""

from datetime import datetime
from typing import Any

import asyncpg

from calais_order_execution.config import DatabaseConfig
from calais_order_execution.models.order import (
    Liquidity,
    Order,
    OrderSide,
    OrderStatus,
    OrderType,
)
from calais_order_execution.repository.order_base import OrderRepository

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    order_id          TEXT PRIMARY KEY,
    exchange          TEXT NOT NULL,
    instrument        TEXT NOT NULL,
    side              TEXT NOT NULL,
    order_type        TEXT NOT NULL,
    amount            DOUBLE PRECISION NOT NULL,
    status            TEXT NOT NULL,
    internal_order_id TEXT,
    price             DOUBLE PRECISION,
    filled_amount     DOUBLE PRECISION DEFAULT 0,
    average_price     DOUBLE PRECISION,
    client_order_id   TEXT,
    label             TEXT,
    liquidity         TEXT DEFAULT 'maker',
    post_only         BOOLEAN DEFAULT FALSE,
    reject_post_only  BOOLEAN DEFAULT FALSE,
    strategy_id       TEXT,
    created_at        TIMESTAMPTZ NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_orders_strategy_id ON orders(strategy_id);",
    "CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);",
    "CREATE INDEX IF NOT EXISTS idx_orders_client_order_id ON orders(client_order_id);",
    "CREATE INDEX IF NOT EXISTS idx_orders_instrument ON orders(instrument);",
]

UPSERT_SQL = """
INSERT INTO orders (
    order_id, exchange, instrument, side, order_type, amount, status,
    internal_order_id, price, filled_amount, average_price,
    client_order_id, label, liquidity, post_only, reject_post_only,
    strategy_id, created_at, updated_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11,
    $12, $13, $14, $15, $16,
    $17, $18, $19
)
ON CONFLICT (order_id) DO UPDATE SET
    exchange = EXCLUDED.exchange,
    instrument = EXCLUDED.instrument,
    side = EXCLUDED.side,
    order_type = EXCLUDED.order_type,
    amount = EXCLUDED.amount,
    status = EXCLUDED.status,
    internal_order_id = EXCLUDED.internal_order_id,
    price = EXCLUDED.price,
    filled_amount = EXCLUDED.filled_amount,
    average_price = EXCLUDED.average_price,
    client_order_id = EXCLUDED.client_order_id,
    label = EXCLUDED.label,
    liquidity = EXCLUDED.liquidity,
    post_only = EXCLUDED.post_only,
    reject_post_only = EXCLUDED.reject_post_only,
    strategy_id = EXCLUDED.strategy_id,
    updated_at = EXCLUDED.updated_at;
"""

ACTIVE_STATUSES = ("pending", "open", "partially_filled")


def _order_to_params(order: Order) -> tuple[Any, ...]:
    """Convert an Order to a tuple of query parameters."""
    return (
        order.order_id,
        order.exchange,
        order.instrument,
        order.side.value,
        order.order_type.value,
        order.amount,
        order.status.value,
        order.internal_order_id,
        order.price,
        order.filled_amount,
        order.average_price,
        order.client_order_id,
        order.label,
        order.liquidity.value,
        order.post_only,
        order.reject_post_only,
        order.strategy_id,
        order.created_at,
        order.updated_at,
    )


def _row_to_order(row: asyncpg.Record) -> Order:
    """Convert a database row to an Order."""
    return Order(
        order_id=row["order_id"],
        exchange=row["exchange"],
        instrument=row["instrument"],
        side=OrderSide(row["side"]),
        order_type=OrderType(row["order_type"]),
        amount=row["amount"],
        status=OrderStatus(row["status"]),
        internal_order_id=row["internal_order_id"],
        price=row["price"],
        filled_amount=row["filled_amount"] or 0.0,
        average_price=row["average_price"],
        client_order_id=row["client_order_id"],
        label=row["label"],
        liquidity=Liquidity(row["liquidity"]) if row["liquidity"] else Liquidity.MAKER,
        post_only=row["post_only"] or False,
        reject_post_only=row["reject_post_only"] or False,
        strategy_id=row["strategy_id"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


class PostgresOrderRepository(OrderRepository):
    """PostgreSQL order storage using asyncpg connection pool."""

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
        """Create the orders table and indexes if they don't exist."""
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
            for idx_sql in CREATE_INDEXES_SQL:
                await conn.execute(idx_sql)

    async def save(self, order: Order) -> None:
        """Save a new order (upsert)."""
        await self._pool.execute(UPSERT_SQL, *_order_to_params(order))

    async def get(self, order_id: str) -> Order | None:
        """Get an order by ID."""
        row = await self._pool.fetchrow(
            "SELECT * FROM orders WHERE order_id = $1", order_id
        )
        return _row_to_order(row) if row else None

    async def get_by_client_order_id(self, client_order_id: str) -> Order | None:
        """Get an order by client order ID."""
        row = await self._pool.fetchrow(
            "SELECT * FROM orders WHERE client_order_id = $1", client_order_id
        )
        return _row_to_order(row) if row else None

    async def get_by_strategy_id(self, strategy_id: str) -> list[Order]:
        """Get all orders for a strategy."""
        rows = await self._pool.fetch(
            "SELECT * FROM orders WHERE strategy_id = $1 ORDER BY created_at DESC",
            strategy_id,
        )
        return [_row_to_order(r) for r in rows]

    async def get_all(self) -> list[Order]:
        """Get all orders."""
        rows = await self._pool.fetch("SELECT * FROM orders ORDER BY created_at DESC")
        return [_row_to_order(r) for r in rows]

    async def get_active_orders(self) -> list[Order]:
        """Get all active orders."""
        rows = await self._pool.fetch(
            "SELECT * FROM orders WHERE status = ANY($1) ORDER BY created_at DESC",
            list(ACTIVE_STATUSES),
        )
        return [_row_to_order(r) for r in rows]

    async def update(self, order: Order) -> None:
        """Update an existing order (upsert)."""
        await self._pool.execute(UPSERT_SQL, *_order_to_params(order))

    async def delete(self, order_id: str) -> bool:
        """Delete an order."""
        result = await self._pool.execute(
            "DELETE FROM orders WHERE order_id = $1", order_id
        )
        return result == "DELETE 1"

    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()
