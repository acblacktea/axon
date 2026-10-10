"""PostgreSQL implementation of account repository using asyncpg."""

from typing import Any

import asyncpg

from axon_order_execution.config import DatabaseConfig
from axon_order_execution.models.portfolio import AccountSummary
from axon_order_execution.repository.account_base import AccountRepository

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS account_summaries (
    exchange              TEXT NOT NULL,
    currency              TEXT NOT NULL,
    equity                DOUBLE PRECISION NOT NULL DEFAULT 0,
    balance               DOUBLE PRECISION NOT NULL DEFAULT 0,
    available_funds       DOUBLE PRECISION NOT NULL DEFAULT 0,
    initial_margin        DOUBLE PRECISION NOT NULL DEFAULT 0,
    maintenance_margin    DOUBLE PRECISION NOT NULL DEFAULT 0,
    margin_balance        DOUBLE PRECISION NOT NULL DEFAULT 0,
    delta_total           DOUBLE PRECISION NOT NULL DEFAULT 0,
    options_delta         DOUBLE PRECISION NOT NULL DEFAULT 0,
    options_gamma         DOUBLE PRECISION NOT NULL DEFAULT 0,
    options_vega          DOUBLE PRECISION NOT NULL DEFAULT 0,
    options_theta         DOUBLE PRECISION NOT NULL DEFAULT 0,
    futures_pl            DOUBLE PRECISION NOT NULL DEFAULT 0,
    options_pl            DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_pl              DOUBLE PRECISION NOT NULL DEFAULT 0,
    timestamp             TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, currency)
);
"""

CREATE_INDEXES_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_account_summaries_exchange ON account_summaries(exchange);",
]

UPSERT_SQL = """
INSERT INTO account_summaries (
    exchange, currency, equity, balance, available_funds,
    initial_margin, maintenance_margin, margin_balance,
    delta_total, options_delta, options_gamma, options_vega, options_theta,
    futures_pl, options_pl, total_pl, timestamp
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17
)
ON CONFLICT (exchange, currency) DO UPDATE SET
    equity = EXCLUDED.equity,
    balance = EXCLUDED.balance,
    available_funds = EXCLUDED.available_funds,
    initial_margin = EXCLUDED.initial_margin,
    maintenance_margin = EXCLUDED.maintenance_margin,
    margin_balance = EXCLUDED.margin_balance,
    delta_total = EXCLUDED.delta_total,
    options_delta = EXCLUDED.options_delta,
    options_gamma = EXCLUDED.options_gamma,
    options_vega = EXCLUDED.options_vega,
    options_theta = EXCLUDED.options_theta,
    futures_pl = EXCLUDED.futures_pl,
    options_pl = EXCLUDED.options_pl,
    total_pl = EXCLUDED.total_pl,
    timestamp = EXCLUDED.timestamp;
"""


def _summary_to_params(summary: AccountSummary) -> tuple[Any, ...]:
    """Convert an AccountSummary to a tuple of query parameters."""
    return (
        summary.exchange,
        summary.currency,
        summary.equity,
        summary.balance,
        summary.available_funds,
        summary.initial_margin,
        summary.maintenance_margin,
        summary.margin_balance,
        summary.delta_total,
        summary.options_delta,
        summary.options_gamma,
        summary.options_vega,
        summary.options_theta,
        summary.futures_pl,
        summary.options_pl,
        summary.total_pl,
        summary.timestamp,
    )


def _row_to_summary(row: asyncpg.Record) -> AccountSummary:
    """Convert a database row to an AccountSummary."""
    return AccountSummary(
        currency=row["currency"],
        exchange=row["exchange"],
        equity=row["equity"],
        balance=row["balance"],
        available_funds=row["available_funds"],
        initial_margin=row["initial_margin"],
        maintenance_margin=row["maintenance_margin"],
        margin_balance=row["margin_balance"],
        delta_total=row["delta_total"],
        options_delta=row["options_delta"],
        options_gamma=row["options_gamma"],
        options_vega=row["options_vega"],
        options_theta=row["options_theta"],
        futures_pl=row["futures_pl"],
        options_pl=row["options_pl"],
        total_pl=row["total_pl"],
        timestamp=row["timestamp"].replace(tzinfo=None) if row["timestamp"] else None,
    )


class PostgresAccountRepository(AccountRepository):
    """PostgreSQL account summary storage using asyncpg connection pool."""

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
        """Create the account_summaries table and indexes if they don't exist."""
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_TABLE_SQL)
            for idx_sql in CREATE_INDEXES_SQL:
                await conn.execute(idx_sql)

    async def save(self, summary: AccountSummary) -> None:
        await self._pool.execute(UPSERT_SQL, *_summary_to_params(summary))

    async def get(self, exchange: str, currency: str) -> AccountSummary | None:
        row = await self._pool.fetchrow(
            "SELECT * FROM account_summaries WHERE exchange = $1 AND currency = $2",
            exchange, currency,
        )
        return _row_to_summary(row) if row else None

    async def get_by_exchange(self, exchange: str) -> list[AccountSummary]:
        rows = await self._pool.fetch(
            "SELECT * FROM account_summaries WHERE exchange = $1", exchange,
        )
        return [_row_to_summary(r) for r in rows]

    async def get_all(self) -> list[AccountSummary]:
        rows = await self._pool.fetch("SELECT * FROM account_summaries")
        return [_row_to_summary(r) for r in rows]

    async def delete(self, exchange: str, currency: str) -> bool:
        result = await self._pool.execute(
            "DELETE FROM account_summaries WHERE exchange = $1 AND currency = $2",
            exchange, currency,
        )
        return result == "DELETE 1"

    async def close(self) -> None:
        """Close the connection pool."""
        await self._pool.close()
