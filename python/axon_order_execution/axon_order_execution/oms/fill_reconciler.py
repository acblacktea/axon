"""Fill reconciliation for WebSocket reliability.

Periodically pulls user trades via REST and pushes them through FillManager.
Duplicates are dropped by the trade_id idempotency in FillManager.save(),
so the worst case for a working WS is wasted REST bandwidth.
"""

import asyncio
import time

from axon_order_execution.config import FillReconciliationConfig, PortfolioConfig
from axon_order_execution.ems.base import BaseEMS
from axon_order_execution.oms.fill_manager import FillManager
from axon_order_execution.util.logging import get_logger
from axon_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class FillReconciler:
    """Periodically reconciles fill state with exchange via REST API."""

    def __init__(
        self,
        ems: BaseEMS,
        fill_manager: FillManager,
        portfolio_config: PortfolioConfig,
        config: FillReconciliationConfig | None = None,
    ):
        self._ems = ems
        self._fill_manager = fill_manager
        self._portfolio_config = portfolio_config
        self._config = config or FillReconciliationConfig()
        # Cursor (epoch ms). Filled in by _initial_cursor() at start time.
        self._cursor_ms: int = 0
        self._running = False
        self._task: asyncio.Task | None = None

    @property
    def is_running(self) -> bool:
        return self._running

    async def start(self) -> None:
        if not self._config.enabled:
            logger.info("Fill reconciliation is disabled")
            return

        self._running = True
        # Backfill window on startup so that fills missed while the engine
        # was offline are picked up.
        self._cursor_ms = (
            int(time.time() * 1000) - self._config.lookback_seconds * 1000
        )

        logger.info(
            f"Running initial fill reconciliation "
            f"(lookback {self._config.lookback_seconds}s)..."
        )
        new_count = await self._do_reconcile()
        logger.info(f"Initial fill reconciliation complete: {new_count} new fills")

        self._task = asyncio.create_task(self._reconcile_loop())
        logger.info(
            f"Started periodic fill reconciliation "
            f"(interval: {self._config.interval_seconds}s)"
        )

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Stopped fill reconciliation")

    async def reconcile_now(self) -> int:
        return await self._do_reconcile()

    async def _reconcile_loop(self) -> None:
        while self._running:
            try:
                await asyncio.sleep(self._config.interval_seconds)
                if not self._running:
                    break
                await self._do_reconcile()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Fill reconciliation error: {e}")

    async def _do_reconcile(self) -> int:
        """Fetch fills since cursor for each currency and push through FillManager.

        Returns:
            Count of newly inserted fills (duplicates excluded).
        """
        # Re-fetch a small overlap window to guard against fills that appear
        # slightly out of timestamp order across REST/WS or clock skew.
        overlap_ms = self._config.overlap_seconds * 1000
        since_ms = max(0, self._cursor_ms - overlap_ms)
        latest_ts = since_ms
        new_total = 0
        exchange_name = self._ems.exchange_name
        any_currency_failed = False

        for currency in self._portfolio_config.currencies:
            try:
                fills = await self._ems.get_user_trades_since(currency, since_ms)
            except Exception as e:
                logger.error(f"Failed to fetch fills for {currency}: {e}")
                any_currency_failed = True
                continue

            for fill in fills:
                fill_ts_ms = int(fill.timestamp.timestamp() * 1000)
                if fill_ts_ms > latest_ts:
                    latest_ts = fill_ts_ms
                try:
                    if await self._fill_manager.add_fill(fill):
                        new_total += 1
                except Exception as e:
                    logger.error(f"Failed to add reconciled fill {fill.trade_id}: {e}")

        if any_currency_failed:
            get_metrics().inc_reconciler_failure("fill", exchange_name)
        # Advance cursor only if we made progress; we keep the lookback window
        # available next round even when no new fills arrive.
        self._cursor_ms = max(self._cursor_ms, latest_ts)
        if new_total > 0:
            logger.warning(
                f"Fill reconciler recovered {new_total} fills missed by WS"
            )
            get_metrics().inc_reconciler_recovered("fill", exchange_name, new_total)
        else:
            logger.debug("Fill reconciler: no new fills")
        return new_total
