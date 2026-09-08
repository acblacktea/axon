"""Periodic position refresh via REST API."""

import asyncio

from axon_order_execution.config import PortfolioConfig
from axon_order_execution.ems.base import BaseEMS
from axon_order_execution.oms.portfolio_manager import PortfolioManager
from axon_order_execution.util.logging import get_logger
from axon_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class PositionRefresher:
    """Periodically fetches positions from exchange REST API.

    Similar to OrderReconciler but for positions.
    """

    def __init__(
        self,
        ems: BaseEMS,
        portfolio_manager: PortfolioManager,
        config: PortfolioConfig | None = None,
    ):
        self._ems = ems
        self._portfolio_manager = portfolio_manager
        self._config = config or PortfolioConfig()
        self._running = False
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start periodic position refresh."""
        self._running = True

        # Initial fetch
        await self._do_refresh()

        self._task = asyncio.create_task(self._refresh_loop())
        logger.info(
            f"Started position refresher "
            f"(interval: {self._config.position_refresh_interval_seconds}s, "
            f"currencies: {self._config.currencies})"
        )

    async def stop(self) -> None:
        """Stop periodic position refresh."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Stopped position refresher")

    async def _refresh_loop(self) -> None:
        """Main refresh loop."""
        while self._running:
            try:
                await asyncio.sleep(self._config.position_refresh_interval_seconds)
                if not self._running:
                    break
                await self._do_refresh()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Position refresh error: {e}")

    async def _do_refresh(self) -> None:
        """Fetch positions for all configured currencies and update PortfolioManager."""
        exchange = self._ems.exchange_name
        all_positions = []
        any_failed = False
        for currency in self._config.currencies:
            try:
                positions = await self._ems.get_positions(currency)
                all_positions.extend(positions)
            except Exception as e:
                logger.error(f"Failed to fetch positions for {currency}: {e}")
                any_failed = True

        if any_failed:
            get_metrics().inc_reconciler_failure("position", exchange)

        await self._portfolio_manager.update_positions(exchange, all_positions)
        if all_positions:
            logger.debug(f"Refreshed {len(all_positions)} positions for {exchange}")
