"""Order reconciliation for WebSocket reliability."""

import asyncio

from calais_order_execution.config import ReconciliationConfig
from calais_order_execution.ems.base import BaseEMS
from calais_order_execution.oms.order_manager import OrderManager
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class OrderReconciler:
    """Periodically reconciles order state with exchange via REST API.

    Addresses potential WebSocket message loss by fetching current order
    state from the exchange and updating local cache.
    """

    def __init__(
        self,
        ems: BaseEMS,
        order_manager: OrderManager,
        config: ReconciliationConfig | None = None,
    ):
        """Initialize reconciler.

        Args:
            ems: EMS client for REST API calls.
            order_manager: Order manager to update.
            config: Reconciliation configuration.
        """
        self._ems = ems
        self._order_manager = order_manager
        self._config = config or ReconciliationConfig()
        self._running = False
        self._task: asyncio.Task | None = None

    @property
    def is_running(self) -> bool:
        """Check if reconciler is running."""
        return self._running

    async def start(self) -> None:
        """Start periodic reconciliation.

        Runs an initial reconciliation before starting the periodic loop
        to ensure local state is synchronized with exchange before trading.
        """
        if not self._config.enabled:
            logger.info("Reconciliation is disabled")
            return

        self._running = True

        # Run initial reconciliation to sync state before trading
        logger.info("Running initial order reconciliation...")
        count = await self._do_reconcile()
        logger.info(f"Initial reconciliation complete: {count} orders synced")

        # Start periodic reconciliation loop
        self._task = asyncio.create_task(self._reconcile_loop())
        logger.info(
            f"Started periodic order reconciliation (interval: {self._config.interval_seconds}s)"
        )

    async def stop(self) -> None:
        """Stop periodic reconciliation."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logger.info("Stopped order reconciliation")

    async def reconcile_now(self) -> int:
        """Run reconciliation immediately.

        Returns:
            Number of orders reconciled.
        """
        return await self._do_reconcile()

    async def _reconcile_loop(self) -> None:
        """Main reconciliation loop."""
        while self._running:
            try:
                await asyncio.sleep(self._config.interval_seconds)

                if not self._running:
                    break

                await self._do_reconcile()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Reconciliation error: {e}")

    async def _do_reconcile(self) -> int:
        """Perform reconciliation.

        Returns:
            Number of orders reconciled.
        """
        try:
            # Get open orders from exchange
            exchange_orders = await self._ems.get_open_orders()
            exchange_order_ids = {o.order_id for o in exchange_orders}

            # Get local active orders to find any that may have closed while offline
            # TODO: Performance issue - get_active_orders() may be slow with many orders
            # in database. Consider adding time-based filtering (e.g., orders updated
            # in last N hours) or pagination.
            local_active_orders = await self._order_manager.get_active_orders()

            # Find orders that are active locally but not in exchange open orders
            # These may have been filled/cancelled while we were offline
            missing_order_ids = [
                o.order_id for o in local_active_orders
                if o.order_id not in exchange_order_ids
            ]

            # Query each missing order individually to get its final state
            for order_id in missing_order_ids:
                order = await self._ems.get_order(order_id)
                if order:
                    exchange_orders.append(order)
                    logger.info(
                        f"Recovered closed order {order_id}: {order.status.value}"
                    )

            if not exchange_orders:
                logger.debug("No orders to reconcile")
                return 0

            # Update order manager with exchange state
            await self._order_manager.reconcile(exchange_orders)

            logger.debug(f"Reconciled {len(exchange_orders)} orders")
            return len(exchange_orders)

        except Exception as e:
            logger.error(f"Failed to reconcile orders: {e}")
            return 0

    async def __aenter__(self) -> "OrderReconciler":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
