#!/usr/bin/env python3
"""Strategy 1 - Portfolio monitor + limit order placer.

Demonstrates:
  - Connecting to engine via ZMQ StrategyClient
  - Registering callbacks for order, account, and position updates
  - Querying account summary and positions
  - Placing and cancelling a limit order

Usage (engine must be running first):
    python examples/strategy_one.py
"""

import asyncio
import signal

from calais_order_execution.client import StrategyClient
from calais_order_execution.config import ZMQConfig
from calais_order_execution.models import (
    AccountSummary,
    Order,
    OrderRequest,
    OrderSide,
    OrderType,
    Position,
)
from calais_order_execution.util.logging import get_logger, init_logging

init_logging(console_output=True)
logger = get_logger(__name__)

EXCHANGE = "deribit"


class PortfolioMonitorStrategy:
    """Strategy that monitors portfolio and places a single limit order."""

    def __init__(self, client: StrategyClient):
        self._client = client

        # Register callbacks
        self._client.register_order_update_callback(self._on_order_update)
        self._client.register_account_update_callback(self._on_account_update)
        self._client.register_position_update_callback(self._on_position_update)

    def _on_order_update(self, order: Order) -> None:
        logger.info(
            f"[S1 ORDER] {order.instrument} {order.side.value} "
            f"status={order.status.value} filled={order.filled_amount}/{order.amount} "
            f"id={order.order_id}"
        )

    def _on_account_update(self, summary: AccountSummary) -> None:
        logger.info(
            f"[S1 ACCOUNT] {summary.currency}: "
            f"equity={summary.equity:.6f} balance={summary.balance:.6f} "
            f"available={summary.available_funds:.6f} "
            f"delta={summary.delta_total:.4f}"
        )

    def _on_position_update(self, positions: list[Position]) -> None:
        logger.info(f"[S1 POSITIONS] Received {len(positions)} positions")
        for p in positions[:5]:  # log first 5
            logger.info(
                f"  {p.instrument}: size={p.size} dir={p.direction} "
                f"delta={p.delta:.4f} pnl={p.total_profit_loss:.6f}"
            )

    async def run(self) -> None:
        """Main strategy loop."""
        logger.info("[S1] Strategy 1 started - portfolio monitor")

        # 1) Query initial portfolio state
        await self._log_portfolio()

        # 2) Place a limit buy order far from market
        order = await self._place_test_order()

        # 3) Continuously monitor portfolio (Ctrl+C to stop)
        count = 0
        while True:
            await asyncio.sleep(10)
            count += 1
            logger.info(f"[S1] --- Periodic check #{count} ---")
            await self._log_portfolio()

    async def _log_portfolio(self) -> None:
        """Query and log account summary + positions."""
        try:
            summary = await self._client.get_account_summary(EXCHANGE, "BTC")
            if summary:
                logger.info(
                    f"[S1 QUERY] Account BTC: equity={summary.equity:.6f} "
                    f"balance={summary.balance:.6f} margin={summary.initial_margin:.6f}"
                )
        except Exception as e:
            logger.warning(f"[S1] get_account_summary failed: {e}")

        try:
            positions = await self._client.get_positions(EXCHANGE, "BTC")
            logger.info(f"[S1 QUERY] Positions: {len(positions)} open")
            for p in positions[:3]:
                logger.info(f"  {p.instrument}: size={p.size} delta={p.delta:.4f}")
        except Exception as e:
            logger.warning(f"[S1] get_positions failed: {e}")

    async def _place_test_order(self) -> Order | None:
        """Place a limit buy far below market for testing."""
        try:
            ticker = await self._client.get_ticker(EXCHANGE, "BTC-PERPETUAL")
            bid_price = ticker.best_bid_price
            # Place 10% below market so it won't fill
            test_price = round(bid_price * 0.90, 1)

            request = OrderRequest(
                instrument="BTC-PERPETUAL",
                side=OrderSide.BUY,
                amount=0.001,  # minimum size
                order_type=OrderType.LIMIT,
                price=test_price,
                label="s1_test",
            )
            order = await self._client.place_order(EXCHANGE, request)
            logger.info(
                f"[S1] Placed test order: BUY 0.001 BTC-PERPETUAL @ {test_price} "
                f"(market bid={bid_price}) id={order.order_id}"
            )
            return order
        except Exception as e:
            logger.error(f"[S1] Failed to place test order: {e}")
            return None


async def main() -> None:
    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="strategy_one")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await client.connect()
        logger.info("[S1] Connected to engine")

        strategy = PortfolioMonitorStrategy(client)

        # Run strategy with cancellation support
        strategy_task = asyncio.create_task(strategy.run())
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [strategy_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()

    except KeyboardInterrupt:
        pass
    finally:
        await client.disconnect()
        logger.info("[S1] Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
