#!/usr/bin/env python3
"""Strategy 2 - Hedge algorithm runner.

Demonstrates:
  - Connecting to engine via ZMQ StrategyClient
  - Running the hedge_deribit_options algorithm (client-side)
  - Receiving real-time order updates during algorithm execution
  - Querying active orders and positions

Usage (engine must be running first):
    python examples/strategy_two.py
    python examples/strategy_two.py --symbol1 BTC-27JUN26-120000-C --symbol2 BTC-27JUN26-120000-P
"""

import argparse
import asyncio
import signal

from calais_order_execution.client import StrategyClient
from calais_order_execution.client.algorithms import hedge_deribit_options
from calais_order_execution.config import ZMQConfig
from calais_order_execution.models import (
    AccountSummary,
    Order,
    OrderSide,
    Position,
)
from calais_order_execution.util.logging import get_logger, init_logging

init_logging(console_output=True)
logger = get_logger(__name__)

EXCHANGE = "deribit"


class HedgeStrategy:
    """Strategy that runs the options hedge algorithm."""

    def __init__(self, client: StrategyClient):
        self._client = client
        self._fill_count = 0

        # Register callbacks
        self._client.register_order_update_callback(self._on_order_update)
        self._client.register_account_update_callback(self._on_account_update)
        self._client.register_position_update_callback(self._on_position_update)

    def _on_order_update(self, order: Order) -> None:
        self._fill_count += 1
        logger.info(
            f"[S2 ORDER #{self._fill_count}] {order.instrument} {order.side.value} "
            f"status={order.status.value} filled={order.filled_amount}/{order.amount} "
            f"price={order.price} avg={order.average_price}"
        )

    def _on_account_update(self, summary: AccountSummary) -> None:
        logger.info(
            f"[S2 ACCOUNT] {summary.currency}: equity={summary.equity:.6f} "
            f"available={summary.available_funds:.6f}"
        )

    def _on_position_update(self, positions: list[Position]) -> None:
        logger.info(f"[S2 POSITIONS] {len(positions)} positions updated")

    async def run(
        self,
        symbol1: str,
        symbol2: str,
        amount: float,
        batch_amount: float,
    ) -> None:
        """Run the hedge algorithm."""
        logger.info("[S2] Strategy 2 started - hedge algorithm")
        logger.info(f"[S2] Hedge: BUY {symbol1} (maker) / SELL {symbol2} (taker)")
        logger.info(f"[S2] Total amount={amount}, batch={batch_amount}")

        # Log pre-hedge state
        await self._log_state("PRE-HEDGE")

        # Run the hedge algorithm
        internal_id = await hedge_deribit_options(
            client=self._client,
            symbol1=symbol1,
            symbol2=symbol2,
            side1=OrderSide.BUY,
            side2=OrderSide.SELL,
            amount=amount,
            batch_amount=batch_amount,
            chase_maker_max_attempt_second=60,
        )
        logger.info(f"[S2] Hedge completed, internal_id={internal_id}")

        # Log post-hedge state
        await self._log_state("POST-HEDGE")

        # Show all orders from this strategy
        try:
            orders = await self._client.get_all_orders()
            logger.info(f"[S2] Total orders: {len(orders)}")
            for o in orders:
                logger.info(
                    f"  {o.instrument} {o.side.value} {o.status.value} "
                    f"filled={o.filled_amount}/{o.amount}"
                )
        except Exception as e:
            logger.warning(f"[S2] get_all_orders failed: {e}")

        logger.info("[S2] Strategy 2 finished")

    async def _log_state(self, label: str) -> None:
        """Log current account and position state."""
        logger.info(f"[S2] --- {label} ---")
        try:
            summary = await self._client.get_account_summary(EXCHANGE, "BTC")
            if summary:
                logger.info(
                    f"[S2] Account: equity={summary.equity:.6f} "
                    f"delta={summary.delta_total:.4f}"
                )
        except Exception as e:
            logger.warning(f"[S2] account query failed: {e}")

        try:
            positions = await self._client.get_positions(EXCHANGE, "BTC")
            for p in positions:
                logger.info(
                    f"[S2] Position: {p.instrument} size={p.size} "
                    f"delta={p.delta:.4f} pnl={p.total_profit_loss:.6f}"
                )
        except Exception as e:
            logger.warning(f"[S2] positions query failed: {e}")


async def main(args) -> None:
    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="strategy_two")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await client.connect()
        logger.info("[S2] Connected to engine")

        strategy = HedgeStrategy(client)

        strategy_task = asyncio.create_task(
            strategy.run(
                symbol1=args.symbol1,
                symbol2=args.symbol2,
                amount=args.amount,
                batch_amount=args.batch_amount,
            )
        )
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
        logger.info("[S2] Disconnected")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Strategy 2 - Hedge Runner")
    parser.add_argument("--symbol1", default="BTC-27JUN26-120000-C", help="Maker leg instrument")
    parser.add_argument("--symbol2", default="BTC-27JUN26-120000-P", help="Taker leg instrument")
    parser.add_argument("--amount", type=float, default=1.0, help="Total hedge amount")
    parser.add_argument("--batch-amount", type=float, default=0.5, help="Per-batch amount")
    args = parser.parse_args()
    asyncio.run(main(args))
