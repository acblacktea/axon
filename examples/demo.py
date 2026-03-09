#!/usr/bin/env python3
"""Quick demo - runs engine + strategy in a single process (for local testing).

For multi-process mode, use the separate scripts:
    Terminal 1:  python examples/run_engine.py --config config.yaml
    Terminal 2:  python examples/strategy_one.py
    Terminal 3:  python examples/strategy_two.py
"""

import asyncio
import signal

from calais_order_execution import CalaisExecutionService, load_config
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


def on_order_update(order: Order) -> None:
    logger.info(
        f"[ORDER] {order.instrument} {order.side.value} "
        f"status={order.status.value} filled={order.filled_amount}/{order.amount}"
    )


def on_account_update(summary: AccountSummary) -> None:
    logger.info(
        f"[ACCOUNT] {summary.currency}: equity={summary.equity:.6f} "
        f"balance={summary.balance:.6f}"
    )


def on_position_update(positions: list[Position]) -> None:
    logger.info(f"[POSITIONS] {len(positions)} positions")
    for p in positions[:3]:
        logger.info(f"  {p.instrument}: size={p.size} delta={p.delta:.4f}")


async def main() -> None:
    config = load_config("config.yaml")
    service = CalaisExecutionService(config)

    service.register_order_update_callback(on_order_update)
    service.register_account_update_callback(on_account_update)
    service.register_position_update_callback(on_position_update)

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await service.start()
        logger.info("Service started (single-process mode). Ctrl+C to stop.")

        # Query portfolio
        summary = service.get_account_summary(EXCHANGE, "BTC")
        if summary:
            logger.info(f"Initial equity: {summary.equity:.6f} BTC")

        positions = service.get_positions(EXCHANGE)
        logger.info(f"Open positions: {len(positions)}")

        # Place a test limit order far below market
        ticker = await service.get_ticker(EXCHANGE, "BTC-PERPETUAL")
        test_price = round(ticker.best_bid_price * 0.90, 1)

        request = OrderRequest(
            instrument="BTC-PERPETUAL",
            side=OrderSide.BUY,
            amount=0.001,
            order_type=OrderType.LIMIT,
            price=test_price,
            label="demo_test",
        )
        order = await service.place_order(EXCHANGE, request)
        logger.info(f"Placed order: {order.order_id} @ {test_price}")

        # Wait a bit then cancel
        await asyncio.sleep(5)
        await service.cancel_order(EXCHANGE, order.order_id)
        logger.info(f"Cancelled order: {order.order_id}")

        # Keep running for callbacks
        await stop_event.wait()

    finally:
        await service.stop()
        logger.info("Done")


if __name__ == "__main__":
    asyncio.run(main())
