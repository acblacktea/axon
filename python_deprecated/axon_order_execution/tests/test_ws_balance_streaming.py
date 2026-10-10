"""Test scenario: Monitor WS balance (account) streaming - place/cancel orders to trigger updates."""

import asyncio
import signal

from axon_order_execution.client import StrategyClient
from axon_order_execution.config import ZMQConfig
from axon_order_execution.models import OrderRequest, OrderSide, OrderType
from axon_order_execution.models.portfolio import AccountSummary
from axon_order_execution.util.logging import get_logger, init_logging

logger = get_logger(__name__)

EXCHANGE = "deribit"
INSTRUMENT = "BTC-27MAR26-70000-C"
TICK_SIZE = 0.0005


async def main() -> None:
    init_logging(console_output=True)

    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="test_ws_balance")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Track WS updates
    updates: list[AccountSummary] = []

    def on_account_update(summary: AccountSummary) -> None:
        updates.append(summary)
        logger.info(
            f"[WS #{len(updates)}] {summary.currency}: "
            f"equity={summary.equity:.6f} balance={summary.balance:.6f} "
            f"available={summary.available_funds:.6f} "
            f"margin={summary.initial_margin:.6f} "
            f"delta={summary.delta_total:.4f}"
        )

    try:
        await client.connect()
        client.register_account_update_callback(on_account_update)
        logger.info("Connected, on_account callback registered")

        # Query initial state
        initial = await client.get_account_summary(EXCHANGE, "BTC")
        if initial:
            logger.info(
                f"[INITIAL] {initial.currency}: "
                f"equity={initial.equity:.6f} balance={initial.balance:.6f} "
                f"available={initial.available_funds:.6f}"
            )
        else:
            logger.warning("No initial account summary available")

        # Place orders to trigger portfolio changes (margin updates)
        ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
        ref_price = ticker.best_bid_price or ticker.mark_price or ticker.best_ask_price

        order_ids = []
        for i in range(3):
            factor = 0.5 - i * 0.05
            price = round(ref_price * factor / TICK_SIZE) * TICK_SIZE
            if price <= 0:
                price = TICK_SIZE

            request = OrderRequest(
                instrument=INSTRUMENT,
                side=OrderSide.BUY,
                amount=0.1,
                order_type=OrderType.LIMIT,
                price=price,
                label=f"bal_test_{i}",
            )
            logger.info(f"Placing order #{i+1} @ {price} to trigger balance update")
            order = await client.place_order(EXCHANGE, request)
            order_ids.append(order.order_id)
            await asyncio.sleep(3)

        # Cancel all to trigger more balance updates
        logger.info("Cancelling orders to trigger balance updates...")
        for oid in order_ids:
            await client.cancel_order(EXCHANGE, oid)
            await asyncio.sleep(3)

        # Wait a bit more for final updates
        await asyncio.sleep(3)

        # Summary
        logger.info("=== Balance Streaming Summary ===")
        logger.info(f"Total updates received: {len(updates)}")
        if updates:
            first = updates[0]
            last = updates[-1]
            logger.info(
                f"First: equity={first.equity:.6f} balance={first.balance:.6f} available={first.available_funds:.6f}"
            )
            logger.info(
                f"Last:  equity={last.equity:.6f} balance={last.balance:.6f} available={last.available_funds:.6f}"
            )
            logger.info("Test PASSED - balance streaming working")
        else:
            logger.error("Test FAILED - no balance updates received")

    except Exception as e:
        logger.error(f"Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
