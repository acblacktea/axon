"""Test scenario: Place order, verify open, cancel, verify cancelled."""

import asyncio
import signal

from calais_order_execution.client.strategy_client import StrategyClient
from calais_order_execution.config import ZMQConfig
from calais_order_execution.models.order import OrderRequest, OrderSide, OrderType
from calais_order_execution.util.logging import get_logger, init_logging

logger = get_logger(__name__)

EXCHANGE = "deribit"
INSTRUMENT = "BTC-27MAR26-70000-C"


async def main() -> None:
    init_logging(console_output=True)

    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="test_cancel_order")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await client.connect()
        logger.info("Connected to engine")

        # Step 1: Get ticker and place order
        ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
        ref_price = ticker.best_bid_price or ticker.mark_price or ticker.best_ask_price
        tick_size = 0.0005
        buy_price = round(ref_price * 0.5 / tick_size) * tick_size
        if buy_price <= 0:
            buy_price = tick_size

        request = OrderRequest(
            instrument=INSTRUMENT,
            side=OrderSide.BUY,
            amount=0.1,
            order_type=OrderType.LIMIT,
            price=buy_price,
            label="test_cancel",
        )

        logger.info(f"Placing order: BUY {request.amount} {INSTRUMENT} @ {buy_price}")
        order = await client.place_order(EXCHANGE, request)
        logger.info(f"Order placed: id={order.order_id}, status={order.status.value}")

        # Step 2: Wait for WS update, then verify order is open
        await asyncio.sleep(2)
        fetched = await client.get_order(order.order_id)
        assert fetched is not None, "Order not found in cache"
        logger.info(f"Before cancel: status={fetched.status.value}")
        assert fetched.status.value == "open", f"Expected 'open', got '{fetched.status.value}'"

        # Step 3: Cancel the order
        cancelled = await client.cancel_order(EXCHANGE, order.order_id)
        logger.info(f"Cancel API returned: {cancelled}")
        assert cancelled, "Cancel returned False"

        # Step 4: Wait for WS cancel update, then verify status is cancelled
        await asyncio.sleep(2)
        fetched_after = await client.get_order(order.order_id)
        assert fetched_after is not None, "Order not found after cancel"
        logger.info(f"After cancel: status={fetched_after.status.value}")
        assert fetched_after.status.value == "cancelled", (
            f"Expected 'cancelled', got '{fetched_after.status.value}'"
        )

        logger.info("Test PASSED - cancel order scenario complete")

    except Exception as e:
        logger.error(f"Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
