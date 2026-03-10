"""Test scenario: Place 5 orders (one every 5s), verify WS order streaming via on_order callback."""

import asyncio
import signal

from calais_order_execution.client import StrategyClient
from calais_order_execution.config import ZMQConfig
from calais_order_execution.models import Order, OrderRequest, OrderSide, OrderType
from calais_order_execution.util.logging import get_logger, init_logging

logger = get_logger(__name__)

EXCHANGE = "deribit"
INSTRUMENT = "BTC-27MAR26-70000-C"
TICK_SIZE = 0.0005
NUM_ORDERS = 5
ORDER_INTERVAL = 5  # seconds


async def main() -> None:
    init_logging(console_output=True)

    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="test_ws_order_streaming")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Track WS updates per order
    ws_updates: dict[str, list[str]] = {}  # order_id -> list of status values

    def on_order_update(order: Order) -> None:
        if order.order_id not in ws_updates:
            ws_updates[order.order_id] = []
        ws_updates[order.order_id].append(order.status.value)
        logger.info(
            f"[WS] order={order.order_id} status={order.status.value} "
            f"filled={order.filled_amount}/{order.amount} "
            f"price={order.price} strategy={order.strategy_id}"
        )

    try:
        await client.connect()
        client.register_order_update_callback(on_order_update)
        logger.info("Connected, on_order callback registered")

        # Get ticker for pricing
        ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
        ref_price = ticker.best_bid_price or ticker.mark_price or ticker.best_ask_price

        placed_order_ids = []

        # Place 5 orders, one every 5 seconds
        for i in range(NUM_ORDERS):
            # Vary price slightly for each order
            factor = 0.5 - i * 0.05  # 0.50, 0.45, 0.40, 0.35, 0.30
            price = round(ref_price * factor / TICK_SIZE) * TICK_SIZE
            if price <= 0:
                price = TICK_SIZE

            request = OrderRequest(
                instrument=INSTRUMENT,
                side=OrderSide.BUY,
                amount=0.1,
                order_type=OrderType.LIMIT,
                price=price,
                label=f"ws_test_{i}",
            )

            logger.info(f"[{i+1}/{NUM_ORDERS}] Placing order @ {price}")
            order = await client.place_order(EXCHANGE, request)
            placed_order_ids.append(order.order_id)
            logger.info(f"[{i+1}/{NUM_ORDERS}] Placed: id={order.order_id}")

            if i < NUM_ORDERS - 1:
                await asyncio.sleep(ORDER_INTERVAL)

        # Wait for final WS updates
        logger.info("All orders placed, waiting 3s for WS updates...")
        await asyncio.sleep(3)

        # Summary: check that every order received at least one WS update
        logger.info("=== WS Streaming Summary ===")
        all_received = True
        for oid in placed_order_ids:
            updates = ws_updates.get(oid, [])
            status = "OK" if updates else "MISSING"
            if not updates:
                all_received = False
            logger.info(f"  {oid}: {len(updates)} updates, statuses={updates} [{status}]")

        if all_received:
            logger.info(f"All {NUM_ORDERS} orders received WS updates")
        else:
            logger.error("Some orders did NOT receive WS updates!")

        # Cleanup: cancel all orders
        logger.info("Cancelling all orders...")
        for oid in placed_order_ids:
            await client.cancel_order(EXCHANGE, oid)

        # Wait for cancel WS updates
        await asyncio.sleep(3)

        # Final summary: verify cancel updates received
        logger.info("=== After Cancel ===")
        all_cancelled = True
        for oid in placed_order_ids:
            updates = ws_updates.get(oid, [])
            has_cancel = "cancelled" in updates
            if not has_cancel:
                all_cancelled = False
            logger.info(f"  {oid}: statuses={updates} [{'OK' if has_cancel else 'NO CANCEL'}]")

        if all_cancelled:
            logger.info("Test PASSED - all orders received open + cancelled WS updates")
        else:
            logger.error("Test FAILED - some orders missing cancel WS update")

    except Exception as e:
        logger.error(f"Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
