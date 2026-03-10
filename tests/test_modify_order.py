"""Test scenario: Place order, modify price and amount, verify changes."""

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
    client = StrategyClient(zmq_config, strategy_id="test_modify_order")

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
            label="test_modify",
        )

        logger.info(f"Placing order: BUY {request.amount} {INSTRUMENT} @ {buy_price}")
        order = await client.place_order(EXCHANGE, request)
        logger.info(f"Order placed: id={order.order_id}, status={order.status.value}")

        # Step 2: Wait for WS update, then get order before modify
        await asyncio.sleep(2)
        before = await client.get_order(order.order_id)
        assert before is not None, "Order not found"
        logger.info(
            f"Before modify:\n"
            f"  price:  {before.price}\n"
            f"  amount: {before.amount}\n"
            f"  status: {before.status.value}"
        )

        # Step 3: Modify price and amount
        new_price = round(ref_price * 0.4 / tick_size) * tick_size
        if new_price <= 0:
            new_price = tick_size
        new_amount = 0.2

        logger.info(f"Modifying order: price {before.price} -> {new_price}, amount {before.amount} -> {new_amount}")
        modified = await client.modify_order(
            EXCHANGE, order.order_id, amount=new_amount, price=new_price
        )
        logger.info(f"Modify returned: price={modified.price}, amount={modified.amount}")

        # Step 4: Wait for WS update, then get order after modify
        await asyncio.sleep(2)
        after = await client.get_order(order.order_id)
        assert after is not None, "Order not found after modify"
        logger.info(
            f"After modify:\n"
            f"  order_id:          {after.order_id}\n"
            f"  price:             {after.price}\n"
            f"  amount:            {after.amount}\n"
            f"  status:            {after.status.value}\n"
            f"  internal_order_id: {after.internal_order_id}\n"
            f"  strategy_id:       {after.strategy_id}"
        )

        # Verify changes
        assert abs(after.price - new_price) < tick_size, f"Price not updated: expected {new_price}, got {after.price}"
        assert abs(after.amount - new_amount) < 1e-9, f"Amount not updated: expected {new_amount}, got {after.amount}"
        logger.info("Price and amount verified OK")

        # Step 5: Cleanup
        await client.cancel_order(EXCHANGE, order.order_id)
        logger.info("Order cancelled, cleanup done")

        logger.info("Test PASSED - modify order scenario complete")

    except Exception as e:
        logger.error(f"Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
