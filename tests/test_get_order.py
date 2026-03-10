"""Test scenario: Place order then get order and print all fields."""

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
    client = StrategyClient(zmq_config, strategy_id="test_get_order")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await client.connect()
        logger.info("Connected to engine")

        # Place an order
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
            label="test_get",
        )

        order = await client.place_order(EXCHANGE, request)
        logger.info(f"Order placed: {order.order_id}")

        # Wait for WS update
        await asyncio.sleep(2)

        # Get order and print all fields
        fetched = await client.get_order(order.order_id)
        if fetched:
            logger.info(
                f"=== Order Details ===\n"
                f"  order_id:          {fetched.order_id}\n"
                f"  exchange:          {fetched.exchange}\n"
                f"  instrument:        {fetched.instrument}\n"
                f"  side:              {fetched.side.value}\n"
                f"  order_type:        {fetched.order_type.value}\n"
                f"  amount:            {fetched.amount}\n"
                f"  price:             {fetched.price}\n"
                f"  status:            {fetched.status.value}\n"
                f"  filled_amount:     {fetched.filled_amount}\n"
                f"  average_price:     {fetched.average_price}\n"
                f"  internal_order_id: {fetched.internal_order_id}\n"
                f"  client_order_id:   {fetched.client_order_id}\n"
                f"  label:             {fetched.label}\n"
                f"  liquidity:         {fetched.liquidity.value}\n"
                f"  post_only:         {fetched.post_only}\n"
                f"  reject_post_only:  {fetched.reject_post_only}\n"
                f"  strategy_id:       {fetched.strategy_id}\n"
                f"  created_at:        {fetched.created_at}\n"
                f"  updated_at:        {fetched.updated_at}"
            )
        else:
            logger.error(f"Order {order.order_id} not found!")

        # Cleanup: cancel the order
        await client.cancel_order(EXCHANGE, order.order_id)
        logger.info("Order cancelled, cleanup done")

        logger.info("Test PASSED - get order scenario complete")

    except Exception as e:
        logger.error(f"Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
