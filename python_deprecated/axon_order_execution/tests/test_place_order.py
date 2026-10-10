"""Test scenario: Place order on BTC option."""

import asyncio
import signal

from axon_order_execution.client.strategy_client import StrategyClient
from axon_order_execution.config import ZMQConfig
from axon_order_execution.models.order import OrderRequest, OrderSide, OrderType
from axon_order_execution.util.logging import get_logger, init_logging

logger = get_logger(__name__)

EXCHANGE = "deribit"
# Deribit option format: BTC-{DMON}{YEAR}-{STRIKE}-{C/P}
INSTRUMENT = "BTC-27MAR26-70000-C"


async def main() -> None:
    init_logging(console_output=True)

    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="test_place_order")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await client.connect()
        logger.info("Connected to engine")

        # Get ticker to determine a reasonable price
        ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
        logger.info(
            f"Ticker for {INSTRUMENT}: "
            f"best_bid={ticker.best_bid_price}, best_ask={ticker.best_ask_price}, "
            f"mark={ticker.mark_price}"
        )

        # Place a limit buy order well below market so it won't fill
        # Use mark_price as fallback when best_bid is 0 (no bids on illiquid options)
        ref_price = ticker.best_bid_price or ticker.mark_price or ticker.best_ask_price
        tick_size = 0.0005  # BTC option tick size on Deribit
        buy_price = round(ref_price * 0.5 / tick_size) * tick_size
        if buy_price <= 0:
            buy_price = tick_size
        request = OrderRequest(
            instrument=INSTRUMENT,
            side=OrderSide.BUY,
            amount=0.1,  # 0.1 BTC notional
            order_type=OrderType.LIMIT,
            price=buy_price,
            label="test_place",
        )

        logger.info(f"Placing order: BUY {request.amount} {INSTRUMENT} @ {buy_price}")
        order = await client.place_order(EXCHANGE, request)
        logger.info(
            f"Order placed successfully!\n"
            f"  order_id: {order.order_id}\n"
            f"  status: {order.status.value}\n"
            f"  instrument: {order.instrument}\n"
            f"  side: {order.side.value}\n"
            f"  price: {order.price}\n"
            f"  amount: {order.amount}"
        )

        # Wait a bit for WS updates, then check order
        await asyncio.sleep(2)

        fetched = await client.get_order(order.order_id)
        if fetched:
            logger.info(
                f"Fetched order from cache: status={fetched.status.value}, "
                f"filled={fetched.filled_amount}/{fetched.amount}"
            )

        '''
        # Cancel the order
        cancelled = await client.cancel_order(EXCHANGE, order.order_id)
        logger.info(f"Cancel result: {cancelled}")
        '''
        logger.info("Test PASSED - place order scenario complete")

    except Exception as e:
        logger.error(f"Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
