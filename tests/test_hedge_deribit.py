"""Test scenario: hedge_deribit_options - buy C / sell P in batches, check positions."""

import asyncio
import signal

from calais_order_execution.client import StrategyClient
from calais_order_execution.client.algorithms.hedge_deribit import hedge_deribit_options
from calais_order_execution.config import ZMQConfig
from calais_order_execution.models import Order, OrderSide
from calais_order_execution.models.portfolio import Position
from calais_order_execution.util.logging import get_logger, init_logging

logger = get_logger(__name__)

EXCHANGE = "deribit"
SYMBOL1 = "BTC-27MAR26-70000-C"
SYMBOL2 = "BTC-27MAR26-70000-P"


async def main() -> None:
    init_logging(console_output=True)

    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="test_hedge")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    def on_order_update(order: Order) -> None:
        logger.info(
            f"[ORDER] {order.instrument} {order.side.value} "
            f"status={order.status.value} filled={order.filled_amount}/{order.amount} "
            f"avg={order.average_price} liquidity={order.liquidity.value}"
        )

    def on_position_update(positions: list[Position]) -> None:
        for p in positions:
            if p.instrument in (SYMBOL1, SYMBOL2):
                logger.info(
                    f"[POSITION] {p.instrument}: size={p.size} dir={p.direction} "
                    f"delta={p.delta:.4f} pnl={p.total_profit_loss:.6f}"
                )

    try:
        await client.connect()
        client.register_order_update_callback(on_order_update)
        client.register_position_update_callback(on_position_update)
        logger.info("Connected")

        # Run hedge algorithm
        logger.info(
            f"Starting hedge: BUY {SYMBOL1} / SELL {SYMBOL2}, "
            f"total=1, batch=0.1, chase_timeout=5s"
        )
        internal_id = await hedge_deribit_options(
            client=client,
            symbol1=SYMBOL1,
            symbol2=SYMBOL2,
            side1=OrderSide.BUY,
            side2=OrderSide.SELL,
            amount=1,
            batch_amount=0.1,
            chase_maker_max_attempt_second=5,
            internal_order_id="233",
        )
        logger.info(f"Hedge complete, internal_order_id={internal_id}")

        # Wait 3s then check positions
        logger.info("Waiting 3s for position updates...")
        await asyncio.sleep(3)

        positions = await client.get_positions(EXCHANGE, "BTC")
        logger.info("=== Final Positions ===")
        for p in positions:
            if p.instrument in (SYMBOL1, SYMBOL2):
                logger.info(
                    f"  {p.instrument}: size={p.size} dir={p.direction} "
                    f"delta={p.delta:.4f} pnl={p.total_profit_loss:.6f}"
                )

        logger.info("Test complete")

    except Exception as e:
        logger.error(f"Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
