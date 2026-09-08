"""Test scenario: Buy/sell taker orders 3 rounds to verify position streaming."""

import asyncio
import signal

from axon_order_execution.client import StrategyClient
from axon_order_execution.config import ZMQConfig
from axon_order_execution.models import Order, OrderRequest, OrderSide, OrderType
from axon_order_execution.models.portfolio import Position
from axon_order_execution.util.logging import get_logger, init_logging

logger = get_logger(__name__)

EXCHANGE = "deribit"
INSTRUMENT = "BTC-27MAR26-70000-C"
TICK_SIZE = 0.0005
AMOUNT = 0.1
ROUNDS = 3


async def main() -> None:
    init_logging(console_output=True)

    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="test_ws_position")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Track updates
    position_updates: list[tuple[float, str]] = []  # (size, direction)

    def on_order_update(order: Order) -> None:
        logger.info(
            f"[ORDER] {order.order_id} {order.side.value} status={order.status.value} "
            f"filled={order.filled_amount}/{order.amount} avg={order.average_price} "
            f"liquidity={order.liquidity.value}"
        )

    def on_position_update(positions: list[Position]) -> None:
        for p in positions:
            if p.instrument == INSTRUMENT:
                position_updates.append((p.size, p.direction))
                logger.info(
                    f"[POSITION #{len(position_updates)}] {p.instrument}: "
                    f"size={p.size} dir={p.direction} delta={p.delta:.4f} "
                    f"pnl={p.total_profit_loss:.6f}"
                )

    try:
        await client.connect()
        client.register_order_update_callback(on_order_update)
        client.register_position_update_callback(on_position_update)
        logger.info("Connected")

        for i in range(ROUNDS):
            logger.info(f"=== Round {i+1}/{ROUNDS} ===")

            # Buy taker: place at ask price
            ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
            ask = ticker.best_ask_price
            if not ask or ask <= 0:
                ask = (ticker.mark_price or 0.05) * 1.5
            buy_price = round(ask / TICK_SIZE) * TICK_SIZE

            logger.info(f"BUY {AMOUNT} @ {buy_price} (taker)")
            buy_order = await client.place_order(EXCHANGE, OrderRequest(
                instrument=INSTRUMENT,
                side=OrderSide.BUY,
                amount=AMOUNT,
                order_type=OrderType.LIMIT,
                price=buy_price,
                label=f"pos_buy_{i}",
            ))
            logger.info(f"Buy placed: id={buy_order.order_id} status={buy_order.status.value}")

            await asyncio.sleep(2)

            # Sell taker: place at bid price
            ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
            bid = ticker.best_bid_price
            if not bid or bid <= 0:
                bid = (ticker.mark_price or 0.05) * 0.5
            sell_price = round(bid / TICK_SIZE) * TICK_SIZE
            if sell_price <= 0:
                sell_price = TICK_SIZE

            logger.info(f"SELL {AMOUNT} @ {sell_price} (taker)")
            sell_order = await client.place_order(EXCHANGE, OrderRequest(
                instrument=INSTRUMENT,
                side=OrderSide.SELL,
                amount=AMOUNT,
                order_type=OrderType.LIMIT,
                price=sell_price,
                label=f"pos_sell_{i}",
            ))
            logger.info(f"Sell placed: id={sell_order.order_id} status={sell_order.status.value}")

            await asyncio.sleep(2)

        # Wait for final position updates
        logger.info("Waiting 3s for final updates...")
        await asyncio.sleep(3)

        # Summary
        logger.info(f"=== Position Streaming Summary ===")
        logger.info(f"Total position updates: {len(position_updates)}")
        for idx, (size, direction) in enumerate(position_updates):
            logger.info(f"  #{idx+1}: size={size} dir={direction}")

        if len(position_updates) > 0:
            logger.info("Test PASSED - position streaming with fills working")
        else:
            logger.error("Test FAILED - no position updates received")

    except Exception as e:
        logger.error(f"Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
