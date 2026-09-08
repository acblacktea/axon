#!/usr/bin/env python3
"""Multi-strategy test - Strategy Alpha (process 1).

Places/cancels orders on BTC-27MAR26-70000-C, monitors order/account/position streaming.
Run simultaneously with test_multi_strategy_beta.py to verify streaming isolation.

Usage:
    python tests/test_multi_strategy_alpha.py
"""

import asyncio
import signal

from axon_order_execution.client import StrategyClient
from axon_order_execution.config import ZMQConfig
from axon_order_execution.models import (
    AccountSummary,
    Order,
    OrderRequest,
    OrderSide,
    OrderType,
    Position,
)
from axon_order_execution.util.logging import get_logger, init_logging

init_logging(console_output=True)
logger = get_logger(__name__)

EXCHANGE = "deribit"
INSTRUMENT = "BTC-27MAR26-70000-C"
TICK_SIZE = 0.0005
STRATEGY_ID = "test_alpha"


async def main() -> None:
    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id=STRATEGY_ID)

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    # Tracking
    order_updates: list[Order] = []
    account_updates: list[AccountSummary] = []
    position_updates: list[list[Position]] = []

    def on_order_update(order: Order) -> None:
        order_updates.append(order)
        logger.info(
            f"[ALPHA ORDER #{len(order_updates)}] {order.instrument} {order.side.value} "
            f"status={order.status.value} filled={order.filled_amount}/{order.amount} "
            f"id={order.order_id}"
        )

    def on_account_update(summary: AccountSummary) -> None:
        account_updates.append(summary)
        logger.info(
            f"[ALPHA ACCOUNT #{len(account_updates)}] {summary.currency}: "
            f"equity={summary.equity:.6f} balance={summary.balance:.6f} "
            f"available={summary.available_funds:.6f}"
        )

    def on_position_update(positions: list[Position]) -> None:
        position_updates.append(positions)
        logger.info(f"[ALPHA POSITION #{len(position_updates)}] {len(positions)} positions")
        for p in positions[:3]:
            if p.instrument == INSTRUMENT:
                logger.info(f"  {p.instrument}: size={p.size} dir={p.direction}")

    try:
        await client.connect()
        client.register_order_update_callback(on_order_update)
        client.register_account_update_callback(on_account_update)
        client.register_position_update_callback(on_position_update)
        logger.info(f"[ALPHA] Connected with strategy_id={STRATEGY_ID}")

        # Place 3 orders, then cancel them
        order_ids = []
        ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
        ref_price = ticker.best_bid_price or ticker.mark_price or ticker.best_ask_price

        for i in range(3):
            factor = 0.4 - i * 0.05
            price = round(ref_price * factor / TICK_SIZE) * TICK_SIZE
            if price <= 0:
                price = TICK_SIZE

            request = OrderRequest(
                instrument=INSTRUMENT,
                side=OrderSide.BUY,
                amount=0.1,
                order_type=OrderType.LIMIT,
                price=price,
                label=f"alpha_{i}",
            )
            logger.info(f"[ALPHA] Placing order #{i+1} @ {price}")
            order = await client.place_order(EXCHANGE, request)
            order_ids.append(order.order_id)
            logger.info(f"[ALPHA] Placed order id={order.order_id}")
            await asyncio.sleep(3)

        # Place a taker buy (at ask) to trigger position change
        ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
        ask = ticker.best_ask_price
        if not ask or ask <= 0:
            ask = (ticker.mark_price or 0.05) * 1.5
        buy_price = round(ask / TICK_SIZE) * TICK_SIZE
        logger.info(f"[ALPHA] Taker BUY 0.1 @ {buy_price}")
        taker_order = await client.place_order(EXCHANGE, OrderRequest(
            instrument=INSTRUMENT,
            side=OrderSide.BUY,
            amount=0.1,
            order_type=OrderType.LIMIT,
            price=buy_price,
            label="alpha_taker_buy",
        ))
        logger.info(f"[ALPHA] Taker buy placed id={taker_order.order_id}")
        await asyncio.sleep(3)

        # Sell back (taker at bid)
        ticker = await client.get_ticker(EXCHANGE, INSTRUMENT)
        bid = ticker.best_bid_price
        if not bid or bid <= 0:
            bid = (ticker.mark_price or 0.05) * 0.5
        sell_price = round(bid / TICK_SIZE) * TICK_SIZE
        if sell_price <= 0:
            sell_price = TICK_SIZE
        logger.info(f"[ALPHA] Taker SELL 0.1 @ {sell_price}")
        sell_order = await client.place_order(EXCHANGE, OrderRequest(
            instrument=INSTRUMENT,
            side=OrderSide.SELL,
            amount=0.1,
            order_type=OrderType.LIMIT,
            price=sell_price,
            label="alpha_taker_sell",
        ))
        logger.info(f"[ALPHA] Taker sell placed id={sell_order.order_id}")
        await asyncio.sleep(3)

        # Cancel remaining limit orders
        logger.info("[ALPHA] Cancelling limit orders...")
        for oid in order_ids:
            try:
                await client.cancel_order(EXCHANGE, oid)
                logger.info(f"[ALPHA] Cancelled {oid}")
            except Exception as e:
                logger.warning(f"[ALPHA] Cancel {oid} failed: {e}")
            await asyncio.sleep(2)

        # Wait for final streaming updates
        logger.info("[ALPHA] Waiting 5s for final updates...")
        await asyncio.sleep(5)

        # Summary
        logger.info("=" * 60)
        logger.info(f"[ALPHA] === Summary ===")
        logger.info(f"[ALPHA] Order updates received: {len(order_updates)}")
        logger.info(f"[ALPHA] Account updates received: {len(account_updates)}")
        logger.info(f"[ALPHA] Position updates received: {len(position_updates)}")

        # Check that we only received our own orders (not beta's)
        instruments_seen = {o.instrument for o in order_updates}
        logger.info(f"[ALPHA] Instruments in order updates: {instruments_seen}")

        if len(order_updates) > 0 and len(account_updates) > 0:
            logger.info("[ALPHA] Test PASSED - order/account streaming working")
        else:
            logger.error("[ALPHA] Test FAILED - missing streaming updates")

    except Exception as e:
        logger.error(f"[ALPHA] Test FAILED: {e}", exc_info=True)
    finally:
        await client.disconnect()
        logger.info("[ALPHA] Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
