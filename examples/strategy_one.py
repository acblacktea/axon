#!/usr/bin/env python3
"""Strategy 1 - Place/cancel orders in a loop, print all streaming updates as JSON.

Usage (engine must be running first):
    python examples/strategy_one.py
"""

import asyncio
import json
import signal
from dataclasses import asdict
from datetime import datetime

from calais_order_execution.client import StrategyClient
from calais_order_execution.config import ZMQConfig
from calais_order_execution.models import (
    AccountSummary,
    Order,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
)
from calais_order_execution.util.logging import get_logger, init_logging

init_logging(console_output=True)
logger = get_logger(__name__)

EXCHANGE = "deribit"
INSTRUMENT = "BTC-25SEP26-260000-P"
TICK_SIZE = 0.0005


def _to_json(obj) -> str:
    """Convert a dataclass to a single-line JSON string."""
    d = asdict(obj)

    def _default(o):
        if isinstance(o, datetime):
            return o.isoformat()
        if isinstance(o, (set, frozenset)):
            return list(o)
        return str(o)

    return json.dumps(d, default=_default, ensure_ascii=False)


class Strategy:
    def __init__(self, client: StrategyClient):
        self._client = client
        self._client.register_order_update_callback(self._on_order_update)
        self._client.register_account_update_callback(self._on_account_update)
        self._client.register_position_update_callback(self._on_position_update)

    def _on_order_update(self, order: Order) -> None:
        logger.info(f"[S1 ORDER] {_to_json(order)}")

    def _on_account_update(self, summary: AccountSummary) -> None:
        logger.info(f"[S1 ACCOUNT] {_to_json(summary)}")

    def _on_position_update(self, positions: list[Position]) -> None:
        for p in positions:
            logger.info(f"[S1 POSITION] {_to_json(p)}")

    async def clear(self) -> None:
        """Cancel all orders and flatten position for INSTRUMENT before running."""
        logger.info(f"[S1] Clearing orders and positions for {INSTRUMENT}")

        # Cancel all active orders for this instrument
        active_orders = await self._client.get_active_orders()
        for order in active_orders:
            if order.instrument == INSTRUMENT:
                logger.info(f"[S1] Cancelling order {order.order_id}")
                await self._client.cancel_order(EXCHANGE, order.order_id)

        # Flatten position if any
        positions = await self._client.get_positions(EXCHANGE)
        for pos in positions:
            if pos.instrument == INSTRUMENT and pos.size != 0:
                side = OrderSide.SELL if pos.direction == "buy" else OrderSide.BUY
                amount = abs(pos.size)
                logger.info(f"[S1] Flattening position: {side.value} {amount} {INSTRUMENT}")
                await self._client.place_order(EXCHANGE, OrderRequest(
                    instrument=INSTRUMENT,
                    side=side,
                    amount=amount,
                    order_type=OrderType.MARKET,
                    label="s1_clear",
                ))

        logger.info("[S1] Clear done")

    async def run(self) -> None:
        logger.info("[S1] Strategy 1 started")

        while True:
            try:
                # Buy at bid price (买一)
                ticker = await self._client.get_ticker(EXCHANGE, INSTRUMENT)
                bid = ticker.best_bid_price
                if not bid or bid <= 0:
                    bid = (ticker.mark_price or 0.05) * 0.95
                buy_price = round(bid / TICK_SIZE) * TICK_SIZE

                logger.info(f"[S1] BUY 0.1 @ {buy_price} (bid)")
                buy_order = await self._client.place_order(EXCHANGE, OrderRequest(
                    instrument=INSTRUMENT,
                    side=OrderSide.BUY,
                    amount=0.1,
                    order_type=OrderType.LIMIT,
                    price=buy_price,
                    label="s1_buy",
                ))
                logger.info(f"[S1] Buy placed id={buy_order.order_id}")

                # Wait 2s then cancel
                await asyncio.sleep(2)
                buy_latest = await self._client.get_order(buy_order.order_id)
                if buy_latest and buy_latest.status not in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.REJECTED):
                    logger.info(f"[S1] Buy not filled, cancelling {buy_order.order_id}")
                    await self._client.cancel_order(EXCHANGE, buy_order.order_id)


            except Exception as e:
                logger.error(f"[S1] Error: {e}")

            # Wait 3s before next round
            await asyncio.sleep(3)


async def main() -> None:
    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="strategy_one")

    stop_event = asyncio.Event()

    def _signal_handler():
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await client.connect()
        logger.info("[S1] Connected to engine")

        strategy = Strategy(client)

        await strategy.clear()

        strategy_task = asyncio.create_task(strategy.run())
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [strategy_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )
        for t in pending:
            t.cancel()

    except KeyboardInterrupt:
        pass
    finally:
        await client.disconnect()
        logger.info("[S1] Disconnected")


if __name__ == "__main__":
    asyncio.run(main())
