"""Chase maker fill algorithm - runs client-side via StrategyClient."""

import asyncio
import time

from calais_order_execution.models.order import (
    Order, OrderRequest, OrderSide, OrderStatus, OrderType,
)
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


async def chase_maker_fill(
    client,
    exchange: str,
    instrument: str,
    side: OrderSide,
    amount: float,
    internal_order_id: str,
    max_attempt_second: float = 60,
    poll_interval: float = 0.2,
) -> float:
    """Chase maker fill by continuously placing limit orders at best price.

    Works with both StrategyClient (multi-process) and CalaisExecutionService
    (in-process). Both expose: place_order(), cancel_order(), get_order(),
    get_ticker(), modify_order().

    If time runs out, remaining amount is filled with a taker (market) order.

    Returns:
        Total filled amount.
    """
    current_order: Order | None = None
    start_time = time.time()

    while time.time() - start_time < max_attempt_second:
        ticker = await client.get_ticker(exchange, instrument)
        if side == OrderSide.BUY:
            price = ticker.best_ask_price
        else:
            price = ticker.best_bid_price

        if price <= 0:
            logger.warning(f"Invalid price {price} for {instrument}, retrying...")
            await asyncio.sleep(poll_interval)
            continue

        # Place order if none exists
        if current_order is None:
            try:
                request = OrderRequest(
                    instrument=instrument,
                    side=side,
                    amount=amount,
                    order_type=OrderType.LIMIT,
                    price=price,
                    internal_order_id=internal_order_id,
                )
                current_order = await client.place_order(exchange, request)
                logger.info(
                    f"Placed maker order {current_order.order_id}: "
                    f"{side.value} {amount} {instrument} @ {price}"
                )
            except Exception as e:
                logger.warning(f"Order rejected: {e}, retrying...")
                await asyncio.sleep(poll_interval)
                continue

        # Modify order if price changed
        if current_order.price != price:
            try:
                current_order = await client.modify_order(
                    exchange, current_order.order_id, price=price
                )
                current_order.internal_order_id = internal_order_id
                logger.info(f"Modified order {current_order.order_id} to price {price}")
            except Exception as e:
                logger.warning(f"Failed to modify order: {e}")

        # Check order status
        updated_order = await client.get_order(current_order.order_id)
        if updated_order:
            current_order = updated_order

            if current_order.status == OrderStatus.FILLED:
                logger.info(f"Maker order filled: {current_order.order_id}")
                return current_order.filled_amount
            elif current_order.status in (OrderStatus.CANCELLED, OrderStatus.REJECTED):
                logger.info(
                    f"Maker order {current_order.status.value}: {current_order.order_id}"
                )
                return current_order.filled_amount

        await asyncio.sleep(poll_interval)

    # Timeout: cancel maker order and get final status
    filled = 0.0
    if current_order:
        await client.cancel_order(exchange, current_order.order_id)
        logger.info(f"Cancelled unfilled maker order: {current_order.order_id}")
        final_order = await client.get_order(current_order.order_id)
        if final_order:
            current_order = final_order
        filled = current_order.filled_amount if current_order else 0.0

    # Place taker order for remaining amount
    remaining = amount - filled
    if remaining > 1e-12:
        logger.info(f"Maker chase timeout, placing taker order for remaining {remaining}")
        try:
            taker_request = OrderRequest(
                instrument=instrument,
                side=side,
                amount=remaining,
                order_type=OrderType.MARKET,
                internal_order_id=internal_order_id,
            )
            await client.place_order(exchange, taker_request)
            filled += remaining
        except Exception as e:
            logger.error(f"Failed to place taker order for remaining amount: {e}")

    logger.info(f"Maker chase ended: filled {filled}/{amount} after {max_attempt_second}s")
    return filled
