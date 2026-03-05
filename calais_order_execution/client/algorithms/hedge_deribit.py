"""Deribit options hedge algorithm - runs client-side."""

import uuid

from calais_order_execution.client.algorithms.chase_maker import chase_maker_fill
from calais_order_execution.models.order import OrderRequest, OrderSide, OrderType
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


async def hedge_deribit_options(
    client,
    symbol1: str,
    symbol2: str,
    side1: OrderSide,
    side2: OrderSide,
    amount: float,
    batch_amount: float,
    chase_maker_max_attempt_second: float = 60,
    internal_order_id: str | None = None,
) -> str:
    """Run the Deribit options hedge algorithm.

    Buy symbol1 as maker, sell symbol2 as taker, in batches.

    Works with both StrategyClient (multi-process) and CalaisExecutionService
    (in-process).

    Returns:
        internal_order_id used for tracking.
    """
    exchange = "deribit"
    if internal_order_id is None:
        internal_order_id = uuid.uuid4().hex

    remaining_amount = amount

    logger.info(
        f"Starting hedge: {symbol1} (maker) / {symbol2} (taker), "
        f"total={amount}, batch={batch_amount}, internal_id={internal_order_id}"
    )

    try:
        while remaining_amount > 1e-12:
            current_batch = min(batch_amount, remaining_amount)

            logger.info(f"Batch: {side1.value} {current_batch} {symbol1} (maker)")
            maker_filled = await chase_maker_fill(
                client=client,
                exchange=exchange,
                instrument=symbol1,
                side=side1,
                amount=current_batch,
                internal_order_id=internal_order_id,
                max_attempt_second=chase_maker_max_attempt_second,
            )

            if maker_filled > 0:
                logger.info(f"Batch: {side2.value} {maker_filled} {symbol2} (taker)")
                taker_request = OrderRequest(
                    instrument=symbol2,
                    side=side2,
                    amount=maker_filled,
                    order_type=OrderType.MARKET,
                    internal_order_id=internal_order_id,
                )
                await client.place_order(exchange, taker_request)

            remaining_amount -= maker_filled
            logger.info(f"Batch complete. Filled: {maker_filled}, Remaining: {remaining_amount}")

        logger.info(f"Hedge {internal_order_id} complete")

    except Exception as e:
        logger.error(f"Hedge {internal_order_id} failed: {e}")

    return internal_order_id
