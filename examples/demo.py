#!/usr/bin/env python3
"""Demo script for Calais Execution Service.

Usage:
    1. Edit config.yaml with your Deribit API credentials
    2. python examples/demo.py
"""

import asyncio
import json
import os
import sys
from dataclasses import asdict
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from calais_order_execution import CalaisExecutionService, load_config
from calais_order_execution.models import Order, OrderRequest, OrderSide, OrderType
from calais_order_execution.util.logging import init_logging, get_logger

init_logging()
logger = get_logger(__name__)


class Strategy:
    """Example strategy that receives order updates from CalaisExecutionService."""

    def __init__(self, service: CalaisExecutionService):
        """Initialize strategy with execution service.

        Args:
            service: CalaisExecutionService instance for order operations.
        """
        self._service = service
        self._service.register_order_update_callback(self.on_order_update)

    def on_order_update(self, order: Order) -> None:
        """Handle order updates from the execution service.

        Args:
            order: Updated order from the exchange.
        """
        order_dict = asdict(order)
        # Convert enums to their values for JSON serialization
        for key, value in order_dict.items():
            if hasattr(value, "value"):
                order_dict[key] = value.value
            elif isinstance(value, datetime):
                order_dict[key] = value.isoformat()
        logger.info(f"[STRATEGY] Order Update: {json.dumps(order_dict, indent=2)}")

    async def place_order(
        self,
        exchange: str,
        instrument: str,
        side: OrderSide,
        amount: float,
        price: float,
        post_only: bool = False,
    ) -> Order:
        """Place an order through the execution service.

        Args:
            exchange: Exchange name (e.g., "deribit").
            instrument: Instrument name.
            side: Order side (BUY/SELL).
            amount: Order amount.
            price: Limit price.
            post_only: If True, order will only be maker.

        Returns:
            Created order.
        """
        request = OrderRequest(
            instrument=instrument,
            side=side,
            amount=amount,
            order_type=OrderType.LIMIT,
            price=price,
            post_only=post_only,
        )
        return await self._service.place_order(exchange, request)

    async def cancel_order(self, exchange: str, order_id: str) -> bool:
        """Cancel an order.

        Args:
            exchange: Exchange name.
            order_id: Order ID to cancel.

        Returns:
            True if cancellation was successful.
        """
        return await self._service.cancel_order(exchange, order_id)


async def main():
    config = load_config("config.yaml")
    service = CalaisExecutionService(config)
    strategy = Strategy(service)

    try:
        await service.start()
        logger.info("Service started. Press Ctrl+C to stop.")

        # Start hedge: buy symbol1 as maker, sell symbol2 as taker
        symbol1 = "BTC-30JAN26-100000-C"
        symbol2 = "BTC-30JAN26-100000-P"
        internal_id = service.place_order_hedge_deribit_options(
            symbol1=symbol1,
            symbol2=symbol2,
            side1=OrderSide.BUY,
            side2=OrderSide.SELL,
            amount=1,
            batch_amount=0.1,
            chase_maker_max_attempts_per_batch=10,
        )
        logger.info(f"Hedge started with internal_id: {internal_id}")

        while True:
            await asyncio.sleep(10)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
