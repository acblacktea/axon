#!/usr/bin/env python3
"""Demo script for Calais Execution Service.

Mode 1 (in-process): python examples/demo.py --mode local
Mode 2 (ZMQ client): python examples/demo.py --mode zmq

For ZMQ mode, start the engine first:
    python -m calais_order_execution.engine --config config.yaml
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
    """Example strategy that works with both CalaisExecutionService and StrategyClient."""

    def __init__(self, client):
        """Initialize strategy.

        Args:
            client: CalaisExecutionService or StrategyClient instance.
        """
        self._client = client
        self._client.register_order_update_callback(self.on_order_update)

    def on_order_update(self, order: Order) -> None:
        """Handle order updates."""
        order_dict = asdict(order)
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
        """Place an order through the execution service."""
        request = OrderRequest(
            instrument=instrument,
            side=side,
            amount=amount,
            order_type=OrderType.LIMIT,
            price=price,
            post_only=post_only,
        )
        return await self._client.place_order(exchange, request)

    async def cancel_order(self, exchange: str, order_id: str) -> bool:
        """Cancel an order."""
        return await self._client.cancel_order(exchange, order_id)


async def run_local():
    """Run strategy in-process (original mode)."""
    config = load_config("config.yaml")
    service = CalaisExecutionService(config)
    strategy = Strategy(service)

    try:
        await service.start()
        logger.info("Service started (local mode). Press Ctrl+C to stop.")

        # Example: run hedge using client-side algorithm
        from calais_order_execution.client.algorithms import hedge_deribit_options

        internal_id = await hedge_deribit_options(
            client=service,
            symbol1="BTC-30JAN26-100000-C",
            symbol2="BTC-30JAN26-100000-P",
            side1=OrderSide.BUY,
            side2=OrderSide.SELL,
            amount=1,
            batch_amount=0.1,
        )
        logger.info(f"Hedge started with internal_id: {internal_id}")

        while True:
            await asyncio.sleep(10)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await service.stop()


async def run_zmq():
    """Run strategy as ZMQ client (multi-process mode).

    Requires engine to be running:
        python -m calais_order_execution.engine --config config.yaml
    """
    from calais_order_execution.client import StrategyClient
    from calais_order_execution.config import ZMQConfig

    zmq_config = ZMQConfig()
    client = StrategyClient(zmq_config, strategy_id="demo_strategy")

    try:
        await client.connect()
        strategy = Strategy(client)
        logger.info("Connected to engine (ZMQ mode). Press Ctrl+C to stop.")

        # Example: run hedge using client-side algorithm
        from calais_order_execution.client.algorithms import hedge_deribit_options

        internal_id = await hedge_deribit_options(
            client=client,
            symbol1="BTC-30JAN26-100000-C",
            symbol2="BTC-30JAN26-100000-P",
            side1=OrderSide.BUY,
            side2=OrderSide.SELL,
            amount=1,
            batch_amount=0.1,
        )
        logger.info(f"Hedge completed with internal_id: {internal_id}")

        while True:
            await asyncio.sleep(10)

    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await client.disconnect()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Calais Demo")
    parser.add_argument(
        "--mode", choices=["local", "zmq"], default="local",
        help="local = in-process, zmq = connect to engine via ZMQ",
    )
    args = parser.parse_args()

    if args.mode == "zmq":
        asyncio.run(run_zmq())
    else:
        asyncio.run(run_local())
