#!/usr/bin/env python3
"""Engine process - starts AxonExecutionService (with ZMQ transport).

Terminal 1:
    python examples/run_engine.py --config config.yaml

Then start strategies in separate terminals:
    python examples/strategy_one.py
    python examples/strategy_two.py
"""

import argparse
import asyncio
import signal

from axon_order_execution.config import load_config
from axon_order_execution.repository.order_postgres import PostgresOrderRepository
from axon_order_execution.repository.account_postgres import PostgresAccountRepository
from axon_order_execution.repository.position_postgres import PostgresPositionRepository
from axon_order_execution.service import AxonExecutionService
from axon_order_execution.util.logging import get_logger, init_logging
from axon_order_execution.util.metrics import init_metrics

init_logging(console_output=True)
logger = get_logger(__name__)


async def main(config_path: str) -> None:
    config = load_config(config_path)

    init_metrics(config.metrics)
    if config.metrics.enabled:
        logger.info(
            f"Metrics endpoint listening on "
            f"http://{config.metrics.host}:{config.metrics.port}/metrics"
        )

    order_repo = None
    account_repo = None
    position_repo = None

    if config.database:
        logger.info("Initializing PostgreSQL repositories...")
        pool = await PostgresOrderRepository.create_pool(config.database)
        order_repo = PostgresOrderRepository(pool)
        await order_repo.ensure_table()
        account_repo = PostgresAccountRepository(pool)
        await account_repo.ensure_table()
        position_repo = PostgresPositionRepository(pool)
        await position_repo.ensure_table()
        logger.info("PostgreSQL repositories initialized")

    service = AxonExecutionService(config, order_repo, account_repo, position_repo)

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await service.start()

        logger.info("=" * 60)
        logger.info("Engine running")
        logger.info(f"  ROUTER: {config.zmq.router_endpoint}")
        logger.info(f"  PUB:    {config.zmq.pub_endpoint}")
        logger.info("Waiting for strategy connections... (Ctrl+C to stop)")
        logger.info("=" * 60)

        await stop_event.wait()
    finally:
        logger.info("Shutting down engine...")
        await service.stop()
        if order_repo:
            await order_repo.close()
        logger.info("Engine stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Axon Execution Engine")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = parser.parse_args()
    asyncio.run(main(args.config))
