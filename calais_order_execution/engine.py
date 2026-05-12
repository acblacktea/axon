"""Engine process entry point.

Starts CalaisExecutionService (ZMQ transport auto-starts if configured).

Usage:
    python -m calais_order_execution.engine --config config.yaml
"""

import argparse
import asyncio
import signal

from calais_order_execution.config import load_config
from calais_order_execution.repository.account_postgres import PostgresAccountRepository
from calais_order_execution.repository.fill_postgres import PostgresFillRepository
from calais_order_execution.repository.order_postgres import PostgresOrderRepository
from calais_order_execution.repository.position_postgres import PostgresPositionRepository
from calais_order_execution.service import CalaisExecutionService
from calais_order_execution.util.logging import get_logger, init_logging
from calais_order_execution.util.metrics import get_metrics, init_metrics

logger = get_logger(__name__)


async def run_engine(config_path: str) -> None:
    """Run the engine process."""
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
    fill_repo = None

    if config.database:
        logger.info("Initializing PostgreSQL repositories...")
        pool = await PostgresOrderRepository.create_pool(config.database)
        order_repo = PostgresOrderRepository(pool)
        await order_repo.ensure_table()
        account_repo = PostgresAccountRepository(pool)
        await account_repo.ensure_table()
        position_repo = PostgresPositionRepository(pool)
        await position_repo.ensure_table()
        fill_repo = PostgresFillRepository(pool)
        await fill_repo.ensure_table()
        logger.info("PostgreSQL repositories initialized")

    service = CalaisExecutionService(
        config, order_repo, account_repo, position_repo, fill_repo
    )

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await service.start()
        logger.info("Engine running. Waiting for strategy connections...")
        await stop_event.wait()
    finally:
        logger.info("Shutting down engine...")
        await service.stop()
        if order_repo:
            await order_repo.close()
        get_metrics().stop_server()
        logger.info("Engine stopped")


def main():
    parser = argparse.ArgumentParser(description="Calais Execution Engine")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = parser.parse_args()

    init_logging(console_output=True)
    asyncio.run(run_engine(args.config))


if __name__ == "__main__":
    main()
