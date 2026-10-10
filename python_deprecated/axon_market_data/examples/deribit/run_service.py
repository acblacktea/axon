"""
Deribit Options Data Service

Auto-discovers and subscribes to ALL unexpired Deribit options,
publishes orderbook/ticker/index_price via ZMQ.

Usage:
    python examples/deribit/run_service.py
"""
import asyncio
import logging

from axon_market_data.server import DeribitOptionsDataService
from axon_market_data.utils import setup_colored_logger


async def main():
    logger = setup_colored_logger(
        "deribit_options", level=logging.INFO, log_file="logs/deribit_options.log"
    )

    service = DeribitOptionsDataService(
        config_path="examples/deribit/config.yml",
        logger=logger,
    )

    try:
        await service.start()

        logger.info("=" * 50)
        logger.info("Deribit Options Data Service running")
        logger.info("  Config: examples/deribit/config.yml")
        logger.info("=" * 50)
        logger.info("Press Ctrl+C to stop...")

        await service.run_forever()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
