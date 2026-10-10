"""
Example: Run Deribit Options Data Service

Reads deribit_options_config.yml for ZMQ and adapter settings.
Auto-discovers and subscribes to ALL unexpired options.

Usage:
    python examples/run_deribit_options_service.py
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
        config_path="examples/deribit_options_config.yml",
        logger=logger,
    )

    try:
        await service.start()

        logger.info("=" * 50)
        logger.info("Deribit Options Data Service running")
        logger.info("  Config: examples/deribit_options_config.yml")
        logger.info("=" * 50)
        logger.info("Press Ctrl+C to stop...")

        await service.run_forever()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    finally:
        await service.stop()


if __name__ == "__main__":
    asyncio.run(main())
