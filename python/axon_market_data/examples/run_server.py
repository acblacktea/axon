"""
Example: Run Market Data Server (config-driven)

Reads config.yml for ZMQ settings, exchange adapters, and subscriptions.

Usage:
    python examples/run_server.py
"""
import asyncio
import logging

from axon_market_data.server import MarketDataServer
from axon_market_data.utils import setup_colored_logger


async def main():
    logger = setup_colored_logger("mds_server", level=logging.INFO, log_file="logs/mds_server.log")

    server = MarketDataServer(
        config_path="examples/config.yml",
        logger=logger,
    )

    try:
        await server.start()

        logger.info("=" * 50)
        logger.info("Market Data Server running")
        logger.info("  Config: examples/config.yml")
        logger.info("=" * 50)
        logger.info("Press Ctrl+C to stop...")

        await server.run_forever()

    except KeyboardInterrupt:
        logger.info("Received shutdown signal...")
    finally:
        await server.stop()


if __name__ == "__main__":
    asyncio.run(main())
