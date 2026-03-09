#!/usr/bin/env python3
"""Engine process - starts CalaisExecutionService (with ZMQ transport).

Terminal 1:
    python examples/run_engine.py --config config.yaml

Then start strategies in separate terminals:
    python examples/strategy_one.py
    python examples/strategy_two.py
"""

import argparse
import asyncio
import signal

from calais_order_execution.config import load_config
from calais_order_execution.service import CalaisExecutionService
from calais_order_execution.util.logging import get_logger, init_logging

init_logging(console_output=True)
logger = get_logger(__name__)


async def main(config_path: str) -> None:
    config = load_config(config_path)
    service = CalaisExecutionService(config)

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
        logger.info("Engine stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Calais Execution Engine")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = parser.parse_args()
    asyncio.run(main(args.config))
