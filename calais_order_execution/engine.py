"""Engine process entry point.

Starts CalaisExecutionService + ZMQ Transport Server.

Usage:
    python -m calais_order_execution.engine --config config.yaml
"""

import argparse
import asyncio
import signal

from calais_order_execution.config import load_config
from calais_order_execution.service import CalaisExecutionService
from calais_order_execution.transport.server import ZMQTransportServer
from calais_order_execution.util.logging import get_logger, init_logging

logger = get_logger(__name__)


async def run_engine(config_path: str) -> None:
    """Run the engine process."""
    config = load_config(config_path)
    service = CalaisExecutionService(config)
    transport = ZMQTransportServer(config.zmq, service)

    stop_event = asyncio.Event()

    def _signal_handler():
        logger.info("Shutdown signal received")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    try:
        await service.start()
        await transport.start()
        logger.info("Engine running. Waiting for strategy connections...")
        await stop_event.wait()
    finally:
        logger.info("Shutting down engine...")
        await transport.stop()
        await service.stop()
        logger.info("Engine stopped")


def main():
    parser = argparse.ArgumentParser(description="Calais Execution Engine")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML")
    args = parser.parse_args()

    init_logging(console_output=True)
    asyncio.run(run_engine(args.config))


if __name__ == "__main__":
    main()
