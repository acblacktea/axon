"""EMS Service - manages all exchange EMS clients."""

from calais_order_execution.config import Config
from calais_order_execution.ems.base import BaseEMS
from calais_order_execution.ems.deribit import DeribitEMS
from calais_order_execution.models import Order, OrderRequest, Ticker
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class EMSService:
    """Manages all EMS clients for different exchanges."""

    def __init__(self, config: Config):
        """Initialize EMS service.

        Args:
            config: Service configuration.
        """
        self._config = config
        self._ems: dict[str, BaseEMS] = {}
        self._init_clients()

    def _init_clients(self) -> None:
        """Initialize EMS clients for configured exchanges."""
        for name, exchange_config in self._config.exchanges.items():
            if name == "deribit":
                self._ems[name] = DeribitEMS(exchange_config)
            else:
                logger.warning(f"Unsupported exchange: {name}")

    async def start(self) -> None:
        """Start all EMS clients."""
        for name, ems in self._ems.items():
            await ems.start()
            logger.info(f"Started EMS: {name}")

    async def stop(self) -> None:
        """Stop all EMS clients."""
        for name, ems in self._ems.items():
            await ems.stop()
            logger.info(f"Stopped EMS: {name}")

    def get(self, exchange: str) -> BaseEMS | None:
        """Get EMS client by exchange name."""
        return self._ems.get(exchange)

    def has(self, exchange: str) -> bool:
        """Check if exchange is supported."""
        return exchange in self._ems

    async def place_order(self, exchange: str, request: OrderRequest) -> Order:
        """Place an order on the specified exchange."""
        if exchange not in self._ems:
            raise ValueError(f"Unsupported exchange: {exchange}")
        return await self._ems[exchange].place_order(request)

    async def cancel_order(self, exchange: str, order_id: str) -> bool:
        """Cancel an order."""
        if exchange not in self._ems:
            raise ValueError(f"Unsupported exchange: {exchange}")
        return await self._ems[exchange].cancel_order(order_id)

    async def get_order(self, exchange: str, order_id: str) -> Order | None:
        """Get order by ID from exchange."""
        if exchange not in self._ems:
            raise ValueError(f"Unsupported exchange: {exchange}")
        return await self._ems[exchange].get_order(order_id)

    async def get_ticker(self, exchange: str, instrument: str) -> Ticker:
        """Get ticker data for an instrument."""
        if exchange not in self._ems:
            raise ValueError(f"Unsupported exchange: {exchange}")
        ems = self._ems[exchange]
        if not hasattr(ems, "get_ticker"):
            raise NotImplementedError(f"Exchange {exchange} does not support get_ticker")
        return await ems.get_ticker(instrument)

    async def modify_order(
        self,
        exchange: str,
        order_id: str,
        amount: float | None = None,
        price: float | None = None,
    ) -> Order:
        """Modify an existing order."""
        if exchange not in self._ems:
            raise ValueError(f"Unsupported exchange: {exchange}")
        return await self._ems[exchange].modify_order(order_id, amount=amount, price=price)

    async def __aenter__(self) -> "EMSService":
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.stop()
