"""
ZMQ-based Market Data Servers

MarketDataServer: Full-featured market data server with ZMQ publishing.
    - Mode 1: config subscriptions section drives what to subscribe on start
    - Mode 2: programmatic subscribe / unsubscribe after start

DeribitOptionsDataService: Built on MarketDataServer, auto-discovers and
    subscribes to ALL unexpired Deribit options.

Both read config.yml for ZMQ and exchange adapter settings.
"""
import asyncio
import logging
from typing import Dict, Optional, List, Set

from .core.models import (
    DataType, MarketDataEvent,
)
from .core.adapter import ExchangeAdapter, MarketDataCallback
from .core.registry import create_adapter, load_config
from .core.zmq_transport import ZmqPublisher
from .utils import metrics


# ── MarketDataServer ────────────────────────────────────────────────────


class MarketDataServer:
    def __init__(
        self,
        config_path: str,
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger("mds.server")
        self._config = load_config(config_path)

        # ZMQ
        zmq_config = self._config.get("zmq", {})
        self._pub_address = zmq_config.get("pub_address", "tcp://*:5558")
        self._publisher = ZmqPublisher(self._pub_address, logger=self.logger)

        # Prometheus metrics endpoint (optional)
        metrics_config = self._config.get("metrics", {})
        self._metrics_port: Optional[int] = metrics_config.get("port")
        self._metrics_addr: str = metrics_config.get("addr", "0.0.0.0")

        # Exchange adapters
        self._adapters: Dict[str, ExchangeAdapter] = {}

        # User callbacks (in addition to ZMQ publishing)
        self._callbacks_by_type: Dict[DataType, List[MarketDataCallback]] = {
            dt: [] for dt in DataType
        }

        self._is_running: bool = False
        self._stop_event = asyncio.Event()

    # ── Exchange management ─────────────────────────────────────────────

    @property
    def exchanges(self) -> List[str]:
        return list(self._adapters.keys())

    @property
    def is_running(self) -> bool:
        return self._is_running

    def get_adapter(self, exchange: str) -> Optional[ExchangeAdapter]:
        return self._adapters.get(exchange.lower())

    async def add_exchange(self, adapter: ExchangeAdapter) -> None:
        exchange = adapter.exchange_name.lower()

        if exchange in self._adapters:
            self.logger.warning(f"Exchange {exchange} already registered, replacing")
            await self.remove_exchange(exchange)

        for data_type in adapter.SUPPORTED_DATA_TYPES:
            adapter.add_callback_for_type(data_type, self._on_market_data_event)

        self._adapters[exchange] = adapter
        self.logger.info(
            f"Added exchange: {exchange} "
            f"(supports: {[dt.value for dt in adapter.SUPPORTED_DATA_TYPES]})"
        )

    async def remove_exchange(self, exchange: str) -> None:
        exchange = exchange.lower()
        if exchange not in self._adapters:
            return

        adapter = self._adapters.pop(exchange)
        for data_type in adapter.SUPPORTED_DATA_TYPES:
            adapter.remove_callback_for_type(data_type, self._on_market_data_event)

        if adapter.is_connected:
            await adapter.disconnect()
        self.logger.info(f"Removed exchange: {exchange}")

    # ── Connection ──────────────────────────────────────────────────────

    async def connect(self, exchange: Optional[str] = None) -> None:
        self._is_running = True
        if exchange:
            adapter = self.get_adapter(exchange)
            if adapter:
                await adapter.connect()
        else:
            for adapter in self._adapters.values():
                await adapter.connect()

    async def disconnect(self, exchange: Optional[str] = None) -> None:
        if exchange:
            adapter = self.get_adapter(exchange)
            if adapter:
                await adapter.disconnect()
        else:
            for adapter in self._adapters.values():
                await adapter.disconnect()
            self._is_running = False

    # ── Subscription ────────────────────────────────────────────────────

    async def subscribe(
        self,
        exchange: str,
        symbols: List[str],
        data_type: DataType = DataType.ORDERBOOK,
        **kwargs,
    ) -> None:
        adapter = self.get_adapter(exchange)
        if not adapter:
            raise ValueError(f"Unknown exchange: {exchange}")
        if not adapter.supports_data_type(data_type):
            raise ValueError(
                f"Exchange {exchange} does not support {data_type.value}. "
                f"Supported: {[dt.value for dt in adapter.SUPPORTED_DATA_TYPES]}"
            )
        if not adapter.is_connected:
            await adapter.connect()

        exchange_symbols = adapter.convert_symbols(symbols)
        await adapter.subscribe(data_type, exchange_symbols, **kwargs)
        metrics.subscribed_symbols.labels(
            exchange=exchange.lower(), data_type=data_type.value
        ).inc(len(symbols))

    async def unsubscribe(
        self,
        exchange: str,
        symbols: List[str],
        data_type: DataType = DataType.ORDERBOOK,
    ) -> None:
        adapter = self.get_adapter(exchange)
        if adapter:
            exchange_symbols = adapter.convert_symbols(symbols)
            await adapter.unsubscribe(data_type, exchange_symbols)
            metrics.subscribed_symbols.labels(
                exchange=exchange.lower(), data_type=data_type.value
            ).dec(len(symbols))

    async def _apply_subscriptions(self) -> None:
        """Subscribe to symbols from the config subscriptions section."""
        subscriptions = self._config.get("subscriptions", {})

        for exchange_name, data_types in subscriptions.items():
            if not isinstance(data_types, dict):
                continue
            for data_type_str, symbols in data_types.items():
                if not symbols:
                    continue
                try:
                    data_type = DataType(data_type_str)
                except ValueError:
                    self.logger.warning(f"Unknown data type: {data_type_str}, skipping")
                    continue

                if isinstance(symbols, str):
                    symbols = [symbols]

                self.logger.info(
                    f"Subscribing {exchange_name}/{data_type_str}: {len(symbols)} symbols"
                )
                try:
                    await self.subscribe(exchange_name, symbols, data_type)
                except Exception as e:
                    self.logger.error(
                        f"Failed to subscribe {exchange_name}/{data_type_str}: {e}",
                        exc_info=True,
                    )

    # ── Callbacks ───────────────────────────────────────────────────────

    def on_market_data(self, data_type: DataType, callback: MarketDataCallback) -> None:
        if callback not in self._callbacks_by_type[data_type]:
            self._callbacks_by_type[data_type].append(callback)

    def remove_market_data_callback(
        self, data_type: DataType, callback: MarketDataCallback
    ) -> None:
        if callback in self._callbacks_by_type[data_type]:
            self._callbacks_by_type[data_type].remove(callback)

    async def _on_market_data_event(self, event: MarketDataEvent) -> None:
        """Publish to ZMQ + forward to registered callbacks."""
        await self._publisher.send(event)

        for callback in self._callbacks_by_type[event.data_type]:
            try:
                await callback(event)
            except Exception as e:
                self.logger.error(f"Callback error for {event.data_type}: {e}", exc_info=True)

    async def start(self) -> None:
        """
        Start the server:
        1. Bind ZMQ PUB socket
        2. Create and register exchange adapters from config
        3. Connect to all exchanges
        4. Subscribe from config subscriptions section (if any)
        """
        self._publisher.start()

        # Prometheus metrics endpoint
        if self._metrics_port:
            metrics.start_metrics_server(self._metrics_port, self._metrics_addr)
            asyncio.create_task(metrics.process_metrics_loop())
            self.logger.info(
                f"Metrics endpoint: http://{self._metrics_addr}:{self._metrics_port}/metrics"
            )

        # Create adapters from config
        exchanges_config = self._config.get("exchanges", {})
        for exchange_name, exchange_conf in exchanges_config.items():
            if not isinstance(exchange_conf, dict):
                continue
            try:
                adapter = create_adapter(exchange_name, exchange_conf, logger=self.logger)
                await self.add_exchange(adapter)
            except Exception as e:
                self.logger.error(f"Failed to create adapter for {exchange_name}: {e}", exc_info=True)

        # Connect all exchanges
        await self.connect()

        # Apply subscriptions from config
        await self._apply_subscriptions()

        self.logger.info(f"Server started: PUB={self._pub_address}")

    async def run_forever(self) -> None:
        await self._stop_event.wait()

    async def stop(self) -> None:
        self._stop_event.set()
        await self.disconnect()
        self._publisher.close()
        self.logger.info("Server stopped")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()


# ── DeribitOptionsDataService ───────────────────────────────────────────


class DeribitOptionsDataService:
    def __init__(
        self,
        config_path: str,
        logger: Optional[logging.Logger] = None,
    ):
        self.logger = logger or logging.getLogger("mds.deribit_options")
        self._config = load_config(config_path)

        # Parse options section
        options = self._config.get("options", {})
        self._currencies: List[str] = options.get("currencies", ["BTC", "ETH"])
        self._months: int = options.get("months", 3)
        self._orderbook_depth: int = options.get("orderbook_depth", 10000)
        self._auto_refresh: bool = options.get("auto_refresh", True)
        self._refresh_time_utc: tuple = tuple(options.get("refresh_time_utc", [8, 15]))

        # Underlying MarketDataServer (reads zmq + exchanges from same config)
        self._server = MarketDataServer(
            config_path=config_path,
            logger=self.logger,
        )

        self._stop_event = asyncio.Event()
        self._refresh_task: Optional[asyncio.Task] = None
        self._subscribed_symbols: Dict[str, Set[str]] = {}

    @property
    def server(self) -> MarketDataServer:
        return self._server

    async def start(self) -> None:
        """
        Start the service:
        1. Start underlying MarketDataServer (ZMQ + adapters, no config subscriptions)
        2. Discover all unexpired options
        3. Subscribe to orderbook + ticker + index price
        4. Start auto-refresh loop
        """
        await self._server.start()

        total_symbols = 0
        for currency in self._currencies:
            symbols = await self._subscribe_currency(currency)
            total_symbols += len(symbols)

            index_name = f"{currency.lower()}_usd"
            await self._server.subscribe("deribit", [index_name], DataType.INDEX_PRICE)
            self.logger.info(f"Subscribed to index price: {index_name}")

        if self._auto_refresh:
            self._refresh_task = asyncio.create_task(self._refresh_loop())
            self.logger.info(
                f"Auto-refresh enabled: daily at "
                f"{self._refresh_time_utc[0]:02d}:{self._refresh_time_utc[1]:02d} UTC"
            )

        self.logger.info(
            f"DeribitOptionsDataService started: "
            f"total_symbols={total_symbols}, currencies={self._currencies}"
        )

    async def _get_option_symbols(self, currency: str) -> List[str]:
        adapter = self._server.get_adapter("deribit")
        if not adapter:
            raise ValueError("Deribit adapter not found in server")

        if hasattr(adapter, "get_option_symbols_by_expiry"):
            return await adapter.get_option_symbols_by_expiry(currency, self._months)

        from datetime import datetime, timezone
        instruments = await adapter.get_instruments(currency, "option", expired=False)
        now = datetime.now(timezone.utc)
        cutoff = datetime(
            now.year + (now.month + self._months - 1) // 12,
            (now.month + self._months - 1) % 12 + 1,
            1,
            tzinfo=timezone.utc,
        )
        return [
            inst.symbol for inst in instruments
            if inst.expiration and inst.expiration <= cutoff
        ]

    async def _subscribe_currency(self, currency: str) -> List[str]:
        symbols = await self._get_option_symbols(currency)
        self.logger.info(f"Found {len(symbols)} {currency} options within {self._months} months")

        if symbols:
            await self._server.subscribe(
                "deribit", symbols, DataType.ORDERBOOK, depth=self._orderbook_depth
            )
            await self._server.subscribe("deribit", symbols, DataType.TICKER)
            self._subscribed_symbols[currency] = set(symbols)

        return symbols

    async def refresh_subscriptions(self) -> Dict[str, Dict[str, int]]:
        results = {}
        for currency in self._currencies:
            new_symbols = set(await self._get_option_symbols(currency))
            current_symbols = self._subscribed_symbols.get(currency, set())

            to_add = new_symbols - current_symbols
            to_remove = current_symbols - new_symbols

            if to_remove:
                self.logger.info(f"[{currency}] Unsubscribing {len(to_remove)} expired symbols")
                await self._server.unsubscribe("deribit", list(to_remove), DataType.ORDERBOOK)
                await self._server.unsubscribe("deribit", list(to_remove), DataType.TICKER)

            if to_add:
                self.logger.info(f"[{currency}] Subscribing {len(to_add)} new symbols")
                await self._server.subscribe(
                    "deribit", list(to_add), DataType.ORDERBOOK, depth=self._orderbook_depth
                )
                await self._server.subscribe("deribit", list(to_add), DataType.TICKER)

            self._subscribed_symbols[currency] = new_symbols
            results[currency] = {"added": len(to_add), "removed": len(to_remove)}
            self.logger.info(
                f"[{currency}] Refresh: +{len(to_add)} -{len(to_remove)} "
                f"(total: {len(new_symbols)})"
            )

        return results

    async def _refresh_loop(self) -> None:
        from datetime import datetime, timedelta, timezone

        while True:
            try:
                now = datetime.now(timezone.utc)
                target_hour, target_minute = self._refresh_time_utc

                next_run = now.replace(
                    hour=target_hour, minute=target_minute, second=0, microsecond=0
                )
                if next_run <= now:
                    next_run += timedelta(days=1)

                wait_seconds = (next_run - now).total_seconds()
                self.logger.info(
                    f"Next refresh at {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                    f"({wait_seconds / 3600:.1f}h)"
                )

                await asyncio.sleep(wait_seconds)

                self.logger.info("Starting scheduled subscription refresh...")
                await self.refresh_subscriptions()

            except asyncio.CancelledError:
                self.logger.info("Refresh loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in refresh loop: {e}", exc_info=True)
                await asyncio.sleep(3600)


    async def run_forever(self) -> None:
        await self._stop_event.wait()

    async def stop(self) -> None:
        self._stop_event.set()
        if self._refresh_task:
            self._refresh_task.cancel()
            self._refresh_task = None
        for symbols in self._subscribed_symbols.values():
            if symbols:
                await self._server.unsubscribe("deribit", list(symbols), DataType.ORDERBOOK)
                await self._server.unsubscribe("deribit", list(symbols), DataType.TICKER)
        self._subscribed_symbols.clear()
        await self._server.stop()
        self.logger.info("DeribitOptionsDataService stopped")
