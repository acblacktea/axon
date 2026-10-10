"""
Market Data Service - Main service class
"""
import asyncio
from typing import Dict, List, Optional, Callable, Awaitable, Set, Type
from datetime import datetime
import logging

from .core.adapter import ExchangeAdapter, MarketDataCallback
from .core.models import Instrument, DataType, MarketDataEvent


class MarketDataService:
    """
    Market Data Service manages market data subscriptions across multiple exchanges.

    Features:
    - Multi-exchange support
    - Unified callback interface for all data types (orderbook, ticker, index price, etc.)
    - Automatic reconnection and recovery
    - Symbol management

    Usage:
        mds = MarketDataService()

        # Add exchange adapter
        await mds.add_exchange(DeribitAdapter())

        # Register callback for specific data type
        mds.on_market_data(DataType.ORDERBOOK, on_orderbook_callback)
        mds.on_market_data(DataType.INDEX_PRICE, on_spot_callback)

        # Subscribe to data (default is orderbook)
        await mds.subscribe("deribit", ["BTC-25DEC24-100000-C"])
        await mds.subscribe("deribit", ["btc_usd"], DataType.INDEX_PRICE)

    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger("mds")
        self._adapters: Dict[str, ExchangeAdapter] = {}
        # Callbacks organized by data type
        self._callbacks_by_type: Dict[DataType, List[MarketDataCallback]] = {
            dt: [] for dt in DataType
        }
        self._is_running: bool = False
    
    @property
    def exchanges(self) -> List[str]:
        """List of registered exchange names"""
        return list(self._adapters.keys())
    
    @property
    def is_running(self) -> bool:
        return self._is_running
    
    def get_adapter(self, exchange: str) -> Optional[ExchangeAdapter]:
        """Get adapter for a specific exchange"""
        return self._adapters.get(exchange.lower())
    
    async def add_exchange(self, adapter: ExchangeAdapter) -> None:
        """
        Add an exchange adapter to the service.

        Args:
            adapter: Exchange adapter instance
        """
        exchange = adapter.exchange_name.lower()

        if exchange in self._adapters:
            self.logger.warning(f"Exchange {exchange} already registered, replacing")
            await self.remove_exchange(exchange)

        # Register internal callback for all supported data types
        for data_type in adapter.SUPPORTED_DATA_TYPES:
            adapter.add_callback_for_type(data_type, self._on_market_data_event)

        self._adapters[exchange] = adapter
        self.logger.info(
            f"Added exchange adapter: {exchange} "
            f"(supports: {[dt.value for dt in adapter.SUPPORTED_DATA_TYPES]})"
        )
    
    async def remove_exchange(self, exchange: str) -> None:
        """
        Remove an exchange adapter.

        Args:
            exchange: Exchange name
        """
        exchange = exchange.lower()

        if exchange not in self._adapters:
            return

        adapter = self._adapters.pop(exchange)

        # Remove callbacks for all data types
        for data_type in adapter.SUPPORTED_DATA_TYPES:
            adapter.remove_callback_for_type(data_type, self._on_market_data_event)

        if adapter.is_connected:
            await adapter.disconnect()

        self.logger.info(f"Removed exchange adapter: {exchange}")
    
    async def connect(self, exchange: Optional[str] = None) -> None:
        """
        Connect to exchange(s).
        
        Args:
            exchange: Specific exchange to connect, or None for all
        """
        self._is_running = True
        
        if exchange:
            adapter = self.get_adapter(exchange)
            if adapter:
                await adapter.connect()
        else:
            for adapter in self._adapters.values():
                await adapter.connect()
    
    async def disconnect(self, exchange: Optional[str] = None) -> None:
        """
        Disconnect from exchange(s).
        
        Args:
            exchange: Specific exchange to disconnect, or None for all
        """
        if exchange:
            adapter = self.get_adapter(exchange)
            if adapter:
                await adapter.disconnect()
        else:
            for adapter in self._adapters.values():
                await adapter.disconnect()
        
        if not exchange:
            self._is_running = False
    
    async def _on_market_data_event(self, event: MarketDataEvent) -> None:
        """Internal handler that forwards market data events to registered callbacks"""
        for callback in self._callbacks_by_type[event.data_type]:
            try:
                await callback(event)
            except Exception as e:
                self.logger.error(f"Callback error for {event.data_type}: {e}", exc_info=True)

    async def subscribe(
        self,
        exchange: str,
        symbols: List[str],
        data_type: DataType = DataType.ORDERBOOK,
        **kwargs
    ) -> None:
        """
        Subscribe to market data for symbols on an exchange.

        Args:
            exchange: Exchange name
            symbols: List of symbols in unified format (e.g., BTC_USDT)
            data_type: Type of data to subscribe (default: ORDERBOOK)
            **kwargs: Data type specific parameters
                - depth (int): For orderbook subscriptions (default: 10000)

        Raises:
            ValueError: If exchange not found or data type not supported
        """
        adapter = self.get_adapter(exchange)
        if not adapter:
            raise ValueError(f"Unknown exchange: {exchange}")

        if not adapter.supports_data_type(data_type):
            raise ValueError(
                f"Exchange {exchange} does not support {data_type.value}. "
                f"Supported types: {[dt.value for dt in adapter.SUPPORTED_DATA_TYPES]}"
            )

        if not adapter.is_connected:
            await adapter.connect()

        # Convert unified symbols to exchange-specific format
        exchange_symbols = adapter.convert_symbols(symbols)
        await adapter.subscribe(data_type, exchange_symbols, **kwargs)

    async def unsubscribe(
        self,
        exchange: str,
        symbols: List[str],
        data_type: DataType = DataType.ORDERBOOK
    ) -> None:
        """
        Unsubscribe from market data.

        Args:
            exchange: Exchange name
            symbols: List of symbols in unified format (e.g., BTC_USDT)
            data_type: Type of data to unsubscribe (default: ORDERBOOK)
        """
        adapter = self.get_adapter(exchange)
        if adapter:
            # Convert unified symbols to exchange-specific format
            exchange_symbols = adapter.convert_symbols(symbols)
            await adapter.unsubscribe(data_type, exchange_symbols)

    def on_market_data(
        self,
        data_type: DataType,
        callback: MarketDataCallback
    ) -> None:
        """
        Register a callback for a specific data type.

        Args:
            data_type: Type of data to receive
            callback: Async callback function receiving MarketDataEvent
        """
        if callback not in self._callbacks_by_type[data_type]:
            self._callbacks_by_type[data_type].append(callback)

    def remove_market_data_callback(
        self,
        data_type: DataType,
        callback: MarketDataCallback
    ) -> None:
        """Remove a callback for a specific data type"""
        if callback in self._callbacks_by_type[data_type]:
            self._callbacks_by_type[data_type].remove(callback)

    async def get_instruments(
        self,
        exchange: str,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """
        Get available instruments from an exchange.
        
        Args:
            exchange: Exchange name
            currency: Filter by base currency
            kind: Filter by instrument type
            expired: Include expired instruments
            
        Returns:
            List of instruments
        """
        adapter = self.get_adapter(exchange)
        if not adapter:
            raise ValueError(f"Unknown exchange: {exchange}")
        
        return await adapter.get_instruments(currency, kind, expired)
    
    def get_subscribed_symbols(self, exchange: Optional[str] = None) -> Dict[str, Set[str]]:
        """
        Get currently subscribed symbols.
        
        Args:
            exchange: Specific exchange or None for all
            
        Returns:
            Dict of exchange -> set of symbols
        """
        result = {}
        
        adapters = (
            [(exchange.lower(), self.get_adapter(exchange))] 
            if exchange 
            else self._adapters.items()
        )
        
        for ex_name, adapter in adapters:
            if adapter:
                result[ex_name] = adapter.subscribed_symbols
        
        return result
    
    async def __aenter__(self):
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()


class DeribitOptionsDataClient:
    """
    High-level client for subscribing to options orderbook data and spot price.

    Features:
    - Automatic symbol discovery for options
    - Subscription to all symbols within expiry range
    - Spot/index price subscription for hedging
    - Unified callback-based updates via MarketDataEvent

    Usage:
        async def on_orderbook(event: MarketDataEvent):
            # event.data is OrderbookSnapshot
            print(f"Orderbook update: {event.symbol}")

        async def on_spot(event: MarketDataEvent):
            # event.data is IndexPriceData
            print(f"BTC spot: ${event.data.price}")

        client = DeribitOptionsDataClient(mds)
        await client.subscribe_btc_options(months=3, callback=on_orderbook)
        await client.subscribe_spot_price("BTC", callback=on_spot)

    """

    def __init__(
        self,
        mds: MarketDataService,
        logger: Optional[logging.Logger] = None,
        auto_refresh: bool = True,
        refresh_time_utc: tuple = (8, 15)  # (hour, minute) in UTC
    ):
        self.mds = mds
        self.exchange = "deribit"
        self.logger = logger or logging.getLogger("mds.client")

        self._subscribed_symbols: Set[str] = set()
        self._orderbook_callbacks: List[MarketDataCallback] = []

        # Ticker subscription tracking
        self._ticker_symbols: Set[str] = set()
        self._ticker_callbacks: List[MarketDataCallback] = []

        # Spot price subscription tracking
        self._spot_price_indices: Set[str] = set()
        self._spot_callbacks: List[MarketDataCallback] = []

        # Auto-refresh configuration
        self._auto_refresh = auto_refresh
        self._refresh_time_utc = refresh_time_utc
        self._refresh_task: Optional[asyncio.Task] = None

        # Subscription parameters (stored for refresh)
        self._currency: Optional[str] = None
        self._months: Optional[int] = None
        self._depth: int = 10000

        # Event for graceful shutdown
        self._stop_event = asyncio.Event()
    
    async def subscribe_btc_options(
        self,
        months: int = 3,
        depth: int = 10000,
        callback: Optional[MarketDataCallback] = None,
        ticker_callback: Optional[MarketDataCallback] = None
    ) -> List[str]:
        """
        Subscribe to all BTC options expiring within specified months.

        Args:
            months: Number of months ahead to include
            depth: Orderbook depth
            callback: Optional callback for orderbook updates (receives MarketDataEvent)
            ticker_callback: Optional callback for ticker updates (receives MarketDataEvent)

        Returns:
            List of subscribed symbols
        """
        return await self.subscribe_options("BTC", months, depth, callback, ticker_callback)

    async def subscribe_eth_options(
        self,
        months: int = 3,
        depth: int = 10000,
        callback: Optional[MarketDataCallback] = None,
        ticker_callback: Optional[MarketDataCallback] = None
    ) -> List[str]:
        """Subscribe to all ETH options expiring within specified months."""
        return await self.subscribe_options("ETH", months, depth, callback, ticker_callback)

    async def subscribe_options(
        self,
        currency: str,
        months: int = 3,
        depth: int = 10000,
        callback: Optional[MarketDataCallback] = None,
        ticker_callback: Optional[MarketDataCallback] = None
    ) -> List[str]:
        """
        Subscribe to all options for a currency expiring within specified months.

        Args:
            currency: Base currency (BTC, ETH)
            months: Number of months ahead
            depth: Orderbook depth
            callback: Optional callback for orderbook updates
            ticker_callback: Optional callback for ticker updates

        Returns:
            List of subscribed symbols
        """
        adapter = self.mds.get_adapter(self.exchange)
        if not adapter:
            raise ValueError(f"Exchange {self.exchange} not found")

        # Store parameters for refresh
        self._currency = currency
        self._months = months
        self._depth = depth

        # Get option symbols
        if hasattr(adapter, 'get_option_symbols_by_expiry'):
            symbols = await adapter.get_option_symbols_by_expiry(currency, months)
        else:
            # Fallback: get all options and filter
            instruments = await adapter.get_instruments(currency, "option", expired=False)

            now = datetime.utcnow()
            cutoff = datetime(
                now.year + (now.month + months - 1) // 12,
                (now.month + months - 1) % 12 + 1,
                1
            )

            symbols = [
                inst.symbol for inst in instruments
                if inst.expiration and inst.expiration <= cutoff
            ]

        self.logger.info(f"Found {len(symbols)} {currency} options within {months} months")

        # Register callbacks if provided
        if callback:
            self.add_orderbook_callback(callback)
        if ticker_callback:
            self.add_ticker_callback(ticker_callback)

        # Subscribe to orderbook and ticker
        if symbols:
            self._subscribed_symbols.update(symbols)
            self._ticker_symbols.update(symbols)
            await self.mds.subscribe(self.exchange, symbols, DataType.ORDERBOOK, depth=depth)
            await self.mds.subscribe(self.exchange, symbols, DataType.TICKER)

        # Start auto-refresh task if enabled
        if self._auto_refresh and not self._refresh_task:
            self._refresh_task = asyncio.create_task(self._refresh_loop())
            self.logger.info(f"Auto-refresh enabled: daily at {self._refresh_time_utc[0]:02d}:{self._refresh_time_utc[1]:02d} UTC")

        return symbols
    
    def add_orderbook_callback(self, callback: MarketDataCallback) -> None:
        """Add callback for orderbook updates"""
        if callback not in self._orderbook_callbacks:
            self._orderbook_callbacks.append(callback)
            # Register wrapped callback with MDS
            wrapped = self._wrap_orderbook_callback(callback)
            self.mds.on_market_data(DataType.ORDERBOOK, wrapped)

    def _wrap_orderbook_callback(
        self,
        callback: MarketDataCallback
    ) -> MarketDataCallback:
        """Wrap callback to filter by subscribed symbols"""
        async def wrapped(event: MarketDataEvent) -> None:
            if event.symbol in self._subscribed_symbols:
                await callback(event)
        return wrapped

    # ==================== Ticker Subscription ====================

    def add_ticker_callback(self, callback: MarketDataCallback) -> None:
        """Add callback for ticker updates"""
        if callback not in self._ticker_callbacks:
            self._ticker_callbacks.append(callback)
            # Register wrapped callback with MDS
            wrapped = self._wrap_ticker_callback(callback)
            self.mds.on_market_data(DataType.TICKER, wrapped)

    def _wrap_ticker_callback(
        self,
        callback: MarketDataCallback
    ) -> MarketDataCallback:
        """Wrap ticker callback to filter by subscribed symbols"""
        async def wrapped(event: MarketDataEvent) -> None:
            if event.symbol in self._ticker_symbols:
                await callback(event)
        return wrapped

    @property
    def ticker_symbols(self) -> Set[str]:
        """Get subscribed ticker symbols"""
        return self._ticker_symbols.copy()

    # ==================== Spot Price Subscription ====================

    async def subscribe_spot_price(
        self,
        currency: str = "BTC",
        callback: Optional[MarketDataCallback] = None
    ) -> str:
        """
        Subscribe to spot/index price for a currency.

        Useful for getting the underlying spot price when trading options.

        Args:
            currency: Currency to track (BTC, ETH)
            callback: Optional callback for price updates

        Returns:
            Index name that was subscribed (e.g., "btc_usd")
        """
        index_name = f"{currency.lower()}_usd"

        if callback:
            self._add_spot_callback(callback)

        # Subscribe via MDS
        await self.mds.subscribe(
            self.exchange,
            [index_name],
            DataType.INDEX_PRICE
        )

        self._spot_price_indices.add(index_name)
        self.logger.info(f"Subscribed to spot price: {index_name}")

        return index_name

    def _add_spot_callback(self, callback: MarketDataCallback) -> None:
        """Add callback for spot price updates"""
        if callback not in self._spot_callbacks:
            self._spot_callbacks.append(callback)
            # Register wrapped callback with MDS
            wrapped = self._wrap_spot_callback(callback)
            self.mds.on_market_data(DataType.INDEX_PRICE, wrapped)

    def _wrap_spot_callback(
        self,
        callback: MarketDataCallback
    ) -> MarketDataCallback:
        """Wrap spot callback to filter by subscribed indices"""
        async def wrapped(event: MarketDataEvent) -> None:
            if event.symbol in self._spot_price_indices:
                await callback(event)
        return wrapped

    @property
    def spot_price_indices(self) -> Set[str]:
        """Get subscribed spot price indices"""
        return self._spot_price_indices.copy()
    
    async def run(self):
        """Keep the program running to receive orderbook updates.

        Press Ctrl+C to stop.
        """
        print("Trading system running. Press Ctrl+C to stop...")
        try:
            # Wait efficiently until stop is requested
            await self._stop_event.wait()
        except KeyboardInterrupt:
            print("\nStopping trading system...")
        finally:
            # Stop auto-refresh when exiting
            self.stop_auto_refresh()

    def stop(self):
        """Signal the run loop to stop."""
        self._stop_event.set()
        
    
    @property
    def subscribed_symbols(self) -> Set[str]:
        return self._subscribed_symbols.copy()
    
    async def unsubscribe_all(self) -> None:
        """Unsubscribe from all symbols"""
        if self._subscribed_symbols:
            await self.mds.unsubscribe(self.exchange, list(self._subscribed_symbols))
            self._subscribed_symbols.clear()

    async def refresh_subscriptions(self) -> Dict[str, int]:
        """
        Refresh subscriptions by checking for new/expired options.
        Updates both orderbook and ticker subscriptions.

        Returns:
            Dict with 'added' and 'removed' counts
        """
        if not self._currency or not self._months:
            self.logger.warning("No subscription parameters stored, cannot refresh")
            return {'added': 0, 'removed': 0}

        adapter = self.mds.get_adapter(self.exchange)
        if not adapter:
            raise ValueError(f"Exchange {self.exchange} not found")

        self.logger.info(f"Refreshing subscriptions for {self._currency} options...")

        # Get current list of symbols that should be subscribed
        if hasattr(adapter, 'get_option_symbols_by_expiry'):
            new_symbols_list = await adapter.get_option_symbols_by_expiry(self._currency, self._months)
        else:
            instruments = await adapter.get_instruments(self._currency, "option", expired=False)

            now = datetime.utcnow()
            cutoff = datetime(
                now.year + (now.month + self._months - 1) // 12,
                (now.month + self._months - 1) % 12 + 1,
                1
            )

            new_symbols_list = [
                inst.symbol for inst in instruments
                if inst.expiration and inst.expiration <= cutoff
            ]

        new_symbols = set(new_symbols_list)
        current_symbols = self._subscribed_symbols.copy()

        # Calculate differences
        to_add = new_symbols - current_symbols
        to_remove = current_symbols - new_symbols

        # Remove expired/delisted symbols (orderbook and ticker)
        if to_remove:
            self.logger.info(f"Unsubscribing from {len(to_remove)} expired/delisted symbols...")
            await self.mds.unsubscribe(self.exchange, list(to_remove))
            await self.mds.unsubscribe(self.exchange, DataType.TICKER, list(to_remove))
            self._subscribed_symbols -= to_remove
            self._ticker_symbols -= to_remove

        # Add new symbols (orderbook and ticker)
        if to_add:
            self.logger.info(f"Subscribing to {len(to_add)} new symbols...")
            await self.mds.subscribe(self.exchange, list(to_add), DataType.ORDERBOOK, depth=self._depth)
            await self.mds.subscribe(self.exchange, list(to_add), DataType.TICKER)
            self._subscribed_symbols.update(to_add)
            self._ticker_symbols.update(to_add)

        result = {'added': len(to_add), 'removed': len(to_remove)}
        self.logger.info(f"Refresh complete: +{result['added']} -{result['removed']} (total: {len(self._subscribed_symbols)})")

        return result

    async def _refresh_loop(self) -> None:
        """Background task that refreshes subscriptions daily at specified UTC time"""
        from datetime import timedelta, timezone

        while True:
            try:
                # Calculate next refresh time
                now = datetime.now(timezone.utc)
                target_hour, target_minute = self._refresh_time_utc

                # Next occurrence of target time
                next_run = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)

                # If target time already passed today, schedule for tomorrow
                if next_run <= now:
                    next_run += timedelta(days=1)

                wait_seconds = (next_run - now).total_seconds()
                self.logger.info(f"Next subscription refresh scheduled at {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC ({wait_seconds/3600:.1f}h)")

                # Wait until target time
                await asyncio.sleep(wait_seconds)

                # Perform refresh
                self.logger.info("Starting scheduled subscription refresh...")
                await self.refresh_subscriptions()

            except asyncio.CancelledError:
                self.logger.info("Refresh loop cancelled")
                break
            except Exception as e:
                self.logger.error(f"Error in refresh loop: {e}", exc_info=True)
                # Wait 1 hour before retrying on error
                await asyncio.sleep(3600)

    def stop_auto_refresh(self) -> None:
        """Stop the auto-refresh background task"""
        if self._refresh_task:
            self._refresh_task.cancel()
            self._refresh_task = None
            self.logger.info("Auto-refresh stopped")
