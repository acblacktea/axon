"""
Deribit Exchange Adapter Implementation

Uses WebSocketManager for connection handling, focuses on Deribit-specific logic.
"""
import asyncio
import ssl
import time
from typing import List, Optional, Dict, Any, Set
from datetime import datetime
from decimal import Decimal
import logging

import aiohttp

from ...core.adapter import ExchangeAdapter, OrderbookManager
from ...core.models import Instrument, Orderbook, OrderbookSnapshot, DataType, IndexPriceData, TickerData
from ...core.websocket import WebSocketManager


def _safe_decimal(value) -> Optional[Decimal]:
    """
    Safely convert a value to Decimal.

    Returns None if value is None or cannot be converted.
    """
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


class DeribitAdapter(ExchangeAdapter):
    """
    Deribit exchange adapter for market data with multi-connection support.

    Automatically manages multiple WebSocket connections to bypass Deribit's
    500 symbols per connection limit.

    Features:
    - Automatic multi-connection management (via WebSocketManager)
    - Options orderbook subscription
    - Index price subscription (btc_usd, eth_usd)
    - Automatic reconnection
    - Sequence validation and recovery
    - Support for unlimited symbols (creates connections as needed)
    """

    # Supported data types
    SUPPORTED_DATA_TYPES = {DataType.ORDERBOOK, DataType.INDEX_PRICE, DataType.TICKER}

    # Deribit WebSocket endpoints
    WS_URL = "wss://www.deribit.com/ws/api/v2"
    WS_TEST_URL = "wss://test.deribit.com/ws/api/v2"

    # REST endpoints
    REST_URL = "https://www.deribit.com/api/v2"
    REST_TEST_URL = "https://test.deribit.com/api/v2"

    def __init__(
        self,
        testnet: bool = False,
        logger: Optional[logging.Logger] = None,
        heartbeat_interval: float = 15.0,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 1000,
        orderbook_interval: str = "agg2",  # agg2 = 1s aggregated
        max_symbols_per_connection: int = 400  # Max symbols per connection (Deribit limit is 500)
    ):
        super().__init__("deribit", logger)

        self.testnet = testnet
        self.ws_url = self.WS_TEST_URL if testnet else self.WS_URL
        self.rest_url = self.REST_TEST_URL if testnet else self.REST_URL

        self.orderbook_interval = orderbook_interval

        # Orderbook manager
        self._ob_manager: Optional[OrderbookManager] = None

        # Subscription tracking
        self._subscription_depth: int = 10000  # Default depth, full depth
        self._subscription_lock = asyncio.Lock()

        # Index price subscription tracking
        self._index_subscriptions: Set[str] = set()

        # Ticker subscription tracking
        self._ticker_subscriptions: Set[str] = set()

        # WebSocket manager (composition)
        self._ws_manager = WebSocketManager(
            ws_url=self.ws_url,
            logger=self.logger,
            heartbeat_interval=heartbeat_interval,
            reconnect_delay=reconnect_delay,
            max_reconnect_attempts=max_reconnect_attempts,
            max_items_per_connection=max_symbols_per_connection,
            on_message=self._handle_message,
            on_reconnect=self._on_reconnect,
            heartbeat_method="public/test",
            exchange=self.exchange_name,
        )

    @property
    def orderbook_manager(self) -> OrderbookManager:
        if self._ob_manager is None:
            self._ob_manager = OrderbookManager(self, self.logger)
        return self._ob_manager

    # ==================== Symbol Conversion ====================
    # Deribit options use their own format (e.g., BTC-27FEB26-100000-C)
    # Index prices use btc_usd format (already unified)
    # No conversion needed - keep original format

    async def _create_session(self) -> aiohttp.ClientSession:
        """Create aiohttp session with SSL context"""
        ssl_context = ssl.create_default_context()
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        return aiohttp.ClientSession(connector=connector)

    async def connect(self) -> None:
        """Initialize adapter (connections created on-demand during subscription)"""
        if self._is_connected:
            self.logger.warning("Already connected")
            return

        self.logger.info(
            f"Initializing Deribit adapter ({'testnet' if self.testnet else 'mainnet'}, "
            f"multi-connection mode, max {self._ws_manager.max_items_per_connection} symbols/connection)..."
        )

        self._ws_manager.start()
        self._is_connected = True
        self._is_running = True
        self.logger.info("Deribit adapter initialized (WebSocket connections created on-demand)")

    async def disconnect(self) -> None:
        """Close all WebSocket connections"""
        self.logger.info(f"Disconnecting {self._ws_manager.connection_count} connection(s)...")

        self._is_running = False
        self._is_connected = False

        await self._ws_manager.stop()
        self.logger.info("All connections disconnected")

    async def _handle_message(self, conn_idx: int, msg: dict) -> None:
        """
        Handle message from WebSocket (callback for WebSocketManager).

        This handles Deribit-specific message formats.
        """
        # Handle subscription notification
        if 'method' in msg and msg['method'] == 'subscription':
            await self._handle_subscription(msg.get('params', {}))

    async def _on_reconnect(self, conn_idx: int, items: List[str]) -> None:
        """
        Handle reconnection (callback for WebSocketManager).

        Resubscribes to all items (orderbooks, tickers, and index prices) on the reconnected connection.
        """
        if not items:
            return

        # Separate orderbook symbols, ticker symbols, and index prices
        orderbook_symbols = []
        ticker_symbols = []
        index_names = []
        for item in items:
            if item.startswith("index:"):
                index_names.append(item[6:])  # Remove "index:" prefix
            elif item.startswith("ticker:"):
                ticker_symbols.append(item[7:])  # Remove "ticker:" prefix
            else:
                orderbook_symbols.append(item)

        self.logger.info(
            f"Resubscribing on connection #{conn_idx}: "
            f"{len(orderbook_symbols)} orderbooks, {len(ticker_symbols)} tickers, "
            f"{len(index_names)} index prices..."
        )

        # Resubscribe orderbooks
        if orderbook_symbols:
            channels = [f"book.{sym}.{self.orderbook_interval}" for sym in orderbook_symbols]
            batch_size = 20
            for i in range(0, len(channels), batch_size):
                batch = channels[i:i + batch_size]
                try:
                    await self._ws_manager.send_request(
                        conn_idx,
                        "public/subscribe",
                        {"channels": batch}
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to resubscribe orderbook batch on connection #{conn_idx}: {e}"
                    )

        # Resubscribe tickers
        if ticker_symbols:
            channels = [f"ticker.{sym}.{self.orderbook_interval}" for sym in ticker_symbols]
            batch_size = 20
            for i in range(0, len(channels), batch_size):
                batch = channels[i:i + batch_size]
                try:
                    await self._ws_manager.send_request(
                        conn_idx,
                        "public/subscribe",
                        {"channels": batch}
                    )
                except Exception as e:
                    self.logger.error(
                        f"Failed to resubscribe ticker batch on connection #{conn_idx}: {e}"
                    )

        # Resubscribe index prices
        if index_names:
            index_channels = [f"deribit_price_index.{idx}" for idx in index_names]
            try:
                await self._ws_manager.send_request(
                    conn_idx,
                    "public/subscribe",
                    {"channels": index_channels}
                )
            except Exception as e:
                self.logger.error(
                    f"Failed to resubscribe index prices on connection #{conn_idx}: {e}"
                )

        self.logger.info(
            f"Connection #{conn_idx} resubscribed to {len(orderbook_symbols)} orderbooks, "
            f"{len(ticker_symbols)} tickers, {len(index_names)} index prices"
        )

        # Emit reconnect event for orderbook symbols
        for symbol in orderbook_symbols:
            await self._emit_reconnect(symbol)

    async def _handle_subscription(self, params: dict) -> None:
        """Handle subscription notification"""
        channel = params.get('channel', '')
        data = params.get('data', {})

        # Handle orderbook updates
        if channel.startswith('book.'):
            await self._handle_orderbook_update(channel, data)
        # Handle index price updates
        elif channel.startswith('deribit_price_index.'):
            await self._handle_index_price_update(channel, data)
        # Handle ticker updates
        elif channel.startswith('ticker.'):
            await self._handle_ticker_update(channel, data)

    async def _handle_orderbook_update(self, channel: str, data: dict) -> None:
        """Handle orderbook update from subscription"""
        # Parse channel: book.{instrument}.{group}.{depth}.{interval} book.{instrument}.{interval}
        parts = channel.split('.')
        if len(parts) < 2:
            return

        symbol = parts[1]
        update_type = data.get('type')  # 'snapshot' or 'change'

        # Parse bids and asks
        bids = []
        for item in data.get('bids', []):
            try:
                if len(item) >= 3:
                    # Update format: [action, price, amount]
                    bids.append((Decimal(str(item[1])), Decimal(str(item[2]))))
                elif len(item) >= 2:
                    # Snapshot format: [price, amount]
                    bids.append((Decimal(str(item[0])), Decimal(str(item[1]))))
            except Exception as e:
                self.logger.warning(f"Failed to parse bid item {item}: {e}")
                continue

        asks = []
        for item in data.get('asks', []):
            try:
                if len(item) >= 3:
                    asks.append((Decimal(str(item[1])), Decimal(str(item[2]))))
                elif len(item) >= 2:
                    asks.append((Decimal(str(item[0])), Decimal(str(item[1]))))
            except Exception as e:
                self.logger.warning(f"Failed to parse ask item {item}: {e}")
                continue

        update_data = {
            'bids': bids,
            'asks': asks,
            'sequence': data.get('change_id'),
            'prev_change_id': data.get('prev_change_id'),
            'timestamp': data.get('timestamp'),
            'local_timestamp': int(time.time() * 1000)
        }

        if update_type == 'snapshot':
            await self.orderbook_manager.apply_snapshot(symbol, update_data)
            snapshot = self.orderbook_manager.get_snapshot(symbol, self._subscription_depth)
            if snapshot:
                await self._emit_snapshot(symbol, snapshot)
        else:
            # Incremental update
            success = await self.orderbook_manager.apply_update(symbol, update_data)
            if success:
                snapshot = self.orderbook_manager.get_snapshot(symbol, self._subscription_depth)
                if snapshot:
                    await self._emit_update(symbol, snapshot)
            else:
                # Sequence gap detected, trigger recovery
                self.logger.warning(f"Sequence gap for {symbol}, triggering recovery")
                asyncio.create_task(self.orderbook_manager.trigger_recovery(symbol))

    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """
        Subscribe to market data updates for given symbols.

        Args:
            data_type: Type of data (ORDERBOOK or INDEX_PRICE)
            symbols: List of symbols to subscribe
            **kwargs:
                - depth (int): For orderbook subscriptions (default: 20)
        """
        if not self._is_connected:
            raise ConnectionError("Not connected to Deribit. Call connect() first.")

        if data_type not in self.SUPPORTED_DATA_TYPES:
            raise ValueError(f"Unsupported data type: {data_type}")

        if data_type == DataType.ORDERBOOK:
            await self._subscribe_orderbook(symbols, kwargs.get('depth', 20))
        elif data_type == DataType.INDEX_PRICE:
            await self._subscribe_index_price(symbols)
        elif data_type == DataType.TICKER:
            await self._subscribe_ticker(symbols)

    async def unsubscribe(self, data_type: DataType, symbols: List[str]) -> None:
        """
        Unsubscribe from market data updates.

        Args:
            data_type: Type of data (ORDERBOOK or INDEX_PRICE)
            symbols: List of symbols to unsubscribe
        """
        if data_type == DataType.ORDERBOOK:
            await self._unsubscribe_orderbook(symbols)
        elif data_type == DataType.INDEX_PRICE:
            await self._unsubscribe_index_price(symbols)
        elif data_type == DataType.TICKER:
            await self._unsubscribe_ticker(symbols)

    async def _subscribe_orderbook(self, symbols: List[str], depth: int = 20) -> None:
        """Internal: Subscribe to orderbook updates for given symbols."""
        async with self._subscription_lock:
            self._subscription_depth = depth
            existing = self._ws_manager.get_all_items()
            new_symbols = [s for s in symbols if s not in existing]

            if not new_symbols:
                self.logger.debug("All symbols already subscribed")
                return

            self.logger.info(f"Subscribing to {len(new_symbols)} orderbook symbols...")

            # Distribute symbols across connections
            distribution = self._ws_manager.distribute_items(new_symbols)

            self.logger.info(
                f"Distribution: {', '.join(f'conn{i}={len(syms)}' for i, syms in distribution.items())}"
            )

            # Subscribe on each connection
            for conn_idx, conn_symbols in distribution.items():
                # Create connection if needed
                if conn_idx >= self._ws_manager.connection_count:
                    await self._ws_manager.create_connection()

                # Track subscriptions BEFORE sending request to prevent race condition
                # (another subscribe call could check get_all_items() while we're awaiting)
                self._ws_manager.add_items(conn_idx, set(conn_symbols))
                self._subscribed_symbols.update(conn_symbols)

                # Build channels
                channels = [f"book.{sym}.{self.orderbook_interval}" for sym in conn_symbols]

                # Subscribe in batches
                batch_size = 20
                for i in range(0, len(channels), batch_size):
                    batch = channels[i:i + batch_size]
                    try:
                        await self._ws_manager.send_request(
                            conn_idx,
                            "public/subscribe",
                            {"channels": batch}
                        )
                    except Exception as e:
                        self.logger.error(f"Failed to subscribe on connection #{conn_idx}: {e}")

                conn_items = self._ws_manager.get_items(conn_idx)
                self.logger.info(
                    f"Connection #{conn_idx}: {len(conn_items)}/"
                    f"{self._ws_manager.max_items_per_connection} symbols"
                )

            # Log total
            self.logger.info(
                f"Total: {len(self._subscribed_symbols)} symbols across "
                f"{self._ws_manager.connection_count} connections"
            )

    async def _unsubscribe_orderbook(self, symbols: List[str]) -> None:
        """Internal: Unsubscribe from orderbook updates."""
        async with self._subscription_lock:
            to_unsub = [s for s in symbols if s in self._subscribed_symbols]

            if not to_unsub:
                return

            self.logger.info(f"Unsubscribing from {len(to_unsub)} orderbook symbols...")

            # Find which connection has each symbol and unsubscribe
            for symbol in to_unsub:
                conn_idx = self._ws_manager.find_connection_for_item(symbol)
                if conn_idx is None:
                    continue

                channel = f"book.{symbol}.{self.orderbook_interval}"

                try:
                    await self._ws_manager.send_request(
                        conn_idx,
                        "public/unsubscribe",
                        {"channels": [channel]}
                    )

                    # Update tracking
                    self._ws_manager.remove_items(conn_idx, {symbol})
                    self._subscribed_symbols.discard(symbol)

                    # Remove from orderbook manager
                    self.orderbook_manager.remove_orderbook(symbol)

                except Exception as e:
                    self.logger.error(f"Failed to unsubscribe {symbol} on connection #{conn_idx}: {e}")

            self.logger.info(f"Unsubscribed from {len(to_unsub)} orderbook symbols")

    def get_connection_stats(self) -> Dict[str, Any]:
        """
        Get statistics about connections and subscriptions.

        Returns:
            Dict with connection statistics
        """
        stats = self._ws_manager.get_stats()
        stats['multi_connection_enabled'] = True
        # Map generic names to Deribit-specific names for API compatibility
        stats['total_subscriptions'] = stats.pop('total_items', 0)
        stats['max_symbols_per_connection'] = stats.pop('max_items_per_connection', 0)
        for conn in stats.get('connections', []):
            conn['subscriptions'] = conn.pop('items', 0)
        return stats

    async def get_instruments(
        self,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """Get available instruments from Deribit via REST API"""
        session = await self._create_session()

        try:
            params = {"expired": str(expired).lower()}
            if currency:
                params["currency"] = currency.upper()
            if kind:
                params["kind"] = kind.lower()

            url = f"{self.rest_url}/public/get_instruments"

            async with session.get(url, params=params) as resp:
                data = await resp.json()

                if 'error' in data:
                    raise Exception(f"API Error: {data['error']}")

                instruments = []
                for item in data.get('result', []):
                    inst = Instrument(
                        symbol=item['instrument_name'],
                        exchange="deribit",
                        base_currency=item['base_currency'],
                        quote_currency=item['quote_currency'],
                        instrument_type=item['kind'],
                        is_active=item.get('is_active', True),
                        tick_size=Decimal(str(item.get('tick_size', 0))),
                        min_trade_amount=Decimal(str(item.get('min_trade_amount', 0))),
                        contract_size=Decimal(str(item.get('contract_size', 1)))
                    )

                    # Option-specific fields
                    if item['kind'] == 'option':
                        inst.strike = Decimal(str(item.get('strike', 0)))
                        inst.option_type = item.get('option_type')  # 'call' or 'put'
                        inst.underlying = item.get('underlying_index')

                        # Parse expiration
                        exp_ts = item.get('expiration_timestamp')
                        if exp_ts:
                            inst.expiration = datetime.utcfromtimestamp(exp_ts / 1000)

                    instruments.append(inst)

                return instruments
        finally:
            await session.close()

    async def get_option_symbols_by_expiry(
        self,
        currency: str = "BTC",
        months_ahead: int = 3
    ) -> List[str]:
        """
        Get all option symbols expiring within the specified months.

        Args:
            currency: Base currency (BTC, ETH)
            months_ahead: Number of months to look ahead

        Returns:
            List of option symbol names
        """
        instruments = await self.get_instruments(
            currency=currency,
            kind="option",
            expired=False
        )

        now = datetime.utcnow()
        cutoff = datetime(
            now.year + (now.month + months_ahead - 1) // 12,
            (now.month + months_ahead - 1) % 12 + 1,
            1
        )

        symbols = [
            inst.symbol for inst in instruments
            if inst.expiration and inst.expiration <= cutoff and inst.is_active
        ]

        return symbols

    async def request_orderbook_snapshot(self, symbol: str, depth: int = 10000) -> Orderbook:
        """Request a full orderbook snapshot via REST API"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}/public/get_order_book"
            params = {
                "instrument_name": symbol,
                "depth": depth
            }

            async with session.get(url, params=params) as resp:
                data = await resp.json()

                if 'error' in data:
                    raise Exception(f"API Error: {data['error']}")

                result = data['result']

                ob = Orderbook(
                    symbol=symbol,
                    exchange="deribit"
                )

                # Parse bids and asks
                # Format: [[price, amount], ...]
                for bid in result.get('bids', []):
                    price = Decimal(str(bid[0]))
                    qty = Decimal(str(bid[1]))
                    ob.bids[price] = qty

                for ask in result.get('asks', []):
                    price = Decimal(str(ask[0]))
                    qty = Decimal(str(ask[1]))
                    ob.asks[price] = qty

                ob.sequence = result.get('change_id')
                ob.timestamp = result.get('timestamp')

                return ob
        finally:
            await session.close()

    # ==================== Index Price Subscription (Internal) ====================

    async def _subscribe_index_price(self, indices: List[str]) -> None:
        """Internal: Subscribe to index price updates."""
        async with self._subscription_lock:
            new_indices = [i for i in indices if i not in self._index_subscriptions]
            if not new_indices:
                self.logger.debug("All indices already subscribed")
                return

            self.logger.info(f"Subscribing to index price for: {new_indices}")

            # Distribute indices across connections (using index: prefix to avoid collision with symbols)
            index_items = [f"index:{idx}" for idx in new_indices]
            distribution = self._ws_manager.distribute_items(index_items)

            for conn_idx, conn_items in distribution.items():
                # Create connection if needed
                if conn_idx >= self._ws_manager.connection_count:
                    await self._ws_manager.create_connection()

                # Build channels (remove index: prefix)
                conn_indices = [item.replace("index:", "") for item in conn_items]
                channels = [f"deribit_price_index.{idx}" for idx in conn_indices]

                # Track subscriptions BEFORE sending request to prevent race condition
                self._ws_manager.add_items(conn_idx, set(conn_items))
                self._index_subscriptions.update(conn_indices)

                try:
                    await self._ws_manager.send_request(
                        conn_idx,
                        "public/subscribe",
                        {"channels": channels}
                    )
                    self.logger.info(
                        f"Connection #{conn_idx}: subscribed to index price for {conn_indices}"
                    )
                except Exception as e:
                    self.logger.error(f"Failed to subscribe to index price on connection #{conn_idx}: {e}")

    async def _unsubscribe_index_price(self, indices: List[str]) -> None:
        """Internal: Unsubscribe from index price updates."""
        to_unsub = [i for i in indices if i in self._index_subscriptions]
        if not to_unsub:
            return

        self.logger.info(f"Unsubscribing from index price: {to_unsub}")

        for idx in to_unsub:
            # Find which connection has this index
            item_key = f"index:{idx}"
            conn_idx = self._ws_manager.find_connection_for_item(item_key)
            if conn_idx is None:
                continue

            channel = f"deribit_price_index.{idx}"
            try:
                await self._ws_manager.send_request(
                    conn_idx,
                    "public/unsubscribe",
                    {"channels": [channel]}
                )
                self._ws_manager.remove_items(conn_idx, {item_key})
                self._index_subscriptions.discard(idx)
                self.logger.debug(f"Unsubscribed from index {idx} on connection #{conn_idx}")
            except Exception as e:
                self.logger.error(f"Failed to unsubscribe from index {idx}: {e}")

    async def _handle_index_price_update(self, channel: str, data: dict) -> None:
        """Handle index price update from subscription"""
        # Parse channel: deribit_price_index.{index_name}
        parts = channel.split('.')
        if len(parts) < 2:
            return

        index_name = parts[1]

        price = _safe_decimal(data.get('price'))
        if price is None:
            self.logger.warning(f"Invalid index price data for {index_name}: {data.get('price')}")
            return

        index_data = IndexPriceData(
            index_name=index_name,
            exchange=self.exchange_name,
            price=price,
            timestamp=data.get('timestamp')
        )

        await self._emit_market_data(
            DataType.INDEX_PRICE,
            "update",
            index_name,
            index_data
        )

    # ==================== Ticker Subscription (Internal) ====================

    async def _subscribe_ticker(self, symbols: List[str]) -> None:
        """Internal: Subscribe to ticker updates for given symbols."""
        async with self._subscription_lock:
            new_symbols = [s for s in symbols if s not in self._ticker_subscriptions]
            if not new_symbols:
                self.logger.debug("All ticker symbols already subscribed")
                return

            self.logger.info(f"Subscribing to {len(new_symbols)} ticker symbols...")

            # Distribute symbols across connections (using ticker: prefix to avoid collision)
            ticker_items = [f"ticker:{sym}" for sym in new_symbols]
            distribution = self._ws_manager.distribute_items(ticker_items)

            for conn_idx, conn_items in distribution.items():
                # Create connection if needed
                if conn_idx >= self._ws_manager.connection_count:
                    await self._ws_manager.create_connection()

                # Build channels (remove ticker: prefix)
                conn_symbols = [item.replace("ticker:", "") for item in conn_items]
                channels = [f"ticker.{sym}.{self.orderbook_interval}" for sym in conn_symbols]

                # Track subscriptions BEFORE sending request
                self._ws_manager.add_items(conn_idx, set(conn_items))
                self._ticker_subscriptions.update(conn_symbols)

                # Subscribe in batches
                batch_size = 20
                for i in range(0, len(channels), batch_size):
                    batch = channels[i:i + batch_size]
                    try:
                        await self._ws_manager.send_request(
                            conn_idx,
                            "public/subscribe",
                            {"channels": batch}
                        )
                    except Exception as e:
                        self.logger.error(f"Failed to subscribe ticker on connection #{conn_idx}: {e}")

                self.logger.info(
                    f"Connection #{conn_idx}: subscribed to {len(conn_symbols)} ticker symbols"
                )

            self.logger.info(f"Total ticker subscriptions: {len(self._ticker_subscriptions)}")

    async def _unsubscribe_ticker(self, symbols: List[str]) -> None:
        """Internal: Unsubscribe from ticker updates."""
        to_unsub = [s for s in symbols if s in self._ticker_subscriptions]
        if not to_unsub:
            return

        self.logger.info(f"Unsubscribing from {len(to_unsub)} ticker symbols...")

        for sym in to_unsub:
            # Find which connection has this ticker
            item_key = f"ticker:{sym}"
            conn_idx = self._ws_manager.find_connection_for_item(item_key)
            if conn_idx is None:
                continue

            channel = f"ticker.{sym}.{self.orderbook_interval}"
            try:
                await self._ws_manager.send_request(
                    conn_idx,
                    "public/unsubscribe",
                    {"channels": [channel]}
                )
                self._ws_manager.remove_items(conn_idx, {item_key})
                self._ticker_subscriptions.discard(sym)
                self.logger.debug(f"Unsubscribed from ticker {sym} on connection #{conn_idx}")
            except Exception as e:
                self.logger.error(f"Failed to unsubscribe from ticker {sym}: {e}")

        self.logger.info(f"Unsubscribed from {len(to_unsub)} ticker symbols")

    async def _handle_ticker_update(self, channel: str, data: dict) -> None:
        """Handle ticker update from subscription"""
        # Parse channel: ticker.{instrument}.{interval}
        parts = channel.split('.')
        if len(parts) < 2:
            return

        symbol = parts[1]
        stats = data.get('stats', {})
        greeks = data.get('greeks', {})

        ticker_data = TickerData(
            symbol=symbol,
            exchange=self.exchange_name,
            timestamp=data.get('timestamp'),
            local_timestamp=int(time.time() * 1000),
            state=data.get('state'),
            # Price data
            last_price=_safe_decimal(data.get('last_price')),
            best_bid_price=_safe_decimal(data.get('best_bid_price')),
            best_bid_amount=_safe_decimal(data.get('best_bid_amount')),
            best_ask_price=_safe_decimal(data.get('best_ask_price')),
            best_ask_amount=_safe_decimal(data.get('best_ask_amount')),
            mark_price=_safe_decimal(data.get('mark_price')),
            index_price=_safe_decimal(data.get('index_price')),
            settlement_price=_safe_decimal(data.get('settlement_price')),
            min_price=_safe_decimal(data.get('min_price')),
            max_price=_safe_decimal(data.get('max_price')),
            # Underlying
            underlying_price=_safe_decimal(data.get('underlying_price')),
            underlying_index=data.get('underlying_index'),
            estimated_delivery_price=_safe_decimal(data.get('estimated_delivery_price')),
            # Greeks
            delta=_safe_decimal(greeks.get('delta')),
            gamma=_safe_decimal(greeks.get('gamma')),
            vega=_safe_decimal(greeks.get('vega')),
            theta=_safe_decimal(greeks.get('theta')),
            rho=_safe_decimal(greeks.get('rho')),
            # IV
            mark_iv=_safe_decimal(data.get('mark_iv')),
            bid_iv=_safe_decimal(data.get('bid_iv')),
            ask_iv=_safe_decimal(data.get('ask_iv')),
            # Stats
            open_interest=_safe_decimal(data.get('open_interest')),
            volume=_safe_decimal(stats.get('volume')),
            volume_usd=_safe_decimal(stats.get('volume_usd')),
            high=_safe_decimal(stats.get('high')),
            low=_safe_decimal(stats.get('low')),
            price_change=_safe_decimal(stats.get('price_change')),
            # Interest rate
            interest_rate=_safe_decimal(data.get('interest_rate')),
        )

        await self._emit_market_data(
            DataType.TICKER,
            "update",
            symbol,
            ticker_data
        )
