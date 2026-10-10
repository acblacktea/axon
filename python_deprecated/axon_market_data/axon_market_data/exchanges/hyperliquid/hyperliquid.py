"""
Hyperliquid Exchange Adapter Implementation

DEX on its own L1 chain. Uses snapshot-only updates (no incremental diffs).
"""
import asyncio
import ssl
import json
from typing import List, Optional, Dict, Any, Set
from decimal import Decimal
import logging
import time

import aiohttp

from ...core.adapter import ExchangeAdapter, OrderbookManager
from ...core.models import Instrument, Orderbook, OrderbookSnapshot, DataType


class HyperliquidAdapter(ExchangeAdapter):
    """
    Hyperliquid exchange adapter for market data.

    Hyperliquid is a DEX running on its own L1 blockchain.

    Features:
    - Snapshot-only updates (pushed on each block, ~500ms intervals)
    - No sequence validation needed (pure snapshots)
    - Automatic reconnection
    - Configurable depth (default 20, max 100)
    """

    SUPPORTED_DATA_TYPES = {DataType.ORDERBOOK}

    # WebSocket endpoints
    WS_URL = "wss://api.hyperliquid.xyz/ws"
    WS_URL_TESTNET = "wss://api.hyperliquid-testnet.xyz/ws"

    # REST API
    REST_URL = "https://api.hyperliquid.xyz"
    REST_URL_TESTNET = "https://api.hyperliquid-testnet.xyz"

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 1000,
        n_levels: int = 100,  # Default 100, max 100
        n_sig_figs: Optional[int] = None,  # Price grouping by significant figures
        testnet: bool = False,
        max_subscriptions_per_connection: int = 100,
    ):
        super().__init__("hyperliquid", logger)

        self.testnet = testnet
        self.ws_url = self.WS_URL_TESTNET if testnet else self.WS_URL
        self.rest_url = self.REST_URL_TESTNET if testnet else self.REST_URL

        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self.n_levels = min(n_levels, 100)  # Max 100 levels
        self.n_sig_figs = n_sig_figs
        self.max_subscriptions_per_connection = max_subscriptions_per_connection

        # Orderbook manager
        self._ob_manager: Optional[OrderbookManager] = None

        # Subscription tracking
        self._subscription_lock = asyncio.Lock()

        # WebSocket connections
        self._connections: List[Dict[str, Any]] = []
        self._connection_symbols: List[Set[str]] = []

    # ==================== Symbol Conversion ====================

    def convert_to_exchange_symbol(self, unified_symbol: str) -> str:
        """Convert BTC_USD_PERP -> BTC (Hyperliquid uses base only for perps)"""
        symbol = unified_symbol
        if symbol.endswith('_PERP'):
            symbol = symbol[:-5]
        if '_' in symbol:
            base, _ = symbol.split('_', 1)
            return base
        return symbol

    def convert_to_unified_symbol(self, exchange_symbol: str) -> str:
        """Convert BTC -> BTC_USD_PERP"""
        return f"{exchange_symbol}_USD_PERP"

    @property
    def orderbook_manager(self) -> OrderbookManager:
        if self._ob_manager is None:
            self._ob_manager = OrderbookManager(self, self.logger)
        return self._ob_manager

    async def _create_session(self) -> aiohttp.ClientSession:
        """Create aiohttp session with SSL context"""
        ssl_context = ssl.create_default_context()
        connector = aiohttp.TCPConnector(ssl=ssl_context)
        return aiohttp.ClientSession(connector=connector)

    async def connect(self) -> None:
        """Initialize adapter"""
        if self._is_connected:
            self.logger.warning("Already connected")
            return

        self.logger.info(
            f"Initializing Hyperliquid adapter "
            f"({'testnet' if self.testnet else 'mainnet'}, {self.n_levels} levels)..."
        )

        self._is_connected = True
        self._is_running = True
        self.logger.info("Hyperliquid adapter initialized")

    async def disconnect(self) -> None:
        """Close all WebSocket connections"""
        self.logger.info(f"Disconnecting {len(self._connections)} connection(s)...")

        self._is_running = False
        self._is_connected = False

        for conn in self._connections:
            await self._close_connection(conn)

        self._connections.clear()
        self._connection_symbols.clear()
        self.logger.info("All connections disconnected")

    async def _close_connection(self, conn: Dict[str, Any]) -> None:
        """Close a single connection"""
        try:
            if conn.get('message_task'):
                conn['message_task'].cancel()
                try:
                    await conn['message_task']
                except asyncio.CancelledError:
                    pass

            if conn.get('ws') and not conn['ws'].closed:
                await conn['ws'].close()

            if conn.get('session') and not conn['session'].closed:
                await conn['session'].close()
                await asyncio.sleep(0.1)

        except Exception as e:
            self.logger.error(f"Error closing connection: {e}")

    async def _create_connection(self) -> int:
        """Create a new WebSocket connection"""
        conn_idx = len(self._connections)
        self.logger.info(f"Creating Hyperliquid WebSocket connection #{conn_idx}...")

        try:
            ssl_context = ssl.create_default_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            session = aiohttp.ClientSession(connector=connector)

            ws = await session.ws_connect(self.ws_url, heartbeat=30.0)

            conn = {
                'ws': ws,
                'session': session,
                'is_connected': True,
                'reconnect_attempts': 0,
                'message_task': None,
            }

            self._connections.append(conn)
            self._connection_symbols.append(set())

            # Start message loop
            conn['message_task'] = asyncio.create_task(
                self._message_loop(conn_idx)
            )

            self.logger.info(f"Connection #{conn_idx} created successfully")
            return conn_idx

        except Exception as e:
            self.logger.error(f"Failed to create connection #{conn_idx}: {e}")
            raise

    async def _message_loop(self, conn_idx: int) -> None:
        """Message receiving loop for a connection"""
        conn = self._connections[conn_idx]
        need_reconnect = False

        while self._is_running and conn.get('ws'):
            try:
                msg = await conn['ws'].receive()

                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._handle_message(conn_idx, data)

                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    self.logger.warning(f"Connection #{conn_idx} closed by server")
                    need_reconnect = True
                    break

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error(f"Connection #{conn_idx} error")
                    need_reconnect = True
                    break

            except asyncio.CancelledError:
                break
            except json.JSONDecodeError as e:
                self.logger.error(f"Connection #{conn_idx} failed to parse message: {e}")
            except Exception as e:
                self.logger.error(f"Connection #{conn_idx} message loop error: {e}", exc_info=True)
                need_reconnect = True
                break

        conn['is_connected'] = False

        if need_reconnect and self._is_running:
            asyncio.create_task(self._reconnect(conn_idx))

    async def _reconnect(self, conn_idx: int) -> bool:
        """Reconnect a specific connection"""
        if conn_idx >= len(self._connections):
            return False

        conn = self._connections[conn_idx]
        symbols_to_restore = list(self._connection_symbols[conn_idx])

        conn['reconnect_attempts'] = conn.get('reconnect_attempts', 0) + 1

        if conn['reconnect_attempts'] > self.max_reconnect_attempts:
            self.logger.error(f"Connection #{conn_idx} exceeded max reconnect attempts")
            return False

        await self._close_connection(conn)

        delay = self.reconnect_delay * (2 ** (conn['reconnect_attempts'] - 1))
        delay = min(delay, 60.0)
        self.logger.info(f"Connection #{conn_idx} waiting {delay:.1f}s before reconnect...")
        await asyncio.sleep(delay)

        if not self._is_running:
            return False

        try:
            ssl_context = ssl.create_default_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            session = aiohttp.ClientSession(connector=connector)

            ws = await session.ws_connect(self.ws_url, heartbeat=30.0)

            conn['ws'] = ws
            conn['session'] = session
            conn['is_connected'] = True

            conn['message_task'] = asyncio.create_task(
                self._message_loop(conn_idx)
            )

            self.logger.info(f"Connection #{conn_idx} reconnected successfully")

            # Resubscribe
            if symbols_to_restore:
                for symbol in symbols_to_restore:
                    await self._send_subscribe(conn_idx, symbol)
                    await self._emit_reconnect(symbol)

            conn['reconnect_attempts'] = 0
            return True

        except Exception as e:
            self.logger.error(f"Failed to reconnect connection #{conn_idx}: {e}")
            if self._is_running:
                asyncio.create_task(self._reconnect(conn_idx))
            return False

    async def _handle_message(self, conn_idx: int, data: dict) -> None:
        """Handle incoming WebSocket message"""
        channel = data.get('channel')

        if channel == 'l2Book':
            await self._handle_orderbook_update(data)
        elif channel == 'subscriptionResponse':
            # Subscription confirmation
            method = data.get('data', {}).get('method')
            if method == 'subscribe':
                self.logger.debug(f"Subscription confirmed: {data.get('data', {}).get('subscription')}")

    async def _handle_orderbook_update(self, data: dict) -> None:
        """Handle orderbook update (always snapshot)"""
        ob_data = data.get('data', {})
        coin = ob_data.get('coin', '')
        timestamp = ob_data.get('time')
        levels = ob_data.get('levels', [[], []])

        if not coin or len(levels) < 2:
            return

        # levels[0] = bids, levels[1] = asks
        # Each level: {"px": "price", "sz": "size", "n": num_orders}
        bids = []
        for level in levels[0]:
            price = Decimal(str(level.get('px', 0)))
            qty = Decimal(str(level.get('sz', 0)))
            bids.append((price, qty))

        asks = []
        for level in levels[1]:
            price = Decimal(str(level.get('px', 0)))
            qty = Decimal(str(level.get('sz', 0)))
            asks.append((price, qty))

        update_data = {
            'bids': bids,
            'asks': asks,
            'timestamp': timestamp,
        }

        # Always apply as snapshot (Hyperliquid doesn't do incremental)
        await self.orderbook_manager.apply_snapshot(coin, update_data)

        snapshot = self.orderbook_manager.get_snapshot(coin, self.n_levels)
        if snapshot:
            await self._emit_update(coin, snapshot)

    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """Subscribe to market data updates"""
        if not self._is_connected:
            raise ConnectionError("Not connected to Hyperliquid. Call connect() first.")

        if data_type not in self.SUPPORTED_DATA_TYPES:
            raise ValueError(f"Unsupported data type: {data_type}")

        if data_type == DataType.ORDERBOOK:
            await self._subscribe_orderbook(symbols)

    async def unsubscribe(self, data_type: DataType, symbols: List[str]) -> None:
        """Unsubscribe from market data updates"""
        if data_type == DataType.ORDERBOOK:
            await self._unsubscribe_orderbook(symbols)

    async def _subscribe_orderbook(self, symbols: List[str]) -> None:
        """Subscribe to orderbook updates"""
        async with self._subscription_lock:
            # Filter already subscribed
            existing = set()
            for conn_symbols in self._connection_symbols:
                existing.update(conn_symbols)

            new_symbols = [s for s in symbols if s not in existing]
            if not new_symbols:
                self.logger.debug("All symbols already subscribed")
                return

            self.logger.info(f"Subscribing to {len(new_symbols)} orderbook symbols...")

            # Distribute symbols
            distribution = self._distribute_symbols(new_symbols)

            for conn_idx, conn_symbols in distribution.items():
                if conn_idx >= len(self._connections):
                    await self._create_connection()

                self._connection_symbols[conn_idx].update(conn_symbols)
                self._subscribed_symbols.update(conn_symbols)

                for symbol in conn_symbols:
                    await self._send_subscribe(conn_idx, symbol)

            self.logger.info(
                f"Total: {len(self._subscribed_symbols)} symbols across "
                f"{len(self._connections)} connections"
            )

    async def _unsubscribe_orderbook(self, symbols: List[str]) -> None:
        """Unsubscribe from orderbook updates"""
        async with self._subscription_lock:
            to_unsub = [s for s in symbols if s in self._subscribed_symbols]
            if not to_unsub:
                return

            self.logger.info(f"Unsubscribing from {len(to_unsub)} symbols...")

            for symbol in to_unsub:
                conn_idx = self._find_connection_for_symbol(symbol)
                if conn_idx is None:
                    continue

                try:
                    await self._send_unsubscribe(conn_idx, symbol)
                    self._connection_symbols[conn_idx].discard(symbol)
                    self._subscribed_symbols.discard(symbol)
                    self.orderbook_manager.remove_orderbook(symbol)
                except Exception as e:
                    self.logger.error(f"Failed to unsubscribe {symbol}: {e}")

    def _distribute_symbols(self, symbols: List[str]) -> Dict[int, List[str]]:
        """Distribute symbols across connections"""
        distribution: Dict[int, List[str]] = {}
        remaining = symbols.copy()

        for i, conn_symbols in enumerate(self._connection_symbols):
            available = self.max_subscriptions_per_connection - len(conn_symbols)
            if available > 0 and remaining:
                to_add = remaining[:available]
                distribution[i] = to_add
                remaining = remaining[available:]

        next_idx = len(self._connections)
        while remaining:
            to_add = remaining[:self.max_subscriptions_per_connection]
            distribution[next_idx] = to_add
            remaining = remaining[self.max_subscriptions_per_connection:]
            next_idx += 1

        return distribution

    def _find_connection_for_symbol(self, symbol: str) -> Optional[int]:
        """Find which connection has a symbol"""
        for i, symbols in enumerate(self._connection_symbols):
            if symbol in symbols:
                return i
        return None

    async def _send_subscribe(self, conn_idx: int, coin: str) -> None:
        """Send subscribe request for a coin"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            raise ConnectionError(f"Connection #{conn_idx} not connected")

        subscription = {
            "type": "l2Book",
            "coin": coin,
        }

        # Add optional parameters
        if self.n_levels:
            subscription["nLevels"] = self.n_levels
        if self.n_sig_figs:
            subscription["nSigFigs"] = self.n_sig_figs

        request = {
            "method": "subscribe",
            "subscription": subscription
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Subscribed to {coin} on connection #{conn_idx}")

    async def _send_unsubscribe(self, conn_idx: int, coin: str) -> None:
        """Send unsubscribe request for a coin"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            return

        subscription = {
            "type": "l2Book",
            "coin": coin,
        }

        request = {
            "method": "unsubscribe",
            "subscription": subscription
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Unsubscribed from {coin} on connection #{conn_idx}")

    async def get_instruments(
        self,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """Get available instruments from Hyperliquid"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}/info"

            # Get meta info
            async with session.post(url, json={"type": "meta"}) as resp:
                data = await resp.json()

            instruments = []
            universe = data.get('universe', [])

            for item in universe:
                name = item.get('name', '')
                sz_decimals = item.get('szDecimals', 0)

                if currency and not name.startswith(currency.upper()):
                    continue

                # Hyperliquid is all perpetual
                inst = Instrument(
                    symbol=name,
                    exchange=self.exchange_name,
                    base_currency=name,
                    quote_currency="USD",
                    instrument_type="perpetual",
                    is_active=True,
                    min_trade_amount=Decimal(str(10 ** -sz_decimals)) if sz_decimals else None,
                )

                instruments.append(inst)

            return instruments

        finally:
            await session.close()

    async def request_orderbook_snapshot(self, symbol: str, depth: int = 100) -> Orderbook:
        """Request a full orderbook snapshot via REST API"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}/info"

            request_data = {
                "type": "l2Book",
                "coin": symbol,
            }

            if self.n_sig_figs:
                request_data["nSigFigs"] = self.n_sig_figs

            async with session.post(url, json=request_data) as resp:
                data = await resp.json()

            ob = Orderbook(
                symbol=symbol,
                exchange=self.exchange_name
            )

            levels = data.get('levels', [[], []])

            for level in levels[0]:  # bids
                price = Decimal(str(level.get('px', 0)))
                qty = Decimal(str(level.get('sz', 0)))
                ob.bids[price] = qty

            for level in levels[1]:  # asks
                price = Decimal(str(level.get('px', 0)))
                qty = Decimal(str(level.get('sz', 0)))
                ob.asks[price] = qty

            ob.timestamp = int(time.time() * 1000)

            return ob

        finally:
            await session.close()

    def get_connection_stats(self) -> Dict[str, Any]:
        """Get statistics about connections and subscriptions"""
        active = sum(1 for conn in self._connections if conn.get('is_connected'))

        details = []
        for i, (conn, symbols) in enumerate(zip(self._connections, self._connection_symbols)):
            details.append({
                'id': i,
                'connected': conn.get('is_connected', False),
                'subscriptions': len(symbols),
                'capacity': self.max_subscriptions_per_connection,
            })

        return {
            'testnet': self.testnet,
            'n_levels': self.n_levels,
            'total_connections': len(self._connections),
            'active_connections': active,
            'total_subscriptions': len(self._subscribed_symbols),
            'max_subscriptions_per_connection': self.max_subscriptions_per_connection,
            'connections': details
        }
