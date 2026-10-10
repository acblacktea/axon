"""
Upbit Exchange Adapter Implementation

Korean cryptocurrency exchange. Uses snapshot/realtime updates.
Primary trading pairs are KRW-based (Korean Won).
"""
import asyncio
import ssl
import json
import uuid
from typing import List, Optional, Dict, Any, Set
from decimal import Decimal
import logging
import time

import aiohttp

from ...core.adapter import ExchangeAdapter, OrderbookManager
from ...core.models import Instrument, Orderbook, OrderbookSnapshot, DataType


class UpbitAdapter(ExchangeAdapter):
    """
    Upbit exchange adapter for market data.

    Upbit is a major Korean cryptocurrency exchange.

    Features:
    - Snapshot/Realtime updates
    - Configurable orderbook units (max 15)
    - Support for KRW, BTC, USDT markets
    - Automatic reconnection
    """

    SUPPORTED_DATA_TYPES = {DataType.ORDERBOOK}

    # WebSocket endpoints by region
    WS_URLS = {
        'kr': 'wss://api.upbit.com/websocket/v1',  # Korea (main)
        'sg': 'wss://sg-api.upbit.com/websocket/v1',
        'id': 'wss://id-api.upbit.com/websocket/v1',
        'th': 'wss://th-api.upbit.com/websocket/v1',
    }

    # REST API
    REST_URL = "https://api.upbit.com"

    # Max orderbook units
    MAX_ORDERBOOK_UNITS = 15

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 1000,
        orderbook_units: int = 15,  # Max 15 levels per side
        region: str = 'kr',  # kr (main), sg, id, th
        use_simple_format: bool = True,  # Use abbreviated field names
        max_subscriptions_per_connection: int = 50,
    ):
        super().__init__("upbit", logger)

        self.region = region
        self.ws_url = self.WS_URLS.get(region, self.WS_URLS['sg'])
        self.rest_url = self.REST_URL

        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self.orderbook_units = min(orderbook_units, self.MAX_ORDERBOOK_UNITS)
        self.use_simple_format = use_simple_format
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
        """Convert BTC_KRW_SPOT -> KRW-BTC (Upbit uses QUOTE-BASE format)"""
        symbol = unified_symbol
        if symbol.endswith('_SPOT'):
            symbol = symbol[:-5]
        if '_' in symbol:
            base, quote = symbol.split('_', 1)
            return f"{quote}-{base}"
        return symbol

    def convert_to_unified_symbol(self, exchange_symbol: str) -> str:
        """Convert KRW-BTC -> BTC_KRW_SPOT"""
        if '-' in exchange_symbol:
            quote, base = exchange_symbol.split('-', 1)
            return f"{base}_{quote}_SPOT"
        return exchange_symbol

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
            f"Initializing Upbit adapter (region={self.region}, "
            f"units={self.orderbook_units})..."
        )

        self._is_connected = True
        self._is_running = True
        self.logger.info("Upbit adapter initialized")

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

            if conn.get('ping_task'):
                conn['ping_task'].cancel()
                try:
                    await conn['ping_task']
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
        self.logger.info(f"Creating Upbit WebSocket connection #{conn_idx}...")

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
                'ping_task': None,
                'ticket': str(uuid.uuid4()),  # Unique ticket for this connection
            }

            self._connections.append(conn)
            self._connection_symbols.append(set())

            # Start message loop
            conn['message_task'] = asyncio.create_task(
                self._message_loop(conn_idx)
            )

            # Start ping loop (Upbit requires activity within 120s)
            conn['ping_task'] = asyncio.create_task(
                self._ping_loop(conn_idx)
            )

            self.logger.info(f"Connection #{conn_idx} created successfully")
            return conn_idx

        except Exception as e:
            self.logger.error(f"Failed to create connection #{conn_idx}: {e}")
            raise

    async def _ping_loop(self, conn_idx: int) -> None:
        """Send periodic ping to keep connection alive"""
        conn = self._connections[conn_idx]

        while self._is_running and conn.get('is_connected'):
            try:
                await asyncio.sleep(60)  # Upbit timeout is 120s

                if conn.get('ws') and conn.get('is_connected'):
                    # Send WebSocket ping frame
                    await conn['ws'].ping()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.warning(f"Connection #{conn_idx} ping failed: {e}")

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

                elif msg.type == aiohttp.WSMsgType.BINARY:
                    # Upbit may send binary data
                    data = json.loads(msg.data.decode('utf-8'))
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
            conn['ticket'] = str(uuid.uuid4())

            conn['message_task'] = asyncio.create_task(
                self._message_loop(conn_idx)
            )

            conn['ping_task'] = asyncio.create_task(
                self._ping_loop(conn_idx)
            )

            self.logger.info(f"Connection #{conn_idx} reconnected successfully")

            # Resubscribe
            if symbols_to_restore:
                await self._send_subscribe(conn_idx, symbols_to_restore)
                for symbol in symbols_to_restore:
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
        # Get message type
        msg_type = data.get('type') or data.get('ty')

        if msg_type == 'orderbook':
            await self._handle_orderbook_update(data)
        elif msg_type == 'error':
            self.logger.error(f"Upbit error: {data.get('message', data)}")

    async def _handle_orderbook_update(self, data: dict) -> None:
        """Handle orderbook update"""
        # Field names depend on format setting
        if self.use_simple_format:
            symbol = data.get('cd', '')  # code
            timestamp = data.get('tms')  # timestamp
            units = data.get('obu', [])  # orderbook_units
            stream_type = data.get('st', '')  # stream_type
        else:
            symbol = data.get('code', '')
            timestamp = data.get('timestamp')
            units = data.get('orderbook_units', [])
            stream_type = data.get('stream_type', '')

        if not symbol:
            return

        # Parse orderbook units
        bids = []
        asks = []

        for unit in units:
            if self.use_simple_format:
                bid_price = Decimal(str(unit.get('bp', 0)))
                bid_qty = Decimal(str(unit.get('bs', 0)))
                ask_price = Decimal(str(unit.get('ap', 0)))
                ask_qty = Decimal(str(unit.get('as', 0)))
            else:
                bid_price = Decimal(str(unit.get('bid_price', 0)))
                bid_qty = Decimal(str(unit.get('bid_size', 0)))
                ask_price = Decimal(str(unit.get('ask_price', 0)))
                ask_qty = Decimal(str(unit.get('ask_size', 0)))

            if bid_price > 0:
                bids.append((bid_price, bid_qty))
            if ask_price > 0:
                asks.append((ask_price, ask_qty))

        update_data = {
            'bids': bids,
            'asks': asks,
            'timestamp': timestamp,
        }

        # Upbit sends snapshot-style updates
        await self.orderbook_manager.apply_snapshot(symbol, update_data)

        snapshot = self.orderbook_manager.get_snapshot(symbol, self.orderbook_units)
        if snapshot:
            if stream_type == 'SNAPSHOT':
                await self._emit_snapshot(symbol, snapshot)
            else:
                await self._emit_update(symbol, snapshot)

    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """Subscribe to market data updates"""
        if not self._is_connected:
            raise ConnectionError("Not connected to Upbit. Call connect() first.")

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

                await self._send_subscribe(conn_idx, conn_symbols)

            self.logger.info(
                f"Total: {len(self._subscribed_symbols)} symbols across "
                f"{len(self._connections)} connections"
            )

    async def _unsubscribe_orderbook(self, symbols: List[str]) -> None:
        """Unsubscribe from orderbook updates

        Note: Upbit doesn't support unsubscribe, need to close and reconnect
        """
        async with self._subscription_lock:
            to_unsub = [s for s in symbols if s in self._subscribed_symbols]
            if not to_unsub:
                return

            self.logger.info(f"Unsubscribing from {len(to_unsub)} symbols...")

            for symbol in to_unsub:
                conn_idx = self._find_connection_for_symbol(symbol)
                if conn_idx is None:
                    continue

                self._connection_symbols[conn_idx].discard(symbol)
                self._subscribed_symbols.discard(symbol)
                self.orderbook_manager.remove_orderbook(symbol)

            # Need to resubscribe remaining symbols (Upbit limitation)
            for conn_idx, conn_symbols in enumerate(self._connection_symbols):
                if conn_symbols:
                    # Close and reopen connection with remaining symbols
                    conn = self._connections[conn_idx]
                    if conn.get('ws') and not conn['ws'].closed:
                        await conn['ws'].close()
                    # Reconnection will happen automatically in message loop

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

    async def _send_subscribe(self, conn_idx: int, symbols: List[str]) -> None:
        """Send subscribe request"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            raise ConnectionError(f"Connection #{conn_idx} not connected")

        # Build codes with orderbook units
        codes = [f"{sym}.{self.orderbook_units}" for sym in symbols]

        # Upbit subscription format
        request = [
            {"ticket": conn['ticket']},
            {
                "type": "orderbook",
                "codes": codes,
                "isOnlyRealtime": False,  # Get snapshot first
            },
        ]

        # Add format option
        if self.use_simple_format:
            request.append({"format": "SIMPLE"})
        else:
            request.append({"format": "DEFAULT"})

        await conn['ws'].send_json(request)
        self.logger.debug(f"Subscribed to {len(symbols)} symbols on connection #{conn_idx}")

    async def get_instruments(
        self,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """Get available instruments from Upbit"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}/v1/market/all"
            params = {"isDetails": "true"}

            async with session.get(url, params=params) as resp:
                data = await resp.json()

                instruments = []

                for item in data:
                    market = item.get('market', '')  # e.g., "KRW-BTC"
                    parts = market.split('-')
                    if len(parts) != 2:
                        continue

                    quote = parts[0]  # KRW, BTC, USDT
                    base = parts[1]

                    # Filter by currency
                    if currency and base.upper() != currency.upper():
                        continue

                    # Check market warning
                    market_warning = item.get('market_warning', '')
                    is_active = market_warning != 'CAUTION'

                    inst = Instrument(
                        symbol=market,
                        exchange=self.exchange_name,
                        base_currency=base,
                        quote_currency=quote,
                        instrument_type='spot',
                        is_active=is_active,
                    )

                    instruments.append(inst)

                return instruments

        finally:
            await session.close()

    async def request_orderbook_snapshot(self, symbol: str, depth: int = 15) -> Orderbook:
        """Request a full orderbook snapshot via REST API"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}/v1/orderbook"
            params = {"markets": symbol}

            async with session.get(url, params=params) as resp:
                data = await resp.json()

                if not data:
                    raise Exception(f"No orderbook data for {symbol}")

                result = data[0]

                ob = Orderbook(
                    symbol=symbol,
                    exchange=self.exchange_name
                )

                for unit in result.get('orderbook_units', []):
                    bid_price = Decimal(str(unit.get('bid_price', 0)))
                    bid_qty = Decimal(str(unit.get('bid_size', 0)))
                    ask_price = Decimal(str(unit.get('ask_price', 0)))
                    ask_qty = Decimal(str(unit.get('ask_size', 0)))

                    if bid_price > 0:
                        ob.bids[bid_price] = bid_qty
                    if ask_price > 0:
                        ob.asks[ask_price] = ask_qty

                ob.timestamp = result.get('timestamp')

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
            'region': self.region,
            'orderbook_units': self.orderbook_units,
            'total_connections': len(self._connections),
            'active_connections': active,
            'total_subscriptions': len(self._subscribed_symbols),
            'max_subscriptions_per_connection': self.max_subscriptions_per_connection,
            'connections': details
        }
