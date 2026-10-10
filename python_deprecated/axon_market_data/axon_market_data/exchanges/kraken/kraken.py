"""
Kraken Exchange Adapter Implementation (WebSocket V2)

Supports Kraken spot trading pairs with optional CRC32 checksum verification.
"""
import asyncio
import ssl
import json
import zlib
from typing import List, Optional, Dict, Any, Set
from decimal import Decimal
import logging
import time

import aiohttp

from ...core.adapter import ExchangeAdapter, OrderbookManager
from ...core.models import Instrument, Orderbook, OrderbookSnapshot, DataType


class KrakenAdapter(ExchangeAdapter):
    """
    Kraken exchange adapter for market data (WebSocket V2).

    Features:
    - Snapshot + update model
    - Optional CRC32 checksum verification
    - Configurable depth (10, 25, 100, 500, 1000)
    - Automatic reconnection
    - Uses readable symbol format (BTC/USD)
    """

    SUPPORTED_DATA_TYPES = {DataType.ORDERBOOK}

    # WebSocket endpoints
    WS_URL = "wss://ws.kraken.com/v2"

    # REST API
    REST_URL = "https://api.kraken.com"

    # Valid depth levels
    VALID_DEPTHS = [10, 25, 100, 500, 1000]

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 1000,
        depth: int = 1000,  # 10, 25, 100, 500, 1000
        max_subscriptions_per_connection: int = 100,  # Kraken limit is 200
        verify_checksum: bool = False,
    ):
        super().__init__("kraken", logger)

        self.ws_url = self.WS_URL
        self.rest_url = self.REST_URL

        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts

        # Validate and set depth
        if depth not in self.VALID_DEPTHS:
            depth = min(self.VALID_DEPTHS, key=lambda x: abs(x - depth))
            self.logger.warning(f"Adjusted depth to {depth} (valid: {self.VALID_DEPTHS})")
        self.depth = depth

        self.max_subscriptions_per_connection = max_subscriptions_per_connection
        self.verify_checksum = verify_checksum

        # Orderbook manager
        self._ob_manager: Optional[OrderbookManager] = None

        # Subscription tracking
        self._subscription_lock = asyncio.Lock()

        # WebSocket connections
        self._connections: List[Dict[str, Any]] = []
        self._connection_symbols: List[Set[str]] = []

        # Request ID counter
        self._request_id = 0

    # ==================== Symbol Conversion ====================

    def convert_to_exchange_symbol(self, unified_symbol: str) -> str:
        """Convert BTC_USD_SPOT -> BTC/USD"""
        symbol = unified_symbol
        if symbol.endswith('_SPOT'):
            symbol = symbol[:-5]
        return symbol.replace('_', '/')

    def convert_to_unified_symbol(self, exchange_symbol: str) -> str:
        """Convert BTC/USD -> BTC_USD_SPOT"""
        return exchange_symbol.replace('/', '_') + '_SPOT'

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
            f"Initializing Kraken adapter (depth={self.depth}, "
            f"checksum={'enabled' if self.verify_checksum else 'disabled'})..."
        )

        self._is_connected = True
        self._is_running = True
        self.logger.info("Kraken adapter initialized")

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
        self.logger.info(f"Creating Kraken WebSocket connection #{conn_idx}...")

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
            }

            self._connections.append(conn)
            self._connection_symbols.append(set())

            # Start message loop
            conn['message_task'] = asyncio.create_task(
                self._message_loop(conn_idx)
            )

            # Start ping loop
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
                await asyncio.sleep(30)

                if conn.get('ws') and conn.get('is_connected'):
                    self._request_id += 1
                    ping_msg = {
                        "method": "ping",
                        "req_id": self._request_id
                    }
                    await conn['ws'].send_json(ping_msg)

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
        method = data.get('method')

        # Handle pong
        if method == 'pong':
            return

        # Handle subscription response
        if method == 'subscribe':
            success = data.get('success', False)
            if not success:
                self.logger.error(f"Subscription failed: {data.get('error')}")
            return

        # Handle book channel data
        channel = data.get('channel')
        if channel == 'book':
            await self._handle_orderbook_update(data)

    async def _handle_orderbook_update(self, data: dict) -> None:
        """Handle orderbook update"""
        update_type = data.get('type', '')  # "snapshot" or "update"
        ob_list = data.get('data', [])

        if not ob_list:
            return

        for ob_data in ob_list:
            symbol = ob_data.get('symbol', '')
            checksum = ob_data.get('checksum')

            # Parse bids and asks
            bids = []
            for item in ob_data.get('bids', []):
                price = Decimal(str(item.get('price', 0)))
                qty = Decimal(str(item.get('qty', 0)))
                bids.append((price, qty))

            asks = []
            for item in ob_data.get('asks', []):
                price = Decimal(str(item.get('price', 0)))
                qty = Decimal(str(item.get('qty', 0)))
                asks.append((price, qty))

            update_data = {
                'bids': bids,
                'asks': asks,
                'timestamp': int(time.time() * 1000),
            }

            if update_type == 'snapshot':
                await self.orderbook_manager.apply_snapshot(symbol, update_data)

                # Verify checksum if enabled
                if self.verify_checksum and checksum:
                    if not self._verify_checksum(symbol, checksum):
                        self.logger.warning(f"Checksum mismatch for {symbol}")
                        continue

                snapshot = self.orderbook_manager.get_snapshot(symbol, self.depth)
                if snapshot:
                    await self._emit_snapshot(symbol, snapshot)

            elif update_type == 'update':
                success = await self.orderbook_manager.apply_update(symbol, update_data)
                if success:
                    # Verify checksum if enabled
                    if self.verify_checksum and checksum:
                        if not self._verify_checksum(symbol, checksum):
                            self.logger.warning(f"Checksum mismatch for {symbol} after update")
                            # Trigger recovery
                            asyncio.create_task(self.orderbook_manager.trigger_recovery(symbol))
                            continue

                    snapshot = self.orderbook_manager.get_snapshot(symbol, self.depth)
                    if snapshot:
                        await self._emit_update(symbol, snapshot)

    def _verify_checksum(self, symbol: str, expected_checksum: int) -> bool:
        """Verify orderbook checksum (CRC32 of top 10 levels)"""
        ob = self.orderbook_manager.get_orderbook(symbol)
        if not ob:
            return False

        # Build checksum string from top 10 bids and asks
        bids = ob.get_sorted_bids(10)
        asks = ob.get_sorted_asks(10)

        parts = []
        for i in range(10):
            if i < len(asks):
                # Remove trailing zeros from price and qty
                price_str = str(asks[i].price).rstrip('0').rstrip('.')
                qty_str = str(asks[i].quantity).rstrip('0').rstrip('.')
                parts.append(price_str)
                parts.append(qty_str)

            if i < len(bids):
                price_str = str(bids[i].price).rstrip('0').rstrip('.')
                qty_str = str(bids[i].quantity).rstrip('0').rstrip('.')
                parts.append(price_str)
                parts.append(qty_str)

        checksum_str = "".join(parts)
        calculated = zlib.crc32(checksum_str.encode()) & 0xffffffff

        return calculated == expected_checksum

    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """Subscribe to market data updates"""
        if not self._is_connected:
            raise ConnectionError("Not connected to Kraken. Call connect() first.")

        if data_type not in self.SUPPORTED_DATA_TYPES:
            raise ValueError(f"Unsupported data type: {data_type}")

        if data_type == DataType.ORDERBOOK:
            depth = kwargs.get('depth', self.depth)
            await self._subscribe_orderbook(symbols, depth)

    async def unsubscribe(self, data_type: DataType, symbols: List[str]) -> None:
        """Unsubscribe from market data updates"""
        if data_type == DataType.ORDERBOOK:
            await self._unsubscribe_orderbook(symbols)

    async def _subscribe_orderbook(self, symbols: List[str], depth: int = 25) -> None:
        """Subscribe to orderbook updates"""
        async with self._subscription_lock:
            # Validate depth
            if depth not in self.VALID_DEPTHS:
                depth = min(self.VALID_DEPTHS, key=lambda x: abs(x - depth))
            self.depth = depth

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
        """Unsubscribe from orderbook updates"""
        async with self._subscription_lock:
            to_unsub = [s for s in symbols if s in self._subscribed_symbols]
            if not to_unsub:
                return

            self.logger.info(f"Unsubscribing from {len(to_unsub)} symbols...")

            # Group by connection
            conn_symbols_map: Dict[int, List[str]] = {}
            for symbol in to_unsub:
                conn_idx = self._find_connection_for_symbol(symbol)
                if conn_idx is not None:
                    conn_symbols_map.setdefault(conn_idx, []).append(symbol)

            for conn_idx, syms in conn_symbols_map.items():
                try:
                    await self._send_unsubscribe(conn_idx, syms)
                    for symbol in syms:
                        self._connection_symbols[conn_idx].discard(symbol)
                        self._subscribed_symbols.discard(symbol)
                        self.orderbook_manager.remove_orderbook(symbol)
                except Exception as e:
                    self.logger.error(f"Failed to unsubscribe: {e}")

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

        self._request_id += 1
        request = {
            "method": "subscribe",
            "params": {
                "channel": "book",
                "symbol": symbols,
                "depth": self.depth,
                "snapshot": True
            },
            "req_id": self._request_id
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Subscribed to {len(symbols)} symbols on connection #{conn_idx}")

    async def _send_unsubscribe(self, conn_idx: int, symbols: List[str]) -> None:
        """Send unsubscribe request"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            return

        self._request_id += 1
        request = {
            "method": "unsubscribe",
            "params": {
                "channel": "book",
                "symbol": symbols
            },
            "req_id": self._request_id
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Unsubscribed from {len(symbols)} symbols on connection #{conn_idx}")

    async def get_instruments(
        self,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """Get available instruments from Kraken"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}/0/public/AssetPairs"

            async with session.get(url) as resp:
                data = await resp.json()

                if data.get('error'):
                    raise Exception(f"API Error: {data['error']}")

                instruments = []
                pairs = data.get('result', {})

                for pair_name, pair_info in pairs.items():
                    # Skip dark pool pairs
                    if pair_name.endswith('.d'):
                        continue

                    base = pair_info.get('base', '')
                    quote = pair_info.get('quote', '')
                    wsname = pair_info.get('wsname', pair_name)  # WS uses different names

                    # Filter by currency
                    if currency:
                        # Kraken uses X prefix for crypto, Z for fiat
                        clean_base = base.lstrip('XZ')
                        if clean_base.upper() != currency.upper():
                            continue

                    status = pair_info.get('status', 'online')
                    if not expired and status != 'online':
                        continue

                    # Parse tick size from pair_decimals
                    pair_decimals = pair_info.get('pair_decimals', 8)
                    tick_size = Decimal(str(10 ** -pair_decimals))

                    lot_decimals = pair_info.get('lot_decimals', 8)
                    min_qty = Decimal(str(pair_info.get('ordermin', 10 ** -lot_decimals)))

                    inst = Instrument(
                        symbol=wsname,  # Use WS name for compatibility
                        exchange=self.exchange_name,
                        base_currency=base,
                        quote_currency=quote,
                        instrument_type='spot',
                        is_active=status == 'online',
                        tick_size=tick_size,
                        min_trade_amount=min_qty,
                    )

                    instruments.append(inst)

                return instruments

        finally:
            await session.close()

    async def request_orderbook_snapshot(self, symbol: str, depth: int = 1000) -> Orderbook:
        """Request a full orderbook snapshot via REST API"""
        session = await self._create_session()

        try:
            # Convert WS symbol (BTC/USD) to REST symbol (XXBTZUSD)
            # This is a simplified conversion; actual mapping may need adjustment
            pair = symbol.replace('/', '')

            url = f"{self.rest_url}/0/public/Depth"
            params = {
                "pair": pair,
                "count": depth
            }

            async with session.get(url, params=params) as resp:
                data = await resp.json()

                if data.get('error'):
                    raise Exception(f"API Error: {data['error']}")

                result = data.get('result', {})
                # Get first result (there should only be one)
                pair_data = next(iter(result.values()), {})

                ob = Orderbook(
                    symbol=symbol,
                    exchange=self.exchange_name
                )

                for bid in pair_data.get('bids', []):
                    price = Decimal(str(bid[0]))
                    qty = Decimal(str(bid[1]))
                    ob.bids[price] = qty

                for ask in pair_data.get('asks', []):
                    price = Decimal(str(ask[0]))
                    qty = Decimal(str(ask[1]))
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
            'depth': self.depth,
            'verify_checksum': self.verify_checksum,
            'total_connections': len(self._connections),
            'active_connections': active,
            'total_subscriptions': len(self._subscribed_symbols),
            'max_subscriptions_per_connection': self.max_subscriptions_per_connection,
            'connections': details
        }
