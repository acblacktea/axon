"""
OKX Exchange Adapter Implementation

Supports all OKX instrument types via unified API.
Uses snapshot + incremental updates with optional checksum verification.
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


class OKXAdapter(ExchangeAdapter):
    """
    OKX exchange adapter for market data.

    Supports all instrument types:
    - SPOT: Spot trading pairs
    - SWAP: Perpetual swaps
    - FUTURES: Delivery futures
    - OPTION: Options

    Features:
    - Snapshot + incremental updates
    - Optional CRC32 checksum verification
    - Sequence validation (seqId/prevSeqId)
    - Automatic reconnection
    """

    SUPPORTED_DATA_TYPES = {DataType.ORDERBOOK}

    # WebSocket endpoint
    WS_URL = "wss://ws.okx.com:8443/ws/v5/public"
    WS_URL_DEMO = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"

    # REST API base
    REST_URL = "https://www.okx.com"

    def __init__(
        self,
        logger: Optional[logging.Logger] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 1000,
        max_subscriptions_per_connection: int = 200,
        verify_checksum: bool = False,  # Enable CRC32 checksum verification
        demo: bool = False,  # Use demo trading environment
    ):
        super().__init__("okx", logger)

        self.demo = demo
        self.ws_url = self.WS_URL_DEMO if demo else self.WS_URL
        self.rest_url = self.REST_URL

        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self.max_subscriptions_per_connection = max_subscriptions_per_connection
        self.verify_checksum = verify_checksum

        # Orderbook manager
        self._ob_manager: Optional[OrderbookManager] = None

        # Subscription tracking
        self._subscription_lock = asyncio.Lock()

        # WebSocket connections
        self._connections: List[Dict[str, Any]] = []
        self._connection_symbols: List[Set[str]] = []

        # Sequence tracking
        self._seq_ids: Dict[str, int] = {}

    # ==================== Symbol Conversion ====================

    def convert_to_exchange_symbol(self, unified_symbol: str) -> str:
        """Convert BTC_USDT_SPOT -> BTC-USDT, BTC_USDT_PERP -> BTC-USDT-SWAP"""
        if unified_symbol.endswith('_SPOT'):
            # BTC_USDT_SPOT -> BTC-USDT
            symbol = unified_symbol[:-5]  # Remove _SPOT
            return symbol.replace('_', '-')
        elif unified_symbol.endswith('_PERP'):
            # BTC_USDT_PERP -> BTC-USDT-SWAP
            symbol = unified_symbol[:-5]  # Remove _PERP
            return symbol.replace('_', '-') + '-SWAP'
        return unified_symbol.replace('_', '-')

    def convert_to_unified_symbol(self, exchange_symbol: str) -> str:
        """Convert BTC-USDT -> BTC_USDT_SPOT, BTC-USDT-SWAP -> BTC_USDT_PERP"""
        if exchange_symbol.endswith('-SWAP'):
            # BTC-USDT-SWAP -> BTC_USDT_PERP
            symbol = exchange_symbol[:-5]  # Remove -SWAP
            return symbol.replace('-', '_') + '_PERP'
        else:
            # BTC-USDT -> BTC_USDT_SPOT
            return exchange_symbol.replace('-', '_') + '_SPOT'

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
            f"Initializing OKX adapter ({'demo' if self.demo else 'production'})..."
        )

        self._is_connected = True
        self._is_running = True
        self.logger.info("OKX adapter initialized")

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
        self.logger.info(f"Creating OKX WebSocket connection #{conn_idx}...")

        try:
            ssl_context = ssl.create_default_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            session = aiohttp.ClientSession(connector=connector)

            ws = await session.ws_connect(self.ws_url, heartbeat=25.0)

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
                await asyncio.sleep(25)  # OKX: send ping within 30s

                if conn.get('ws') and conn.get('is_connected'):
                    await conn['ws'].send_str("ping")

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
                    # Handle pong
                    if msg.data == 'pong':
                        continue

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

            ws = await session.ws_connect(self.ws_url, heartbeat=25.0)

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
                await self._resubscribe(conn_idx, symbols_to_restore)

            conn['reconnect_attempts'] = 0
            return True

        except Exception as e:
            self.logger.error(f"Failed to reconnect connection #{conn_idx}: {e}")
            if self._is_running:
                asyncio.create_task(self._reconnect(conn_idx))
            return False

    async def _resubscribe(self, conn_idx: int, symbols: List[str]) -> None:
        """Resubscribe to symbols after reconnection"""
        self.logger.info(f"Resubscribing to {len(symbols)} symbols on connection #{conn_idx}...")

        args = [{"channel": "books", "instId": sym} for sym in symbols]

        # Subscribe in batches
        batch_size = 10
        for i in range(0, len(args), batch_size):
            batch = args[i:i + batch_size]
            await self._send_subscribe(conn_idx, batch)

        for symbol in symbols:
            await self._emit_reconnect(symbol)

    async def _handle_message(self, conn_idx: int, data: dict) -> None:
        """Handle incoming WebSocket message"""
        # Handle subscription response
        event = data.get('event')
        if event == 'subscribe':
            if data.get('code') != '0':
                self.logger.error(f"Subscription failed: {data.get('msg')}")
            return
        elif event == 'unsubscribe':
            return
        elif event == 'error':
            self.logger.error(f"WebSocket error: {data.get('msg')}")
            return

        # Handle orderbook data
        arg = data.get('arg', {})
        channel = arg.get('channel', '')

        if channel == 'books':
            await self._handle_orderbook_update(data)

    async def _handle_orderbook_update(self, data: dict) -> None:
        """Handle orderbook update"""
        arg = data.get('arg', {})
        action = data.get('action', '')  # "snapshot" or "update"
        ob_list = data.get('data', [])

        if not ob_list:
            return

        ob_data = ob_list[0]
        symbol = arg.get('instId', '')

        # Parse bids and asks: [price, size, deprecated, numOrders]
        bids = []
        for item in ob_data.get('bids', []):
            price = Decimal(str(item[0]))
            qty = Decimal(str(item[1]))
            bids.append((price, qty))

        asks = []
        for item in ob_data.get('asks', []):
            price = Decimal(str(item[0]))
            qty = Decimal(str(item[1]))
            asks.append((price, qty))

        seq_id = int(ob_data.get('seqId', 0))
        prev_seq_id = int(ob_data.get('prevSeqId', 0)) if ob_data.get('prevSeqId') else None
        timestamp = int(ob_data.get('ts', 0))
        checksum = ob_data.get('checksum')

        update_data = {
            'bids': bids,
            'asks': asks,
            'sequence': seq_id,
            'prev_change_id': prev_seq_id,
            'timestamp': timestamp,
        }

        if action == 'snapshot':
            await self.orderbook_manager.apply_snapshot(symbol, update_data)
            self._seq_ids[symbol] = seq_id

            # Verify checksum if enabled
            if self.verify_checksum and checksum:
                if not self._verify_checksum(symbol, checksum):
                    self.logger.warning(f"Checksum mismatch for {symbol}, waiting for new snapshot")
                    return

            snapshot = self.orderbook_manager.get_snapshot(symbol, 400)
            if snapshot:
                await self._emit_snapshot(symbol, snapshot)

        elif action == 'update':
            # Validate sequence
            expected_prev = self._seq_ids.get(symbol)
            if expected_prev is not None and prev_seq_id is not None:
                if prev_seq_id != expected_prev:
                    self.logger.warning(
                        f"Sequence gap for {symbol}: expected prevSeqId={expected_prev}, "
                        f"got prevSeqId={prev_seq_id}"
                    )
                    # Will wait for new snapshot from server
                    return

            success = await self.orderbook_manager.apply_update(symbol, update_data)
            if success:
                self._seq_ids[symbol] = seq_id

                # Verify checksum if enabled
                if self.verify_checksum and checksum:
                    if not self._verify_checksum(symbol, checksum):
                        self.logger.warning(f"Checksum mismatch for {symbol} after update")
                        # Don't emit, wait for recovery
                        return

                snapshot = self.orderbook_manager.get_snapshot(symbol, 400)
                if snapshot:
                    await self._emit_update(symbol, snapshot)

    def _verify_checksum(self, symbol: str, expected_checksum: int) -> bool:
        """Verify orderbook checksum (CRC32)"""
        ob = self.orderbook_manager.get_orderbook(symbol)
        if not ob:
            return False

        # Build checksum string: bid1:ask1:bid2:ask2:...
        # Use top 25 levels
        bids = ob.get_sorted_bids(25)
        asks = ob.get_sorted_asks(25)

        parts = []
        for i in range(25):
            if i < len(bids):
                parts.append(f"{bids[i].price}:{bids[i].quantity}")
            if i < len(asks):
                parts.append(f"{asks[i].price}:{asks[i].quantity}")

        checksum_str = ":".join(parts)
        calculated = zlib.crc32(checksum_str.encode()) & 0xffffffff

        # OKX uses signed int32
        if calculated > 0x7fffffff:
            calculated = calculated - 0x100000000

        return calculated == expected_checksum

    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """Subscribe to market data updates"""
        if not self._is_connected:
            raise ConnectionError("Not connected to OKX. Call connect() first.")

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

                args = [{"channel": "books", "instId": sym} for sym in conn_symbols]

                # Subscribe in batches
                batch_size = 10
                for i in range(0, len(args), batch_size):
                    batch = args[i:i + batch_size]
                    await self._send_subscribe(conn_idx, batch)

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

                args = [{"channel": "books", "instId": symbol}]

                try:
                    await self._send_unsubscribe(conn_idx, args)
                    self._connection_symbols[conn_idx].discard(symbol)
                    self._subscribed_symbols.discard(symbol)
                    self.orderbook_manager.remove_orderbook(symbol)
                    self._seq_ids.pop(symbol, None)
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

    async def _send_subscribe(self, conn_idx: int, args: List[dict]) -> None:
        """Send subscribe request"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            raise ConnectionError(f"Connection #{conn_idx} not connected")

        request = {
            "op": "subscribe",
            "args": args
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Subscribed to {len(args)} instruments on connection #{conn_idx}")

    async def _send_unsubscribe(self, conn_idx: int, args: List[dict]) -> None:
        """Send unsubscribe request"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            return

        request = {
            "op": "unsubscribe",
            "args": args
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Unsubscribed from {len(args)} instruments on connection #{conn_idx}")

    async def get_instruments(
        self,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """Get available instruments from OKX"""
        session = await self._create_session()

        try:
            instruments = []

            # Map kind to instType
            inst_types = []
            if kind:
                kind_map = {
                    'spot': ['SPOT'],
                    'perpetual': ['SWAP'],
                    'future': ['FUTURES'],
                    'option': ['OPTION'],
                }
                inst_types = kind_map.get(kind, [kind.upper()])
            else:
                inst_types = ['SPOT', 'SWAP', 'FUTURES', 'OPTION']

            for inst_type in inst_types:
                url = f"{self.rest_url}/api/v5/public/instruments"
                params = {"instType": inst_type}

                if currency and inst_type != 'SPOT':
                    params["uly"] = f"{currency.upper()}-USD"

                async with session.get(url, params=params) as resp:
                    data = await resp.json()

                    if data.get('code') != '0':
                        self.logger.warning(f"Failed to get {inst_type} instruments: {data.get('msg')}")
                        continue

                    for item in data.get('data', []):
                        state = item.get('state', '')
                        if not expired and state != 'live':
                            continue

                        base = item.get('baseCcy', item.get('ctValCcy', ''))
                        quote = item.get('quoteCcy', item.get('settleCcy', ''))

                        if currency and base.upper() != currency.upper():
                            continue

                        symbol = item.get('instId', '')

                        # Map instType to our type
                        type_map = {
                            'SPOT': 'spot',
                            'SWAP': 'perpetual',
                            'FUTURES': 'future',
                            'OPTION': 'option',
                        }
                        inst_type_name = type_map.get(inst_type, inst_type.lower())

                        tick_size = Decimal(str(item.get('tickSz', 0))) if item.get('tickSz') else None
                        min_qty = Decimal(str(item.get('minSz', 0))) if item.get('minSz') else None
                        contract_size = Decimal(str(item.get('ctVal', 1))) if item.get('ctVal') else None

                        inst = Instrument(
                            symbol=symbol,
                            exchange=self.exchange_name,
                            base_currency=base,
                            quote_currency=quote,
                            instrument_type=inst_type_name,
                            is_active=state == 'live',
                            tick_size=tick_size,
                            min_trade_amount=min_qty,
                            contract_size=contract_size,
                        )

                        # Option specific
                        if inst_type == 'OPTION':
                            inst.option_type = item.get('optType', '').lower()
                            inst.strike = Decimal(str(item.get('stk', 0))) if item.get('stk') else None

                        instruments.append(inst)

            return instruments

        finally:
            await session.close()

    async def request_orderbook_snapshot(self, symbol: str, depth: int = 400) -> Orderbook:
        """Request a full orderbook snapshot via REST API"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}/api/v5/market/books"
            params = {
                "instId": symbol,
                "sz": str(min(depth, 400))  # Max 400 for REST
            }

            async with session.get(url, params=params) as resp:
                data = await resp.json()

                if data.get('code') != '0':
                    raise Exception(f"API Error: {data.get('msg')}")

                result = data.get('data', [{}])[0]

                ob = Orderbook(
                    symbol=symbol,
                    exchange=self.exchange_name
                )

                for bid in result.get('bids', []):
                    price = Decimal(str(bid[0]))
                    qty = Decimal(str(bid[1]))
                    ob.bids[price] = qty

                for ask in result.get('asks', []):
                    price = Decimal(str(ask[0]))
                    qty = Decimal(str(ask[1]))
                    ob.asks[price] = qty

                ob.timestamp = int(result.get('ts', time.time() * 1000))

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
            'total_connections': len(self._connections),
            'active_connections': active,
            'total_subscriptions': len(self._subscribed_symbols),
            'max_subscriptions_per_connection': self.max_subscriptions_per_connection,
            'verify_checksum': self.verify_checksum,
            'connections': details
        }
