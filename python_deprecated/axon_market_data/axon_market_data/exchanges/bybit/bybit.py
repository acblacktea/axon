"""
Bybit Exchange Adapter Implementation (V5 API)

Supports Spot, Linear (USDT), Inverse, and Option markets.
Uses snapshot + delta updates for orderbook management.
"""
import asyncio
import ssl
import json
from typing import List, Optional, Dict, Any, Set
from decimal import Decimal
from enum import Enum
import logging
import time

import aiohttp

from ...core.adapter import ExchangeAdapter, OrderbookManager
from ...core.models import Instrument, Orderbook, OrderbookSnapshot, DataType


class BybitMarketType(Enum):
    """Bybit market types"""
    SPOT = "spot"
    LINEAR = "linear"  # USDT/USDC perpetual
    INVERSE = "inverse"  # Coin-margined
    OPTION = "option"


class BybitAdapter(ExchangeAdapter):
    """
    Bybit exchange adapter for market data (V5 API).

    Supports:
    - Spot market
    - Linear perpetual (USDT/USDC margined)
    - Inverse perpetual (Coin margined)
    - Options

    Features:
    - Snapshot + Delta updates
    - Automatic reconnection
    - Sequence validation (u field)
    - Multi-connection support (500 subscriptions per connection)
    """

    SUPPORTED_DATA_TYPES = {DataType.ORDERBOOK}

    # WebSocket endpoints (V5 public)
    WS_URLS = {
        BybitMarketType.SPOT: "wss://stream.bybit.com/v5/public/spot",
        BybitMarketType.LINEAR: "wss://stream.bybit.com/v5/public/linear",
        BybitMarketType.INVERSE: "wss://stream.bybit.com/v5/public/inverse",
        BybitMarketType.OPTION: "wss://stream.bybit.com/v5/public/option",
    }

    # REST API base
    REST_URL = "https://api.bybit.com"

    # Available depth levels (updated to match Bybit API v5 documentation)
    # https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook
    DEPTH_LEVELS = {
        BybitMarketType.SPOT: [1, 50, 200, 1000],
        BybitMarketType.LINEAR: [1, 50, 200, 1000],
        BybitMarketType.INVERSE: [1, 50, 200, 1000],
        BybitMarketType.OPTION: [25, 100],
    }

    def __init__(
        self,
        market_type: BybitMarketType = BybitMarketType.LINEAR,
        logger: Optional[logging.Logger] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 1000,
        orderbook_depth: int = 200,  # 1, 50, 200, 1000 (market dependent)
        max_subscriptions_per_connection: int = 200,  # Bybit limit is ~500
        testnet: bool = False,
    ):
        exchange_name = f"bybit_{market_type.value}"
        super().__init__(exchange_name, logger)

        self.market_type = market_type
        self.testnet = testnet

        # Adjust URLs for testnet
        if testnet:
            base_ws = "wss://stream-testnet.bybit.com/v5/public"
            self.ws_url = f"{base_ws}/{market_type.value}"
            self.rest_url = "https://api-testnet.bybit.com"
        else:
            self.ws_url = self.WS_URLS[market_type]
            self.rest_url = self.REST_URL

        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self.orderbook_depth = orderbook_depth
        self.max_subscriptions_per_connection = max_subscriptions_per_connection

        # Orderbook manager
        self._ob_manager: Optional[OrderbookManager] = None

        # Subscription tracking
        self._subscription_lock = asyncio.Lock()

        # WebSocket connections
        self._connections: List[Dict[str, Any]] = []
        self._connection_symbols: List[Set[str]] = []

        # Sequence tracking
        self._update_ids: Dict[str, int] = {}

        # Ping task
        self._ping_task: Optional[asyncio.Task] = None

    # ==================== Symbol Conversion ====================

    def convert_to_exchange_symbol(self, unified_symbol: str) -> str:
        """Convert BTC_USDT_SPOT/BTC_USDT_PERP -> BTCUSDT"""
        symbol = unified_symbol
        if symbol.endswith('_SPOT') or symbol.endswith('_PERP'):
            symbol = symbol.rsplit('_', 1)[0]
        return symbol.replace('_', '')

    def convert_to_unified_symbol(self, exchange_symbol: str) -> str:
        """Convert BTCUSDT -> BTC_USDT_SPOT/BTC_USDT_PERP"""
        suffix = '_SPOT' if self.market_type == BybitMarketType.SPOT else '_PERP'
        for quote in ['USDT', 'USDC', 'BTC', 'ETH']:
            if exchange_symbol.endswith(quote):
                base = exchange_symbol[:-len(quote)]
                return f"{base}_{quote}{suffix}"
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
            f"Initializing Bybit adapter ({self.market_type.value}, "
            f"{'testnet' if self.testnet else 'mainnet'})..."
        )

        self._is_connected = True
        self._is_running = True
        self.logger.info("Bybit adapter initialized")

    async def disconnect(self) -> None:
        """Close all WebSocket connections"""
        self.logger.info(f"Disconnecting {len(self._connections)} connection(s)...")

        self._is_running = False
        self._is_connected = False

        # Cancel ping tasks
        if self._ping_task:
            self._ping_task.cancel()
            try:
                await self._ping_task
            except asyncio.CancelledError:
                pass

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
        self.logger.info(f"Creating Bybit WebSocket connection #{conn_idx}...")

        try:
            ssl_context = ssl.create_default_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            session = aiohttp.ClientSession(connector=connector)

            ws = await session.ws_connect(self.ws_url, heartbeat=20.0)

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
                await asyncio.sleep(20)  # Bybit requires ping every 20s

                if conn.get('ws') and conn.get('is_connected'):
                    ping_msg = {"op": "ping"}
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

            ws = await session.ws_connect(self.ws_url, heartbeat=20.0)

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

        topics = [f"orderbook.{self.orderbook_depth}.{sym}" for sym in symbols]

        # Subscribe in batches
        batch_size = 10
        for i in range(0, len(topics), batch_size):
            batch = topics[i:i + batch_size]
            await self._send_subscribe(conn_idx, batch)

        for symbol in symbols:
            await self._emit_reconnect(symbol)

    async def _handle_message(self, conn_idx: int, data: dict) -> None:
        """Handle incoming WebSocket message"""
        # Handle pong
        if data.get('op') == 'pong':
            return

        # Handle subscription response
        if data.get('op') == 'subscribe':
            success = data.get('success', False)
            if not success:
                self.logger.error(f"Subscription failed: {data.get('ret_msg')}")
            return

        # Handle orderbook data
        topic = data.get('topic', '')
        if topic.startswith('orderbook.'):
            await self._handle_orderbook_update(data)

    async def _handle_orderbook_update(self, data: dict) -> None:
        """Handle orderbook update"""
        topic = data.get('topic', '')
        update_type = data.get('type', '')  # "snapshot" or "delta"
        ob_data = data.get('data', {})

        # Parse topic: orderbook.{depth}.{symbol}
        parts = topic.split('.')
        if len(parts) < 3:
            return

        symbol = parts[2]
        timestamp = data.get('ts')

        # Parse bids and asks
        bids = [(Decimal(str(p)), Decimal(str(q))) for p, q in ob_data.get('b', [])]
        asks = [(Decimal(str(p)), Decimal(str(q))) for p, q in ob_data.get('a', [])]

        update_id = ob_data.get('u')
        seq = ob_data.get('seq')

        update_data = {
            'bids': bids,
            'asks': asks,
            'sequence': update_id,
            'timestamp': timestamp,
        }

        if update_type == 'snapshot':
            await self.orderbook_manager.apply_snapshot(symbol, update_data)
            self._update_ids[symbol] = update_id

            snapshot = self.orderbook_manager.get_snapshot(symbol, self.orderbook_depth)
            if snapshot:
                await self._emit_snapshot(symbol, snapshot)

        elif update_type == 'delta':
            # Validate sequence (u=1 means service restart, need new snapshot)
            if update_id == 1:
                self.logger.warning(f"Service restart detected for {symbol}, waiting for new snapshot")
                return

            # Apply delta
            success = await self.orderbook_manager.apply_update(symbol, update_data)
            if success:
                self._update_ids[symbol] = update_id
                snapshot = self.orderbook_manager.get_snapshot(symbol, self.orderbook_depth)
                if snapshot:
                    await self._emit_update(symbol, snapshot)
            else:
                self.logger.warning(f"Failed to apply delta for {symbol}, waiting for snapshot")

    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """Subscribe to market data updates"""
        if not self._is_connected:
            raise ConnectionError("Not connected to Bybit. Call connect() first.")

        if data_type not in self.SUPPORTED_DATA_TYPES:
            raise ValueError(f"Unsupported data type: {data_type}")

        if data_type == DataType.ORDERBOOK:
            depth = kwargs.get('depth', self.orderbook_depth)
            await self._subscribe_orderbook(symbols, depth)

    async def unsubscribe(self, data_type: DataType, symbols: List[str]) -> None:
        """Unsubscribe from market data updates"""
        if data_type == DataType.ORDERBOOK:
            await self._unsubscribe_orderbook(symbols)

    async def _subscribe_orderbook(self, symbols: List[str], depth: int = 50) -> None:
        """Subscribe to orderbook updates"""
        async with self._subscription_lock:
            # Validate depth
            valid_depths = self.DEPTH_LEVELS.get(self.market_type, [50])
            if depth not in valid_depths:
                depth = min(valid_depths, key=lambda x: abs(x - depth))
                self.logger.warning(f"Adjusted depth to {depth} (valid: {valid_depths})")

            self.orderbook_depth = depth

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

                topics = [f"orderbook.{depth}.{sym}" for sym in conn_symbols]

                # Subscribe in batches
                batch_size = 10
                for i in range(0, len(topics), batch_size):
                    batch = topics[i:i + batch_size]
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

                topic = f"orderbook.{self.orderbook_depth}.{symbol}"

                try:
                    await self._send_unsubscribe(conn_idx, [topic])
                    self._connection_symbols[conn_idx].discard(symbol)
                    self._subscribed_symbols.discard(symbol)
                    self.orderbook_manager.remove_orderbook(symbol)
                    self._update_ids.pop(symbol, None)
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

    async def _send_subscribe(self, conn_idx: int, topics: List[str]) -> None:
        """Send subscribe request"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            raise ConnectionError(f"Connection #{conn_idx} not connected")

        request = {
            "op": "subscribe",
            "args": topics
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Subscribed to {len(topics)} topics on connection #{conn_idx}")

    async def _send_unsubscribe(self, conn_idx: int, topics: List[str]) -> None:
        """Send unsubscribe request"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            return

        request = {
            "op": "unsubscribe",
            "args": topics
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Unsubscribed from {len(topics)} topics on connection #{conn_idx}")

    async def get_instruments(
        self,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """Get available instruments from Bybit"""
        session = await self._create_session()

        try:
            # Map market type to category
            category_map = {
                BybitMarketType.SPOT: "spot",
                BybitMarketType.LINEAR: "linear",
                BybitMarketType.INVERSE: "inverse",
                BybitMarketType.OPTION: "option",
            }
            category = category_map[self.market_type]

            url = f"{self.rest_url}/v5/market/instruments-info"
            params = {"category": category}

            if currency and self.market_type == BybitMarketType.OPTION:
                params["baseCoin"] = currency.upper()

            async with session.get(url, params=params) as resp:
                data = await resp.json()

                if data.get('retCode') != 0:
                    raise Exception(f"API Error: {data.get('retMsg')}")

                instruments = []
                items = data.get('result', {}).get('list', [])

                for item in items:
                    status = item.get('status', '')
                    if not expired and status != 'Trading':
                        continue

                    base = item.get('baseCoin', '')
                    quote = item.get('quoteCoin', '')

                    if currency and base.upper() != currency.upper():
                        continue

                    # Determine type
                    contract_type = item.get('contractType', '')
                    if self.market_type == BybitMarketType.SPOT:
                        inst_type = 'spot'
                    elif self.market_type == BybitMarketType.OPTION:
                        inst_type = 'option'
                    elif 'Perpetual' in contract_type:
                        inst_type = 'perpetual'
                    else:
                        inst_type = 'future'

                    if kind and inst_type != kind:
                        continue

                    symbol = item.get('symbol', '')

                    tick_size = None
                    min_qty = None
                    if 'priceFilter' in item:
                        tick_size = Decimal(str(item['priceFilter'].get('tickSize', 0)))
                    if 'lotSizeFilter' in item:
                        min_qty = Decimal(str(item['lotSizeFilter'].get('minOrderQty', 0)))

                    inst = Instrument(
                        symbol=symbol,
                        exchange=self.exchange_name,
                        base_currency=base,
                        quote_currency=quote,
                        instrument_type=inst_type,
                        is_active=status == 'Trading',
                        tick_size=tick_size,
                        min_trade_amount=min_qty,
                    )

                    # Option specific
                    if self.market_type == BybitMarketType.OPTION:
                        inst.option_type = item.get('optionsType', '').lower()

                    instruments.append(inst)

                return instruments

        finally:
            await session.close()

    async def request_orderbook_snapshot(self, symbol: str, depth: int = 50) -> Orderbook:
        """Request a full orderbook snapshot via REST API"""
        session = await self._create_session()

        try:
            category_map = {
                BybitMarketType.SPOT: "spot",
                BybitMarketType.LINEAR: "linear",
                BybitMarketType.INVERSE: "inverse",
                BybitMarketType.OPTION: "option",
            }
            category = category_map[self.market_type]

            url = f"{self.rest_url}/v5/market/orderbook"
            params = {
                "category": category,
                "symbol": symbol,
                "limit": depth
            }

            async with session.get(url, params=params) as resp:
                data = await resp.json()

                if data.get('retCode') != 0:
                    raise Exception(f"API Error: {data.get('retMsg')}")

                result = data.get('result', {})

                ob = Orderbook(
                    symbol=symbol,
                    exchange=self.exchange_name
                )

                for bid in result.get('b', []):
                    price = Decimal(str(bid[0]))
                    qty = Decimal(str(bid[1]))
                    ob.bids[price] = qty

                for ask in result.get('a', []):
                    price = Decimal(str(ask[0]))
                    qty = Decimal(str(ask[1]))
                    ob.asks[price] = qty

                ob.sequence = result.get('u')
                ob.timestamp = result.get('ts')

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
            'market_type': self.market_type.value,
            'total_connections': len(self._connections),
            'active_connections': active,
            'total_subscriptions': len(self._subscribed_symbols),
            'max_subscriptions_per_connection': self.max_subscriptions_per_connection,
            'connections': details
        }
