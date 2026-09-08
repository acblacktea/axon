"""
Binance Exchange Adapter Implementation

Supports Spot, USDT-M Futures, and Coin-M Futures markets.
Uses diff depth streams with REST API snapshot for orderbook management.
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


class BinanceMarketType(Enum):
    """Binance market types"""
    SPOT = "spot"
    USDT_FUTURES = "usdt_futures"
    COIN_FUTURES = "coin_futures"


class BinanceAdapter(ExchangeAdapter):
    """
    Binance exchange adapter for market data.

    Supports:
    - Spot market
    - USDT-M Perpetual/Delivery Futures
    - Coin-M Perpetual/Delivery Futures

    Features:
    - Diff depth stream with REST snapshot synchronization
    - Automatic reconnection
    - Sequence validation (U/u fields)
    - Multi-connection support (1024 streams per connection)
    """

    SUPPORTED_DATA_TYPES = {DataType.ORDERBOOK}

    # WebSocket endpoints
    WS_URLS = {
        BinanceMarketType.SPOT: "wss://stream.binance.com:9443/ws",
        BinanceMarketType.USDT_FUTURES: "wss://fstream.binance.com/ws",
        BinanceMarketType.COIN_FUTURES: "wss://dstream.binance.com/ws",
    }

    # REST endpoints
    REST_URLS = {
        BinanceMarketType.SPOT: "https://api.binance.com",
        BinanceMarketType.USDT_FUTURES: "https://fapi.binance.com",
        BinanceMarketType.COIN_FUTURES: "https://dapi.binance.com",
    }

    # Depth REST API paths
    DEPTH_PATHS = {
        BinanceMarketType.SPOT: "/api/v3/depth",
        BinanceMarketType.USDT_FUTURES: "/fapi/v1/depth",
        BinanceMarketType.COIN_FUTURES: "/dapi/v1/depth",
    }

    # Exchange info REST API paths
    EXCHANGE_INFO_PATHS = {
        BinanceMarketType.SPOT: "/api/v3/exchangeInfo",
        BinanceMarketType.USDT_FUTURES: "/fapi/v1/exchangeInfo",
        BinanceMarketType.COIN_FUTURES: "/dapi/v1/exchangeInfo",
    }

    def __init__(
        self,
        market_type: BinanceMarketType = BinanceMarketType.SPOT,
        logger: Optional[logging.Logger] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 1000,
        update_speed: str = "100ms",  # "100ms" or "1000ms"
        max_streams_per_connection: int = 200,  # Binance limit is 1024, use lower for safety
    ):
        exchange_name = f"binance_{market_type.value}"
        super().__init__(exchange_name, logger)

        self.market_type = market_type
        self.ws_url = self.WS_URLS[market_type]
        self.rest_url = self.REST_URLS[market_type]
        self.depth_path = self.DEPTH_PATHS[market_type]
        self.exchange_info_path = self.EXCHANGE_INFO_PATHS[market_type]

        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self.update_speed = update_speed
        self.max_streams_per_connection = max_streams_per_connection

        # Orderbook manager
        self._ob_manager: Optional[OrderbookManager] = None

        # Subscription tracking
        self._subscription_depth: int = 1000  # Default depth
        self._subscription_lock = asyncio.Lock()

        # WebSocket connections
        self._connections: List[Dict[str, Any]] = []
        self._connection_symbols: List[Set[str]] = []

        # Buffered events for initial sync
        self._event_buffers: Dict[str, List[dict]] = {}
        self._syncing_symbols: Set[str] = set()

        # Last update IDs for sequence validation
        self._last_update_ids: Dict[str, int] = {}

        # Request ID counter for subscribe/unsubscribe
        self._request_id = 0

    # ==================== Symbol Conversion ====================

    def convert_to_exchange_symbol(self, unified_symbol: str) -> str:
        """Convert BTC_USDT_SPOT/BTC_USDT_PERP -> BTCUSDT"""
        # Remove _SPOT or _PERP suffix, then remove remaining underscores
        symbol = unified_symbol
        if symbol.endswith('_SPOT') or symbol.endswith('_PERP'):
            symbol = symbol.rsplit('_', 1)[0]
        return symbol.replace('_', '')

    def convert_to_unified_symbol(self, exchange_symbol: str) -> str:
        """Convert BTCUSDT -> BTC_USDT_SPOT/BTC_USDT_PERP"""
        # Determine suffix based on market type
        suffix = '_PERP' if self.market_type != BinanceMarketType.SPOT else '_SPOT'
        # Common quote currencies
        for quote in ['USDT', 'USDC', 'BUSD', 'BTC', 'ETH', 'BNB']:
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
        """Initialize adapter (connections created on-demand during subscription)"""
        if self._is_connected:
            self.logger.warning("Already connected")
            return

        self.logger.info(
            f"Initializing Binance adapter ({self.market_type.value}, "
            f"max {self.max_streams_per_connection} streams/connection)..."
        )

        self._is_connected = True
        self._is_running = True
        self.logger.info("Binance adapter initialized (WebSocket connections created on-demand)")

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
        self.logger.info(f"Creating Binance WebSocket connection #{conn_idx}...")

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
        self.logger.debug(f"Connection #{conn_idx} message loop ended")

        if need_reconnect and self._is_running:
            self.logger.info(f"Connection #{conn_idx} triggering reconnection...")
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

        # Cleanup old connection
        await self._close_connection(conn)

        # Exponential backoff
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

            # Start message loop
            conn['message_task'] = asyncio.create_task(
                self._message_loop(conn_idx)
            )

            self.logger.info(f"Connection #{conn_idx} reconnected successfully")

            # Resubscribe to symbols
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

        # Build stream names
        streams = [f"{sym.lower()}@depth@{self.update_speed}" for sym in symbols]

        # Subscribe in batches
        batch_size = 20
        for i in range(0, len(streams), batch_size):
            batch = streams[i:i + batch_size]
            await self._send_subscribe(conn_idx, batch)

        # Re-sync orderbooks
        for symbol in symbols:
            self._syncing_symbols.add(symbol)
            self._event_buffers[symbol] = []
            asyncio.create_task(self._sync_orderbook(symbol))

        # Emit reconnect events
        for symbol in symbols:
            await self._emit_reconnect(symbol)

    async def _handle_message(self, conn_idx: int, data: dict) -> None:
        """Handle incoming WebSocket message"""
        # Handle subscription response
        if 'result' in data or 'id' in data:
            return

        # Handle depth update
        if 'e' in data and data['e'] == 'depthUpdate':
            await self._handle_depth_update(data)

    async def _handle_depth_update(self, data: dict) -> None:
        """Handle orderbook depth update"""
        symbol = data.get('s', '').upper()
        if not symbol:
            return

        # Extract update IDs
        first_update_id = data.get('U')  # First update ID
        final_update_id = data.get('u')  # Final update ID
        prev_update_id = data.get('pu')  # Previous final update ID (futures only)

        # If syncing, buffer the event
        if symbol in self._syncing_symbols:
            self._event_buffers.setdefault(symbol, []).append(data)
            return

        # Validate sequence
        last_id = self._last_update_ids.get(symbol)
        if last_id is not None:
            # For futures, check prev_update_id
            if prev_update_id is not None:
                if prev_update_id != last_id:
                    self.logger.warning(
                        f"Sequence gap for {symbol}: expected pu={last_id}, got pu={prev_update_id}"
                    )
                    asyncio.create_task(self._trigger_resync(symbol))
                    return
            else:
                # For spot, check first_update_id
                if first_update_id > last_id + 1:
                    self.logger.warning(
                        f"Sequence gap for {symbol}: expected U<={last_id + 1}, got U={first_update_id}"
                    )
                    asyncio.create_task(self._trigger_resync(symbol))
                    return

        # Parse bids and asks
        bids = [(Decimal(str(p)), Decimal(str(q))) for p, q in data.get('b', [])]
        asks = [(Decimal(str(p)), Decimal(str(q))) for p, q in data.get('a', [])]

        update_data = {
            'bids': bids,
            'asks': asks,
            'sequence': final_update_id,
            'prev_change_id': last_id,
            'timestamp': data.get('E'),
        }

        # Apply update
        success = await self.orderbook_manager.apply_update(symbol, update_data)
        if success:
            self._last_update_ids[symbol] = final_update_id
            snapshot = self.orderbook_manager.get_snapshot(symbol, self._subscription_depth)
            if snapshot:
                await self._emit_update(symbol, snapshot)
        else:
            asyncio.create_task(self._trigger_resync(symbol))

    async def _trigger_resync(self, symbol: str) -> None:
        """Trigger orderbook resync for a symbol"""
        if symbol in self._syncing_symbols:
            return

        self.logger.info(f"Triggering resync for {symbol}")
        self._syncing_symbols.add(symbol)
        self._event_buffers[symbol] = []
        await self._sync_orderbook(symbol)

    async def _sync_orderbook(self, symbol: str) -> None:
        """Synchronize orderbook with REST snapshot"""
        try:
            self.logger.info(f"Syncing orderbook for {symbol}...")

            # Get REST snapshot
            snapshot = await self.request_orderbook_snapshot(symbol, self._subscription_depth)
            snapshot_update_id = snapshot.sequence

            # Process buffered events
            buffered = self._event_buffers.get(symbol, [])
            valid_events = []

            for event in buffered:
                first_id = event.get('U')
                final_id = event.get('u')

                # For spot: drop events where u < lastUpdateId
                # Keep events where U <= lastUpdateId+1 <= u
                if final_id < snapshot_update_id:
                    continue
                if first_id <= snapshot_update_id + 1 <= final_id:
                    valid_events.append(event)
                elif first_id > snapshot_update_id + 1:
                    valid_events.append(event)

            # Apply snapshot
            await self.orderbook_manager.apply_snapshot(symbol, {
                'bids': list(snapshot.bids.items()),
                'asks': list(snapshot.asks.items()),
                'sequence': snapshot_update_id,
                'timestamp': snapshot.timestamp,
            })

            self._last_update_ids[symbol] = snapshot_update_id

            # Apply valid buffered events
            for event in valid_events:
                bids = [(Decimal(str(p)), Decimal(str(q))) for p, q in event.get('b', [])]
                asks = [(Decimal(str(p)), Decimal(str(q))) for p, q in event.get('a', [])]

                update_data = {
                    'bids': bids,
                    'asks': asks,
                    'sequence': event.get('u'),
                    'prev_change_id': self._last_update_ids.get(symbol),
                    'timestamp': event.get('E'),
                }

                await self.orderbook_manager.apply_update(symbol, update_data)
                self._last_update_ids[symbol] = event.get('u')

            # Clear buffer and sync state
            self._event_buffers.pop(symbol, None)
            self._syncing_symbols.discard(symbol)

            # Emit snapshot
            ob_snapshot = self.orderbook_manager.get_snapshot(symbol, self._subscription_depth)
            if ob_snapshot:
                await self._emit_snapshot(symbol, ob_snapshot)

            self.logger.info(f"Orderbook sync complete for {symbol}")

        except Exception as e:
            self.logger.error(f"Failed to sync orderbook for {symbol}: {e}", exc_info=True)
            self._syncing_symbols.discard(symbol)
            # Retry after delay
            await asyncio.sleep(1.0)
            asyncio.create_task(self._sync_orderbook(symbol))

    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """Subscribe to market data updates"""
        if not self._is_connected:
            raise ConnectionError("Not connected to Binance. Call connect() first.")

        if data_type not in self.SUPPORTED_DATA_TYPES:
            raise ValueError(f"Unsupported data type: {data_type}")

        if data_type == DataType.ORDERBOOK:
            await self._subscribe_orderbook(symbols, kwargs.get('depth', 1000))

    async def unsubscribe(self, data_type: DataType, symbols: List[str]) -> None:
        """Unsubscribe from market data updates"""
        if data_type == DataType.ORDERBOOK:
            await self._unsubscribe_orderbook(symbols)

    async def _subscribe_orderbook(self, symbols: List[str], depth: int = 1000) -> None:
        """Subscribe to orderbook updates"""
        async with self._subscription_lock:
            self._subscription_depth = depth

            # Filter already subscribed symbols
            existing = set()
            for conn_symbols in self._connection_symbols:
                existing.update(conn_symbols)

            new_symbols = [s for s in symbols if s.upper() not in existing]
            if not new_symbols:
                self.logger.debug("All symbols already subscribed")
                return

            self.logger.info(f"Subscribing to {len(new_symbols)} orderbook symbols...")

            # Distribute symbols across connections
            distribution = self._distribute_symbols(new_symbols)

            for conn_idx, conn_symbols in distribution.items():
                # Create connection if needed
                if conn_idx >= len(self._connections):
                    await self._create_connection()

                # Track subscriptions
                upper_symbols = [s.upper() for s in conn_symbols]
                self._connection_symbols[conn_idx].update(upper_symbols)
                self._subscribed_symbols.update(upper_symbols)

                # Build stream names
                streams = [f"{sym.lower()}@depth@{self.update_speed}" for sym in conn_symbols]

                # Subscribe
                await self._send_subscribe(conn_idx, streams)

                # Start sync for each symbol
                for sym in upper_symbols:
                    self._syncing_symbols.add(sym)
                    self._event_buffers[sym] = []
                    asyncio.create_task(self._sync_orderbook(sym))

            self.logger.info(
                f"Total: {len(self._subscribed_symbols)} symbols across "
                f"{len(self._connections)} connections"
            )

    async def _unsubscribe_orderbook(self, symbols: List[str]) -> None:
        """Unsubscribe from orderbook updates"""
        async with self._subscription_lock:
            to_unsub = [s.upper() for s in symbols if s.upper() in self._subscribed_symbols]
            if not to_unsub:
                return

            self.logger.info(f"Unsubscribing from {len(to_unsub)} symbols...")

            for symbol in to_unsub:
                # Find connection
                conn_idx = self._find_connection_for_symbol(symbol)
                if conn_idx is None:
                    continue

                stream = f"{symbol.lower()}@depth@{self.update_speed}"

                try:
                    await self._send_unsubscribe(conn_idx, [stream])
                    self._connection_symbols[conn_idx].discard(symbol)
                    self._subscribed_symbols.discard(symbol)
                    self.orderbook_manager.remove_orderbook(symbol)
                    self._last_update_ids.pop(symbol, None)
                    self._event_buffers.pop(symbol, None)
                    self._syncing_symbols.discard(symbol)
                except Exception as e:
                    self.logger.error(f"Failed to unsubscribe {symbol}: {e}")

    def _distribute_symbols(self, symbols: List[str]) -> Dict[int, List[str]]:
        """Distribute symbols across connections"""
        distribution: Dict[int, List[str]] = {}
        remaining = symbols.copy()

        # Fill existing connections
        for i, conn_symbols in enumerate(self._connection_symbols):
            available = self.max_streams_per_connection - len(conn_symbols)
            if available > 0 and remaining:
                to_add = remaining[:available]
                distribution[i] = to_add
                remaining = remaining[available:]

        # Plan new connections
        next_idx = len(self._connections)
        while remaining:
            to_add = remaining[:self.max_streams_per_connection]
            distribution[next_idx] = to_add
            remaining = remaining[self.max_streams_per_connection:]
            next_idx += 1

        return distribution

    def _find_connection_for_symbol(self, symbol: str) -> Optional[int]:
        """Find which connection has a symbol"""
        for i, symbols in enumerate(self._connection_symbols):
            if symbol in symbols:
                return i
        return None

    async def _send_subscribe(self, conn_idx: int, streams: List[str]) -> None:
        """Send subscribe request"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            raise ConnectionError(f"Connection #{conn_idx} not connected")

        self._request_id += 1
        request = {
            "method": "SUBSCRIBE",
            "params": streams,
            "id": self._request_id
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Subscribed to {len(streams)} streams on connection #{conn_idx}")

    async def _send_unsubscribe(self, conn_idx: int, streams: List[str]) -> None:
        """Send unsubscribe request"""
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            return

        self._request_id += 1
        request = {
            "method": "UNSUBSCRIBE",
            "params": streams,
            "id": self._request_id
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Unsubscribed from {len(streams)} streams on connection #{conn_idx}")

    async def get_instruments(
        self,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """Get available instruments from Binance"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}{self.exchange_info_path}"

            async with session.get(url) as resp:
                data = await resp.json()

                instruments = []
                symbols_data = data.get('symbols', [])

                for item in symbols_data:
                    # Filter by status
                    status = item.get('status', item.get('contractStatus', ''))
                    if not expired and status not in ['TRADING', 'PENDING_TRADING']:
                        continue

                    # Filter by currency
                    base = item.get('baseAsset', '')
                    quote = item.get('quoteAsset', '')
                    if currency and base.upper() != currency.upper():
                        continue

                    # Determine instrument type
                    if self.market_type == BinanceMarketType.SPOT:
                        inst_type = 'spot'
                    else:
                        contract_type = item.get('contractType', '')
                        if 'PERPETUAL' in contract_type:
                            inst_type = 'perpetual'
                        else:
                            inst_type = 'future'

                    # Filter by kind
                    if kind and inst_type != kind:
                        continue

                    symbol = item.get('symbol', '')

                    # Parse tick size and min qty
                    tick_size = None
                    min_qty = None
                    for f in item.get('filters', []):
                        if f.get('filterType') == 'PRICE_FILTER':
                            tick_size = Decimal(str(f.get('tickSize', 0)))
                        elif f.get('filterType') == 'LOT_SIZE':
                            min_qty = Decimal(str(f.get('minQty', 0)))

                    inst = Instrument(
                        symbol=symbol,
                        exchange=self.exchange_name,
                        base_currency=base,
                        quote_currency=quote,
                        instrument_type=inst_type,
                        is_active=status == 'TRADING',
                        tick_size=tick_size,
                        min_trade_amount=min_qty,
                        contract_size=Decimal(str(item.get('contractSize', 1)))
                    )

                    instruments.append(inst)

                return instruments

        finally:
            await session.close()

    async def request_orderbook_snapshot(self, symbol: str, depth: int = 1000) -> Orderbook:
        """Request a full orderbook snapshot via REST API"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}{self.depth_path}"
            # Binance depth limits: 5, 10, 20, 50, 100, 500, 1000, 5000
            valid_limits = [5, 10, 20, 50, 100, 500, 1000, 5000]
            limit = min([l for l in valid_limits if l >= depth], default=5000)

            params = {
                "symbol": symbol.upper(),
                "limit": limit
            }

            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"API Error: {resp.status} - {text}")

                data = await resp.json()

                ob = Orderbook(
                    symbol=symbol.upper(),
                    exchange=self.exchange_name
                )

                # Parse bids and asks
                for bid in data.get('bids', []):
                    price = Decimal(str(bid[0]))
                    qty = Decimal(str(bid[1]))
                    ob.bids[price] = qty

                for ask in data.get('asks', []):
                    price = Decimal(str(ask[0]))
                    qty = Decimal(str(ask[1]))
                    ob.asks[price] = qty

                ob.sequence = data.get('lastUpdateId')
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
                'capacity': self.max_streams_per_connection,
            })

        return {
            'market_type': self.market_type.value,
            'total_connections': len(self._connections),
            'active_connections': active,
            'total_subscriptions': len(self._subscribed_symbols),
            'max_streams_per_connection': self.max_streams_per_connection,
            'connections': details
        }
