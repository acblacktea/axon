"""
Coinbase Exchange Adapter Implementation (Advanced Trade API)

Requires JWT authentication. Uses level2 channel for orderbook data.
"""
import asyncio
import ssl
import json
import time
import hmac
import hashlib
import base64
import secrets
from typing import List, Optional, Dict, Any, Set
from decimal import Decimal
import logging

import aiohttp
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.backends import default_backend

from ...core.adapter import ExchangeAdapter, OrderbookManager
from ...core.models import Instrument, Orderbook, OrderbookSnapshot, DataType


class CoinbaseAdapter(ExchangeAdapter):
    """
    Coinbase exchange adapter for market data (Advanced Trade API).

    Requires API credentials for JWT authentication.

    Features:
    - Level2 orderbook channel (full depth)
    - JWT authentication (auto-renewal)
    - Sequence validation
    - Automatic reconnection
    """

    SUPPORTED_DATA_TYPES = {DataType.ORDERBOOK}

    # WebSocket endpoint
    WS_URL = "wss://advanced-trade-ws.coinbase.com"

    # REST API
    REST_URL = "https://api.coinbase.com"

    def __init__(
        self,
        api_key: str,
        api_secret: str,
        logger: Optional[logging.Logger] = None,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 1000,
        max_subscriptions_per_connection: int = 50,
        orderbook_depth: int = 1000,  # Depth for snapshot output
    ):
        super().__init__("coinbase", logger)

        self.api_key = api_key
        self.api_secret = api_secret

        self.ws_url = self.WS_URL
        self.rest_url = self.REST_URL

        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self.max_subscriptions_per_connection = max_subscriptions_per_connection
        self.orderbook_depth = orderbook_depth

        # Orderbook manager
        self._ob_manager: Optional[OrderbookManager] = None

        # Subscription tracking
        self._subscription_lock = asyncio.Lock()

        # WebSocket connections
        self._connections: List[Dict[str, Any]] = []
        self._connection_symbols: List[Set[str]] = []

        # Sequence tracking per product
        self._sequences: Dict[str, int] = {}

    # ==================== Symbol Conversion ====================

    def convert_to_exchange_symbol(self, unified_symbol: str) -> str:
        """Convert BTC_USD_SPOT -> BTC-USD"""
        symbol = unified_symbol
        if symbol.endswith('_SPOT'):
            symbol = symbol[:-5]
        return symbol.replace('_', '-')

    def convert_to_unified_symbol(self, exchange_symbol: str) -> str:
        """Convert BTC-USD -> BTC_USD_SPOT"""
        return exchange_symbol.replace('-', '_') + '_SPOT'

    @property
    def orderbook_manager(self) -> OrderbookManager:
        if self._ob_manager is None:
            self._ob_manager = OrderbookManager(self, self.logger)
        return self._ob_manager

    def _generate_jwt(self) -> str:
        """Generate JWT token for authentication using ES256 (ECDSA P-256)"""
        # JWT header
        header = {
            "alg": "ES256",
            "typ": "JWT",
            "kid": self.api_key,
            "nonce": secrets.token_hex(16)
        }

        # JWT payload - use "cdp" issuer for Cloud API keys
        now = int(time.time())
        payload = {
            "iss": "cdp",
            "nbf": now,
            "exp": now + 120,  # 2 minute expiry
            "sub": self.api_key,
        }

        # Encode header and payload
        def b64_encode(data: dict) -> str:
            return base64.urlsafe_b64encode(
                json.dumps(data, separators=(',', ':')).encode()
            ).rstrip(b'=').decode()

        header_b64 = b64_encode(header)
        payload_b64 = b64_encode(payload)
        message = f"{header_b64}.{payload_b64}"

        # Load EC private key and sign with ES256
        private_key = serialization.load_pem_private_key(
            self.api_secret.encode(),
            password=None,
            backend=default_backend()
        )

        # Sign using ECDSA with SHA-256
        signature_der = private_key.sign(
            message.encode(),
            ec.ECDSA(hashes.SHA256())
        )

        # Convert DER signature to raw r||s format (64 bytes for P-256)
        # DER format: 0x30 [total-len] 0x02 [r-len] [r] 0x02 [s-len] [s]
        from cryptography.hazmat.primitives.asymmetric.utils import decode_dss_signature
        r, s = decode_dss_signature(signature_der)
        # Each of r and s should be 32 bytes for P-256
        signature_raw = r.to_bytes(32, byteorder='big') + s.to_bytes(32, byteorder='big')

        signature_b64 = base64.urlsafe_b64encode(signature_raw).rstrip(b'=').decode()

        return f"{message}.{signature_b64}"

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

        if not self.api_key or not self.api_secret:
            raise ValueError("Coinbase requires API key and secret for authentication")

        self.logger.info("Initializing Coinbase adapter (JWT authenticated)...")

        self._is_connected = True
        self._is_running = True
        self.logger.info("Coinbase adapter initialized")

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
        self.logger.info(f"Creating Coinbase WebSocket connection #{conn_idx}...")

        try:
            ssl_context = ssl.create_default_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            session = aiohttp.ClientSession(connector=connector)

            ws = await session.ws_connect(
                self.ws_url,
                heartbeat=None,
                receive_timeout=60.0,
                max_msg_size=20 * 1024 * 1024  # 20MB max message size for large orderbooks
            )

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

        self.logger.info(f"Connection #{conn_idx} message loop started")

        while self._is_running and conn.get('ws'):
            try:
                msg = await conn['ws'].receive()

                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)
                    await self._handle_message(conn_idx, data)

                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    self.logger.warning(f"Connection #{conn_idx} closed by server, close_code={conn['ws'].close_code}")
                    need_reconnect = True
                    break

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error(f"Connection #{conn_idx} WebSocket error: exception={conn['ws'].exception()}, msg.data={msg.data}, msg.extra={getattr(msg, 'extra', None)}")
                    need_reconnect = True
                    break

                else:
                    self.logger.warning(f"Connection #{conn_idx} unexpected msg type: {msg.type}")

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

            ws = await session.ws_connect(
                self.ws_url,
                heartbeat=None,
                receive_timeout=60.0,
                max_msg_size=20 * 1024 * 1024  # 20MB max message size for large orderbooks
            )

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
        channel = data.get('channel', '')
        msg_type = data.get('type', '')

        if channel == 'l2_data':
            await self._handle_orderbook_update(data)
        elif channel == 'subscriptions':
            # Subscription confirmation
            self.logger.info(f"Subscription confirmed: {data.get('events', [])}")
        elif msg_type == 'error':
            self.logger.error(f"Coinbase error: {data}")
        else:
            # Log unknown messages for debugging
            self.logger.debug(f"Unknown message: {data}")

    async def _handle_orderbook_update(self, data: dict) -> None:
        """Handle orderbook update

        Note: Uses exchange symbol (BTC-USD) for internal storage.
        The base class _emit_* methods will convert to unified symbol via get_unified_symbol().
        """
        events = data.get('events', [])
        sequence_num = data.get('sequence_num')
        timestamp = data.get('timestamp')

        for event in events:
            event_type = event.get('type', '')  # "snapshot" or "update"
            exchange_symbol = event.get('product_id', '')
            updates = event.get('updates', [])

            if not exchange_symbol:
                continue

            # Validate sequence (use exchange symbol as key)
            if sequence_num is not None:
                last_seq = self._sequences.get(exchange_symbol)
                if last_seq is not None and sequence_num != last_seq + 1:
                    # Check for gap (but level2 guarantees delivery)
                    if sequence_num > last_seq + 1:
                        self.logger.warning(
                            f"Sequence gap for {exchange_symbol}: expected {last_seq + 1}, got {sequence_num}"
                        )

            # Parse updates
            bids = []
            asks = []

            for update in updates:
                side = update.get('side', '')
                price = Decimal(str(update.get('price_level', 0)))
                qty = Decimal(str(update.get('new_quantity', 0)))

                if side == 'bid':
                    bids.append((price, qty))
                elif side == 'offer':
                    asks.append((price, qty))

            update_data = {
                'bids': bids,
                'asks': asks,
                'sequence': sequence_num,
                'timestamp': timestamp,
            }

            if event_type == 'snapshot':
                # Use exchange symbol for storage
                await self.orderbook_manager.apply_snapshot(exchange_symbol, update_data)
                self._sequences[exchange_symbol] = sequence_num

                snapshot = self.orderbook_manager.get_snapshot(exchange_symbol, self.orderbook_depth)
                if snapshot:
                    # _emit_snapshot will convert exchange_symbol to unified via get_unified_symbol()
                    await self._emit_snapshot(exchange_symbol, snapshot)

            elif event_type == 'update':
                success = await self.orderbook_manager.apply_update(exchange_symbol, update_data)
                if success:
                    self._sequences[exchange_symbol] = sequence_num
                    snapshot = self.orderbook_manager.get_snapshot(exchange_symbol, self.orderbook_depth)
                    if snapshot:
                        await self._emit_update(exchange_symbol, snapshot)

    async def subscribe(
        self,
        data_type: DataType,
        symbols: List[str],
        **kwargs
    ) -> None:
        """Subscribe to market data updates"""
        if not self._is_connected:
            raise ConnectionError("Not connected to Coinbase. Call connect() first.")

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

                # Subscribe one at a time (Coinbase limitation: one channel per message)
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
                    self._sequences.pop(symbol, None)
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

    async def _send_subscribe(self, conn_idx: int, exchange_symbol: str) -> None:
        """Send subscribe request for a product

        Note: symbol is already in exchange format (BTC-USD) because
        MarketDataService calls convert_symbols() before calling subscribe()
        """
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            raise ConnectionError(f"Connection #{conn_idx} not connected")

        # Generate fresh JWT for each subscription
        jwt = self._generate_jwt()

        request = {
            "type": "subscribe",
            "product_ids": [exchange_symbol],
            "channel": "level2",
            "jwt": jwt,
        }

        await conn['ws'].send_str(json.dumps(request))
        self.logger.info(f"Sent subscribe request for {exchange_symbol} on connection #{conn_idx}")

    async def _send_unsubscribe(self, conn_idx: int, exchange_symbol: str) -> None:
        """Send unsubscribe request for a product

        Note: symbol is already in exchange format (BTC-USD)
        """
        conn = self._connections[conn_idx]
        if not conn.get('is_connected') or not conn.get('ws'):
            return

        jwt = self._generate_jwt()

        request = {
            "type": "unsubscribe",
            "product_ids": [exchange_symbol],
            "channel": "level2",
            "jwt": jwt,
        }

        await conn['ws'].send_json(request)
        self.logger.debug(f"Unsubscribed from {exchange_symbol} on connection #{conn_idx}")

    async def get_instruments(
        self,
        currency: Optional[str] = None,
        kind: Optional[str] = None,
        expired: bool = False
    ) -> List[Instrument]:
        """Get available instruments from Coinbase"""
        session = await self._create_session()

        try:
            url = f"{self.rest_url}/api/v3/brokerage/products"

            # Add auth headers
            timestamp = str(int(time.time()))
            message = f"{timestamp}GET/api/v3/brokerage/products"
            signature = hmac.new(
                self.api_secret.encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()

            headers = {
                "CB-ACCESS-KEY": self.api_key,
                "CB-ACCESS-SIGN": signature,
                "CB-ACCESS-TIMESTAMP": timestamp,
            }

            async with session.get(url, headers=headers) as resp:
                data = await resp.json()

                instruments = []
                products = data.get('products', [])

                for item in products:
                    product_id = item.get('product_id', '')
                    base = item.get('base_currency_id', '')
                    quote = item.get('quote_currency_id', '')

                    # Filter by currency
                    if currency and base.upper() != currency.upper():
                        continue

                    # Filter by type
                    product_type = item.get('product_type', '')
                    if kind == 'spot' and product_type != 'SPOT':
                        continue

                    status = item.get('status', '')
                    is_active = status == 'online'

                    if not expired and not is_active:
                        continue

                    # Parse tick size
                    quote_increment = item.get('quote_increment')
                    tick_size = Decimal(str(quote_increment)) if quote_increment else None

                    base_increment = item.get('base_increment')
                    min_qty = Decimal(str(base_increment)) if base_increment else None

                    inst = Instrument(
                        symbol=product_id,
                        exchange=self.exchange_name,
                        base_currency=base,
                        quote_currency=quote,
                        instrument_type='spot' if product_type == 'SPOT' else product_type.lower(),
                        is_active=is_active,
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
            url = f"{self.rest_url}/api/v3/brokerage/products/{symbol}/book"
            params = {"limit": depth}

            # Add auth headers
            timestamp = str(int(time.time()))
            path = f"/api/v3/brokerage/products/{symbol}/book"
            message = f"{timestamp}GET{path}"
            signature = hmac.new(
                self.api_secret.encode(),
                message.encode(),
                hashlib.sha256
            ).hexdigest()

            headers = {
                "CB-ACCESS-KEY": self.api_key,
                "CB-ACCESS-SIGN": signature,
                "CB-ACCESS-TIMESTAMP": timestamp,
            }

            async with session.get(url, headers=headers, params=params) as resp:
                data = await resp.json()

                ob = Orderbook(
                    symbol=symbol,
                    exchange=self.exchange_name
                )

                pricebook = data.get('pricebook', {})

                for bid in pricebook.get('bids', []):
                    price = Decimal(str(bid.get('price', 0)))
                    qty = Decimal(str(bid.get('size', 0)))
                    ob.bids[price] = qty

                for ask in pricebook.get('asks', []):
                    price = Decimal(str(ask.get('price', 0)))
                    qty = Decimal(str(ask.get('size', 0)))
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
            'authenticated': bool(self.api_key),
            'orderbook_depth': self.orderbook_depth,
            'total_connections': len(self._connections),
            'active_connections': active,
            'total_subscriptions': len(self._subscribed_symbols),
            'max_subscriptions_per_connection': self.max_subscriptions_per_connection,
            'connections': details
        }
