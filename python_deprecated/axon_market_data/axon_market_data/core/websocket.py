"""
Generic WebSocket Connection Manager

Provides reusable WebSocket connection management with:
- Multi-connection support
- Automatic reconnection with exponential backoff
- Heartbeat monitoring
- JSON-RPC request/response handling
- Item distribution across connections
"""
import asyncio
import json
import ssl
import logging
import uuid
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Set, Callable, Awaitable

import aiohttp

from ..utils import metrics

# Type aliases for callbacks
MessageHandler = Callable[[int, dict], Awaitable[None]]
ReconnectHandler = Callable[[int, List[str]], Awaitable[None]]


@dataclass
class WebSocketConnection:
    """State for a single WebSocket connection"""
    ws: Optional[aiohttp.ClientWebSocketResponse] = None
    session: Optional[aiohttp.ClientSession] = None
    request_id: int = 0
    pending_requests: Dict[str, asyncio.Future] = field(default_factory=dict)
    message_task: Optional[asyncio.Task] = None
    heartbeat_task: Optional[asyncio.Task] = None
    is_connected: bool = False
    reconnect_attempts: int = 0
    reconnect_task: Optional[asyncio.Task] = None  # Track active reconnect task


class WebSocketManager:
    """
    Generic WebSocket connection manager with multi-connection support.

    Features:
    - Automatic multi-connection management
    - Automatic reconnection with exponential backoff
    - Heartbeat monitoring
    - JSON-RPC request/response tracking
    - Item distribution across connections

    Usage:
        manager = WebSocketManager(
            ws_url="wss://example.com/ws",
            logger=logger,
            on_message=my_message_handler,
            on_reconnect=my_reconnect_handler,
        )
        await manager.start()
        conn_idx = await manager.create_connection()
        await manager.send_request(conn_idx, "subscribe", {"channel": "data"})
    """

    def __init__(
        self,
        ws_url: str,
        logger: logging.Logger,
        heartbeat_interval: float = 15.0,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 10,
        max_items_per_connection: int = 400,
        on_message: Optional[MessageHandler] = None,
        on_reconnect: Optional[ReconnectHandler] = None,
        heartbeat_method: str = "public/test",
        heartbeat_params: Optional[dict] = None,
        exchange: str = "unknown",
    ):
        """
        Initialize WebSocket manager.

        Args:
            ws_url: WebSocket endpoint URL
            logger: Logger instance
            heartbeat_interval: Seconds between heartbeat requests
            reconnect_delay: Initial delay before reconnection (exponential backoff)
            max_reconnect_attempts: Maximum reconnection attempts before giving up
            max_items_per_connection: Maximum items (e.g., subscriptions) per connection
            on_message: Callback for handling messages (conn_idx, msg) -> None
            on_reconnect: Callback for reconnection (conn_idx, items) -> None
            heartbeat_method: JSON-RPC method for heartbeat
            heartbeat_params: Parameters for heartbeat request
        """
        self.ws_url = ws_url
        self.logger = logger
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        self.max_items_per_connection = max_items_per_connection

        # Callbacks
        self._on_message = on_message
        self._on_reconnect = on_reconnect
        self.heartbeat_method = heartbeat_method
        self.heartbeat_params = heartbeat_params or {}
        self.exchange = exchange

        # Connection state
        self._connections: List[WebSocketConnection] = []
        self._connection_items: List[Set[str]] = []
        self._is_running: bool = False

    @property
    def is_running(self) -> bool:
        """Check if manager is running"""
        return self._is_running

    @property
    def connection_count(self) -> int:
        """Get total number of connections"""
        return len(self._connections)

    def start(self) -> None:
        """Start the manager (connections created on-demand)"""
        self._is_running = True
        self.logger.info("WebSocket manager started")

    async def stop(self) -> None:
        """Stop the manager and close all connections"""
        self._is_running = False
        await self.close_all()
        self.logger.info("WebSocket manager stopped")

    async def create_connection(self) -> int:
        """
        Create a new WebSocket connection.

        Returns:
            Connection index
        """
        conn_idx = len(self._connections)
        self.logger.info(f"Creating WebSocket connection #{conn_idx}...")

        conn = WebSocketConnection()

        try:
            # Create session with SSL
            ssl_context = ssl.create_default_context()
            connector = aiohttp.TCPConnector(ssl=ssl_context)
            conn.session = aiohttp.ClientSession(connector=connector)

            # Connect WebSocket
            conn.ws = await conn.session.ws_connect(
                self.ws_url,
                heartbeat=self.heartbeat_interval
            )

            conn.is_connected = True

            # Add to pool first (so loops can access it)
            self._connections.append(conn)
            self._connection_items.append(set())

            # Start message and heartbeat loops
            conn.message_task = asyncio.create_task(
                self._message_loop(conn_idx)
            )
            
            conn.heartbeat_task = asyncio.create_task(
                self._heartbeat_loop(conn_idx)
            )

            metrics.connections_active.labels(exchange=self.exchange).inc()
            self.logger.info(f"Connection #{conn_idx} created successfully")
            return conn_idx

        except Exception as e:
            metrics.errors_total.labels(exchange=self.exchange, category="ws").inc()
            self.logger.error(f"Failed to create connection #{conn_idx}: {e}")
            if conn.ws:
                await conn.ws.close()
            if conn.session:
                await conn.session.close()
            raise

    async def close_connection(self, conn_idx: int) -> None:
        """
        Close a specific connection.

        Args:
            conn_idx: Connection index to close
        """
        if conn_idx >= len(self._connections):
            self.logger.warning(f"Invalid connection index: {conn_idx}")
            return

        conn = self._connections[conn_idx]
        self.logger.debug(f"Closing connection #{conn_idx}...")

        try:
            # Cancel tasks
            if conn.message_task:
                conn.message_task.cancel()
                try:
                    await conn.message_task
                except asyncio.CancelledError:
                    pass

            if conn.heartbeat_task:
                conn.heartbeat_task.cancel()
                try:
                    await conn.heartbeat_task
                except asyncio.CancelledError:
                    pass

            # Close WebSocket
            if conn.ws and not conn.ws.closed:
                await conn.ws.close()

            # Close session
            if conn.session and not conn.session.closed:
                await conn.session.close()
                await asyncio.sleep(0.1)

            if conn.is_connected:
                metrics.connections_active.labels(exchange=self.exchange).dec()
            conn.is_connected = False
            self.logger.debug(f"Connection #{conn_idx} closed")

        except Exception as e:
            self.logger.error(f"Error closing connection #{conn_idx}: {e}")

    async def close_all(self) -> None:
        """Close all connections"""
        self.logger.info(f"Closing {len(self._connections)} connection(s)...")

        for i in range(len(self._connections)):
            await self.close_connection(i)

        self._connections.clear()
        self._connection_items.clear()
        self.logger.info("All connections closed")

    def schedule_reconnect(self, conn_idx: int) -> None:
        """
        Schedule a reconnect task for a connection.
        If a reconnect is already in progress or connection is healthy, this is a no-op.
        """
        if conn_idx >= len(self._connections):
            return

        conn = self._connections[conn_idx]

        # Skip if connection is already connected (avoid spurious reconnects from old loops)
        if conn.is_connected:
            self.logger.debug(f"Connection #{conn_idx} is already connected, skipping reconnect")
            return

        # Skip if reconnect already in progress
        if conn.reconnect_task and not conn.reconnect_task.done():
            self.logger.debug(f"Connection #{conn_idx} reconnect already scheduled, skipping")
            return

        # Create new reconnect task
        conn.reconnect_task = asyncio.create_task(self._do_reconnect(conn_idx))

    async def _do_reconnect(self, conn_idx: int) -> bool:
        """
        Internal: Perform the actual reconnection with retry loop.
        This method keeps retrying until success or max attempts reached.
        """
        if conn_idx >= len(self._connections):
            self.logger.error(f"Invalid connection index: {conn_idx}")
            return False

        conn = self._connections[conn_idx]
        items_to_restore = list(self._connection_items[conn_idx])

        self.logger.info(
            f"Reconnecting connection #{conn_idx} "
            f"({len(items_to_restore)} items to restore)..."
        )

        while self._is_running:
            # Increment reconnect attempts
            conn.reconnect_attempts += 1

            if conn.reconnect_attempts > self.max_reconnect_attempts:
                self.logger.error(
                    f"Connection #{conn_idx} exceeded max reconnect attempts "
                    f"({self.max_reconnect_attempts}), giving up"
                )
                return False

            # Cleanup old connection
            try:
                if conn.ws and not conn.ws.closed:
                    await conn.ws.close()
                if conn.session and not conn.session.closed:
                    await conn.session.close()
            except Exception as e:
                self.logger.warning(f"Error cleaning up connection #{conn_idx}: {e}")

            # Exponential backoff delay
            delay = self.reconnect_delay * (2 ** (conn.reconnect_attempts - 1))
            delay = min(delay, 60.0)
            self.logger.info(
                f"Connection #{conn_idx} waiting {delay:.1f}s before reconnect "
                f"(attempt {conn.reconnect_attempts}/{self.max_reconnect_attempts})..."
            )
            await asyncio.sleep(delay)

            if not self._is_running:
                self.logger.info(f"Connection #{conn_idx} reconnect cancelled (manager stopped)")
                return False

            try:
                # Create new session and WebSocket
                ssl_context = ssl.create_default_context()
                connector = aiohttp.TCPConnector(ssl=ssl_context)
                conn.session = aiohttp.ClientSession(connector=connector)

                conn.ws = await conn.session.ws_connect(
                    self.ws_url,
                    heartbeat=self.heartbeat_interval
                )

                conn.is_connected = True
                conn.request_id = 0
                conn.pending_requests = {}

                # Start message and heartbeat loops
                conn.message_task = asyncio.create_task(
                    self._message_loop(conn_idx)
                )
                conn.heartbeat_task = asyncio.create_task(
                    self._heartbeat_loop(conn_idx)
                )

                metrics.reconnect_total.labels(
                    exchange=self.exchange, conn_idx=str(conn_idx), result="success"
                ).inc()
                metrics.connections_active.labels(exchange=self.exchange).inc()
                self.logger.info(f"Connection #{conn_idx} reconnected successfully")

                # Call reconnect callback to restore subscriptions
                if self._on_reconnect and items_to_restore:
                    self.logger.info(
                        f"Restoring {len(items_to_restore)} items on connection #{conn_idx}..."
                    )
                    await self._on_reconnect(conn_idx, items_to_restore)

                # Reset reconnect attempts on success
                conn.reconnect_attempts = 0
                return True

            except Exception as e:
                metrics.reconnect_total.labels(
                    exchange=self.exchange, conn_idx=str(conn_idx), result="failure"
                ).inc()
                self.logger.error(f"Failed to reconnect connection #{conn_idx}: {e}")
                conn.is_connected = False
                # Continue the loop to retry

        return False

    async def _message_loop(self, conn_idx: int) -> None:
        """Message receiving loop for a connection"""
        conn = self._connections[conn_idx]
        need_reconnect = False

        while self._is_running and conn.ws:
            try:
                msg = await conn.ws.receive()

                if msg.type == aiohttp.WSMsgType.TEXT:
                    data = json.loads(msg.data)

                    # Handle RPC response internally
                    # Only process responses with our unique prefix to avoid conflicts
                    # with exchange data that may contain 'id' fields
                    if 'id' in data and isinstance(data['id'], str) and data['id'].startswith('axon_'):
                        request_id = data['id']
                        if request_id in conn.pending_requests:
                            future = conn.pending_requests.pop(request_id)
                            if not future.done():
                                if 'error' in data:
                                    future.set_exception(Exception(f"RPC Error: {data['error']}"))
                                else:
                                    future.set_result(data.get('result'))
                            continue

                    # Forward to message handler
                    if self._on_message:
                        await self._on_message(conn_idx, data)

                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    self.logger.warning(
                        f"Connection #{conn_idx} closed by server, "
                        f"code={conn.ws.close_code}, reason={getattr(conn.ws, 'close_reason', None)}"
                    )
                    need_reconnect = True
                    break

                elif msg.type == aiohttp.WSMsgType.ERROR:
                    self.logger.error(f"Connection #{conn_idx} error: {conn.ws.exception()}")
                    need_reconnect = True
                    break

            except asyncio.CancelledError:
                break
            except json.JSONDecodeError as e:
                metrics.errors_total.labels(exchange=self.exchange, category="parse").inc()
                self.logger.error(f"Connection #{conn_idx} failed to parse message: {e}")
            except Exception as e:
                metrics.errors_total.labels(exchange=self.exchange, category="ws").inc()
                self.logger.error(f"Connection #{conn_idx} message loop error: {e}", exc_info=True)
                need_reconnect = True
                break

        if conn.is_connected:
            metrics.connections_active.labels(exchange=self.exchange).dec()
        conn.is_connected = False
        self.logger.debug(f"Connection #{conn_idx} message loop ended")

        # Trigger reconnection if needed
        if need_reconnect and self._is_running:
            self.logger.info(f"Connection #{conn_idx} triggering reconnection...")
            self.schedule_reconnect(conn_idx)

    async def _heartbeat_loop(self, conn_idx: int) -> None:
        """Heartbeat monitoring loop for a connection"""
        conn = self._connections[conn_idx]
        consecutive_failures = 0
        max_consecutive_failures = 5

        while self._is_running and conn.is_connected:
            try:
                await asyncio.sleep(self.heartbeat_interval)

                if conn.is_connected and conn.ws:
                    await self.send_request(
                        conn_idx,
                        self.heartbeat_method,
                        self.heartbeat_params,
                        timeout=5.0,
                        max_retries=1,
                    )
                    consecutive_failures = 0

            except asyncio.CancelledError:
                break
            except Exception as e:
                consecutive_failures += 1
                metrics.heartbeat_failures_total.labels(
                    exchange=self.exchange, conn_idx=str(conn_idx)
                ).inc()
                self.logger.warning(
                    f"Connection #{conn_idx} heartbeat failed "
                    f"({consecutive_failures}/{max_consecutive_failures}): {e}"
                )

                if consecutive_failures >= max_consecutive_failures:
                    self.logger.error(
                        f"Connection #{conn_idx} heartbeat failed {max_consecutive_failures} times, "
                        f"triggering reconnect"
                    )
                    conn.is_connected = False

                    # Cancel message loop
                    if conn.message_task and not conn.message_task.done():
                        conn.message_task.cancel()

                    # Trigger reconnection
                    if self._is_running:
                        self.schedule_reconnect(conn_idx)
                    break

    async def send_request(
        self,
        conn_idx: int,
        method: str,
        params: Optional[dict] = None,
        timeout: float = 10.0,
        max_retries: int = 20,
        retry_delay: float = 1.0
    ) -> Any:
        """
        Send a JSON-RPC request and wait for response with retry logic.

        Args:
            conn_idx: Connection index
            method: RPC method name
            params: Request parameters
            timeout: Response timeout in seconds
            max_retries: Maximum number of retry attempts (default: 5)
            retry_delay: Delay between retries in seconds (default: 1.0)

        Returns:
            Response result
        """
        last_error: Optional[Exception] = None

        for attempt in range(max_retries):
            conn = self._connections[conn_idx]

            if not conn.is_connected or not conn.ws:
                if attempt < max_retries - 1:
                    self.logger.warning(
                        f"Connection #{conn_idx} not connected, retry {attempt + 1}/{max_retries}"
                    )
                    await asyncio.sleep(retry_delay)
                    continue
                raise ConnectionError(f"Connection #{conn_idx} not connected")

            conn.request_id += 1
            request_id = f"axon_{uuid.uuid4().hex[:8]}_{conn.request_id}"

            request = {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params or {}
            }

            future = asyncio.get_event_loop().create_future()
            conn.pending_requests[request_id] = future

            try:
                await conn.ws.send_json(request)
                result = await asyncio.wait_for(future, timeout=timeout)
                return result
            except asyncio.TimeoutError as e:
                last_error = e
                conn.pending_requests.pop(request_id, None)
                if attempt < max_retries - 1:
                    self.logger.warning(
                        f"Connection #{conn_idx} request timeout: {method}, "
                        f"retry {attempt + 1}/{max_retries}"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error(
                        f"Connection #{conn_idx} request timeout: {method}, "
                        f"all {max_retries} retries exhausted"
                    )
            except Exception as e:
                last_error = e
                conn.pending_requests.pop(request_id, None)
                if attempt < max_retries - 1:
                    self.logger.warning(
                        f"Connection #{conn_idx} request failed: {method}, "
                        f"error: {e}, retry {attempt + 1}/{max_retries}"
                    )
                    await asyncio.sleep(retry_delay)
                else:
                    self.logger.error(
                        f"Connection #{conn_idx} request failed: {method}, "
                        f"error: {e}, all {max_retries} retries exhausted"
                    )

        raise last_error

    def distribute_items(self, items: List[str]) -> Dict[int, List[str]]:
        """
        Distribute items across connections.

        Args:
            items: List of items to distribute

        Returns:
            Dict mapping connection index to items
        """
        distribution: Dict[int, List[str]] = {}
        remaining = items.copy()

        # Fill existing connections first
        for i, conn_items in enumerate(self._connection_items):
            available = self.max_items_per_connection - len(conn_items)
            if available > 0 and remaining:
                to_add = remaining[:available]
                distribution[i] = to_add
                remaining = remaining[available:]

        # Plan new connections for remaining items
        next_conn_idx = len(self._connections)
        while remaining:
            to_add = remaining[:self.max_items_per_connection]
            distribution[next_conn_idx] = to_add
            remaining = remaining[self.max_items_per_connection:]
            next_conn_idx += 1

        return distribution

    def add_items(self, conn_idx: int, items: Set[str]) -> None:
        """Add items to a connection's tracking set"""
        if conn_idx < len(self._connection_items):
            self._connection_items[conn_idx].update(items)

    def remove_items(self, conn_idx: int, items: Set[str]) -> None:
        """Remove items from a connection's tracking set"""
        if conn_idx < len(self._connection_items):
            self._connection_items[conn_idx] -= items

    def get_items(self, conn_idx: int) -> Set[str]:
        """Get items tracked for a connection"""
        if conn_idx < len(self._connection_items):
            return self._connection_items[conn_idx].copy()
        return set()

    def get_all_items(self) -> Set[str]:
        """Get all items across all connections"""
        all_items: Set[str] = set()
        for items in self._connection_items:
            all_items.update(items)
        return all_items

    def get_connection(self, conn_idx: int) -> Optional[WebSocketConnection]:
        """Get connection by index"""
        if conn_idx < len(self._connections):
            return self._connections[conn_idx]
        return None

    def find_connection_for_item(self, item: str) -> Optional[int]:
        """Find which connection has a specific item"""
        for i, items in enumerate(self._connection_items):
            if item in items:
                return i
        return None

    def get_stats(self) -> Dict[str, Any]:
        """Get connection statistics"""
        active = sum(1 for conn in self._connections if conn.is_connected)

        details = []
        for i, (conn, items) in enumerate(zip(self._connections, self._connection_items)):
            details.append({
                'id': i,
                'connected': conn.is_connected,
                'items': len(items),
                'capacity': self.max_items_per_connection,
                'utilization': len(items) / self.max_items_per_connection * 100 if self.max_items_per_connection > 0 else 0
            })

        return {
            'total_connections': len(self._connections),
            'active_connections': active,
            'total_items': sum(len(items) for items in self._connection_items),
            'max_items_per_connection': self.max_items_per_connection,
            'connections': details
        }
