"""Base WebSocket client with heartbeat and reconnection support."""

import asyncio
import json
import random
from abc import ABC, abstractmethod
from typing import Any, Callable

import websockets
from websockets.client import WebSocketClientProtocol

from calais_order_execution.config import WebSocketConfig
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class WebSocketBase(ABC):
    """Base WebSocket client with automatic heartbeat and reconnection.

    Subclasses should implement:
    - _get_ws_url(): Return the WebSocket URL
    - _on_authenticated(): Called after authentication
    - _handle_message(): Process incoming messages
    - _get_heartbeat_message(): Return heartbeat message (optional)
    """

    def __init__(self, config: WebSocketConfig | None = None):
        """Initialize WebSocket base.

        Args:
            config: WebSocket configuration. Uses defaults if not provided.
        """
        self._config = config or WebSocketConfig()
        self._ws: WebSocketClientProtocol | None = None
        self._running = False
        self._connected = False
        self._reconnect_attempts = 0
        self._tasks: list[asyncio.Task] = []
        self._message_handlers: list[Callable[[dict[str, Any]], None]] = []
        self._request_id = 0
        self._pending_requests: dict[int, asyncio.Future] = {}

    @property
    def is_connected(self) -> bool:
        """Check if WebSocket is connected."""
        return self._connected and self._ws is not None

    @abstractmethod
    def _get_ws_url(self) -> str:
        """Get WebSocket URL to connect to."""
        ...

    @abstractmethod
    async def _authenticate(self) -> None:
        """Authenticate the WebSocket connection."""
        ...

    @abstractmethod
    async def _on_authenticated(self) -> None:
        """Called after successful authentication. Setup subscriptions here."""
        ...

    @abstractmethod
    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Handle incoming WebSocket message.

        Args:
            message: Parsed JSON message.
        """
        ...

    def _get_heartbeat_message(self) -> dict[str, Any] | None:
        """Get heartbeat message to send. Override in subclass if needed."""
        return None

    @abstractmethod
    def _build_request_message(
        self, request_id: int, method: str, params: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Build request message for the specific protocol.

        Args:
            request_id: Unique request identifier.
            method: Method name.
            params: Request parameters.

        Returns:
            Request message dict.
        """
        ...

    @abstractmethod
    def _parse_response(
        self, message: dict[str, Any]
    ) -> tuple[int | None, Any, Exception | None]:
        """Parse response message for the specific protocol.

        Args:
            message: Response message.

        Returns:
            Tuple of (request_id, result, error).
            request_id is None if this is not a response to a pending request.
        """
        ...

    def add_message_handler(self, handler: Callable[[dict[str, Any]], None]) -> None:
        """Add a message handler callback.

        Args:
            handler: Function to call with each message.
        """
        self._message_handlers.append(handler)

    def remove_message_handler(self, handler: Callable[[dict[str, Any]], None]) -> None:
        """Remove a message handler callback."""
        if handler in self._message_handlers:
            self._message_handlers.remove(handler)

    async def connect(self) -> None:
        """Connect to WebSocket and start background tasks."""
        self._running = True
        await self._connect_ws()

        # Start background tasks (must start BEFORE authentication so responses can be received)
        self._tasks = [
            asyncio.create_task(self._receive_loop()),
            asyncio.create_task(self._heartbeat_loop()),
        ]

        # Authenticate and setup subscriptions
        await self._authenticate()
        await self._on_authenticated()

    async def disconnect(self) -> None:
        """Disconnect and cleanup."""
        self._running = False
        self._connected = False

        # Cancel all tasks
        for task in self._tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        self._tasks.clear()

        # Close WebSocket
        if self._ws:
            await self._ws.close()
            self._ws = None

        # Cancel pending requests
        for future in self._pending_requests.values():
            future.cancel()
        self._pending_requests.clear()

    async def _connect_ws(self) -> None:
        """Establish WebSocket connection (without authentication)."""
        url = self._get_ws_url()
        logger.info(f"Connecting to WebSocket: {url}")

        try:
            self._ws = await websockets.connect(url)
            self._connected = True
            self._reconnect_attempts = 0
            logger.info("WebSocket connected")

        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            self._connected = False
            raise

    async def _reconnect(self) -> None:
        """Reconnect with exponential backoff.

        Closes existing connection first, then establishes a new one.
        Auth uses limited retries (3) - if it fails, we tear down and retry
        the full reconnect cycle instead of retrying auth on a dead connection.
        """
        if not self._running:
            return

        # Close existing connection cleanly
        self._connected = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception:
                pass
            self._ws = None

        self._reconnect_attempts += 1

        # Calculate delay with exponential backoff and jitter
        base_delay = self._config.reconnect_delay_seconds
        max_delay = self._config.max_reconnect_delay_seconds
        delay = min(base_delay * (2 ** self._reconnect_attempts), max_delay)
        delay = delay * (0.5 + random.random())  # Add jitter

        logger.info(f"Reconnecting in {delay:.1f}s (attempt {self._reconnect_attempts})")
        await asyncio.sleep(delay)

        # Cancel old tasks before reconnecting
        for task in self._tasks:
            task.cancel()
        self._tasks.clear()

        try:
            await self._connect_ws()

            # Restart background tasks so responses can be received
            self._tasks = [
                asyncio.create_task(self._receive_loop()),
                asyncio.create_task(self._heartbeat_loop()),
            ]

            # Auth with limited retries - fail fast so we can rebuild the connection
            await self._authenticate()
            await self._on_authenticated()
        except Exception as e:
            logger.error(f"Reconnection failed: {e}")
            # Schedule another reconnection attempt (full cycle: close + reconnect)
            asyncio.create_task(self._reconnect())

    async def _receive_loop(self) -> None:
        """Main loop to receive and process messages."""
        while self._running:
            try:
                if not self._ws or not self._connected:
                    await asyncio.sleep(0.1)
                    continue

                try:
                    raw_message = await asyncio.wait_for(
                        self._ws.recv(),
                        timeout=self._config.heartbeat_interval_seconds * 2
                    )
                except asyncio.TimeoutError:
                    logger.warning("WebSocket receive timeout")
                    continue

                message = json.loads(raw_message)

                # Handle response to pending request
                request_id, result, error = self._parse_response(message)
                if request_id is not None:
                    future = self._pending_requests.pop(request_id)
                    if error:
                        future.set_exception(error)
                    else:
                        future.set_result(result)
                    continue

                # Handle subscription message
                await self._handle_message(message)

                # Call registered handlers
                for handler in self._message_handlers:
                    try:
                        handler(message)
                    except Exception as e:
                        logger.error(f"Message handler error: {e}")

            except websockets.ConnectionClosed as e:
                logger.warning(f"WebSocket connection closed: {e}")
                self._connected = False
                if self._running:
                    asyncio.create_task(self._reconnect())
                break
            except Exception as e:
                logger.error(f"Error in receive loop: {e}")
                await asyncio.sleep(0.1)

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat messages."""
        while self._running:
            try:
                await asyncio.sleep(self._config.heartbeat_interval_seconds)

                if not self._connected or not self._ws:
                    continue

                heartbeat = self._get_heartbeat_message()
                if heartbeat:
                    await self._send(heartbeat)
                    logger.debug("Heartbeat sent")

            except (websockets.ConnectionClosed, RuntimeError) as e:
                logger.warning(f"Heartbeat detected broken connection: {e}")
                self._connected = False
                if self._running:
                    asyncio.create_task(self._reconnect())
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    async def _send(self, message: dict[str, Any]) -> None:
        """Send a message through WebSocket.

        Args:
            message: Message to send.
        """
        if not self._ws or not self._connected:
            raise RuntimeError("WebSocket not connected")

        await self._ws.send(json.dumps(message))

    async def _send_request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        max_retries: int | None = None,
    ) -> Any:
        """Send a request and wait for response with retry logic.

        Args:
            method: RPC method name.
            params: Request parameters.
            max_retries: Maximum number of retries. Uses config default if not specified.

        Returns:
            Response result.

        Raises:
            TimeoutError: If all retries are exhausted.
            Exception: If a non-retryable error occurs.
        """
        retries = max_retries if max_retries is not None else self._config.max_request_retries
        last_error: Exception | None = None

        for attempt in range(retries + 1):
            self._request_id += 1
            request_id = self._request_id

            message = self._build_request_message(request_id, method, params)

            future: asyncio.Future = asyncio.get_event_loop().create_future()
            self._pending_requests[request_id] = future

            try:
                await self._send(message)
                result = await asyncio.wait_for(
                    future, timeout=self._config.request_timeout_seconds
                )
                return result

            except asyncio.TimeoutError:
                self._pending_requests.pop(request_id, None)
                last_error = TimeoutError(f"Request {method} timed out")
                logger.warning(
                    f"Request {method} timed out (attempt {attempt + 1}/{retries + 1})"
                )

            except (RuntimeError, websockets.ConnectionClosed) as e:
                self._pending_requests.pop(request_id, None)
                last_error = e
                logger.warning(
                    f"Request {method} failed: {e} (attempt {attempt + 1}/{retries + 1})"
                )

            except Exception as e:
                self._pending_requests.pop(request_id, None)
                raise

            if attempt < retries:
                delay = min(
                    self._config.retry_delay_seconds * (2 ** attempt),
                    self._config.max_reconnect_delay_seconds,
                )
                delay = delay * (0.5 + random.random())
                logger.info(f"Retrying {method} in {delay:.2f}s...")
                await asyncio.sleep(delay)

        raise last_error or TimeoutError(f"Request {method} failed after {retries + 1} attempts")

    async def __aenter__(self) -> "WebSocketBase":
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.disconnect()
