"""Async HTTP client utilities."""

import asyncio
import random
from typing import Any, Literal

import aiohttp

from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)

HttpMethod = Literal["GET", "POST", "PUT", "DELETE"]


class AsyncHttpClient:
    """Async HTTP client with lazy session initialization and retry support.

    Provides a reusable HTTP client for REST API calls with automatic
    session lifecycle management and configurable retry logic.

    Example:
        ```python
        client = AsyncHttpClient("https://api.example.com", max_retries=3)

        # GET request (session auto-created on first call)
        async with await client.get("/users", params={"id": 1}) as resp:
            data = await resp.json()

        # POST request with JSON body
        async with await client.post("/orders", json={"amount": 100}) as resp:
            result = await resp.json()

        # Close when done
        await client.close()
        ```
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 30.0,
        headers: dict[str, str] | None = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
    ):
        """Initialize HTTP client.

        Args:
            base_url: Base URL for all requests.
            timeout: Request timeout in seconds.
            headers: Default headers for all requests.
            max_retries: Maximum number of retries for failed requests.
            retry_delay: Base delay between retries in seconds (uses exponential backoff).
        """
        self._base_url = base_url.rstrip("/")
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._default_headers = headers or {}
        self._session: aiohttp.ClientSession | None = None
        self._max_retries = max_retries
        self._retry_delay = retry_delay

    @property
    def is_connected(self) -> bool:
        """Check if session is active."""
        return self._session is not None and not self._session.closed

    def _get_session(self) -> aiohttp.ClientSession:
        """Get or create session (lazy initialization)."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self._timeout,
                headers=self._default_headers,
            )
        return self._session

    async def close(self) -> None:
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    def _should_retry(self, error: Exception) -> bool:
        """Check if error is retryable."""
        if isinstance(error, aiohttp.ClientResponseError):
            # Retry on 5xx server errors, not 4xx client errors
            return error.status >= 500
        # Retry on connection/timeout errors
        return isinstance(error, (aiohttp.ClientError, asyncio.TimeoutError, OSError))

    async def _request(
        self,
        method: HttpMethod,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        data: Any = None,
        json: dict[str, Any] | None = None,
    ) -> aiohttp.ClientResponse:
        """Make HTTP request with retry logic."""
        session = self._get_session()
        url = f"{self._base_url}/{path.lstrip('/')}"
        last_error: Exception | None = None

        for attempt in range(self._max_retries + 1):
            try:
                response = await session.request(
                    method,
                    url,
                    params=params,
                    headers=headers,
                    data=data,
                    json=json,
                )
                # Check for 5xx errors that should be retried
                if response.status >= 500 and attempt < self._max_retries:
                    logger.warning(
                        f"{method} {path} returned {response.status}, retrying ({attempt + 1}/{self._max_retries})"
                    )
                    await response.read()  # Consume response body
                    response.close()
                    await self._sleep_with_backoff(attempt)
                    continue
                return response

            except Exception as e:
                last_error = e
                if attempt < self._max_retries and self._should_retry(e):
                    logger.warning(
                        f"{method} {path} failed: {e}, retrying ({attempt + 1}/{self._max_retries})"
                    )
                    await self._sleep_with_backoff(attempt)
                    continue
                raise

        raise last_error or RuntimeError("Request failed")

    async def _sleep_with_backoff(self, attempt: int) -> None:
        """Sleep with exponential backoff and jitter."""
        delay = self._retry_delay * (2**attempt)
        delay = delay * (0.5 + random.random())  # Add jitter
        await asyncio.sleep(delay)

    async def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> aiohttp.ClientResponse:
        """Make a GET request with retry.

        Args:
            path: Request path (appended to base_url).
            params: Query parameters.
            headers: Additional headers for this request.

        Returns:
            Response object.
        """
        return await self._request("GET", path, params=params, headers=headers)

    async def post(
        self,
        path: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> aiohttp.ClientResponse:
        """Make a POST request with retry.

        Args:
            path: Request path (appended to base_url).
            data: Form data.
            json: JSON body.
            params: Query parameters.
            headers: Additional headers for this request.

        Returns:
            Response object.
        """
        return await self._request("POST", path, params=params, headers=headers, data=data, json=json)

    async def put(
        self,
        path: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> aiohttp.ClientResponse:
        """Make a PUT request with retry.

        Args:
            path: Request path (appended to base_url).
            data: Form data.
            json: JSON body.
            params: Query parameters.
            headers: Additional headers for this request.

        Returns:
            Response object.
        """
        return await self._request("PUT", path, params=params, headers=headers, data=data, json=json)

    async def delete(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> aiohttp.ClientResponse:
        """Make a DELETE request with retry.

        Args:
            path: Request path (appended to base_url).
            params: Query parameters.
            headers: Additional headers for this request.

        Returns:
            Response object.
        """
        return await self._request("DELETE", path, params=params, headers=headers)

    async def get_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Make a GET request and return JSON response.

        Args:
            path: Request path.
            params: Query parameters.
            headers: Additional headers.

        Returns:
            Parsed JSON response.

        Raises:
            aiohttp.ClientResponseError: If response status is not OK.
        """
        async with await self.get(path, params=params, headers=headers) as response:
            response.raise_for_status()
            return await response.json()

    async def post_json(
        self,
        path: str,
        data: Any = None,
        json: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        """Make a POST request and return JSON response.

        Args:
            path: Request path.
            data: Form data.
            json: JSON body.
            params: Query parameters.
            headers: Additional headers.

        Returns:
            Parsed JSON response.

        Raises:
            aiohttp.ClientResponseError: If response status is not OK.
        """
        async with await self.post(path, data=data, json=json, params=params, headers=headers) as response:
            response.raise_for_status()
            return await response.json()

    async def __aenter__(self) -> "AsyncHttpClient":
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
