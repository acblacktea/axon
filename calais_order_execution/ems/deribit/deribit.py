"""Deribit EMS implementation using REST API."""

import time
from datetime import datetime
from typing import Any

from calais_order_execution.config import ExchangeConfig
from calais_order_execution.ems.base import BaseEMS
from calais_order_execution.models import Order, OrderRequest, OrderSide, OrderStatus, OrderType, Ticker
from calais_order_execution.util import AsyncHttpClient
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class DeribitEMS(BaseEMS):
    """Deribit execution management system using REST API."""

    TESTNET_URL = "https://test.deribit.com/api/v2"
    PRODUCTION_URL = "https://www.deribit.com/api/v2"

    def __init__(self, config: ExchangeConfig):
        """Initialize Deribit EMS.

        Args:
            config: Exchange configuration with API credentials.
        """
        self._config = config
        base_url = self.TESTNET_URL if config.is_testnet else self.PRODUCTION_URL
        self._http = AsyncHttpClient(base_url)
        self._access_token: str | None = None
        self._token_expiry: float = 0

    @property
    def exchange_name(self) -> str:
        return "deribit"

    async def start(self) -> None:
        """No-op. Authentication is lazy on first private request."""
        pass

    async def stop(self) -> None:
        """Close HTTP session."""
        await self._http.close()

    async def _ensure_authenticated(self) -> None:
        """Ensure we have a valid access token (lazy authentication)."""
        if self._access_token is None or time.time() >= self._token_expiry:
            params = {
                "grant_type": "client_credentials",
                "client_id": self._config.api_key,
                "client_secret": self._config.api_secret,
            }
            result = await self._public_request("public/auth", params)
            self._access_token = result["access_token"]
            self._token_expiry = time.time() + result["expires_in"] - 60

    async def _public_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Make a public API request."""
        async with await self._http.get(path, params=params) as response:
            data = await response.json()
            if "error" in data:
                raise Exception(f"Deribit API error: {data['error']}")
            return data["result"]

    async def _private_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Make a private (authenticated) API request."""
        await self._ensure_authenticated()
        headers = {"Authorization": f"Bearer {self._access_token}"}
        async with await self._http.get(path, params=params, headers=headers) as response:
            data = await response.json()
            if "error" in data:
                raise Exception(f"Deribit API error: {data['error']}")
            return data["result"]

    async def place_order(self, request: OrderRequest) -> Order:
        """Place a new order on Deribit."""
        # Determine endpoint based on side
        method = "private/buy" if request.side == OrderSide.BUY else "private/sell"

        params: dict[str, Any] = {
            "instrument_name": request.instrument,
            "amount": request.amount,
            "type": request.order_type.value,
        }

        if request.order_type == OrderType.LIMIT:
            params["price"] = request.price

        if request.label:
            params["label"] = request.label

        if request.post_only:
            params["post_only"] = "true"
            if request.reject_post_only:
                params["reject_post_only"] = "true"

        logger.info(f"Placing order: {method} {params}")
        result = await self._private_request(method, params)

        order_data = result["order"]
        order = self._parse_order(order_data)
        order.internal_order_id = request.internal_order_id
        return order

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel an order."""
        try:
            await self._private_request("private/cancel", {"order_id": order_id})
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    async def modify_order(
        self,
        order_id: str,
        amount: float | None = None,
        price: float | None = None,
    ) -> Order:
        """Modify an existing order."""
        params: dict[str, Any] = {"order_id": order_id}

        if amount is not None:
            params["amount"] = amount
        if price is not None:
            params["price"] = price

        logger.info(f"Modifying order: {params}")
        result = await self._private_request("private/edit", params)

        order_data = result["order"]
        return self._parse_order(order_data)

    async def get_order(self, order_id: str) -> Order | None:
        """Get order by ID."""
        try:
            result = await self._private_request("private/get_order_state", {"order_id": order_id})
            return self._parse_order(result)
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            return None

    async def get_open_orders(self, instrument: str | None = None) -> list[Order]:
        """Get all open orders."""
        params: dict[str, Any] = {}
        if instrument:
            params["instrument_name"] = instrument
            method = "private/get_open_orders_by_instrument"
        else:
            # Get all open orders for options
            params["kind"] = "option"
            method = "private/get_open_orders_by_currency"
            params["currency"] = "BTC"  # Default to BTC options

        try:
            result = await self._private_request(method, params)
            return [self._parse_order(o) for o in result]
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []

    async def get_open_orders_by_currency(self, currency: str = "BTC", kind: str = "option") -> list[Order]:
        """Get all open orders for a currency and kind.

        Args:
            currency: Currency (BTC, ETH, etc.)
            kind: Instrument kind (option, future, etc.)

        Returns:
            List of open orders.
        """
        params = {"currency": currency, "kind": kind}
        try:
            result = await self._private_request("private/get_open_orders_by_currency", params)
            return [self._parse_order(o) for o in result]
        except Exception as e:
            logger.error(f"Failed to get open orders for {currency} {kind}: {e}")
            return []

    async def get_ticker(self, instrument: str) -> Ticker:
        """Get ticker data for an instrument.

        Args:
            instrument: Instrument name (e.g., "BTC-28MAR25-50000-C").

        Returns:
            Ticker with best bid/ask prices.
        """
        result = await self._public_request("public/ticker", {"instrument_name": instrument})
        return Ticker(
            instrument=instrument,
            best_bid_price=result.get("best_bid_price") or 0,
            best_bid_amount=result.get("best_bid_amount") or 0,
            best_ask_price=result.get("best_ask_price") or 0,
            best_ask_amount=result.get("best_ask_amount") or 0,
            last_price=result.get("last_price"),
            mark_price=result.get("mark_price"),
        )

    def _parse_order(self, data: dict[str, Any]) -> Order:
        """Parse Deribit order response into Order model."""
        # Map Deribit status to our OrderStatus
        status_map = {
            "open": OrderStatus.OPEN,
            "filled": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELLED,
            "rejected": OrderStatus.REJECTED,
            "untriggered": OrderStatus.PENDING,
        }

        deribit_status = data.get("order_state", "open")
        status = status_map.get(deribit_status, OrderStatus.OPEN)

        # Check for partial fill
        filled_amount = data.get("filled_amount", 0)
        amount = data.get("amount", 0)
        if status == OrderStatus.OPEN and filled_amount > 0:
            status = OrderStatus.PARTIALLY_FILLED

        # Parse timestamps from API response (milliseconds)
        creation_ts = data["creation_timestamp"]
        last_update_ts = data.get("last_update_timestamp", creation_ts)

        return Order(
            order_id=data["order_id"],
            exchange="deribit",
            instrument=data["instrument_name"],
            side=OrderSide.BUY if data["direction"] == "buy" else OrderSide.SELL,
            order_type=OrderType.LIMIT if data.get("order_type") == "limit" else OrderType.MARKET,
            amount=amount,
            price=data.get("price"),
            filled_amount=filled_amount,
            average_price=data.get("average_price"),
            status=status,
            label=data.get("label"),
            post_only=data.get("post_only", False),
            reject_post_only=data.get("reject_post_only", False),
            created_at=datetime.fromtimestamp(creation_ts / 1000),
            updated_at=datetime.fromtimestamp(last_update_ts / 1000),
        )
