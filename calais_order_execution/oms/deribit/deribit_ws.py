"""Deribit WebSocket implementation for order updates."""

from datetime import datetime
from typing import Any

from calais_order_execution.config import ExchangeConfig, PortfolioConfig, WebSocketConfig
from calais_order_execution.models import Liquidity, Order, OrderSide, OrderStatus, OrderType
from calais_order_execution.models.portfolio import AccountSummary
from calais_order_execution.oms.base import BaseOMS
from calais_order_execution.oms.order_manager import OrderManager
from calais_order_execution.oms.portfolio_manager import PortfolioManager
from calais_order_execution.util.logging import get_logger

logger = get_logger(__name__)


class DeribitOMS(BaseOMS):
    """Deribit OMS WebSocket client for order and trade updates."""

    TESTNET_URL = "wss://test.deribit.com/ws/api/v2"
    PRODUCTION_URL = "wss://www.deribit.com/ws/api/v2"

    def __init__(
        self,
        order_manager: OrderManager,
        exchange_config: ExchangeConfig,
        ws_config: WebSocketConfig | None = None,
        portfolio_manager: PortfolioManager | None = None,
        portfolio_config: PortfolioConfig | None = None,
    ):
        """Initialize Deribit WebSocket.

        Args:
            order_manager: OrderManager instance for order state management.
            exchange_config: Exchange configuration with API credentials.
            ws_config: WebSocket configuration.
            portfolio_manager: PortfolioManager for account/position updates.
            portfolio_config: Portfolio configuration (currencies to subscribe).
        """
        super().__init__(order_manager, ws_config)
        self._exchange_config = exchange_config
        self._portfolio_manager = portfolio_manager
        self._portfolio_config = portfolio_config

    @property
    def exchange_name(self) -> str:
        """Get the exchange name."""
        return "deribit"

    def _get_ws_url(self) -> str:
        """Get Deribit WebSocket URL."""
        if self._exchange_config.is_testnet:
            return self.TESTNET_URL
        return self.PRODUCTION_URL

    def _get_heartbeat_message(self) -> dict[str, Any] | None:
        """Get Deribit heartbeat message."""
        return {
            "jsonrpc": "2.0",
            "id": 9999,
            "method": "public/test",
            "params": {},
        }

    def _build_request_message(
        self, request_id: int, method: str, params: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Build JSON-RPC 2.0 request message for Deribit."""
        return {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params or {},
        }

    def _parse_response(
        self, message: dict[str, Any]
    ) -> tuple[int | None, Any, Exception | None]:
        """Parse JSON-RPC 2.0 response message from Deribit."""
        request_id = message.get("id")
        if request_id is None or request_id not in self._pending_requests:
            return None, None, None

        if "error" in message:
            return request_id, None, Exception(message["error"])

        return request_id, message.get("result"), None

    async def _authenticate(self) -> None:
        """Authenticate with Deribit."""
        result = await self._send_request(
            "public/auth",
            {
                "grant_type": "client_credentials",
                "client_id": self._exchange_config.api_key,
                "client_secret": self._exchange_config.api_secret,
            },
        )
        logger.info(f"Deribit WebSocket authenticated, token expires in {result.get('expires_in')}s")

    async def _on_authenticated(self) -> None:
        """Subscribe to order updates after authentication."""
        # Enable heartbeat
        await self._send_request(
            "public/set_heartbeat",
            {"interval": self._config.heartbeat_interval_seconds},
        )

        # Subscribe to user orders for all instruments
        # Using user.orders.any.any.raw for all order updates
        await self.subscribe_orders()

        # Subscribe to user trades
        await self.subscribe_trades()

        # Subscribe to portfolio (balance) updates
        if self._portfolio_manager and self._portfolio_config:
            await self.subscribe_portfolio(self._portfolio_config.currencies)

    async def subscribe_orders(self, instrument: str | None = None) -> None:
        """Subscribe to order updates.

        Args:
            instrument: Specific instrument to subscribe to.
                       If None, subscribes to all orders.
        """
        if instrument:
            channel = f"user.orders.{instrument}.raw"
        else:
            # Subscribe to all user orders
            channel = "user.orders.any.any.raw"

        if channel in self._subscribed_channels:
            return

        await self._send_request(
            "private/subscribe",
            {"channels": [channel]},
        )
        self._subscribed_channels.add(channel)
        logger.info(f"Subscribed to {channel}")

    async def unsubscribe_orders(self, instrument: str | None = None) -> None:
        """Unsubscribe from order updates.

        Args:
            instrument: Specific instrument to unsubscribe from.
        """
        if instrument:
            channel = f"user.orders.{instrument}.raw"
        else:
            channel = "user.orders.any.any.raw"

        if channel not in self._subscribed_channels:
            return

        await self._send_request(
            "private/unsubscribe",
            {"channels": [channel]},
        )
        self._subscribed_channels.discard(channel)
        logger.info(f"Unsubscribed from {channel}")

    async def subscribe_portfolio(self, currencies: list[str] | None = None) -> None:
        """Subscribe to portfolio (account summary) updates for given currencies."""
        currencies = currencies or ["BTC"]
        channels = []
        for currency in currencies:
            channel = f"user.portfolio.{currency}"
            if channel not in self._subscribed_channels:
                channels.append(channel)

        if not channels:
            return

        await self._send_request(
            "private/subscribe",
            {"channels": channels},
        )
        for channel in channels:
            self._subscribed_channels.add(channel)
        logger.info(f"Subscribed to portfolio channels: {channels}")

    async def subscribe_trades(self) -> None:
        """Subscribe to user trades for all instruments."""
        channel = "user.trades.any.any.raw"

        if channel in self._subscribed_channels:
            return

        await self._send_request(
            "private/subscribe",
            {"channels": [channel]},
        )
        self._subscribed_channels.add(channel)
        logger.info(f"Subscribed to {channel}")

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Handle incoming WebSocket messages."""
        # Handle heartbeat
        if message.get("method") == "heartbeat":
            if message.get("params", {}).get("type") == "test_request":
                await self._send({
                    "jsonrpc": "2.0",
                    "id": 9998,
                    "method": "public/test",
                    "params": {},
                })
            return

        # Handle subscription data
        if message.get("method") == "subscription":
            params = message.get("params", {})
            channel = params.get("channel", "")

            if "user.orders" in channel:
                await self._handle_order_update(params.get("data", {}))
            elif "user.trades" in channel:
                await self._handle_trade_update(params.get("data", []))
            elif "user.portfolio" in channel:
                await self._handle_portfolio_update(params.get("data", {}))

    async def _handle_order_update(self, data: dict[str, Any]) -> None:
        """Handle order update from subscription.

        Args:
            data: Order data from WebSocket.
        """
        try:
            order = self._parse_order(data)
            logger.debug(f"Order update: {order.order_id} -> {order.status.value}")

            # Preserve fields from existing order that WS doesn't provide
            existing = await self._order_manager.get_order(order.order_id)
            if existing:
                if existing.liquidity == Liquidity.TAKER:
                    order.liquidity = Liquidity.TAKER
                if existing.internal_order_id:
                    order.internal_order_id = existing.internal_order_id
                if existing.strategy_id:
                    order.strategy_id = existing.strategy_id

            # Update order via order_manager
            await self._order_manager.update_from_ws(order)

        except Exception as e:
            logger.error(f"Failed to parse order update: {e}, data: {data}")

    async def _handle_trade_update(self, data: list[dict[str, Any]]) -> None:
        """Handle trade update from subscription.

        Only updates liquidity (maker/taker) status. Other fields like filled_amount,
        average_price, and status are updated via order subscription to avoid timing issues.

        Args:
            data: List of trade data from WebSocket.
        """
        for trade in data:
            try:
                order_id = trade.get("order_id")
                liquidity = trade.get("liquidity")  # "M" for maker, "T" for taker

                if not order_id:
                    continue

                # Only update liquidity if taker
                if liquidity == "T":
                    order = await self._order_manager.get_order(order_id)
                    if not order:
                        logger.warning(f"Trade for unknown order {order_id}")
                        continue

                    order.liquidity = Liquidity.TAKER
                    logger.debug(f"Trade update: order {order_id} marked as taker")
                    await self._order_manager.update_from_ws(order)

            except Exception as e:
                logger.error(f"Failed to handle trade update: {e}, trade: {trade}")

    async def _handle_portfolio_update(self, data: dict[str, Any]) -> None:
        """Handle portfolio (account summary) update from WS subscription."""
        if not self._portfolio_manager:
            return
        try:
            summary = AccountSummary(
                currency=data.get("currency", ""),
                equity=data.get("equity", 0),
                balance=data.get("balance", 0),
                available_funds=data.get("available_funds", 0),
                initial_margin=data.get("initial_margin", 0),
                maintenance_margin=data.get("maintenance_margin", 0),
                margin_balance=data.get("margin_balance", 0),
                delta_total=data.get("delta_total", 0),
                options_delta=data.get("options_delta", 0),
                options_gamma=data.get("options_gamma", 0),
                options_vega=data.get("options_vega", 0),
                options_theta=data.get("options_theta", 0),
                futures_pl=data.get("futures_pl", 0),
                options_pl=data.get("options_pl", 0),
                total_pl=data.get("total_pl", 0),
                timestamp=datetime.utcnow(),
            )
            await self._portfolio_manager.update_account(summary)
        except Exception as e:
            logger.error(f"Failed to handle portfolio update: {e}, data: {data}")

    def _parse_order(self, data: dict[str, Any]) -> Order:
        """Parse Deribit order data into Order model."""
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

