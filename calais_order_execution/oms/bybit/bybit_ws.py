"""Bybit WebSocket implementation for USDT perpetual order/trade/position updates."""

import hashlib
import hmac
import time
from datetime import datetime
from typing import Any

from calais_order_execution.config import ExchangeConfig, PortfolioConfig, WebSocketConfig
from calais_order_execution.models import Fill, Liquidity, Order, OrderSide, OrderStatus, OrderType
from calais_order_execution.models.portfolio import AccountSummary
from calais_order_execution.oms.base import BaseOMS
from calais_order_execution.oms.fill_manager import FillManager
from calais_order_execution.oms.order_manager import OrderManager
from calais_order_execution.oms.portfolio_manager import PortfolioManager
from calais_order_execution.util.logging import get_logger
from calais_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class BybitOMS(BaseOMS):
    """Bybit OMS WebSocket client for USDT perpetual order/trade updates."""

    TESTNET_URL = "wss://stream-testnet.bybit.com/v5/private"
    PRODUCTION_URL = "wss://stream.bybit.com/v5/private"

    def __init__(
        self,
        order_manager: OrderManager,
        exchange_config: ExchangeConfig,
        ws_config: WebSocketConfig | None = None,
        portfolio_manager: PortfolioManager | None = None,
        portfolio_config: PortfolioConfig | None = None,
        fill_manager: FillManager | None = None,
    ):
        super().__init__(order_manager, ws_config)
        self._exchange_config = exchange_config
        self._portfolio_manager = portfolio_manager
        self._portfolio_config = portfolio_config
        self._fill_manager = fill_manager

    @property
    def exchange_name(self) -> str:
        return "bybit"

    def _get_ws_url(self) -> str:
        if self._exchange_config.is_testnet:
            return self.TESTNET_URL
        return self.PRODUCTION_URL

    def _get_heartbeat_message(self) -> dict[str, Any] | None:
        return {"op": "ping"}

    def _build_request_message(
        self, request_id: int, method: str, params: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Build Bybit WebSocket request message."""
        msg: dict[str, Any] = {"req_id": str(request_id), "op": method}
        if params and "args" in params:
            msg["args"] = params["args"]
        elif params:
            msg.update(params)
        return msg

    def _parse_response(
        self, message: dict[str, Any]
    ) -> tuple[int | None, Any, Exception | None]:
        """Parse Bybit WebSocket response."""
        req_id = message.get("req_id")
        if req_id is not None:
            try:
                req_id = int(req_id)
            except (ValueError, TypeError):
                return None, None, None
            if req_id not in self._pending_requests:
                return None, None, None
            if message.get("success") is False:
                return req_id, None, Exception(message.get("ret_msg", "Unknown error"))
            return req_id, message, None
        return None, None, None

    async def _authenticate(self) -> None:
        """Authenticate with Bybit WebSocket using HMAC signature."""
        expires = int((time.time() + 10) * 1000)
        pre_sign = f"GET/realtime{expires}"
        signature = hmac.new(
            self._exchange_config.api_secret.encode(),
            pre_sign.encode(),
            hashlib.sha256,
        ).hexdigest()

        await self._send_request("auth", {
            "args": [self._exchange_config.api_key, expires, signature],
        })
        logger.info("Bybit WebSocket authenticated")

    async def _on_authenticated(self) -> None:
        """Subscribe to private channels after authentication."""
        await self.subscribe_orders()
        await self.subscribe_trades()
        if self._portfolio_manager:
            await self._subscribe_wallet()

    async def subscribe_orders(self, instrument: str | None = None) -> None:
        channel = "order"
        if channel in self._subscribed_channels:
            return
        await self._send_request("subscribe", {"args": [channel]})
        self._subscribed_channels.add(channel)
        logger.info(f"Subscribed to Bybit {channel}")

    async def unsubscribe_orders(self, instrument: str | None = None) -> None:
        channel = "order"
        if channel not in self._subscribed_channels:
            return
        await self._send_request("unsubscribe", {"args": [channel]})
        self._subscribed_channels.discard(channel)

    async def subscribe_trades(self) -> None:
        channel = "execution"
        if channel in self._subscribed_channels:
            return
        await self._send_request("subscribe", {"args": [channel]})
        self._subscribed_channels.add(channel)
        logger.info(f"Subscribed to Bybit {channel}")

    async def _subscribe_wallet(self) -> None:
        channel = "wallet"
        if channel in self._subscribed_channels:
            return
        await self._send_request("subscribe", {"args": [channel]})
        self._subscribed_channels.add(channel)
        logger.info(f"Subscribed to Bybit {channel}")

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Handle incoming Bybit WebSocket messages."""
        # Handle pong
        if message.get("op") == "pong":
            return

        topic = message.get("topic", "")
        data = message.get("data", [])

        if not topic or not data:
            return

        if topic == "order":
            for item in data:
                await self._handle_order_update(item)
        elif topic == "execution":
            await self._handle_trade_update(data)
        elif topic == "wallet":
            for item in data:
                await self._handle_wallet_update(item)

    async def _handle_order_update(self, data: dict[str, Any]) -> None:
        try:
            # Only process linear (USDT perp) orders
            if data.get("category") != "linear":
                return

            order = self._parse_order(data)
            logger.debug(f"Bybit order update: {order.order_id} -> {order.status.value}")

            existing = await self._order_manager.get_order(order.order_id)
            if existing:
                if existing.liquidity == Liquidity.TAKER:
                    order.liquidity = Liquidity.TAKER
                if existing.internal_order_id:
                    order.internal_order_id = existing.internal_order_id
                if existing.strategy_id:
                    order.strategy_id = existing.strategy_id

            await self._order_manager.update_from_ws(order)
        except Exception as e:
            logger.error(f"Failed to handle Bybit order update: {e}, data: {data}")

    async def _handle_trade_update(self, data: list[dict[str, Any]]) -> None:
        for trade in data:
            try:
                if trade.get("category") != "linear":
                    continue
                fill = self._parse_fill(trade)
            except Exception as e:
                logger.error(f"Failed to parse Bybit trade: {e}, trade: {trade}")
                continue

            order = await self._order_manager.get_order(fill.order_id)
            if order is None:
                logger.warning(f"Bybit trade for unknown order {fill.order_id}")
            else:
                fill.strategy_id = order.strategy_id
                if fill.liquidity == Liquidity.TAKER and order.liquidity != Liquidity.TAKER:
                    order.liquidity = Liquidity.TAKER
                    await self._order_manager.update_from_ws(order)

            if self._fill_manager is not None:
                try:
                    await self._fill_manager.add_fill(fill)
                except Exception as e:
                    logger.error(f"Failed to record fill {fill.trade_id}: {e}")

    async def _handle_wallet_update(self, data: dict[str, Any]) -> None:
        if not self._portfolio_manager:
            return
        try:
            for coin in data.get("coin", []):
                summary = AccountSummary(
                    currency=coin.get("coin", ""),
                    exchange="bybit",
                    equity=float(coin.get("equity", 0)),
                    balance=float(coin.get("walletBalance", 0)),
                    available_funds=float(coin.get("availableToWithdraw", 0)),
                    initial_margin=float(coin.get("totalOrderIM", 0)),
                    maintenance_margin=float(coin.get("totalPositionMM", 0)),
                    margin_balance=float(coin.get("walletBalance", 0)),
                    delta_total=0,
                    options_delta=0,
                    options_gamma=0,
                    options_vega=0,
                    options_theta=0,
                    futures_pl=float(coin.get("unrealisedPnl", 0)),
                    options_pl=0,
                    total_pl=float(coin.get("cumRealisedPnl", 0)),
                    timestamp=datetime.utcnow(),
                )
                await self._portfolio_manager.update_account(summary)
        except Exception as e:
            logger.error(f"Failed to handle Bybit wallet update: {e}, data: {data}")

    def _parse_order(self, data: dict[str, Any]) -> Order:
        status_map = {
            "New": OrderStatus.OPEN,
            "PartiallyFilled": OrderStatus.PARTIALLY_FILLED,
            "Filled": OrderStatus.FILLED,
            "Cancelled": OrderStatus.CANCELLED,
            "Rejected": OrderStatus.REJECTED,
            "Deactivated": OrderStatus.CANCELLED,
            "Untriggered": OrderStatus.PENDING,
            "Triggered": OrderStatus.OPEN,
        }
        raw_status = data.get("orderStatus", "New")
        status = status_map.get(raw_status, OrderStatus.OPEN)

        created_ms = data.get("createdTime", "0")
        updated_ms = data.get("updatedTime", created_ms)

        return Order(
            order_id=data.get("orderId", ""),
            exchange="bybit",
            instrument=data.get("symbol", ""),
            side=OrderSide.BUY if data.get("side") == "Buy" else OrderSide.SELL,
            order_type=OrderType.LIMIT if data.get("orderType") == "Limit" else OrderType.MARKET,
            amount=float(data.get("qty", 0)),
            price=float(data["price"]) if data.get("price") and data["price"] != "0" else None,
            filled_amount=float(data.get("cumExecQty", 0)),
            average_price=float(data["avgPrice"]) if data.get("avgPrice") and data["avgPrice"] != "0" else None,
            status=status,
            label=data.get("orderLinkId") or None,
            created_at=datetime.fromtimestamp(int(created_ms) / 1000) if created_ms != "0" else datetime.utcnow(),
            updated_at=datetime.fromtimestamp(int(updated_ms) / 1000) if updated_ms != "0" else datetime.utcnow(),
        )

    def _parse_fill(self, data: dict[str, Any]) -> Fill:
        ts = data.get("execTime", "0")
        return Fill(
            trade_id=data.get("execId", ""),
            order_id=data.get("orderId", ""),
            exchange="bybit",
            instrument=data.get("symbol", ""),
            side=OrderSide.BUY if data.get("side") == "Buy" else OrderSide.SELL,
            amount=float(data.get("execQty", 0)),
            price=float(data.get("execPrice", 0)),
            fee=float(data.get("execFee", 0)),
            fee_currency=data.get("feeCurrency", "USDT"),
            liquidity=Liquidity.MAKER if data.get("isMaker") == "true" or data.get("isMaker") is True else Liquidity.TAKER,
            timestamp=datetime.fromtimestamp(int(ts) / 1000) if ts != "0" else datetime.utcnow(),
            mark_price=float(data["markPrice"]) if data.get("markPrice") else None,
            label=data.get("orderLinkId") or None,
        )
