"""OKX WebSocket implementation for perpetual swap order/trade/position updates."""

import asyncio
import base64
import hashlib
import hmac
import time
from datetime import datetime
from typing import Any

from axon_order_execution.config import ExchangeConfig, PortfolioConfig, WebSocketConfig
from axon_order_execution.models import Fill, Liquidity, Order, OrderSide, OrderStatus, OrderType
from axon_order_execution.models.portfolio import AccountSummary
from axon_order_execution.oms.base import BaseOMS
from axon_order_execution.oms.fill_manager import FillManager
from axon_order_execution.oms.order_manager import OrderManager
from axon_order_execution.oms.portfolio_manager import PortfolioManager
from axon_order_execution.util.logging import get_logger
from axon_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class OkxOMS(BaseOMS):
    """OKX OMS WebSocket client for perpetual swap order/trade updates."""

    TESTNET_URL = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
    PRODUCTION_URL = "wss://ws.okx.com:8443/ws/v5/private"

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
        return "okx"

    def _get_ws_url(self) -> str:
        if self._exchange_config.is_testnet:
            return self.TESTNET_URL
        return self.PRODUCTION_URL

    def _get_heartbeat_message(self) -> dict[str, Any] | None:
        # OKX heartbeat is handled in _heartbeat_loop override
        return None

    async def _heartbeat_loop(self) -> None:
        """OKX uses plain text 'ping' for heartbeat."""
        while self._running:
            try:
                await asyncio.sleep(self._config.heartbeat_interval_seconds)
                if not self._connected or not self._ws:
                    continue
                await self._ws.send("ping")
            except Exception:
                break

    def _build_request_message(
        self, request_id: int, method: str, params: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Build OKX WebSocket request message."""
        msg: dict[str, Any] = {"op": method}
        if params:
            msg.update(params)
        # OKX doesn't have a standard request_id, we use the op + channel as key
        msg["_req_id"] = request_id  # internal tracking
        return msg

    def _parse_response(
        self, message: dict[str, Any]
    ) -> tuple[int | None, Any, Exception | None]:
        """Parse OKX WebSocket response."""
        # OKX responses have an "event" field for subscription confirmations
        req_id = message.get("_req_id")
        if req_id is not None and req_id in self._pending_requests:
            event = message.get("event", "")
            if event == "error":
                return req_id, None, Exception(message.get("msg", "Unknown error"))
            return req_id, message, None

        # Check for subscribe/login responses by matching pending requests
        event = message.get("event", "")
        if event in ("login", "subscribe", "unsubscribe"):
            # Try to match by recent pending request
            for rid, future in list(self._pending_requests.items()):
                if not future.done():
                    if event == "error":
                        return rid, None, Exception(message.get("msg", ""))
                    return rid, message, None
        return None, None, None

    async def _authenticate(self) -> None:
        """Authenticate with OKX WebSocket."""
        timestamp = str(int(time.time()))
        pre_sign = f"{timestamp}GET/users/self/verify"
        signature = base64.b64encode(
            hmac.new(
                self._exchange_config.api_secret.encode(),
                pre_sign.encode(),
                hashlib.sha256,
            ).digest()
        ).decode()

        await self._send_request("login", {
            "args": [{
                "apiKey": self._exchange_config.api_key,
                "passphrase": self._exchange_config.passphrase,
                "timestamp": timestamp,
                "sign": signature,
            }],
        })
        logger.info("OKX WebSocket authenticated")

    async def _on_authenticated(self) -> None:
        """Subscribe to private channels after authentication."""
        await self.subscribe_orders()
        await self.subscribe_trades()
        if self._portfolio_manager:
            await self._subscribe_account()

    async def subscribe_orders(self, instrument: str | None = None) -> None:
        channel = "orders"
        key = f"{channel}:SWAP"
        if key in self._subscribed_channels:
            return
        arg: dict[str, str] = {"channel": channel, "instType": "SWAP"}
        if instrument:
            arg["instId"] = instrument
        await self._send_request("subscribe", {"args": [arg]})
        self._subscribed_channels.add(key)
        logger.info(f"Subscribed to OKX {key}")

    async def unsubscribe_orders(self, instrument: str | None = None) -> None:
        channel = "orders"
        key = f"{channel}:SWAP"
        if key not in self._subscribed_channels:
            return
        arg: dict[str, str] = {"channel": channel, "instType": "SWAP"}
        if instrument:
            arg["instId"] = instrument
        await self._send_request("unsubscribe", {"args": [arg]})
        self._subscribed_channels.discard(key)

    async def subscribe_trades(self) -> None:
        channel = "fills"
        if channel in self._subscribed_channels:
            return
        await self._send_request("subscribe", {"args": [{"channel": channel, "instType": "SWAP"}]})
        self._subscribed_channels.add(channel)
        logger.info(f"Subscribed to OKX {channel}")

    async def _subscribe_account(self) -> None:
        channel = "account"
        if channel in self._subscribed_channels:
            return
        await self._send_request("subscribe", {"args": [{"channel": channel}]})
        self._subscribed_channels.add(channel)
        logger.info(f"Subscribed to OKX {channel}")

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Handle incoming OKX WebSocket messages."""
        # Skip non-push messages
        if "event" in message:
            return

        arg = message.get("arg", {})
        channel = arg.get("channel", "")
        data = message.get("data", [])

        if not channel or not data:
            return

        if channel == "orders":
            for item in data:
                await self._handle_order_update(item)
        elif channel == "fills":
            await self._handle_trade_update(data)
        elif channel == "account":
            for item in data:
                await self._handle_account_update(item)

    async def _handle_order_update(self, data: dict[str, Any]) -> None:
        try:
            order = self._parse_order(data)
            logger.debug(f"OKX order update: {order.order_id} -> {order.status.value}")

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
            logger.error(f"Failed to handle OKX order update: {e}, data: {data}")

    async def _handle_trade_update(self, data: list[dict[str, Any]]) -> None:
        for trade in data:
            try:
                fill = self._parse_fill(trade)
            except Exception as e:
                logger.error(f"Failed to parse OKX trade: {e}, trade: {trade}")
                continue

            order = await self._order_manager.get_order(fill.order_id)
            if order is None:
                logger.warning(f"OKX trade for unknown order {fill.order_id}")
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

    async def _handle_account_update(self, data: dict[str, Any]) -> None:
        if not self._portfolio_manager:
            return
        try:
            for detail in data.get("details", []):
                summary = AccountSummary(
                    currency=detail.get("ccy", ""),
                    exchange="okx",
                    equity=float(detail.get("eq", 0)),
                    balance=float(detail.get("cashBal", 0)),
                    available_funds=float(detail.get("availBal", 0)),
                    initial_margin=float(data.get("imr", 0)),
                    maintenance_margin=float(data.get("mmr", 0)),
                    margin_balance=float(detail.get("eq", 0)),
                    delta_total=0,
                    options_delta=0,
                    options_gamma=0,
                    options_vega=0,
                    options_theta=0,
                    futures_pl=float(detail.get("upl", 0)),
                    options_pl=0,
                    total_pl=float(detail.get("upl", 0)),
                    timestamp=datetime.utcnow(),
                )
                await self._portfolio_manager.update_account(summary)
        except Exception as e:
            logger.error(f"Failed to handle OKX account update: {e}, data: {data}")

    def _parse_order(self, data: dict[str, Any]) -> Order:
        status_map = {
            "live": OrderStatus.OPEN,
            "partially_filled": OrderStatus.PARTIALLY_FILLED,
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELLED,
            "mmp_canceled": OrderStatus.CANCELLED,
        }
        raw_status = data.get("state", "live")
        status = status_map.get(raw_status, OrderStatus.OPEN)

        created_ms = data.get("cTime", "0")
        updated_ms = data.get("uTime", created_ms)

        ord_type = data.get("ordType", "limit")
        if ord_type in ("limit", "post_only"):
            order_type = OrderType.LIMIT
        else:
            order_type = OrderType.MARKET

        avg_px_str = data.get("avgPx", "")
        avg_px = float(avg_px_str) if avg_px_str else None

        return Order(
            order_id=data.get("ordId", ""),
            exchange="okx",
            instrument=data.get("instId", ""),
            side=OrderSide.BUY if data.get("side") == "buy" else OrderSide.SELL,
            order_type=order_type,
            amount=float(data.get("sz", 0)),
            price=float(data["px"]) if data.get("px") else None,
            filled_amount=float(data.get("accFillSz", 0)),
            average_price=avg_px,
            status=status,
            label=data.get("clOrdId") or None,
            post_only=ord_type == "post_only",
            created_at=datetime.fromtimestamp(int(created_ms) / 1000) if created_ms != "0" else datetime.utcnow(),
            updated_at=datetime.fromtimestamp(int(updated_ms) / 1000) if updated_ms != "0" else datetime.utcnow(),
        )

    def _parse_fill(self, data: dict[str, Any]) -> Fill:
        ts = data.get("ts", data.get("fillTime", "0"))
        exec_type = data.get("execType", "")
        return Fill(
            trade_id=data.get("tradeId", data.get("billId", "")),
            order_id=data.get("ordId", ""),
            exchange="okx",
            instrument=data.get("instId", ""),
            side=OrderSide.BUY if data.get("side") == "buy" else OrderSide.SELL,
            amount=float(data.get("fillSz", 0)),
            price=float(data.get("fillPx", 0)),
            fee=abs(float(data.get("fee", 0))),
            fee_currency=data.get("feeCcy", "USDT"),
            liquidity=Liquidity.MAKER if exec_type == "M" else Liquidity.TAKER,
            timestamp=datetime.fromtimestamp(int(ts) / 1000) if ts != "0" else datetime.utcnow(),
            label=data.get("clOrdId") or None,
        )
