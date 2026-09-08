"""Binance WebSocket implementation for USDT-M perpetual order/trade updates.

Binance Futures uses a listenKey-based user data stream. The WebSocket receives
ORDER_TRADE_UPDATE and ACCOUNT_UPDATE events without explicit subscription.
"""

import asyncio
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


class BinanceOMS(BaseOMS):
    """Binance OMS WebSocket client for USDT-M perpetual order/trade updates.

    Uses the listenKey-based user data stream. The listenKey is obtained via
    the EMS REST client and must be kept alive every 30 minutes.
    """

    TESTNET_WS_URL = "wss://stream.binancefuture.com/ws"
    PRODUCTION_WS_URL = "wss://fstream.binance.com/ws"

    def __init__(
        self,
        order_manager: OrderManager,
        exchange_config: ExchangeConfig,
        ws_config: WebSocketConfig | None = None,
        portfolio_manager: PortfolioManager | None = None,
        portfolio_config: PortfolioConfig | None = None,
        fill_manager: FillManager | None = None,
        ems=None,  # BinanceEMS, typed loosely to avoid circular import
    ):
        super().__init__(order_manager, ws_config)
        self._exchange_config = exchange_config
        self._portfolio_manager = portfolio_manager
        self._portfolio_config = portfolio_config
        self._fill_manager = fill_manager
        self._ems = ems  # Used to create/keepalive listenKey
        self._listen_key: str = ""
        self._keepalive_task: asyncio.Task | None = None

    @property
    def exchange_name(self) -> str:
        return "binance"

    def _get_ws_url(self) -> str:
        base = self.TESTNET_WS_URL if self._exchange_config.is_testnet else self.PRODUCTION_WS_URL
        if self._listen_key:
            return f"{base}/{self._listen_key}"
        return base

    def _get_heartbeat_message(self) -> dict[str, Any] | None:
        # Binance user data stream doesn't need WS-level heartbeat;
        # keepalive is done via REST PUT to listenKey endpoint.
        return None

    def _build_request_message(
        self, request_id: int, method: str, params: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Binance user data stream doesn't support request-response pattern."""
        return {"method": method, "id": request_id, "params": params or {}}

    def _parse_response(
        self, message: dict[str, Any]
    ) -> tuple[int | None, Any, Exception | None]:
        """Parse Binance WebSocket response."""
        msg_id = message.get("id")
        if msg_id is not None and msg_id in self._pending_requests:
            if message.get("error"):
                return msg_id, None, Exception(str(message["error"]))
            return msg_id, message.get("result"), None
        return None, None, None

    async def _authenticate(self) -> None:
        """Obtain listenKey from Binance REST API.

        The listenKey is appended to the WS URL. We need to obtain it
        before connecting, so we reconnect with the proper URL.
        """
        if self._ems is None:
            raise RuntimeError("BinanceOMS requires BinanceEMS for listenKey management")

        self._listen_key = await self._ems.create_listen_key()
        logger.info(f"Binance listenKey obtained: {self._listen_key[:8]}...")

        # The initial connect() in WebSocketBase connected without listenKey.
        # We need to reconnect with the listenKey in the URL.
        # Close current connection and reconnect.
        if self._ws:
            await self._ws.close()
            self._ws = None
            self._connected = False

        await self._connect_ws()

    async def _on_authenticated(self) -> None:
        """Start listenKey keepalive task. No explicit subscriptions needed."""
        # Binance user data stream auto-sends ORDER_TRADE_UPDATE and ACCOUNT_UPDATE
        self._subscribed_channels.add("ORDER_TRADE_UPDATE")
        self._subscribed_channels.add("ACCOUNT_UPDATE")

        # Start keepalive task (every 30 minutes)
        if self._keepalive_task is None or self._keepalive_task.done():
            self._keepalive_task = asyncio.create_task(self._keepalive_loop())

        logger.info("Binance user data stream ready")

    async def _keepalive_loop(self) -> None:
        """Keep the listenKey alive every 30 minutes."""
        while self._running:
            try:
                await asyncio.sleep(30 * 60)  # 30 minutes
                if self._ems:
                    await self._ems.keepalive_listen_key()
                    logger.debug("Binance listenKey keepalive sent")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Binance listenKey keepalive failed: {e}")

    async def disconnect(self) -> None:
        """Override to cancel keepalive task."""
        if self._keepalive_task:
            self._keepalive_task.cancel()
            try:
                await self._keepalive_task
            except asyncio.CancelledError:
                pass
            self._keepalive_task = None
        await super().disconnect()

    async def subscribe_orders(self, instrument: str | None = None) -> None:
        # Auto-subscribed via user data stream
        pass

    async def unsubscribe_orders(self, instrument: str | None = None) -> None:
        pass

    async def subscribe_trades(self) -> None:
        # Auto-subscribed via user data stream
        pass

    async def _handle_message(self, message: dict[str, Any]) -> None:
        """Handle incoming Binance user data stream messages."""
        event_type = message.get("e", "")

        if event_type == "ORDER_TRADE_UPDATE":
            await self._handle_order_trade_update(message)
        elif event_type == "ACCOUNT_UPDATE":
            await self._handle_account_update(message)
        elif event_type == "listenKeyExpired":
            logger.warning("Binance listenKey expired, reconnecting...")
            asyncio.create_task(self._reconnect())

    async def _handle_order_trade_update(self, message: dict[str, Any]) -> None:
        """Handle ORDER_TRADE_UPDATE event (contains both order and fill info)."""
        data = message.get("o", {})
        try:
            order = self._parse_order(data)
            logger.debug(f"Binance order update: {order.order_id} -> {order.status.value}")

            existing = await self._order_manager.get_order(order.order_id)
            if existing:
                if existing.liquidity == Liquidity.TAKER:
                    order.liquidity = Liquidity.TAKER
                if existing.internal_order_id:
                    order.internal_order_id = existing.internal_order_id
                if existing.strategy_id:
                    order.strategy_id = existing.strategy_id

            await self._order_manager.update_from_ws(order)

            # If there's a trade in this update, create a fill
            exec_type = data.get("x", "")
            if exec_type == "TRADE":
                fill = self._parse_fill_from_order_update(data)
                order_for_fill = await self._order_manager.get_order(fill.order_id)
                if order_for_fill:
                    fill.strategy_id = order_for_fill.strategy_id
                    if fill.liquidity == Liquidity.TAKER and order_for_fill.liquidity != Liquidity.TAKER:
                        order_for_fill.liquidity = Liquidity.TAKER
                        await self._order_manager.update_from_ws(order_for_fill)

                if self._fill_manager is not None:
                    try:
                        await self._fill_manager.add_fill(fill)
                    except Exception as e:
                        logger.error(f"Failed to record fill {fill.trade_id}: {e}")

        except Exception as e:
            logger.error(f"Failed to handle Binance order update: {e}, data: {data}")

    async def _handle_account_update(self, message: dict[str, Any]) -> None:
        """Handle ACCOUNT_UPDATE event."""
        if not self._portfolio_manager:
            return
        try:
            data = message.get("a", {})
            for balance in data.get("B", []):
                summary = AccountSummary(
                    currency=balance.get("a", ""),
                    exchange="binance",
                    equity=float(balance.get("wb", 0)),
                    balance=float(balance.get("wb", 0)),
                    available_funds=float(balance.get("cw", 0)),
                    initial_margin=0,
                    maintenance_margin=0,
                    margin_balance=float(balance.get("wb", 0)),
                    delta_total=0,
                    options_delta=0,
                    options_gamma=0,
                    options_vega=0,
                    options_theta=0,
                    futures_pl=0,
                    options_pl=0,
                    total_pl=0,
                    timestamp=datetime.utcnow(),
                )
                await self._portfolio_manager.update_account(summary)
        except Exception as e:
            logger.error(f"Failed to handle Binance account update: {e}, data: {message}")

    def _parse_order(self, data: dict[str, Any]) -> Order:
        status_map = {
            "NEW": OrderStatus.OPEN,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELLED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.CANCELLED,
            "EXPIRED_IN_MATCH": OrderStatus.CANCELLED,
        }
        raw_status = data.get("X", "NEW")
        status = status_map.get(raw_status, OrderStatus.OPEN)

        order_time = data.get("T", 0)

        avg_price_str = data.get("ap", "0")
        avg_price = float(avg_price_str) if avg_price_str and float(avg_price_str) != 0 else None

        return Order(
            order_id=str(data.get("i", "")),
            exchange="binance",
            instrument=data.get("s", ""),
            side=OrderSide.BUY if data.get("S") == "BUY" else OrderSide.SELL,
            order_type=OrderType.LIMIT if data.get("o") == "LIMIT" else OrderType.MARKET,
            amount=float(data.get("q", 0)),
            price=float(data["p"]) if data.get("p") and float(data.get("p", 0)) != 0 else None,
            filled_amount=float(data.get("z", 0)),
            average_price=avg_price,
            status=status,
            label=data.get("c") or None,
            post_only=data.get("f") == "GTX",
            created_at=datetime.fromtimestamp(order_time / 1000) if order_time else datetime.utcnow(),
            updated_at=datetime.utcnow(),
        )

    def _parse_fill_from_order_update(self, data: dict[str, Any]) -> Fill:
        """Parse fill from ORDER_TRADE_UPDATE 'o' payload."""
        trade_time = data.get("T", 0)
        return Fill(
            trade_id=str(data.get("t", "")),
            order_id=str(data.get("i", "")),
            exchange="binance",
            instrument=data.get("s", ""),
            side=OrderSide.BUY if data.get("S") == "BUY" else OrderSide.SELL,
            amount=float(data.get("l", 0)),  # last filled quantity
            price=float(data.get("L", 0)),  # last filled price
            fee=float(data.get("n", 0)),  # commission
            fee_currency=data.get("N", "USDT"),  # commission asset
            liquidity=Liquidity.MAKER if data.get("m") else Liquidity.TAKER,
            timestamp=datetime.fromtimestamp(trade_time / 1000) if trade_time else datetime.utcnow(),
            label=data.get("c") or None,
        )
