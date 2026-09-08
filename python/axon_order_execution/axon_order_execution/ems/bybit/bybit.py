"""Bybit EMS implementation for USDT perpetual futures using REST API."""

import hashlib
import hmac
import time
from datetime import datetime
from typing import Any

from axon_order_execution.config import ExchangeConfig
from axon_order_execution.ems.base import BaseEMS
from axon_order_execution.models import Fill, Liquidity, Order, OrderRequest, OrderSide, OrderStatus, OrderType, Ticker
from axon_order_execution.models.portfolio import AccountSummary, Position
from axon_order_execution.util import AsyncHttpClient
from axon_order_execution.util.logging import get_logger
from axon_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class BybitEMS(BaseEMS):
    """Bybit execution management system for USDT perpetual futures."""

    TESTNET_URL = "https://api-testnet.bybit.com"
    PRODUCTION_URL = "https://api.bybit.com"

    def __init__(self, config: ExchangeConfig):
        self._config = config
        base_url = self.TESTNET_URL if config.is_testnet else self.PRODUCTION_URL
        self._http = AsyncHttpClient(base_url)

    @property
    def exchange_name(self) -> str:
        return "bybit"

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        await self._http.close()

    def _sign(self, timestamp: str, params_str: str) -> str:
        """Generate HMAC-SHA256 signature for Bybit V5 API."""
        recv_window = "5000"
        pre_sign = f"{timestamp}{self._config.api_key}{recv_window}{params_str}"
        return hmac.new(
            self._config.api_secret.encode(), pre_sign.encode(), hashlib.sha256
        ).hexdigest()

    def _auth_headers(self, timestamp: str, signature: str) -> dict[str, str]:
        return {
            "X-BAPI-API-KEY": self._config.api_key,
            "X-BAPI-SIGN": signature,
            "X-BAPI-SIGN-TYPE": "2",
            "X-BAPI-TIMESTAMP": timestamp,
            "X-BAPI-RECV-WINDOW": "5000",
            "Content-Type": "application/json",
        }

    async def _get_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        """Signed GET request."""
        metrics = get_metrics()
        start = time.monotonic()
        timestamp = str(int(time.time() * 1000))
        query = "&".join(f"{k}={v}" for k, v in sorted((params or {}).items()))
        signature = self._sign(timestamp, query)
        headers = self._auth_headers(timestamp, signature)
        try:
            async with await self._http.get(path, params=params, headers=headers) as resp:
                data = await resp.json()
            metrics.observe_ems_request("bybit", path, time.monotonic() - start)
            if data.get("retCode") != 0:
                raise Exception(f"Bybit API error: {data.get('retMsg')} (code={data.get('retCode')})")
            return data.get("result", {})
        except Exception as e:
            metrics.observe_ems_request("bybit", path, time.monotonic() - start)
            metrics.inc_ems_request_error("bybit", path, type(e).__name__)
            raise

    async def _post_request(self, path: str, body: dict[str, Any]) -> Any:
        """Signed POST request."""
        import json as _json
        metrics = get_metrics()
        start = time.monotonic()
        timestamp = str(int(time.time() * 1000))
        body_str = _json.dumps(body)
        signature = self._sign(timestamp, body_str)
        headers = self._auth_headers(timestamp, signature)
        try:
            async with await self._http.post(path, json=body, headers=headers) as resp:
                data = await resp.json()
            metrics.observe_ems_request("bybit", path, time.monotonic() - start)
            if data.get("retCode") != 0:
                raise Exception(f"Bybit API error: {data.get('retMsg')} (code={data.get('retCode')})")
            return data.get("result", {})
        except Exception as e:
            metrics.observe_ems_request("bybit", path, time.monotonic() - start)
            metrics.inc_ems_request_error("bybit", path, type(e).__name__)
            raise

    async def place_order(self, request: OrderRequest) -> Order:
        body: dict[str, Any] = {
            "category": "linear",
            "symbol": request.instrument,
            "side": "Buy" if request.side == OrderSide.BUY else "Sell",
            "orderType": "Limit" if request.order_type == OrderType.LIMIT else "Market",
            "qty": str(request.amount),
        }
        if request.order_type == OrderType.LIMIT:
            body["price"] = str(request.price)
        if request.post_only:
            body["timeInForce"] = "PostOnly"
        else:
            body["timeInForce"] = "GTC" if request.order_type == OrderType.LIMIT else "IOC"
        if request.label:
            body["orderLinkId"] = request.label

        logger.info(f"Placing Bybit order: {body}")
        result = await self._post_request("/v5/order/create", body)

        order_id = result["orderId"]
        # Fetch full order state
        order = await self.get_order(order_id)
        if order is None:
            # Fallback: build from request
            order = Order(
                order_id=order_id,
                exchange="bybit",
                instrument=request.instrument,
                side=request.side,
                order_type=request.order_type,
                amount=request.amount,
                price=request.price,
                status=OrderStatus.OPEN,
                label=request.label,
                post_only=request.post_only,
                reject_post_only=request.reject_post_only,
            )
        order.internal_order_id = request.internal_order_id
        return order

    async def cancel_order(self, order_id: str) -> bool:
        try:
            await self._post_request("/v5/order/cancel", {
                "category": "linear",
                "orderId": order_id,
            })
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    async def modify_order(
        self, order_id: str, amount: float | None = None, price: float | None = None,
    ) -> Order:
        body: dict[str, Any] = {"category": "linear", "orderId": order_id}
        if amount is not None:
            body["qty"] = str(amount)
        if price is not None:
            body["price"] = str(price)
        logger.info(f"Modifying Bybit order: {body}")
        await self._post_request("/v5/order/amend", body)
        order = await self.get_order(order_id)
        if order is None:
            raise Exception(f"Order {order_id} not found after modify")
        return order

    async def get_order(self, order_id: str) -> Order | None:
        try:
            result = await self._get_request("/v5/order/realtime", {
                "category": "linear",
                "orderId": order_id,
            })
            orders = result.get("list", [])
            if not orders:
                return None
            return self._parse_order(orders[0])
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            return None

    async def get_open_orders(self, instrument: str | None = None) -> list[Order]:
        params: dict[str, Any] = {"category": "linear"}
        if instrument:
            params["symbol"] = instrument
        try:
            result = await self._get_request("/v5/order/realtime", params)
            return [self._parse_order(o) for o in result.get("list", [])]
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []

    async def get_ticker(self, instrument: str) -> Ticker:
        result = await self._get_request("/v5/market/tickers", {
            "category": "linear",
            "symbol": instrument,
        })
        data = result.get("list", [{}])[0]
        return Ticker(
            instrument=instrument,
            best_bid_price=float(data.get("bid1Price", 0)),
            best_bid_amount=float(data.get("bid1Size", 0)),
            best_ask_price=float(data.get("ask1Price", 0)),
            best_ask_amount=float(data.get("ask1Size", 0)),
            last_price=float(data.get("lastPrice", 0)),
            mark_price=float(data.get("markPrice", 0)),
        )

    async def get_account_summary(self, currency: str = "USDT") -> AccountSummary:
        result = await self._get_request("/v5/account/wallet-balance", {
            "accountType": "UNIFIED",
        })
        accounts = result.get("list", [])
        coin_data: dict[str, Any] = {}
        total_equity = 0.0
        for acct in accounts:
            total_equity = float(acct.get("totalEquity", 0))
            for coin in acct.get("coin", []):
                if coin.get("coin") == currency:
                    coin_data = coin
                    break
        return AccountSummary(
            currency=currency,
            exchange="bybit",
            equity=float(coin_data.get("equity", total_equity)),
            balance=float(coin_data.get("walletBalance", 0)),
            available_funds=float(coin_data.get("availableToWithdraw", 0)),
            initial_margin=float(coin_data.get("totalOrderIM", 0)),
            maintenance_margin=float(coin_data.get("totalPositionMM", 0)),
            margin_balance=float(coin_data.get("walletBalance", 0)),
            delta_total=0,
            options_delta=0,
            options_gamma=0,
            options_vega=0,
            options_theta=0,
            futures_pl=float(coin_data.get("unrealisedPnl", 0)),
            options_pl=0,
            total_pl=float(coin_data.get("cumRealisedPnl", 0)),
            timestamp=datetime.utcnow(),
        )

    async def get_positions(self, currency: str = "USDT", kind: str = "linear") -> list[Position]:
        result = await self._get_request("/v5/position/list", {
            "category": "linear",
            "settleCoin": currency,
        })
        return [self._parse_position(p) for p in result.get("list", []) if float(p.get("size", 0)) != 0]

    async def get_user_trades_since(
        self, currency: str, since_ms: int, kind: str = "any",
    ) -> list[Fill]:
        fills: list[Fill] = []
        cursor = ""
        for _ in range(100):
            params: dict[str, Any] = {
                "category": "linear",
                "startTime": str(since_ms),
                "endTime": str(int(time.time() * 1000) + 1000),
                "limit": "100",
            }
            if cursor:
                params["cursor"] = cursor
            try:
                result = await self._get_request("/v5/execution/list", params)
            except Exception as e:
                logger.error(f"get_user_trades_since failed: {e}")
                break
            trades = result.get("list", [])
            if not trades:
                break
            for t in trades:
                try:
                    fills.append(self._parse_fill(t))
                except Exception as e:
                    logger.error(f"Failed to parse trade: {e}, raw: {t}")
            cursor = result.get("nextPageCursor", "")
            if not cursor:
                break
        return fills

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
            order_id=data["orderId"],
            exchange="bybit",
            instrument=data.get("symbol", ""),
            side=OrderSide.BUY if data.get("side") == "Buy" else OrderSide.SELL,
            order_type=OrderType.LIMIT if data.get("orderType") == "Limit" else OrderType.MARKET,
            amount=float(data.get("qty", 0)),
            price=float(data["price"]) if data.get("price") else None,
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
            trade_id=data["execId"],
            order_id=data["orderId"],
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

    def _parse_position(self, data: dict[str, Any]) -> Position:
        size = float(data.get("size", 0))
        side = data.get("side", "")
        if side == "Buy":
            direction = "buy"
        elif side == "Sell":
            direction = "sell"
        else:
            direction = "zero"
        created_ms = data.get("createdTime", "0")
        return Position(
            instrument=data.get("symbol", ""),
            exchange="bybit",
            kind="future",
            direction=direction,
            size=size,
            average_price=float(data.get("avgPrice", 0)),
            mark_price=float(data.get("markPrice", 0)),
            index_price=0,
            initial_margin=float(data.get("positionIM", 0)),
            maintenance_margin=float(data.get("positionMM", 0)),
            delta=size if direction == "buy" else -size,
            gamma=0,
            vega=0,
            theta=0,
            total_profit_loss=float(data.get("cumRealisedPnl", 0)),
            floating_profit_loss=float(data.get("unrealisedPnl", 0)),
            realized_profit_loss=float(data.get("cumRealisedPnl", 0)),
            timestamp=datetime.fromtimestamp(int(created_ms) / 1000) if created_ms != "0" else datetime.utcnow(),
        )
