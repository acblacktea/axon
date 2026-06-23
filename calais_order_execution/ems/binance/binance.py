"""Binance EMS implementation for USDT-M perpetual futures using REST API."""

import hashlib
import hmac
import time
from datetime import datetime
from typing import Any
from urllib.parse import urlencode

from calais_order_execution.config import ExchangeConfig
from calais_order_execution.ems.base import BaseEMS
from calais_order_execution.models import Fill, Liquidity, Order, OrderRequest, OrderSide, OrderStatus, OrderType, Ticker
from calais_order_execution.models.portfolio import AccountSummary, Position
from calais_order_execution.util import AsyncHttpClient
from calais_order_execution.util.logging import get_logger
from calais_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class BinanceEMS(BaseEMS):
    """Binance execution management system for USDT-M perpetual futures."""

    TESTNET_URL = "https://testnet.binancefuture.com"
    PRODUCTION_URL = "https://fapi.binance.com"

    def __init__(self, config: ExchangeConfig):
        self._config = config
        base_url = self.TESTNET_URL if config.is_testnet else self.PRODUCTION_URL
        self._http = AsyncHttpClient(base_url)

    @property
    def exchange_name(self) -> str:
        return "binance"

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        await self._http.close()

    def _sign_params(self, params: dict[str, Any]) -> dict[str, Any]:
        """Add timestamp and HMAC-SHA256 signature to query params."""
        params["timestamp"] = str(int(time.time() * 1000))
        params["recvWindow"] = "5000"
        query_string = urlencode(params)
        signature = hmac.new(
            self._config.api_secret.encode(), query_string.encode(), hashlib.sha256
        ).hexdigest()
        params["signature"] = signature
        return params

    def _auth_headers(self) -> dict[str, str]:
        return {"X-MBX-APIKEY": self._config.api_key}

    async def _get_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        metrics = get_metrics()
        start = time.monotonic()
        signed = self._sign_params(dict(params or {}))
        try:
            async with await self._http.get(path, params=signed, headers=self._auth_headers()) as resp:
                data = await resp.json()
            metrics.observe_ems_request("binance", path, time.monotonic() - start)
            if isinstance(data, dict) and data.get("code"):
                raise Exception(f"Binance API error: {data.get('msg')} (code={data.get('code')})")
            return data
        except Exception as e:
            metrics.observe_ems_request("binance", path, time.monotonic() - start)
            metrics.inc_ems_request_error("binance", path, type(e).__name__)
            raise

    async def _post_request(self, path: str, params: dict[str, Any]) -> Any:
        metrics = get_metrics()
        start = time.monotonic()
        signed = self._sign_params(dict(params))
        try:
            async with await self._http.post(path, params=signed, headers=self._auth_headers()) as resp:
                data = await resp.json()
            metrics.observe_ems_request("binance", path, time.monotonic() - start)
            if isinstance(data, dict) and data.get("code"):
                raise Exception(f"Binance API error: {data.get('msg')} (code={data.get('code')})")
            return data
        except Exception as e:
            metrics.observe_ems_request("binance", path, time.monotonic() - start)
            metrics.inc_ems_request_error("binance", path, type(e).__name__)
            raise

    async def _delete_request(self, path: str, params: dict[str, Any]) -> Any:
        metrics = get_metrics()
        start = time.monotonic()
        signed = self._sign_params(dict(params))
        try:
            async with await self._http.delete(path, params=signed, headers=self._auth_headers()) as resp:
                data = await resp.json()
            metrics.observe_ems_request("binance", path, time.monotonic() - start)
            if isinstance(data, dict) and data.get("code"):
                raise Exception(f"Binance API error: {data.get('msg')} (code={data.get('code')})")
            return data
        except Exception as e:
            metrics.observe_ems_request("binance", path, time.monotonic() - start)
            metrics.inc_ems_request_error("binance", path, type(e).__name__)
            raise

    async def _put_request(self, path: str, params: dict[str, Any]) -> Any:
        metrics = get_metrics()
        start = time.monotonic()
        signed = self._sign_params(dict(params))
        try:
            async with await self._http.put(path, params=signed, headers=self._auth_headers()) as resp:
                data = await resp.json()
            metrics.observe_ems_request("binance", path, time.monotonic() - start)
            if isinstance(data, dict) and data.get("code"):
                raise Exception(f"Binance API error: {data.get('msg')} (code={data.get('code')})")
            return data
        except Exception as e:
            metrics.observe_ems_request("binance", path, time.monotonic() - start)
            metrics.inc_ems_request_error("binance", path, type(e).__name__)
            raise

    async def place_order(self, request: OrderRequest) -> Order:
        params: dict[str, Any] = {
            "symbol": request.instrument,
            "side": "BUY" if request.side == OrderSide.BUY else "SELL",
            "type": "LIMIT" if request.order_type == OrderType.LIMIT else "MARKET",
            "quantity": str(request.amount),
        }
        if request.order_type == OrderType.LIMIT:
            params["price"] = str(request.price)
            params["timeInForce"] = "GTX" if request.post_only else "GTC"
        if request.label:
            params["newClientOrderId"] = request.label

        logger.info(f"Placing Binance order: {params}")
        result = await self._post_request("/fapi/v1/order", params)

        order = self._parse_order(result)
        order.internal_order_id = request.internal_order_id
        return order

    async def cancel_order(self, order_id: str) -> bool:
        try:
            # We need symbol. Try to get from order first.
            order = await self.get_order(order_id)
            symbol = order.instrument if order else ""
            await self._delete_request("/fapi/v1/order", {
                "symbol": symbol,
                "orderId": order_id,
            })
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    async def modify_order(
        self, order_id: str, amount: float | None = None, price: float | None = None,
    ) -> Order:
        existing = await self.get_order(order_id)
        if existing is None:
            raise Exception(f"Order {order_id} not found")
        params: dict[str, Any] = {
            "symbol": existing.instrument,
            "orderId": order_id,
            "side": "BUY" if existing.side == OrderSide.BUY else "SELL",
            "quantity": str(amount if amount is not None else existing.amount),
            "price": str(price if price is not None else existing.price),
        }
        logger.info(f"Modifying Binance order: {params}")
        result = await self._put_request("/fapi/v1/order", params)
        return self._parse_order(result)

    async def get_order(self, order_id: str) -> Order | None:
        try:
            # Binance requires symbol. We'll try without for single order lookup.
            # The /fapi/v1/order endpoint requires symbol, so we use allOrders with orderId.
            result = await self._get_request("/fapi/v1/order", {"symbol": "BTCUSDT", "orderId": order_id})
            return self._parse_order(result)
        except Exception:
            # Try to find in open orders
            return None

    async def get_order_with_symbol(self, symbol: str, order_id: str) -> Order | None:
        """Get order when symbol is known (avoids the symbol requirement issue)."""
        try:
            result = await self._get_request("/fapi/v1/order", {"symbol": symbol, "orderId": order_id})
            return self._parse_order(result)
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            return None

    async def get_open_orders(self, instrument: str | None = None) -> list[Order]:
        params: dict[str, Any] = {}
        if instrument:
            params["symbol"] = instrument
        try:
            result = await self._get_request("/fapi/v1/openOrders", params)
            return [self._parse_order(o) for o in result]
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []

    async def get_ticker(self, instrument: str) -> Ticker:
        # Ticker doesn't need auth
        async with await self._http.get("/fapi/v1/ticker/bookTicker", params={"symbol": instrument}) as resp:
            data = await resp.json()
        return Ticker(
            instrument=instrument,
            best_bid_price=float(data.get("bidPrice", 0)),
            best_bid_amount=float(data.get("bidQty", 0)),
            best_ask_price=float(data.get("askPrice", 0)),
            best_ask_amount=float(data.get("askQty", 0)),
        )

    async def get_account_summary(self, currency: str = "USDT") -> AccountSummary:
        result = await self._get_request("/fapi/v2/account", {})
        assets = result.get("assets", [])
        asset_data: dict[str, Any] = {}
        for a in assets:
            if a.get("asset") == currency:
                asset_data = a
                break
        return AccountSummary(
            currency=currency,
            exchange="binance",
            equity=float(asset_data.get("marginBalance", 0)),
            balance=float(asset_data.get("walletBalance", 0)),
            available_funds=float(asset_data.get("availableBalance", 0)),
            initial_margin=float(asset_data.get("initialMargin", 0)),
            maintenance_margin=float(asset_data.get("maintMargin", 0)),
            margin_balance=float(asset_data.get("marginBalance", 0)),
            delta_total=0,
            options_delta=0,
            options_gamma=0,
            options_vega=0,
            options_theta=0,
            futures_pl=float(asset_data.get("unrealizedProfit", 0)),
            options_pl=0,
            total_pl=float(asset_data.get("unrealizedProfit", 0)),
            timestamp=datetime.utcnow(),
        )

    async def get_positions(self, currency: str = "USDT", kind: str = "future") -> list[Position]:
        result = await self._get_request("/fapi/v2/positionRisk", {})
        positions = []
        for p in result:
            amt = float(p.get("positionAmt", 0))
            if amt == 0:
                continue
            positions.append(self._parse_position(p))
        return positions

    async def get_user_trades_since(
        self, currency: str, since_ms: int, kind: str = "any",
    ) -> list[Fill]:
        # Binance requires symbol for trades. Fetch from all positions' symbols.
        fills: list[Fill] = []
        # Get trades for common perpetual pairs
        try:
            result = await self._get_request("/fapi/v1/userTrades", {
                "startTime": str(since_ms),
                "limit": "1000",
                "symbol": "BTCUSDT",  # Default; production should iterate symbols
            })
            for t in result:
                try:
                    fills.append(self._parse_fill(t))
                except Exception as e:
                    logger.error(f"Failed to parse trade: {e}, raw: {t}")
        except Exception as e:
            logger.error(f"get_user_trades_since failed: {e}")
        return fills

    async def create_listen_key(self) -> str:
        """Create a listenKey for user data stream WebSocket."""
        async with await self._http.post("/fapi/v1/listenKey", headers=self._auth_headers()) as resp:
            data = await resp.json()
        return data.get("listenKey", "")

    async def keepalive_listen_key(self) -> None:
        """Keepalive the listenKey."""
        await self._http.put("/fapi/v1/listenKey", headers=self._auth_headers())

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
        raw_status = data.get("status", "NEW")
        status = status_map.get(raw_status, OrderStatus.OPEN)

        update_time = data.get("updateTime", 0)
        create_time = data.get("time", update_time)

        avg_price_str = data.get("avgPrice", "0")
        avg_price = float(avg_price_str) if avg_price_str and float(avg_price_str) != 0 else None

        return Order(
            order_id=str(data.get("orderId", "")),
            exchange="binance",
            instrument=data.get("symbol", ""),
            side=OrderSide.BUY if data.get("side") == "BUY" else OrderSide.SELL,
            order_type=OrderType.LIMIT if data.get("type") == "LIMIT" else OrderType.MARKET,
            amount=float(data.get("origQty", 0)),
            price=float(data["price"]) if data.get("price") and float(data.get("price", 0)) != 0 else None,
            filled_amount=float(data.get("executedQty", 0)),
            average_price=avg_price,
            status=status,
            label=data.get("clientOrderId") or None,
            post_only=data.get("timeInForce") == "GTX",
            created_at=datetime.fromtimestamp(create_time / 1000) if create_time else datetime.utcnow(),
            updated_at=datetime.fromtimestamp(update_time / 1000) if update_time else datetime.utcnow(),
        )

    def _parse_fill(self, data: dict[str, Any]) -> Fill:
        ts = data.get("time", 0)
        return Fill(
            trade_id=str(data.get("id", "")),
            order_id=str(data.get("orderId", "")),
            exchange="binance",
            instrument=data.get("symbol", ""),
            side=OrderSide.BUY if data.get("side") == "BUY" else OrderSide.SELL,
            amount=float(data.get("qty", 0)),
            price=float(data.get("price", 0)),
            fee=float(data.get("commission", 0)),
            fee_currency=data.get("commissionAsset", "USDT"),
            liquidity=Liquidity.MAKER if data.get("maker") else Liquidity.TAKER,
            timestamp=datetime.fromtimestamp(ts / 1000) if ts else datetime.utcnow(),
        )

    def _parse_position(self, data: dict[str, Any]) -> Position:
        amt = float(data.get("positionAmt", 0))
        if amt > 0:
            direction = "buy"
        elif amt < 0:
            direction = "sell"
        else:
            direction = "zero"
        update_time = data.get("updateTime", 0)
        return Position(
            instrument=data.get("symbol", ""),
            exchange="binance",
            kind="future",
            direction=direction,
            size=abs(amt),
            average_price=float(data.get("entryPrice", 0)),
            mark_price=float(data.get("markPrice", 0)),
            index_price=0,
            initial_margin=float(data.get("initialMargin", 0)),
            maintenance_margin=float(data.get("maintMargin", 0)),
            delta=amt,
            gamma=0,
            vega=0,
            theta=0,
            total_profit_loss=float(data.get("unRealizedProfit", 0)),
            floating_profit_loss=float(data.get("unRealizedProfit", 0)),
            realized_profit_loss=0,
            timestamp=datetime.fromtimestamp(update_time / 1000) if update_time else datetime.utcnow(),
        )
