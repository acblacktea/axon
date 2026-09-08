"""OKX EMS implementation for perpetual swaps using REST API."""

import base64
import hashlib
import hmac
import time
from datetime import datetime, timezone
from typing import Any

from axon_order_execution.config import ExchangeConfig
from axon_order_execution.ems.base import BaseEMS
from axon_order_execution.models import Fill, Liquidity, Order, OrderRequest, OrderSide, OrderStatus, OrderType, Ticker
from axon_order_execution.models.portfolio import AccountSummary, Position
from axon_order_execution.util import AsyncHttpClient
from axon_order_execution.util.logging import get_logger
from axon_order_execution.util.metrics import get_metrics

logger = get_logger(__name__)


class OkxEMS(BaseEMS):
    """OKX execution management system for perpetual swaps."""

    PRODUCTION_URL = "https://www.okx.com"
    # OKX uses the same base URL for testnet, distinguished by header
    TESTNET_URL = "https://www.okx.com"

    def __init__(self, config: ExchangeConfig):
        self._config = config
        base_url = self.TESTNET_URL if config.is_testnet else self.PRODUCTION_URL
        self._http = AsyncHttpClient(base_url)

    @property
    def exchange_name(self) -> str:
        return "okx"

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        await self._http.close()

    def _sign(self, timestamp: str, method: str, path: str, body: str = "") -> str:
        """Generate OKX HMAC-SHA256 + Base64 signature."""
        pre_sign = f"{timestamp}{method}{path}{body}"
        mac = hmac.new(
            self._config.api_secret.encode(), pre_sign.encode(), hashlib.sha256
        )
        return base64.b64encode(mac.digest()).decode()

    def _auth_headers(self, method: str, path: str, body: str = "") -> dict[str, str]:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        signature = self._sign(timestamp, method, path, body)
        headers = {
            "OK-ACCESS-KEY": self._config.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self._config.passphrase,
            "Content-Type": "application/json",
        }
        if self._config.is_testnet:
            headers["x-simulated-trading"] = "1"
        return headers

    async def _get_request(self, path: str, params: dict[str, Any] | None = None) -> Any:
        metrics = get_metrics()
        start = time.monotonic()
        query = ""
        if params:
            query = "?" + "&".join(f"{k}={v}" for k, v in params.items())
        headers = self._auth_headers("GET", path + query)
        try:
            async with await self._http.get(path, params=params, headers=headers) as resp:
                data = await resp.json()
            metrics.observe_ems_request("okx", path, time.monotonic() - start)
            if data.get("code") != "0":
                raise Exception(f"OKX API error: {data.get('msg')} (code={data.get('code')})")
            return data.get("data", [])
        except Exception as e:
            metrics.observe_ems_request("okx", path, time.monotonic() - start)
            metrics.inc_ems_request_error("okx", path, type(e).__name__)
            raise

    async def _post_request(self, path: str, body: dict[str, Any]) -> Any:
        import json as _json
        metrics = get_metrics()
        start = time.monotonic()
        body_str = _json.dumps(body)
        headers = self._auth_headers("POST", path, body_str)
        try:
            async with await self._http.post(path, json=body, headers=headers) as resp:
                data = await resp.json()
            metrics.observe_ems_request("okx", path, time.monotonic() - start)
            if data.get("code") != "0":
                raise Exception(f"OKX API error: {data.get('msg')} (code={data.get('code')})")
            return data.get("data", [])
        except Exception as e:
            metrics.observe_ems_request("okx", path, time.monotonic() - start)
            metrics.inc_ems_request_error("okx", path, type(e).__name__)
            raise

    async def place_order(self, request: OrderRequest) -> Order:
        body: dict[str, Any] = {
            "instId": request.instrument,
            "tdMode": "cross",
            "side": "buy" if request.side == OrderSide.BUY else "sell",
            "ordType": "limit" if request.order_type == OrderType.LIMIT else "market",
            "sz": str(request.amount),
        }
        if request.order_type == OrderType.LIMIT:
            body["px"] = str(request.price)
        if request.post_only:
            body["ordType"] = "post_only"
        if request.label:
            body["clOrdId"] = request.label

        logger.info(f"Placing OKX order: {body}")
        result = await self._post_request("/api/v5/trade/order", body)

        if not result:
            raise Exception("OKX place_order returned empty result")
        order_id = result[0].get("ordId", "")
        if result[0].get("sCode") != "0":
            raise Exception(f"OKX order failed: {result[0].get('sMsg')}")

        order = await self.get_order(order_id)
        if order is None:
            order = Order(
                order_id=order_id,
                exchange="okx",
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
            # We need instId for OKX cancel. Try to find from order.
            order = await self.get_order(order_id)
            inst_id = order.instrument if order else ""
            await self._post_request("/api/v5/trade/cancel-order", {
                "instId": inst_id,
                "ordId": order_id,
            })
            return True
        except Exception as e:
            logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    async def modify_order(
        self, order_id: str, amount: float | None = None, price: float | None = None,
    ) -> Order:
        # Need instId for amend
        existing = await self.get_order(order_id)
        if existing is None:
            raise Exception(f"Order {order_id} not found")
        body: dict[str, Any] = {"instId": existing.instrument, "ordId": order_id}
        if amount is not None:
            body["newSz"] = str(amount)
        if price is not None:
            body["newPx"] = str(price)
        logger.info(f"Modifying OKX order: {body}")
        await self._post_request("/api/v5/trade/amend-order", body)
        order = await self.get_order(order_id)
        if order is None:
            raise Exception(f"Order {order_id} not found after modify")
        return order

    async def get_order(self, order_id: str) -> Order | None:
        try:
            result = await self._get_request("/api/v5/trade/order", {
                "instId": "",  # OKX requires instId but accepts empty for ordId lookup
                "ordId": order_id,
            })
            if not result:
                return None
            return self._parse_order(result[0])
        except Exception as e:
            logger.error(f"Failed to get order {order_id}: {e}")
            return None

    async def get_open_orders(self, instrument: str | None = None) -> list[Order]:
        params: dict[str, Any] = {"instType": "SWAP"}
        if instrument:
            params["instId"] = instrument
        try:
            result = await self._get_request("/api/v5/trade/orders-pending", params)
            return [self._parse_order(o) for o in result]
        except Exception as e:
            logger.error(f"Failed to get open orders: {e}")
            return []

    async def get_ticker(self, instrument: str) -> Ticker:
        result = await self._get_request("/api/v5/market/ticker", {"instId": instrument})
        data = result[0] if result else {}
        return Ticker(
            instrument=instrument,
            best_bid_price=float(data.get("bidPx", 0)),
            best_bid_amount=float(data.get("bidSz", 0)),
            best_ask_price=float(data.get("askPx", 0)),
            best_ask_amount=float(data.get("askSz", 0)),
            last_price=float(data.get("last", 0)),
        )

    async def get_account_summary(self, currency: str = "USDT") -> AccountSummary:
        result = await self._get_request("/api/v5/account/balance", {"ccy": currency})
        acct = result[0] if result else {}
        detail: dict[str, Any] = {}
        for d in acct.get("details", []):
            if d.get("ccy") == currency:
                detail = d
                break
        return AccountSummary(
            currency=currency,
            exchange="okx",
            equity=float(detail.get("eq", acct.get("totalEq", 0))),
            balance=float(detail.get("cashBal", 0)),
            available_funds=float(detail.get("availBal", 0)),
            initial_margin=float(acct.get("imr", 0)),
            maintenance_margin=float(acct.get("mmr", 0)),
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

    async def get_positions(self, currency: str = "USDT", kind: str = "SWAP") -> list[Position]:
        result = await self._get_request("/api/v5/account/positions", {"instType": kind})
        return [self._parse_position(p) for p in result if float(p.get("pos", 0)) != 0]

    async def get_user_trades_since(
        self, currency: str, since_ms: int, kind: str = "any",
    ) -> list[Fill]:
        fills: list[Fill] = []
        after = ""
        for _ in range(100):
            params: dict[str, Any] = {
                "instType": "SWAP",
                "begin": str(since_ms),
                "end": str(int(time.time() * 1000) + 1000),
                "limit": "100",
            }
            if after:
                params["after"] = after
            try:
                result = await self._get_request("/api/v5/trade/fills-history", params)
            except Exception as e:
                logger.error(f"get_user_trades_since failed: {e}")
                break
            if not result:
                break
            for t in result:
                try:
                    fills.append(self._parse_fill(t))
                except Exception as e:
                    logger.error(f"Failed to parse trade: {e}, raw: {t}")
            after = result[-1].get("billId", "")
            if len(result) < 100:
                break
        return fills

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

        fill_sz = float(data.get("accFillSz", 0))
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
            filled_amount=fill_sz,
            average_price=avg_px,
            status=status,
            label=data.get("clOrdId") or None,
            post_only=ord_type == "post_only",
            created_at=datetime.fromtimestamp(int(created_ms) / 1000) if created_ms != "0" else datetime.utcnow(),
            updated_at=datetime.fromtimestamp(int(updated_ms) / 1000) if updated_ms != "0" else datetime.utcnow(),
        )

    def _parse_fill(self, data: dict[str, Any]) -> Fill:
        ts = data.get("ts", "0")
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

    def _parse_position(self, data: dict[str, Any]) -> Position:
        pos = float(data.get("pos", 0))
        pos_side = data.get("posSide", "")
        if pos > 0 or pos_side == "long":
            direction = "buy"
        elif pos < 0 or pos_side == "short":
            direction = "sell"
        else:
            direction = "zero"
        created_ms = data.get("cTime", "0")
        return Position(
            instrument=data.get("instId", ""),
            exchange="okx",
            kind="future",
            direction=direction,
            size=abs(pos),
            average_price=float(data.get("avgPx", 0)),
            mark_price=float(data.get("markPx", 0)),
            index_price=0,
            initial_margin=float(data.get("imr", 0)),
            maintenance_margin=float(data.get("mmr", 0)),
            delta=abs(pos) if direction == "buy" else -abs(pos),
            gamma=0,
            vega=0,
            theta=0,
            total_profit_loss=float(data.get("upl", 0)) + float(data.get("realizedPnl", 0)),
            floating_profit_loss=float(data.get("upl", 0)),
            realized_profit_loss=float(data.get("realizedPnl", 0)),
            timestamp=datetime.fromtimestamp(int(created_ms) / 1000) if created_ms != "0" else datetime.utcnow(),
        )
