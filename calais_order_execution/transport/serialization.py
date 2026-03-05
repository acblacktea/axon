"""JSON serialization for protocol messages and domain objects."""

import json
from datetime import datetime
from typing import Any, Optional

from calais_order_execution.models.order import (
    Liquidity, Order, OrderRequest, OrderSide, OrderStatus, OrderType, Ticker,
)
from calais_order_execution.models.messages import Command, Response, Event
from calais_order_execution.models.portfolio import AccountSummary, Position


# ============= Order Serialization =============

def serialize_order(order: Order) -> dict[str, Any]:
    """Serialize an Order to a JSON-safe dict."""
    return {
        "order_id": order.order_id,
        "exchange": order.exchange,
        "instrument": order.instrument,
        "side": order.side.value,
        "order_type": order.order_type.value,
        "amount": order.amount,
        "status": order.status.value,
        "internal_order_id": order.internal_order_id,
        "price": order.price,
        "filled_amount": order.filled_amount,
        "average_price": order.average_price,
        "client_order_id": order.client_order_id,
        "label": order.label,
        "liquidity": order.liquidity.value,
        "post_only": order.post_only,
        "reject_post_only": order.reject_post_only,
        "strategy_id": order.strategy_id,
        "created_at": order.created_at.isoformat(),
        "updated_at": order.updated_at.isoformat(),
    }


def deserialize_order(data: dict[str, Any]) -> Order:
    """Deserialize a dict into an Order."""
    return Order(
        order_id=data["order_id"],
        exchange=data["exchange"],
        instrument=data["instrument"],
        side=OrderSide(data["side"]),
        order_type=OrderType(data["order_type"]),
        amount=data["amount"],
        status=OrderStatus(data["status"]),
        internal_order_id=data.get("internal_order_id"),
        price=data.get("price"),
        filled_amount=data.get("filled_amount", 0.0),
        average_price=data.get("average_price"),
        client_order_id=data.get("client_order_id"),
        label=data.get("label"),
        liquidity=Liquidity(data["liquidity"]) if data.get("liquidity") else Liquidity.MAKER,
        post_only=data.get("post_only", False),
        reject_post_only=data.get("reject_post_only", False),
        strategy_id=data.get("strategy_id"),
        created_at=datetime.fromisoformat(data["created_at"]),
        updated_at=datetime.fromisoformat(data["updated_at"]),
    )


# ============= OrderRequest Serialization =============

def serialize_order_request(req: OrderRequest) -> dict[str, Any]:
    """Serialize an OrderRequest to a JSON-safe dict."""
    return {
        "instrument": req.instrument,
        "side": req.side.value,
        "amount": req.amount,
        "order_type": req.order_type.value,
        "price": req.price,
        "client_order_id": req.client_order_id,
        "label": req.label,
        "post_only": req.post_only,
        "reject_post_only": req.reject_post_only,
        "internal_order_id": req.internal_order_id,
        "strategy_id": req.strategy_id,
    }


def deserialize_order_request(data: dict[str, Any]) -> OrderRequest:
    """Deserialize a dict into an OrderRequest."""
    return OrderRequest(
        instrument=data["instrument"],
        side=OrderSide(data["side"]),
        amount=data["amount"],
        order_type=OrderType(data.get("order_type", "limit")),
        price=data.get("price"),
        client_order_id=data.get("client_order_id"),
        label=data.get("label"),
        post_only=data.get("post_only", False),
        reject_post_only=data.get("reject_post_only", False),
        internal_order_id=data.get("internal_order_id", ""),
        strategy_id=data.get("strategy_id"),
    )


# ============= Ticker Serialization =============

def serialize_ticker(ticker: Ticker) -> dict[str, Any]:
    """Serialize a Ticker to a JSON-safe dict."""
    return {
        "instrument": ticker.instrument,
        "best_bid_price": ticker.best_bid_price,
        "best_bid_amount": ticker.best_bid_amount,
        "best_ask_price": ticker.best_ask_price,
        "best_ask_amount": ticker.best_ask_amount,
        "last_price": ticker.last_price,
        "mark_price": ticker.mark_price,
        "timestamp": ticker.timestamp.isoformat() if ticker.timestamp else None,
    }


def deserialize_ticker(data: dict[str, Any]) -> Ticker:
    """Deserialize a dict into a Ticker."""
    return Ticker(
        instrument=data["instrument"],
        best_bid_price=data["best_bid_price"],
        best_bid_amount=data["best_bid_amount"],
        best_ask_price=data["best_ask_price"],
        best_ask_amount=data["best_ask_amount"],
        last_price=data.get("last_price"),
        mark_price=data.get("mark_price"),
        timestamp=datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else None,
    )


# ============= AccountSummary Serialization =============

def serialize_account_summary(summary: AccountSummary) -> dict[str, Any]:
    """Serialize an AccountSummary to a JSON-safe dict."""
    return {
        "currency": summary.currency,
        "equity": summary.equity,
        "balance": summary.balance,
        "available_funds": summary.available_funds,
        "initial_margin": summary.initial_margin,
        "maintenance_margin": summary.maintenance_margin,
        "margin_balance": summary.margin_balance,
        "delta_total": summary.delta_total,
        "options_delta": summary.options_delta,
        "options_gamma": summary.options_gamma,
        "options_vega": summary.options_vega,
        "options_theta": summary.options_theta,
        "futures_pl": summary.futures_pl,
        "options_pl": summary.options_pl,
        "total_pl": summary.total_pl,
        "timestamp": summary.timestamp.isoformat(),
    }


def deserialize_account_summary(data: dict[str, Any]) -> AccountSummary:
    """Deserialize a dict into an AccountSummary."""
    return AccountSummary(
        currency=data["currency"],
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
        timestamp=datetime.fromisoformat(data["timestamp"]),
    )


# ============= Position Serialization =============

def serialize_position(pos: Position) -> dict[str, Any]:
    """Serialize a Position to a JSON-safe dict."""
    return {
        "instrument": pos.instrument,
        "exchange": pos.exchange,
        "kind": pos.kind,
        "direction": pos.direction,
        "size": pos.size,
        "average_price": pos.average_price,
        "mark_price": pos.mark_price,
        "index_price": pos.index_price,
        "initial_margin": pos.initial_margin,
        "maintenance_margin": pos.maintenance_margin,
        "delta": pos.delta,
        "gamma": pos.gamma,
        "vega": pos.vega,
        "theta": pos.theta,
        "total_profit_loss": pos.total_profit_loss,
        "floating_profit_loss": pos.floating_profit_loss,
        "realized_profit_loss": pos.realized_profit_loss,
        "timestamp": pos.timestamp.isoformat(),
    }


def deserialize_position(data: dict[str, Any]) -> Position:
    """Deserialize a dict into a Position."""
    return Position(
        instrument=data["instrument"],
        exchange=data.get("exchange", ""),
        kind=data.get("kind", "option"),
        direction=data.get("direction", "zero"),
        size=data.get("size", 0),
        average_price=data.get("average_price", 0),
        mark_price=data.get("mark_price", 0),
        index_price=data.get("index_price", 0),
        initial_margin=data.get("initial_margin", 0),
        maintenance_margin=data.get("maintenance_margin", 0),
        delta=data.get("delta", 0),
        gamma=data.get("gamma", 0),
        vega=data.get("vega", 0),
        theta=data.get("theta", 0),
        total_profit_loss=data.get("total_profit_loss", 0),
        floating_profit_loss=data.get("floating_profit_loss", 0),
        realized_profit_loss=data.get("realized_profit_loss", 0),
        timestamp=datetime.fromisoformat(data["timestamp"]),
    )


# ============= Protocol Message Serialization =============

def serialize_command(cmd: Command) -> bytes:
    """Serialize a Command to JSON bytes for ZMQ."""
    return json.dumps({
        "command_type": cmd.command_type,
        "payload": cmd.payload,
        "request_id": cmd.request_id,
        "strategy_id": cmd.strategy_id,
    }).encode("utf-8")


def deserialize_command(data: bytes) -> Command:
    """Deserialize JSON bytes into a Command."""
    d = json.loads(data)
    return Command(
        command_type=d["command_type"],
        payload=d["payload"],
        request_id=d["request_id"],
        strategy_id=d.get("strategy_id", ""),
    )


def serialize_response(resp: Response) -> bytes:
    """Serialize a Response to JSON bytes for ZMQ."""
    return json.dumps({
        "request_id": resp.request_id,
        "success": resp.success,
        "data": resp.data,
        "error": resp.error,
    }).encode("utf-8")


def deserialize_response(data: bytes) -> Response:
    """Deserialize JSON bytes into a Response."""
    d = json.loads(data)
    return Response(
        request_id=d["request_id"],
        success=d["success"],
        data=d.get("data"),
        error=d.get("error"),
    )


def serialize_event(event: Event) -> bytes:
    """Serialize an Event to JSON bytes for ZMQ."""
    return json.dumps({
        "event_type": event.event_type,
        "data": event.data,
        "strategy_id": event.strategy_id,
    }).encode("utf-8")


def deserialize_event(data: bytes) -> Event:
    """Deserialize JSON bytes into an Event."""
    d = json.loads(data)
    return Event(
        event_type=d["event_type"],
        data=d["data"],
        strategy_id=d.get("strategy_id", ""),
    )
