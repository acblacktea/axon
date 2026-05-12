"""Generic serialization for dataclasses over ZMQ.

Uses dataclasses.fields() + type annotations to auto-serialize/deserialize,
eliminating per-class boilerplate.
"""

import dataclasses
import json
from datetime import datetime
from enum import Enum
from typing import Any, Type, TypeVar, get_type_hints

from calais_order_execution.models.order import Order, OrderRequest, Ticker
from calais_order_execution.models.messages import Command, Response, Event
from calais_order_execution.models.portfolio import AccountSummary, Position
from calais_order_execution.models.fill import Fill

T = TypeVar("T")

# ============= Generic Serialization =============


def serialize(obj: Any) -> dict[str, Any]:
    """Serialize any dataclass to a JSON-safe dict.

    Handles Enum → .value, datetime → .isoformat() automatically.
    """
    result = {}
    for f in dataclasses.fields(obj):
        value = getattr(obj, f.name)
        if isinstance(value, Enum):
            value = value.value
        elif isinstance(value, datetime):
            value = value.isoformat()
        result[f.name] = value
    return result


def _resolve_type(hint: Any) -> type | None:
    """Extract the concrete type from Optional[X] / X | None hints."""
    origin = getattr(hint, "__origin__", None)
    # typing.Optional[X] == Union[X, None]
    if origin is type(int | str):  # types.UnionType (PEP 604: X | Y)
        args = [a for a in hint.__args__ if a is not type(None)]
        return args[0] if args else None
    # typing.Union
    if origin is not None:
        import typing
        if origin is typing.Union:
            args = [a for a in hint.__args__ if a is not type(None)]
            return args[0] if args else None
    return hint if isinstance(hint, type) else None


def deserialize(cls: Type[T], data: dict[str, Any]) -> T:
    """Deserialize a dict into a dataclass instance.

    Handles str → Enum, str → datetime automatically based on type hints.
    """
    hints = get_type_hints(cls)
    kwargs = {}
    for f in dataclasses.fields(cls):
        if f.name not in data:
            continue
        value = data[f.name]
        if value is None:
            kwargs[f.name] = None
            continue

        target_type = _resolve_type(hints[f.name])
        if target_type is not None and isinstance(target_type, type):
            if issubclass(target_type, Enum):
                value = target_type(value)
            elif issubclass(target_type, datetime) and isinstance(value, str):
                value = datetime.fromisoformat(value)

        kwargs[f.name] = value
    return cls(**kwargs)


# ============= Convenience aliases (keep call sites clean) =============

serialize_order = serialize
serialize_order_request = serialize
serialize_ticker = serialize
serialize_account_summary = serialize
serialize_position = serialize
serialize_fill = serialize


def deserialize_order(data: dict[str, Any]) -> Order:
    return deserialize(Order, data)


def deserialize_order_request(data: dict[str, Any]) -> OrderRequest:
    return deserialize(OrderRequest, data)


def deserialize_ticker(data: dict[str, Any]) -> Ticker:
    return deserialize(Ticker, data)


def deserialize_account_summary(data: dict[str, Any]) -> AccountSummary:
    return deserialize(AccountSummary, data)


def deserialize_position(data: dict[str, Any]) -> Position:
    return deserialize(Position, data)


def deserialize_fill(data: dict[str, Any]) -> Fill:
    return deserialize(Fill, data)


# ============= Protocol Messages (bytes for ZMQ) =============

def serialize_command(cmd: Command) -> bytes:
    return json.dumps(serialize(cmd)).encode("utf-8")


def deserialize_command(data: bytes) -> Command:
    return deserialize(Command, json.loads(data))


def serialize_response(resp: Response) -> bytes:
    return json.dumps(serialize(resp)).encode("utf-8")


def deserialize_response(data: bytes) -> Response:
    return deserialize(Response, json.loads(data))


def serialize_event(event: Event) -> bytes:
    return json.dumps(serialize(event)).encode("utf-8")


def deserialize_event(data: bytes) -> Event:
    return deserialize(Event, json.loads(data))
