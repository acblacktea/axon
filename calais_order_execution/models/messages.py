"""ZMQ protocol message definitions."""

import uuid
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class Command:
    """Command sent from strategy client to engine."""

    command_type: str
    payload: dict[str, Any]
    request_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    strategy_id: str = ""


@dataclass
class Response:
    """Response sent from engine to strategy client."""

    request_id: str
    success: bool
    data: Any = None
    error: Optional[str] = None


@dataclass
class Event:
    """Event published from engine to strategy clients."""

    event_type: str
    data: dict[str, Any]
    strategy_id: str = ""
