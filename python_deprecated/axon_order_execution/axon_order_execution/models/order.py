"""Order data models."""

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


def _generate_internal_id() -> str:
    """Generate a random internal order ID."""
    return uuid.uuid4().hex


@dataclass
class Ticker:
    """Ticker data with best bid/ask prices."""

    instrument: str
    best_bid_price: float
    best_bid_amount: float
    best_ask_price: float
    best_ask_amount: float
    last_price: Optional[float] = None
    mark_price: Optional[float] = None
    timestamp: Optional[datetime] = None


class OrderStatus(Enum):
    """Order status enum."""

    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    REJECTED = "rejected"


class OrderSide(Enum):
    """Order side enum."""

    BUY = "buy"
    SELL = "sell"


class OrderType(Enum):
    """Order type enum."""

    LIMIT = "limit"
    MARKET = "market"


class Liquidity(Enum):
    """Trade liquidity type enum."""

    MAKER = "maker"
    TAKER = "taker"


@dataclass
class OrderRequest:
    """Request to place an order."""

    instrument: str
    side: OrderSide
    amount: float
    order_type: OrderType = OrderType.LIMIT
    price: Optional[float] = None
    client_order_id: Optional[str] = None
    label: Optional[str] = None
    post_only: bool = False
    reject_post_only: bool = False
    internal_order_id: str = field(default_factory=_generate_internal_id)
    strategy_id: Optional[str] = None

    def __post_init__(self):
        if self.order_type == OrderType.LIMIT and self.price is None:
            raise ValueError("Price is required for limit orders")

@dataclass
class Order:
    """Order data model."""

    order_id: str
    exchange: str
    instrument: str
    side: OrderSide
    order_type: OrderType
    amount: float
    status: OrderStatus
    internal_order_id: Optional[str] = None
    price: Optional[float] = None
    filled_amount: float = 0.0
    average_price: Optional[float] = None
    client_order_id: Optional[str] = None
    label: Optional[str] = None
    liquidity: Liquidity = Liquidity.MAKER
    post_only: bool = False
    reject_post_only: bool = False
    strategy_id: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def remaining_amount(self) -> float:
        """Get remaining amount to be filled."""
        return self.amount - self.filled_amount

    @property
    def is_active(self) -> bool:
        """Check if order is still active."""
        return self.status in (
            OrderStatus.PENDING,
            OrderStatus.OPEN,
            OrderStatus.PARTIALLY_FILLED,
        )

