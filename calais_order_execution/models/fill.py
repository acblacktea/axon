"""Fill (trade execution) data model."""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from calais_order_execution.models.order import Liquidity, OrderSide


@dataclass
class Fill:
    """A single trade execution against an order.

    Fills are append-only facts from the exchange. Multiple fills can belong
    to one Order (partial fills). `trade_id` is the exchange-assigned unique
    identifier and is used as the idempotency key when the same fill arrives
    via both WebSocket and REST reconciliation.
    """

    trade_id: str
    order_id: str
    exchange: str
    instrument: str
    side: OrderSide
    amount: float
    price: float
    fee: float
    fee_currency: str
    liquidity: Liquidity
    timestamp: datetime
    index_price: Optional[float] = None
    mark_price: Optional[float] = None
    iv: Optional[float] = None
    profit_loss: Optional[float] = None
    label: Optional[str] = None
    strategy_id: Optional[str] = None
