"""Portfolio data models - account summary and positions."""

from dataclasses import dataclass
from datetime import datetime


@dataclass
class AccountSummary:
    """Account summary from exchange (balance, margin, Greeks)."""

    currency: str
    equity: float
    balance: float
    available_funds: float
    initial_margin: float
    maintenance_margin: float
    margin_balance: float
    delta_total: float
    options_delta: float
    options_gamma: float
    options_vega: float
    options_theta: float
    futures_pl: float
    options_pl: float
    total_pl: float
    timestamp: datetime


@dataclass
class Position:
    """Per-instrument position."""

    instrument: str
    exchange: str
    kind: str  # "option", "future"
    direction: str  # "buy", "sell", "zero"
    size: float
    average_price: float
    mark_price: float
    index_price: float
    initial_margin: float
    maintenance_margin: float
    delta: float
    gamma: float
    vega: float
    theta: float
    total_profit_loss: float
    floating_profit_loss: float
    realized_profit_loss: float
    timestamp: datetime
