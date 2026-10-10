"""
Core data models for Market Data Service
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Union
from decimal import Decimal
from enum import Enum
from datetime import datetime
import time


class OrderbookSide(Enum):
    BID = "bid"
    ASK = "ask"


class DataType(Enum):
    """Supported market data types"""
    ORDERBOOK = "orderbook"
    TICKER = "ticker"
    KLINE = "kline"
    FUNDING_RATE = "funding_rate"
    OPEN_INTEREST = "open_interest"
    INDEX_PRICE = "index_price"
    TRADES = "trades"


@dataclass
class PriceLevel:
    """Single price level in orderbook"""
    price: Decimal
    quantity: Decimal
    
    def __post_init__(self):
        if isinstance(self.price, (int, float, str)):
            self.price = Decimal(str(self.price))
        if isinstance(self.quantity, (int, float, str)):
            self.quantity = Decimal(str(self.quantity))


@dataclass
class Orderbook:
    """Orderbook data structure with sorted bids/asks"""
    symbol: str
    exchange: str
    bids: Dict[Decimal, Decimal] = field(default_factory=dict)  # price -> quantity
    asks: Dict[Decimal, Decimal] = field(default_factory=dict)  # price -> quantity
    sequence: Optional[int] = None
    prev_sequence: Optional[int] = None
    timestamp: Optional[int] = None  # exchange timestamp in ms
    local_timestamp: int = field(default_factory=lambda: int(time.time() * 1000))
    is_snapshot: bool = False
    
    def update_bids(self, updates: List[Tuple[Decimal, Decimal]]):
        """Update bid levels. Quantity of 0 means delete."""
        for price, qty in updates:
            price = Decimal(str(price)) if not isinstance(price, Decimal) else price
            qty = Decimal(str(qty)) if not isinstance(qty, Decimal) else qty
            if qty == 0:
                self.bids.pop(price, None)
            else:
                self.bids[price] = qty
    
    def update_asks(self, updates: List[Tuple[Decimal, Decimal]]):
        """Update ask levels. Quantity of 0 means delete."""
        for price, qty in updates:
            price = Decimal(str(price)) if not isinstance(price, Decimal) else price
            qty = Decimal(str(qty)) if not isinstance(qty, Decimal) else qty
            if qty == 0:
                self.asks.pop(price, None)
            else:
                self.asks[price] = qty
    
    def get_sorted_bids(self, depth: int = 20) -> List[PriceLevel]:
        """Get top N bids sorted by price descending"""
        sorted_prices = sorted(self.bids.keys(), reverse=True)[:depth]
        return [PriceLevel(p, self.bids[p]) for p in sorted_prices]
    
    def get_sorted_asks(self, depth: int = 20) -> List[PriceLevel]:
        """Get top N asks sorted by price ascending"""
        sorted_prices = sorted(self.asks.keys())[:depth]
        return [PriceLevel(p, self.asks[p]) for p in sorted_prices]
    
    def get_best_bid(self) -> Optional[PriceLevel]:
        if not self.bids:
            return None
        price = max(self.bids.keys())
        return PriceLevel(price, self.bids[price])
    
    def get_best_ask(self) -> Optional[PriceLevel]:
        if not self.asks:
            return None
        price = min(self.asks.keys())
        return PriceLevel(price, self.asks[price])
    
    def get_mid_price(self) -> Optional[Decimal]:
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return (best_bid.price + best_ask.price) / 2
        return None
    
    def get_spread(self) -> Optional[Decimal]:
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if best_bid and best_ask:
            return best_ask.price - best_bid.price
        return None
    
    def to_snapshot(self, depth: int) -> 'OrderbookSnapshot':
        """Convert to immutable snapshot"""
        return OrderbookSnapshot(
            symbol=self.symbol,
            exchange=self.exchange,
            bids=self.get_sorted_bids(depth),
            asks=self.get_sorted_asks(depth),
            sequence=self.sequence,
            timestamp=self.timestamp,
            local_timestamp=self.local_timestamp
        )
    
    def clear(self):
        """Clear all orderbook data"""
        self.bids.clear()
        self.asks.clear()
        self.sequence = None
        self.prev_sequence = None
        self.timestamp = None


@dataclass
class OrderbookSnapshot:
    """Immutable orderbook snapshot for client consumption"""
    symbol: str
    exchange: str
    bids: List[PriceLevel]
    asks: List[PriceLevel]
    sequence: Optional[int]
    timestamp: Optional[int]
    local_timestamp: int
    
    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "bids": [[str(l.price), str(l.quantity)] for l in self.bids],
            "asks": [[str(l.price), str(l.quantity)] for l in self.asks],
            "sequence": self.sequence,
            "timestamp": self.timestamp,
            "local_timestamp": self.local_timestamp
        }


@dataclass
class Instrument:
    """Instrument/Symbol information"""
    symbol: str
    exchange: str
    base_currency: str
    quote_currency: str
    instrument_type: str  # option, future, spot, etc.
    
    # Option specific fields
    strike: Optional[Decimal] = None
    option_type: Optional[str] = None  # call, put
    expiration: Optional[datetime] = None
    underlying: Optional[str] = None
    
    # Status
    is_active: bool = True
    
    # Additional metadata
    tick_size: Optional[Decimal] = None
    min_trade_amount: Optional[Decimal] = None
    contract_size: Optional[Decimal] = None
    
    def is_expired(self) -> bool:
        if self.expiration is None:
            return False
        return datetime.utcnow() > self.expiration


@dataclass
class TickerData:
    """Ticker/quote data for options"""
    symbol: str
    exchange: str
    timestamp: Optional[int] = None
    local_timestamp: Optional[int] = None
    state: Optional[str] = None  # "open", "closed"

    # Price data
    last_price: Optional[Decimal] = None
    best_bid_price: Optional[Decimal] = None
    best_bid_amount: Optional[Decimal] = None
    best_ask_price: Optional[Decimal] = None
    best_ask_amount: Optional[Decimal] = None
    mark_price: Optional[Decimal] = None
    index_price: Optional[Decimal] = None
    settlement_price: Optional[Decimal] = None
    min_price: Optional[Decimal] = None
    max_price: Optional[Decimal] = None

    # Underlying
    underlying_price: Optional[Decimal] = None
    underlying_index: Optional[str] = None
    estimated_delivery_price: Optional[Decimal] = None

    # Greeks
    delta: Optional[Decimal] = None
    gamma: Optional[Decimal] = None
    vega: Optional[Decimal] = None
    theta: Optional[Decimal] = None
    rho: Optional[Decimal] = None

    # IV
    mark_iv: Optional[Decimal] = None
    bid_iv: Optional[Decimal] = None
    ask_iv: Optional[Decimal] = None

    # Stats
    open_interest: Optional[Decimal] = None
    volume: Optional[Decimal] = None
    volume_usd: Optional[Decimal] = None
    high: Optional[Decimal] = None
    low: Optional[Decimal] = None
    price_change: Optional[Decimal] = None

    # Interest rate
    interest_rate: Optional[Decimal] = None

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "exchange": self.exchange,
            "timestamp": self.timestamp,
            "local_timestamp": self.local_timestamp,
            "state": self.state,
            "last_price": str(self.last_price) if self.last_price else None,
            "best_bid_price": str(self.best_bid_price) if self.best_bid_price else None,
            "best_bid_amount": str(self.best_bid_amount) if self.best_bid_amount else None,
            "best_ask_price": str(self.best_ask_price) if self.best_ask_price else None,
            "best_ask_amount": str(self.best_ask_amount) if self.best_ask_amount else None,
            "mark_price": str(self.mark_price) if self.mark_price else None,
            "index_price": str(self.index_price) if self.index_price else None,
            "settlement_price": str(self.settlement_price) if self.settlement_price else None,
            "underlying_price": str(self.underlying_price) if self.underlying_price else None,
            "underlying_index": self.underlying_index,
            "delta": str(self.delta) if self.delta else None,
            "gamma": str(self.gamma) if self.gamma else None,
            "vega": str(self.vega) if self.vega else None,
            "theta": str(self.theta) if self.theta else None,
            "rho": str(self.rho) if self.rho else None,
            "mark_iv": str(self.mark_iv) if self.mark_iv else None,
            "bid_iv": str(self.bid_iv) if self.bid_iv else None,
            "ask_iv": str(self.ask_iv) if self.ask_iv else None,
            "open_interest": str(self.open_interest) if self.open_interest else None,
            "volume": str(self.volume) if self.volume else None,
            "volume_usd": str(self.volume_usd) if self.volume_usd else None,
        }


@dataclass
class IndexPriceData:
    """Index price data (e.g., btc_usd, eth_usd)"""
    index_name: str
    exchange: str
    price: Decimal
    timestamp: Optional[int] = None

    def to_dict(self) -> dict:
        return {
            "index_name": self.index_name,
            "exchange": self.exchange,
            "price": str(self.price),
            "timestamp": self.timestamp
        }


@dataclass
class MarketDataEvent:
    """
    Unified event wrapper for all market data types.
    Replaces OrderbookEvent with a generic event that can carry any data type.
    """
    data_type: DataType
    event_type: str  # "snapshot", "update", "error", "reconnect"
    symbol: str
    exchange: str
    data: Optional[Union[OrderbookSnapshot, TickerData, IndexPriceData]] = None
    error: Optional[str] = None
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    @property
    def orderbook(self) -> Optional[OrderbookSnapshot]:
        """Backward compatible accessor for orderbook data"""
        if self.data_type == DataType.ORDERBOOK:
            return self.data
        return None
