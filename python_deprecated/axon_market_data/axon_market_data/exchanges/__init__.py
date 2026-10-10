"""
Exchange Adapters Module

Supported exchanges:
- Deribit: Options and futures
- Binance: Spot, USDT-M, Coin-M futures
- Bybit: Spot, Linear, Inverse, Options
- OKX: All instrument types
- Hyperliquid: DEX perpetuals
- Kraken: Spot trading
- Upbit: Korean exchange (KRW pairs)
- Coinbase: Spot trading (requires JWT auth)
"""
from .deribit import DeribitAdapter
from .binance import BinanceAdapter, BinanceMarketType
from .bybit import BybitAdapter, BybitMarketType
from .okx import OKXAdapter
from .hyperliquid import HyperliquidAdapter
from .kraken import KrakenAdapter
from .upbit import UpbitAdapter
from .coinbase import CoinbaseAdapter

__all__ = [
    # Deribit
    'DeribitAdapter',
    # Binance
    'BinanceAdapter',
    'BinanceMarketType',
    # Bybit
    'BybitAdapter',
    'BybitMarketType',
    # OKX
    'OKXAdapter',
    # Hyperliquid
    'HyperliquidAdapter',
    # Kraken
    'KrakenAdapter',
    # Upbit
    'UpbitAdapter',
    # Coinbase
    'CoinbaseAdapter',
]
