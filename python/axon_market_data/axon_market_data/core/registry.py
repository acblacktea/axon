"""
Exchange adapter registry and factory.
"""
import logging
from typing import Dict, Optional

import yaml

from .adapter import ExchangeAdapter
from ..exchanges.binance import BinanceAdapter
from ..exchanges.bybit import BybitAdapter
from ..exchanges.coinbase import CoinbaseAdapter
from ..exchanges.deribit import DeribitAdapter
from ..exchanges.hyperliquid import HyperliquidAdapter
from ..exchanges.kraken import KrakenAdapter
from ..exchanges.okx import OKXAdapter
from ..exchanges.upbit import UpbitAdapter


_EXCHANGE_REGISTRY: Dict[str, type] = {
    "binance": BinanceAdapter,
    "bybit": BybitAdapter,
    "coinbase": CoinbaseAdapter,
    "deribit": DeribitAdapter,
    "hyperliquid": HyperliquidAdapter,
    "kraken": KrakenAdapter,
    "okx": OKXAdapter,
    "upbit": UpbitAdapter,
}


def create_adapter(
    exchange_name: str,
    exchange_config: dict,
    logger: Optional[logging.Logger] = None,
) -> ExchangeAdapter:
    """Create an exchange adapter from config."""
    cls = _EXCHANGE_REGISTRY.get(exchange_name.lower())
    if not cls:
        raise ValueError(
            f"Unknown exchange: {exchange_name}. "
            f"Available: {list(_EXCHANGE_REGISTRY.keys())}"
        )
    kwargs = dict(exchange_config)
    if logger:
        kwargs["logger"] = logger
    return cls(**kwargs)


def load_config(config_path: str) -> dict:
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}
