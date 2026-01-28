"""Calais Order Execution System - EMS and OMS for crypto trading."""

from calais_order_execution.config import Config, ExchangeConfig, load_config
from calais_order_execution.models import Order, OrderRequest, OrderSide, OrderStatus, OrderType
from calais_order_execution.service import CalaisExecutionService

__version__ = "0.1.0"

__all__ = [
    # Service
    "CalaisExecutionService",
    # Config
    "Config",
    "ExchangeConfig",
    "load_config",
    # Models
    "Order",
    "OrderRequest",
    "OrderSide",
    "OrderStatus",
    "OrderType",
]
