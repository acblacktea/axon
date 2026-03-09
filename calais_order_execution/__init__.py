"""Calais Order Execution System - EMS and OMS for crypto trading."""

from calais_order_execution.config import Config, DatabaseConfig, ExchangeConfig, PortfolioConfig, ZMQConfig, load_config
from calais_order_execution.models import Order, OrderRequest, OrderSide, OrderStatus, OrderType, AccountSummary, Position
from calais_order_execution.service import CalaisExecutionService

__version__ = "0.3.0"

__all__ = [
    # Service
    "CalaisExecutionService",
    # Config
    "Config",
    "DatabaseConfig",
    "ExchangeConfig",
    "PortfolioConfig",
    "ZMQConfig",
    "load_config",
    # Models
    "Order",
    "OrderRequest",
    "OrderSide",
    "OrderStatus",
    "OrderType",
    "AccountSummary",
    "Position",
]
