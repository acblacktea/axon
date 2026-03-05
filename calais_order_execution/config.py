"""Configuration management."""

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class ExchangeConfig:
    """Configuration for a single exchange."""

    name: str
    env: str  # "testnet" or "production"
    api_key: str
    api_secret: str

    @property
    def is_testnet(self) -> bool:
        return self.env == "testnet"


@dataclass
class ReconciliationConfig:
    """Configuration for order reconciliation."""

    enabled: bool = True
    interval_seconds: int = 30


@dataclass
class WebSocketConfig:
    """Configuration for WebSocket connections."""

    heartbeat_interval_seconds: int = 10
    reconnect_delay_seconds: int = 1
    max_reconnect_delay_seconds: int = 60
    request_timeout_seconds: int = 30
    max_request_retries: int = 3
    retry_delay_seconds: float = 1.0


@dataclass
class PortfolioConfig:
    """Configuration for portfolio (balance + positions) tracking."""

    currencies: list[str] = field(default_factory=lambda: ["BTC"])
    position_refresh_interval_seconds: int = 10


@dataclass
class ZMQConfig:
    """Configuration for ZMQ transport."""

    router_endpoint: str = "tcp://*:5555"
    pub_endpoint: str = "tcp://*:5556"
    router_connect: str = "tcp://localhost:5555"
    pub_connect: str = "tcp://localhost:5556"


@dataclass
class Config:
    """Main configuration class."""

    exchanges: dict[str, ExchangeConfig] = field(default_factory=dict)
    reconciliation: ReconciliationConfig = field(default_factory=ReconciliationConfig)
    websocket: WebSocketConfig = field(default_factory=WebSocketConfig)
    portfolio: PortfolioConfig = field(default_factory=PortfolioConfig)
    zmq: ZMQConfig = field(default_factory=ZMQConfig)


def load_config(path: str | Path) -> Config:
    """Load configuration from YAML file.

    Args:
        path: Path to the YAML configuration file.

    Returns:
        Config object with all settings.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Configuration file not found: {path}")

    with open(path) as f:
        raw_config = yaml.safe_load(f)

    exchanges: dict[str, ExchangeConfig] = {}
    for name, exchange_data in raw_config.get("exchanges", {}).items():
        exchanges[name] = ExchangeConfig(
            name=name,
            env=exchange_data.get("env", "testnet"),
            api_key=exchange_data.get("api_key", ""),
            api_secret=exchange_data.get("api_secret", ""),
        )

    reconciliation_data = raw_config.get("reconciliation", {})
    reconciliation = ReconciliationConfig(
        enabled=reconciliation_data.get("enabled", True),
        interval_seconds=reconciliation_data.get("interval_seconds", 30),
    )

    ws_data = raw_config.get("websocket", {})
    websocket = WebSocketConfig(
        heartbeat_interval_seconds=ws_data.get("heartbeat_interval_seconds", 10),
        reconnect_delay_seconds=ws_data.get("reconnect_delay_seconds", 1),
        max_reconnect_delay_seconds=ws_data.get("max_reconnect_delay_seconds", 60),
        request_timeout_seconds=ws_data.get("request_timeout_seconds", 30),
        max_request_retries=ws_data.get("max_request_retries", 20),
        retry_delay_seconds=ws_data.get("retry_delay_seconds", 1.0),
    )

    zmq_data = raw_config.get("zmq", {})
    zmq = ZMQConfig(
        router_endpoint=zmq_data.get("router_endpoint", "tcp://*:5555"),
        pub_endpoint=zmq_data.get("pub_endpoint", "tcp://*:5556"),
        router_connect=zmq_data.get("router_connect", "tcp://localhost:5555"),
        pub_connect=zmq_data.get("pub_connect", "tcp://localhost:5556"),
    )

    portfolio_data = raw_config.get("portfolio", {})
    portfolio = PortfolioConfig(
        currencies=portfolio_data.get("currencies", ["BTC"]),
        position_refresh_interval_seconds=portfolio_data.get("position_refresh_interval_seconds", 10),
    )

    return Config(
        exchanges=exchanges,
        reconciliation=reconciliation,
        websocket=websocket,
        portfolio=portfolio,
        zmq=zmq,
    )
