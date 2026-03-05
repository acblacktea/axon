# Calais Order Execution

Crypto options order execution system with EMS + OMS, supporting multi-strategy multi-process architecture via ZMQ.

## Architecture

```
Strategy A (process)        Strategy B (process)
  StrategyClient              StrategyClient
    DEALER + SUB                DEALER + SUB
         \                       /
          ---- ZMQ ROUTER :5555 --    (request-response)
          ---- ZMQ PUB   :5556 ----   (order update events)
                    |
            Engine Process
         CalaisExecutionService
           EMS         OMS
```

## Module Overview

### `models/`

Data models and protocol definitions.

- `order.py` — Core data models: `Order`, `OrderRequest`, `Ticker`, and enums (`OrderStatus`, `OrderSide`, `OrderType`, `Liquidity`). Each order carries a `strategy_id` for multi-strategy isolation.
- `messages.py` — ZMQ protocol messages: `Command` (strategy → engine), `Response` (engine → strategy), `Event` (engine broadcasts order updates).

### `ems/`

Execution Management System — handles order operations via exchange REST APIs.

- `base.py` — `BaseEMS` abstract class defining the interface: `place_order`, `cancel_order`, `modify_order`, `get_order`, `get_open_orders`, `get_ticker`.
- `ems_service.py` — `EMSService` manages multiple EMS clients, one per exchange. Routes operations to the correct exchange implementation.
- `deribit/deribit.py` — `DeribitEMS` implements `BaseEMS` for Deribit. Uses `AsyncHttpClient` for REST API calls with lazy authentication.

### `oms/`

Order Management System — maintains order state, receives real-time updates via WebSocket, and reconciles with exchange.

- `base.py` — `BaseOMS` abstract class extending `WebSocketBase` for exchange-specific WebSocket implementations.
- `order_manager.py` — `OrderManager` manages in-memory order cache with persistence to repository. Provides stale-data protection (timestamp-based), sync/async update callbacks, and preserves `strategy_id` across updates.
- `reconciler.py` — `OrderReconciler` periodically reconciles local order state with exchange via REST API. Handles WebSocket message loss by detecting orders that are locally active but closed on the exchange.
- `oms_service.py` — `OMSService` orchestrates WebSocket clients, reconcilers, and the order manager.
- `deribit/deribit_ws.py` — `DeribitOMS` implements WebSocket connection to Deribit. Handles JSON-RPC 2.0 protocol, order/trade subscriptions, and heartbeats.

### `transport/`

ZMQ transport layer — bridges strategy processes with the engine.

- `types.py` — Protocol enums: `CommandType` (place_order, cancel_order, modify_order, get_order, get_ticker, etc.) and `EventType` (order_update).
- `serialization.py` — JSON serialization/deserialization for all domain objects (`Order`, `OrderRequest`, `Ticker`) and protocol messages (`Command`, `Response`, `Event`).
- `server.py` — `ZMQTransportServer` runs in the engine process. ROUTER socket receives commands from strategies, dispatches to `CalaisExecutionService`, returns responses. PUB socket broadcasts order updates with `strategy_id` as topic for per-strategy filtering.

### `client/`

Strategy-side SDK — lightweight client for strategies running in separate processes.

- `strategy_client.py` — `StrategyClient` provides the same interface as `CalaisExecutionService` (`place_order`, `cancel_order`, `get_order`, `get_ticker`, etc.) but communicates via ZMQ. DEALER socket for request-response, SUB socket for receiving order update events.
- `algorithms/chase_maker.py` — `chase_maker_fill` algorithm: continuously places limit orders at best price, falls back to market order on timeout. Accepts duck-typed client (works with both `StrategyClient` and `CalaisExecutionService`).
- `algorithms/hedge_deribit.py` — `hedge_deribit_options` algorithm: hedges two Deribit options instruments in batches (maker leg + taker leg).

### `repository/`

Order persistence layer.

- `base.py` — `OrderRepository` abstract interface: `save`, `get`, `get_all`, `get_active_orders`, `update`, `delete`.
- `memory.py` — `InMemoryOrderRepository` dictionary-based implementation with asyncio.Lock.

### `util/`

Shared utilities.

- `http.py` — `AsyncHttpClient` with retry logic and exponential backoff.
- `websocket_base.py` — `WebSocketBase` abstract class with auto-reconnection, heartbeat, JSON-RPC request-response pattern.
- `logging.py` — Colored console logging, file logging with daily rotation.

### Root files

- `service.py` — `CalaisExecutionService` unified interface that combines EMS + OMS. Works both in-process (strategies call it directly) and as the backend for `ZMQTransportServer`.
- `engine.py` — Engine process entry point. Starts `CalaisExecutionService` + `ZMQTransportServer`, handles SIGINT/SIGTERM for graceful shutdown.
- `config.py` — Configuration dataclasses (`ExchangeConfig`, `ReconciliationConfig`, `WebSocketConfig`, `ZMQConfig`) and YAML loader.

## Usage

### Multi-process mode (recommended for production)

```bash
# Terminal 1: Start engine
python -m calais_order_execution.engine --config config.yaml

# Terminal 2: Run strategy
python strategies/my_strategy.py
```

Strategy example:

```python
import asyncio
from calais_order_execution.client import StrategyClient
from calais_order_execution.config import ZMQConfig
from calais_order_execution.models import OrderRequest, OrderSide, OrderType

async def main():
    client = StrategyClient(ZMQConfig(), strategy_id="my_strategy")
    await client.connect()

    client.register_order_update_callback(
        lambda order: print(f"Update: {order.order_id} -> {order.status.value}")
    )

    order = await client.place_order("deribit", OrderRequest(
        instrument="BTC-30JAN26-100000-C",
        side=OrderSide.BUY,
        amount=0.1,
        order_type=OrderType.LIMIT,
        price=500.0,
    ))
    print(f"Placed: {order.order_id}")

    await client.disconnect()

asyncio.run(main())
```

### In-process mode (for development/testing)

```python
from calais_order_execution import CalaisExecutionService, load_config

config = load_config("config.yaml")
service = CalaisExecutionService(config)
await service.start()

# Use service directly — same interface as StrategyClient
order = await service.place_order("deribit", request)
```

## Config

```yaml
exchanges:
  deribit:
    env: testnet
    api_key: "your_key"
    api_secret: "your_secret"

reconciliation:
  enabled: true
  interval_seconds: 30

websocket:
  heartbeat_interval_seconds: 10

zmq:
  router_endpoint: "tcp://*:5555"
  pub_endpoint: "tcp://*:5556"
  router_connect: "tcp://localhost:5555"
  pub_connect: "tcp://localhost:5556"
```
