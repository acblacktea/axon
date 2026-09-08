# Axon

Crypto trading infrastructure. Two services, each implemented twice — a
production Python implementation and a low-latency C++ port.

| Service | Python | C++ |
|---|---|---|
| **Order execution** (EMS + OMS) | [python/axon_order_execution/](python/axon_order_execution/) | [cpp/axon_order_execution/](cpp/axon_order_execution/) |
| **Market data** (MDS) | [python/axon_market_data/](python/axon_market_data/) | [cpp/axon_market_data/](cpp/axon_market_data/) |

---

## Architecture

### Order execution — EMS + OMS

A standalone engine process owns every exchange connection and all order
state. Strategies run as separate processes and talk to it over ZMQ.

```
  Strategy A (Py)      Strategy B (Py)      Strategy C (C++, same host)
  StrategyClient       StrategyClient       StrategyClient
   DEALER + SUB         DEALER + SUB         shared-memory ring (optional)
         \                   /                        |
          -- ZMQ ROUTER :5555 --                      |  ~0.1-0.3 us one-way
          -- ZMQ PUB    :5556 --                      |
                  |  ~30-60 us round trip             |
          +-------+-----------------------------------+--------+
          |                   Execution engine                  |
          |          AxonExecutionService / axon_engine         |
          |                                                     |
          |  EMS  -- REST: place / cancel / modify / quotes      |
          |  OMS  -- WebSocket: order, fill and position push,   |
          |          local state, periodic reconciliation        |
          |  Repository -- Postgres or in-memory                 |
          |  Metrics    -- Prometheus :9100/metrics              |
          +--------------------------+--------------------------+
                                     |
              Deribit (options) / Binance / Bybit / OKX (USDT perps)
```

- **EMS** performs actions over REST. **OMS** tracks truth over WebSocket and
  reconciles periodically, because sockets drop and messages get lost.
- Fills are de-duplicated. Counting one twice means a wrong position.
- The C++ engine adds a **second plane**: a shared-memory SPSC ring carrying
  only `place / cancel / modify / order_update / fill` as fixed-size POD
  structs. It is fire-and-forget — no ack, results arrive as later order
  updates. Everything else stays on the JSON control plane.
- Both implementations speak a **byte-identical JSON protocol**, enforced by
  the `python_wire_compat` test, so Python strategies run unchanged against
  the C++ engine.

### Market data — MDS

Connects to exchange WebSockets, maintains local order books, and republishes
everything as normalized ZMQ PUB messages.

```
  Binance / OKX / Bybit / Hyperliquid / Deribit / Kraken / Coinbase / Upbit
                                 |
                      WebSocket (+ REST snapshots)
                                 |
                    +------------+------------+
                    |       Market data       |
                    |  local order books,     |
                    |  symbol normalization   |
                    +------------+------------+
                                 |
                          ZMQ PUB :5558
                                 |
                      subscribers (Py / C++)
```

Messages are two frames, `[topic, json]`, with the topic shaped as
`{exchange}.{data_type}.{symbol}`:

```
binance_spot.depth.ETH_USDT_SPOT
binance_usdt_futures.kline.BTC_USDT_PERP
okx.ticker.ETH_USDT_SPOT
bybit_inverse.depth.BTC_USD_PERP
```

The C++ service covers Binance, OKX, Bybit and Hyperliquid (depth, ticker,
kline). The Python service additionally covers Deribit, Kraken, Coinbase and
Upbit.

### Ports

Both services may run on one host, so their ZMQ ports must not overlap.
Register new services here.

| Port | Service | Purpose |
|---|---|---|
| 5555 | Order execution | ZMQ ROUTER — place / cancel control plane |
| 5556 | Order execution | ZMQ PUB — order and fill updates |
| 5557 | Market data | ZMQ PUB — Deribit options feed |
| 5558 | Market data | ZMQ PUB — general multi-exchange feed |

---

## Usage

The order execution service reads `config.yaml` at the repository root; the
market data service takes its own `config.yml`.

### Order execution — Python

```bash
cd python/axon_order_execution

# terminal 1: the engine
python -m axon_order_execution.engine --config ../../config.yaml

# terminal 2: a strategy
python examples/strategy_one.py
```

```python
import asyncio
from axon_order_execution.client import StrategyClient
from axon_order_execution.config import ZMQConfig
from axon_order_execution.models import OrderRequest, OrderSide, OrderType

async def main():
    client = StrategyClient(ZMQConfig(), strategy_id="my_strategy")
    await client.connect()

    client.register_order_update_callback(lambda o: print(o.order_id, o.status.value))
    client.register_fill_update_callback(lambda f: print(f.trade_id, f.amount, f.price))

    order = await client.place_order("deribit", OrderRequest(
        instrument="BTC-30JAN26-100000-C",
        side=OrderSide.BUY,
        amount=0.1,
        order_type=OrderType.LIMIT,
        price=500.0,
    ))
    await client.disconnect()

asyncio.run(main())
```

To run the engine in-process instead (development and tests), the interface is
the same as `StrategyClient`:

```python
from axon_order_execution import AxonExecutionService, load_config

service = AxonExecutionService(load_config("config.yaml"))
await service.start()
order = await service.place_order("deribit", request)
```

### Order execution — C++

Needs CMake >= 3.24, a C++20 compiler, OpenSSL, and yaml-cpp / spdlog /
prometheus-cpp / cppzmq / libpqxx. On macOS CMake finds these under the brew
prefix. The first configure needs network access to fetch GoogleTest.

```bash
cd cpp/axon_order_execution
cmake --preset default && cmake --build build
ctest --test-dir build

./build/bin/axon_engine --config ../../config.yaml
```

### Market data — Python

```bash
cd python/axon_market_data
pip install -r requirements.txt
python examples/run_server.py          # reads examples/config.yml
```

### Market data — C++

Needs a C++23 compiler, CMake 3.25+, Ninja and vcpkg. The first build is slow
because vcpkg compiles every dependency.

```bash
cd cpp/axon_market_data
cmake -B build -G Ninja -DCMAKE_TOOLCHAIN_FILE=~/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build build

./build/axon_market_data config.yml
```

Subscribing:

```bash
./build/mds_subscriber                                            # every topic
./build/mds_subscriber tcp://localhost:5558 binance_spot.depth    # depth only
python example/subscriber.py tcp://localhost:5558 binance_spot.depth.ETH_USDT_SPOT
```

---

Per-service detail — exchange quirks, configuration reference, benchmarks and
design rationale — lives in each service's own README:
[order execution (C++)](cpp/axon_order_execution/README.md) ·
[market data (C++)](cpp/axon_market_data/README.md).
