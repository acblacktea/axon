# Axon

Crypto quant trading infrastructure in C++: a market data service (MDS), an
order execution engine (EMS + OMS) and a pre-trade risk layer. Strategies run
as separate processes and connect to both over ZMQ, or over shared memory when
they share a host with the engine.

| Service | Code | Binaries |
|---|---|---|
| **Order execution** (EMS + OMS + risk) | [cpp/axon_order_execution/](cpp/axon_order_execution/) | `axon_engine`, `axon_strategy` |
| **Market data** (MDS) | [cpp/axon_market_data/](cpp/axon_market_data/) | `axon_market_data`, `mds_subscriber` |

> The Python implementations under [python_deprecated/](python_deprecated/)
> are deprecated and no longer maintained. The Python order-execution code is
> kept only so the `python_wire_compat` test can keep checking that the C++
> engine stays wire-compatible with Python strategy clients.

---

## Architecture

```
  Binance / OKX / Bybit / Hyperliquid                 Strategy processes
              |                                 StrategyClient (C++ or Python)
     WebSocket (+ REST snapshots)                  |                  |
              |                              ZMQ ROUTER :5555    shared-memory
     +--------+---------+                    ZMQ PUB    :5556    SPSC rings
     |   Market data    |  ZMQ PUB :5558           |                  |
     |  order books,    | ------------------> +----+------------------+----+
     |  normalization   |   (to strategies)   |       axon_engine          |
     +------------------+                     |                            |
                                              |  risk  -- pre-trade checks |
                                              |  EMS   -- place / cancel / |
                                              |           modify           |
                                              |  OMS   -- orders, fills,   |
                                              |           positions,       |
                                              |           reconciliation   |
                                              +-------------+--------------+
                                                            |
                                         Binance / Bybit / OKX (USDT perps)
                                                Deribit (options)
```

### Order execution — `axon_engine`

One single-threaded, busy-polling process owns every exchange connection and
all order state. It is built from three independent modules:

- **EMS** (`ems/`) sends orders. Each venue gets a function table
  (`VenueOps`) that builds requests and interprets replies. Every order goes out
  with a client order id (the strategy's label, or the engine's
  `internal_order_id`). The venue echoes that id back, which is how a placement
  whose reply was lost still gets resolved. A send with no verdict is reported
  as `OUTCOME_UNKNOWN`, never as a failure. A strategy's natural reaction to a
  failure is to retry, and if the first order actually went through, that retry
  doubles the position. A duplicate guard on `internal_order_id` refuses
  resubmissions.
- **OMS** (`oms/`) is the source of truth. It keeps the order, fill and
  portfolio stores up to date from the venues' WebSocket feeds, de-duplicates
  fills, persists to Postgres when configured, and reconciles periodically
  against REST snapshots. Reconciliation catches open orders, missed fills and
  positions, and looks up orders that were sent but never heard from.
  `OmsService` publishes changes to `OmsListener`s and has no transport of its
  own. `VenueConnections` owns the TLS, HTTP, REST and WebSocket sessions, and
  routes feed events to the OMS and RPC replies to the EMS.
- **Risk** (`risk/`) depends only on config and models. `RiskManager` checks
  every order before it reaches the EMS, in this order: kill switch → order
  rate → quantity / notional → price deviation from the reference price →
  worst-case position. If there is no reference price, the order is refused.
  `RiskFeed` gives it fills and positions from the OMS. The kill switch is
  tripped by `SIGUSR1` (`SIGUSR2` releases it) or by creating the configured
  kill file (deleting it releases). When `cancel_all_on_kill` is set, tripping
  it also cancels active orders.

Everything specific to one exchange lives in
`include/axon/exchanges/<venue>/` and `src/exchanges/<venue>/`. Supported
venues are `binance`, `bybit`, `okx` and `deribit`. Each venue has:

| File | Role |
|---|---|
| `<venue>_builder` / `<venue>_parser` | Wire protocol: request building, simdjson parsing of feed frames |
| `<venue>_ems` | The venue's `VenueOps` for the EMS |
| `<venue>_oms` | The venue's `OmsVenue`: feed session, order-entry session, REST client |

To add a venue, add one directory and register it with the EMS and OMS. See
[GUIDE.md](cpp/axon_order_execution/GUIDE.md).

Strategies have two planes:

- **Control plane**: JSON over ZMQ (ROUTER `:5555` for requests, PUB `:5556`
  for order and fill updates). It is byte-identical to the Python engine's
  protocol.
- **Hot path**: shared-memory SPSC rings carrying only
  `place / cancel / modify / order_update / fill` as fixed-size POD structs.
  It is fire-and-forget: there is no ack, and results arrive as later order
  updates.

The engine uses its own networking (`net/`): an RFC 6455 WebSocket client
over OpenSSL memory BIOs, polled from the main loop. Prices and quantities are
fixed-point decimals.

### Market data — `axon_market_data`

The MDS connects to exchange WebSockets for Binance, OKX, Bybit and
Hyperliquid. It maintains local order books and republishes depth, ticker and
kline data as normalized ZMQ PUB messages on `:5558`. Each message is two
frames, `[topic, json]`, and the topic has the form
`{exchange}.{data_type}.{symbol}`:

```
binance_spot.depth.ETH_USDT_SPOT
binance_usdt_futures.kline.BTC_USDT_PERP
okx.ticker.ETH_USDT_SPOT
bybit_inverse.depth.BTC_USD_PERP
```

### Ports

Both services may run on one host, so their ports must not overlap. Register
new services here.

| Port | Service | Purpose |
|---|---|---|
| 5555 | Order execution | ZMQ ROUTER: strategy requests |
| 5556 | Order execution | ZMQ PUB: order and fill updates |
| 5558 | Market data | ZMQ PUB: multi-exchange feed |
| 9100 | Order execution | Prometheus `/metrics` |
| 9101 | Market data | Prometheus `/metrics` |

---

## Build

Both services build with GCC on Linux. [cpp/BUILD.md](cpp/BUILD.md) covers the
dependencies to install, and section 4 covers optimization flags
(`-march=native`, LTO, PGO).

```bash
# order execution: C++20, CMake >= 3.24
cd cpp/axon_order_execution
CC=gcc CXX=g++ cmake --preset default
cmake --build build -j$(nproc)
ctest --test-dir build -j$(nproc)

# market data: C++23, CMake >= 3.25, Ninja
cd cpp/axon_market_data
CC=gcc CXX=g++ cmake -B build -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
ctest --test-dir build -j$(nproc)
```

`ctest` runs the offline suites. The order-execution engine also has a
Binance testnet suite, `axon_testnet_tests`. It is not registered with
`ctest`: it places real testnet orders and runs only with `AXON_TESTNET=1` and
credentials in the environment.

## Run

[test/](test/) is an end-to-end workspace that runs both services together
against Binance testnet, with risk limits and a kill file. Its
[README](test/README.md) covers copying binaries into `test/bin/`, starting
the services and checking that they are connected:

```bash
cd test
./bin/axon_market_data mds_config.yml

source ~/.config/axon/binance-testnet.env   # API keys come only from the environment
./bin/axon_engine --config engine_config.yml

./bin/mds_subscriber tcp://localhost:5558 binance_usdt_futures.depth
```

---

How the order execution engine is put together, and how to add a venue:
[cpp/axon_order_execution/GUIDE.md](cpp/axon_order_execution/GUIDE.md).
