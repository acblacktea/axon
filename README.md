# Axon

Crypto quant trading infrastructure in C++: a market data service (MDS), an
order execution engine (EMS + OMS) and a pre-trade risk layer. Strategies run
as separate processes and connect to both over ZMQ, or over shared memory when
they share a host with the engine.

| Service | Code | Binaries |
|---|---|---|
| **Order execution** (EMS + OMS + risk) | [cpp/axon_order_execution/](cpp/axon_order_execution/) | `axon_engine`, `axon_strategy` |
| **Market data** (MDS) | [cpp/axon_market_data/](cpp/axon_market_data/) | `axon_market_data`, `mds_subscriber`, `mds_shm_subscriber` |

> The Python implementations under [python_deprecated/](python_deprecated/)
> are deprecated and no longer maintained. The Python order-execution code is
> kept only so the `python_wire_compat` test can keep checking that the C++
> engine stays wire-compatible with Python strategy clients.

---

## Architecture

```
  Binance / OKX / Bybit / Hyperliquid
              |  WebSocket (+ REST snapshots)
     +--------+---------+
     |   Market data    |-- shm /dev/shm/axon_mds ---+  (same host)
     |  order books,    |                            |
     |  normalization   |-- ZMQ PUB :5558 -----------+  (any host)
     +------------------+                            v
                                          Strategy processes
                                    StrategyClient (C++ or Python)
                                       |                      |
                                 ZMQ ROUTER :5555        shared-memory
                                 ZMQ PUB    :5556        SPSC rings
                                       |                      |
                          +------------+----------------------+---+
                          |              axon_engine              |
                          |  risk  -- pre-trade checks            |
                          |  EMS   -- place / cancel / modify     |
                          |  OMS   -- orders, fills, positions,   |
                          |           reconciliation              |
                          +-------------------+-------------------+
                                              |
                               Binance / Bybit / OKX (USDT perps)
                                       Deribit (options)
```

### Order execution — `axon_engine`

One single-threaded, busy-polling process owns every exchange connection and
all order state. It is built from three independent modules:

- **EMS** (`ems/`) sends orders. Each exchange gets a function table
  (`ExchangeOps`) that builds requests and interprets replies. Every order goes out
  with a client order id (the strategy's label, or the engine's
  `internal_order_id`). The exchange echoes that id back, which is how a placement
  whose reply was lost still gets resolved. A send with no verdict is reported
  as `OUTCOME_UNKNOWN`, never as a failure. A strategy's natural reaction to a
  failure is to retry, and if the first order actually went through, that retry
  doubles the position. A duplicate guard on `internal_order_id` refuses
  resubmissions.
- **OMS** (`oms/`) is the source of truth. It keeps the order, fill and
  portfolio stores up to date from the exchanges' WebSocket feeds, de-duplicates
  fills, persists to Postgres when configured, and reconciles periodically
  against REST snapshots. Reconciliation catches open orders, missed fills and
  positions, and looks up orders that were sent but never heard from.
  `OmsService` publishes changes to `OmsListener`s and has no transport of its
  own. `ExchangeConnections` owns the TLS, HTTP, REST and WebSocket sessions, and
  routes feed events to the OMS and RPC replies to the EMS.
- **Risk** (`risk/`) depends only on config and models. `RiskManager` checks
  every order before it reaches the EMS, in this order: kill switch →
  exchange feed down → order rate → quantity / notional → price deviation
  from the reference price → worst-case position → self-trade against our own
  resting orders. If there is no reference price, the order is refused.
  It also tracks profit and loss (from engine start, and from 00:00 UTC each
  day), and trips the kill switch by itself below `max_loss`. `RiskFeed`
  gives it fills and positions from the OMS. The kill switch is also tripped
  by `SIGUSR1` (`SIGUSR2` releases it) or by creating the configured kill file
  (deleting it releases). When `cancel_all_on_kill` is set, tripping it also
  cancels active orders.
- **Exchange rate limits** are counted in the EMS against each exchange's
  published windows, corrected by the usage the exchange reports on its
  replies. New orders stop at 80% of a window; cancels may use all of it.

Everything specific to one exchange lives in
`include/axon/exchanges/<exchange>/` and `src/exchanges/<exchange>/`. Supported
exchanges are `binance`, `bybit`, `okx` and `deribit`. Each exchange has:

| File | Role |
|---|---|
| `<exchange>_builder` / `<exchange>_parser` | Wire protocol: request building, simdjson parsing of feed frames |
| `<exchange>_ems` | The exchange's `ExchangeOps` for the EMS |
| `<exchange>_oms` | The exchange's `OmsExchange`: feed session, order-entry session, REST client |

To add an exchange, add one directory and register it with the EMS and OMS. See
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
kline data on two transports at once:

- **Shared memory** for strategies on the same host: a lock-free
  single-writer, many-reader broadcast ring (after Agrona's broadcast buffer)
  of fixed binary records in `/dev/shm/axon_mds`. Readers include the
  header-only `axon/market_data/shm_feed.h`; the writer never waits for them,
  and a reader that falls a whole ring behind is told how much it missed.
  Measured one hop, publish to prices in hand: p50 under 1 µs.
- **ZMQ PUB** on `:5558` for strategies on other hosts, at p50 around 40 µs
  on the same host (`mds_transport_bench`). Each message is two frames,
  `[topic, json]`.

Both carry the same events. The topic, the ZMQ subscription key and the
shared-memory reader's filter, has the form `{exchange}.{data_type}.{symbol}`:

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

How the order execution engine is put together, and how to add an exchange:
[cpp/axon_order_execution/GUIDE.md](cpp/axon_order_execution/GUIDE.md).
