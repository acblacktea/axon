# calais_order_execution_cpp

Low-latency C++ port of the [Python engine](../calais_order_execution/), living
in the same repository:

```
calais_order_execution/            <- repo root
├── calais_order_execution/        Python implementation (unchanged)
├── tests/                         Python tests
└── calais_order_execution_cpp/    this project
```

One repo on purpose. The two implementations share a wire protocol, and the
test that proves it (`python_wire_compat`, below) runs the *actual* Python
serialiser against C++ output. Split across two repositories that test becomes
a cross-repo version-matching problem, and the first time it breaks it gets
disabled instead of fixed.

**Status: transport complete.** TCP, TLS and WebSocket connect end to end, and
all four venue protocols (Deribit options, Binance / Bybit / OKX perpetuals)
are implemented against them. Verified against a real in-process TLS server on
loopback — never yet against an exchange. See
[What is not here](#what-is-not-here).

**Target: under 10 µs internal, deployed in the same region as the venue.**
That number is what justifies hand-rolling the WebSocket layer instead of
using Boost.Beast, and it is why every component here is measured rather than
assumed.

```
cmake --preset default && cmake --build build && ctest --test-dir build
./build/bench/calais_bench
```

Requires CMake ≥ 3.24, a C++20 compiler, and network access on first configure
(GoogleTest and the nlohmann single header are fetched). No package manager
needed.

---

## Why this exists, and what it is not trying to fix

The dominant latency in crypto options execution is **not** in the engine. It is
the WAN round trip to the exchange and the exchange's own matching latency.

Worth being precise about what "colocated" means here: the major crypto venues
run on public cloud, not in an exchange cage with a cross-connect. In practice
"same datacentre" means **same cloud region, ideally same AZ**, over ordinary
TCP on an ordinary NIC. Same-AZ network RTT is roughly 50–150 µs, but an
application-level round trip through the venue's gateway and matching engine is
typically **0.5–2 ms**. Measure it per venue; do not trust anyone's
documentation, including this paragraph.

So a 10 µs internal budget is about **1–5 %** of the end-to-end path. It is
still worth having — but the reason is **tail control**, not the median. Once
you are colocated, your own jitter is what moves your position in the queue.

Ordering of wins:

1. **Deployment region** — worth ~100 ms if you get it wrong. Measure p99 RTT
   from candidate regions to each venue over 24 h before committing. Nothing in
   this repo helps with this.
2. **Order entry over WebSocket instead of REST** — worth 1–5 RTTs. Architectural,
   not language-level. The Python engine splits EMS (REST) / OMS (WS); the C++
   version must send orders on the already-authenticated WS connection.
3. **Strategy↔engine transport** — worth 15–60 µs. `core/shm_ring.h` addresses this.
4. **Internal code path** — worth 5–50 µs. Everything else here.

This repo is items 3 and 4. Do not let it distract from 1 and 2.

### Where a 10 µs budget actually goes

At this target, things that were rounding errors before become the main costs:

| Step | Cost | Share of 10 µs |
|---|---|---|
| `send()` syscall | 1–3 µs | **10–30 %** |
| TLS record encrypt + decrypt (AES-NI) | 0.5–2 µs each way | **10–40 %** |
| JSON field extraction (simdjson on-demand) | 0.2–0.5 µs | 2–5 % |
| WebSocket frame decode + assemble | **16 ns** (measured) | 0.2 % |

Two consequences, the second of which reverses an earlier judgement:

- **Beast is out.** Its `dynamic_buffer` allocates on the receive path, and a
  malloc tail inside a trade decision is exactly what a 10 µs budget cannot
  absorb. Hence `net/` below.
- **Kernel bypass moves from "overkill" to "worth evaluating."** At WAN
  distances the syscall cost is noise. Colocated, `send()` is a tenth to a
  third of the entire budget. There is no Solarflare in a cloud VM, so the
  realistic option is **io_uring with SQPOLL**. Decide it with measurements
  once the socket layer exists — not before.

---

## What is here

### `core/` — dependency-free foundation

Nothing in `core/` links anything beyond libc++ and pthreads. That is
deliberate: hot-path code must not be able to reach a JSON parser or a
third-party allocator by accident.

| Header | What it is | Why it exists |
|---|---|---|
| `decimal.h` | int64 fixed-point, 9 decimals | Formatting a double into JSON costs 20–50 ns and allocates; an int64 costs ~9 ns. And fill accumulation in double drifts into phantom residual positions. |
| `timestamp.h` | ns-since-epoch UTC | Formats byte-identically to Python's `datetime.isoformat()`, including its rule of dropping the fraction when microseconds are zero. |
| `clock.h` | TSC / CNTVCT counter | `now_ticks()` is 0.29 ns. Hot path records raw ticks; conversion to ns happens on the reporting thread. |
| `histogram.h` | HDR-style latency histogram | Tails decide P&L; means hide them. Constant-cost `record()` so you can afford to measure every message. |
| `spsc_ring.h` | lock-free in-process ring | A contended mutex costs 2–10 µs — more than the whole internal budget. |
| `shm_ring.h` | cross-process ring over mmap | Replaces the ZMQ hop on the hot path. |
| `object_pool.h` | fixed-capacity freelist | malloc's *tail* is unbounded, and it spikes exactly when you are busiest. |
| `latency.h` | multi-stage timestamping | Answers "which stage" rather than "it's slow". |
| `platform.h` | cache line, pause, pinning | Pinning is Linux-only and says so rather than silently no-op'ing. |

### `models/` — domain types

`Order`, `OrderRequest`, `Ticker`, `Fill`, `AccountSummary`, `Position`, and the
enums. Field declaration order mirrors the Python dataclasses because the JSON
codec emits keys in that order.

### `transport/` — two planes, on purpose

**Control plane** (`wire.h`, `messages.h`) — JSON over ZMQ, structurally
identical to the Python implementation's `transport/serialization.py`. Same
keys, same order, same values. This is what lets an existing Python
`StrategyClient` talk to the C++ engine unmodified, which is what makes an
incremental port possible.

**Data plane** (`hot_messages.h`) — fixed-layout POD, memcpy only. Producing a
message is a few stores; consuming one is a pointer cast.

The measured gap between them is the argument for the split:

| | binary | JSON | ratio |
|---|---|---|---|
| build a place-order message | **37.5 ns** | 2 125 ns | 57× |
| consume it | **0.29 ns** (cast) | 2 417 ns (parse) | ~8 000× |
| wire size | 216 B | 264 B | |

Only place / cancel / modify / order-update / fill live in the binary plane.
Everything else stays in JSON, where adding a field is free.

### `venue/` — per-exchange protocol, all four venues

Inbound parsing and outbound message construction. Pure byte transformation, no
sockets, which is why it can be tested exhaustively against captured payloads.

| | product | inbound shape | numbers | WS auth |
|---|---|---|---|---|
| **Deribit** | options | JSON-RPC 2.0 subscription | bare | client_id / secret |
| **Binance** | USDT-M perp | event-tagged user data stream | quoted | none (listenKey in URL) |
| **Bybit** | USDT perp | topic / data array | quoted, incl. timestamps | HMAC-SHA256, hex |
| **OKX** | perp swap | arg / data array | quoted, incl. timestamps | HMAC-SHA256, **base64** + passphrase |

Shared pieces: `json_view.h` (simdjson wrapper), `json_writer.h` (byte writer),
`venue_events.h` (one result shape for all four, so the OMS above does not care
which protocol produced a message).

Order-update parse, measured on each venue's own feed shape:

```
deribit (options, bare JSON numbers)   280 ns
binance (perp, quoted numbers)         308 ns
bybit   (perp, quoted numbers + ts)    307 ns
okx     (perp, quoted numbers + ts)    278 ns

build private/buy request (deribit)     69 ns
build private/cancel request            24 ns
```

#### Per-venue traps, each one load-bearing

**Deribit** — no partially-filled state of its own; it is derived from
`filled_amount > 0`, and only for open orders.

**Binance** — one `ORDER_TRADE_UPDATE` is an order update *and*, when its
execution type is `TRADE`, a fill. Missing that is how a fill silently never
gets recorded. Field names are single letters, including `o` for both the
payload object and the order type inside it. `"p":"0"` means *no price*, not a
price of zero. `listenKeyExpired` is reported as `kSessionExpired` — the
correct response is to re-establish the session, not to log and continue.

**Bybit** — one private stream carries linear, inverse, option **and** spot.
Anything whose `category` is not `linear` must be dropped, or a spot fill lands
on a perp position. `isMaker` arrives as a JSON boolean on some paths and the
string `"true"` on others. `req_id` is a string, unlike every other venue's
integer.

**OKX** — the heartbeat is the literal text `ping`/`pong`, not JSON, so it never
reaches the parser. `"px":""` means absent, not zero (`json_view` treats an
empty string as `nullopt` precisely for this). **Fees are reported negative**
and are normalised to positive, or fee accounting flips sign against the other
three. `post_only` is an *order type*, not a flag. Its signature is base64
while Bybit's and Binance's are hex, and it needs a passphrase that is not
derivable from anything.

#### What is deliberately not built: WebSocket order entry

All three perp venues can accept orders over the WebSocket
(`ws-fapi` on Binance, `/v5/trade` on Bybit, `op:"order"` on OKX), which would
remove an HTTP round trip per order — **1–2 RTTs, most of a colocated latency
budget**. None of it is implemented, because the Python uses REST on all three
and there is no proven field mapping to port. Guessing at one and shipping it
unverified is exactly the failure mode this layer is built to avoid.

This is the largest remaining latency win, and it should be the first thing
done after the socket layer lands and one venue has been validated live.

**Every field mapping was transcribed from the corresponding Python OMS and
EMS, not from the venues' documentation.** The Python has been reconciling
against live venues for a long time; the documentation has not. Where they
disagree the running code wins, and any unintended divergence is a bug that
only appears in production.

Three divergences are intentional and each is marked `DIVERGENCE` in the source:

1. **`post_only` is emitted as a JSON boolean, not the string `"true"`.** The
   Python sends `"true"` and Deribit coerces it, but boolean is the documented
   type. **This one is unverified — check it against testnet before trusting
   it**, since it is the only place the venue layer does something the running
   Python does not.
2. **A trade with no `timestamp` stays at the epoch** rather than being stamped
   with `utcnow()`. The Python's fallback makes an exchange-side delay
   indistinguishable from zero latency; an obviously-wrong epoch is visible.
3. **An unrecognised `order_state` is reported, not just defaulted.** The
   Python silently maps anything unknown to `open`; this keeps that behaviour
   (losing the update entirely would be worse) but surfaces
   `unknown_order_state` so it can be counted and alerted on.

#### A bug found in the Python while porting

`_parse_order` and `_parse_fill` build timestamps with
`datetime.fromtimestamp(ts / 1000)`, which returns **local** time, while
`_handle_portfolio_update` and the `Order` model defaults use
`datetime.utcnow()`, which is **UTC**. The two are mixed in the same objects.

On a UTC server there is no visible difference, which is why it has survived.
On any host with a non-UTC timezone, Deribit-sourced `created_at` / `updated_at`
are offset from everything else by the UTC offset — silently corrupting
anything that compares them, including latency measurement and time-based
reconciliation. The C++ port uses UTC throughout.

### `net/` — WebSocket protocol, hand-rolled

RFC 6455 client side: framing, masking, fragmentation reassembly, and the
opening handshake. No sockets and no TLS — this layer is pure byte
transformation, which is why it can be tested exhaustively without a network.

| Header | What it does |
|---|---|
| `ws_frame.h` | Frame decode (zero-copy, returns a pointer into your buffer) and encode. Word-at-a-time masking. UTF-8 validation. Close-code parsing. |
| `ws_message.h` | Reassembles fragments; passes interleaved control frames straight through so a ping mid-message can be answered immediately. |
| `ws_handshake.h` | Builds the upgrade request, validates the response strictly. |
| `crypto_lite.h` | SHA-1 + base64 for `Sec-WebSocket-Accept`, and SHA-256 + HMAC-SHA256 for Bybit's and OKX's signed logins. Local rather than OpenSSL — all four run once per connection and all four have authoritative published vectors. |
| `byte_buffer.h` | Contiguous receive buffer with compaction. One allocation, ever. |
| `tcp_socket.h` | Non-blocking TCP. `TCP_NODELAY` on by default; Linux `TCP_QUICKACK` / `SO_BUSY_POLL` / keepalive knobs exposed. |
| `tls_stream.h` | OpenSSL over **memory BIOs**, so the caller keeps the socket and the buffers. Verification and hostname checking on by default. |
| `ws_connection.h` | The state machine that ties them together, driven by one non-blocking `poll()`. |

#### The connection state machine

```
kTcpConnecting -> kTlsHandshaking -> kWsHandshaking -> kOpen
```

A flat state machine, not a coroutine. The three setup states would read better
as one — but `kOpen`, where every message spends its life, never actually
suspends: the data is already in the buffer by the time `poll()` looks. A
coroutine there pays for machinery it never uses, and once the handle escapes
into a scheduler the frame is a 45 ns malloc with an unbounded tail (measured;
`bench/bench_coroutine.cpp`). So no coroutines anywhere here, and the setup
path trades a little readability for one consistent model.

`on_message` hands out a pointer **into the receive buffer**, valid for exactly
that call. That is the point: a venue parser reads its six fields straight out
of the socket buffer with nothing copied in between. The buffer reserves
simdjson's padding so this is safe rather than a heap overread.

Ping/pong is answered internally. A venue that does not get its pong closes the
connection, and making that the application's problem is how it gets forgotten.

**TLS goes through memory BIOs rather than `SSL_set_fd`.** The usual approach
hands OpenSSL the socket and lets it do its own `read`/`write`, which puts the
syscalls somewhere they cannot be batched, timed, or driven from a busy-poll
loop, and forces a copy into OpenSSL's buffers. The pump model here keeps all
of that in our hands.

Two decisions worth knowing about:

**permessage-deflate is not offered, and is rejected if a server returns it.**
Compressing a 200-byte order costs tens of microseconds and saves nothing on a
colocated link. A server that enables it anyway would frame with RSV1 set,
which the decoder treats as a protocol error — so the handshake fails loudly
instead of the connection breaking later.

**The decoder is lenient where it is safe and strict where it is not.** It
accepts a non-minimal payload length encoding, which the RFC says a *sender*
must not produce: dropping a live exchange connection mid-session over three
wasted bytes is a far worse failure than tolerating them. It rejects reserved
bits, reserved opcodes, oversized or fragmented control frames, and invalid
UTF-8, because each of those means the stream is no longer parseable. The
handshake validator, by contrast, is strict throughout — a handshake failure
costs a reconnect, while wrongly accepting one means framing against something
that is not a WebSocket server and discovering it much later.

---

## Measured

Apple M-series laptop, unpinned, macOS. **Relative comparisons only** — a tuned,
core-isolated Linux host will show a much tighter tail. Reproduce with
`./build/bench/calais_bench`.

```
now_ticks()                              0.29 ns
Price::from_string("64000.5")            5.25 ns      vs std::stod        20.32 ns
Price::write() -> chars                  8.63 ns      vs snprintf %.9g    94.91 ns
Price::mul() [__int128, saturating]      0.29 ns
Histogram::record()                      3.25 ns
ObjectPool acquire+release               2.62 ns      vs new/delete       14.29 ns
SPSC ring push+pop (single thread)       2.10 ns
SPSC ring thread-to-thread round trip    p50 83 ns   p99 209 ns   p99.9 3.7 µs
shm ring push+pop (216 B)               10.48 ns
encode PlaceOrderMsg                    37.53 ns
serialise OrderRequest (JSON)            p50 2.1 µs  p99 2.7 µs   p99.9 9.5 µs
parse Order (JSON, DOM)                  p50 4.3 µs  p99 14.0 µs  p99.9 60.8 µs

--- websocket, on a 288-byte exchange payload ---
decode frame header                      5.58 ns
encode frame (masked)                   12.02 ns
mask payload                             5.50 ns
validate UTF-8                          12.42 ns
decode + assemble (full rx path)        16.25 ns
```

**On the clock, and why the benchmark has two modes.** Apple Silicon's
`CNTFRQ_EL0` reports 1 GHz, but the counter actually advances in ~41 ns steps.
Trusting the nominal frequency made every sub-41 ns measurement read as "0 or
42" — instrumentation that looks like it works and does not. `clock_info()` now
*measures* the granularity at startup, and the benchmark times sub-resolution
operations in batches, reporting them as `[amortised]` (p50 only, no meaningful
tail). Check `clock_info().resolution_ns` before trusting a sub-100 ns number on
a new machine.

---

### Library and language choices, decided by measurement

Three questions that come up constantly, answered with the benchmarks in
`bench/` rather than from folklore. Reproduce with:

```
cmake --preset default -DCALAIS_BENCH_JSON_LIBS=ON && cmake --build build
./build/bench/calais_bench_json
./build/bench/calais_bench_coroutine
```

**Which JSON parser?** Extracting the 6 fields an OMS needs from a 456-byte
Deribit order update (p50):

| | parse | vs nlohmann |
|---|---|---|
| nlohmann DOM | 4127 ns | 1× |
| rapidjson DOM | 1000 ns | 4× |
| rapidjson DOM in-situ | 833 ns | 5× |
| rapidjson SAX | 792 ns | 5× |
| **simdjson On-Demand** | **125 ns** | **33×** |
| hand-written `memmem` scanner | 1105 ns | 4× |

rapidjson is a real 4–5× improvement, but it is the wrong place to stop:
**simdjson On-Demand is another 8× beyond it**, because it never materialises
the document — it walks lazily and touches only the fields asked for, in one
SIMD pass. On a message where 6 of ~15 fields are wanted, that structural
difference dominates raw parsing speed.

The hand-written scanner is the surprise, and the useful lesson: it is
**slower than rapidjson SAX**. Six separate `memmem` passes over the buffer
plus `strtod` beat a single vectorised pass at nothing. Hand-rolling is not
automatically faster, and here it costs 9× against simdjson while also
breaking silently when a venue reorders a field.

For **outbound** messages no parser is the answer at all:

| | build | |
|---|---|---|
| nlohmann `dump()` | 1458 ns | |
| rapidjson `Writer` | 250 ns | 6× |
| **byte template + `Decimal::write`** | **12 ns** | **120×** |

Decision: keep nlohmann on the ZMQ control plane (cold, and its ergonomics are
worth more there than nanoseconds); use **simdjson On-Demand** for the exchange
feed; build outbound order messages from a byte template with fixed-point
fields written directly. rapidjson earns a place nowhere in this design.

One integration note for later: simdjson On-Demand requires `SIMDJSON_PADDING`
readable bytes past the end of the input, so `net/ByteBuffer` will need to
over-allocate by that much.

**C++23 or C++26?** Apple Clang 16 accepts `-std=c++26`, which means nothing:

```
std::expected      202211   available (C++23)
std::print         202207   available (C++23)
coroutines         201902   available (C++20)
pack indexing      MISSING  (a C++26 core feature)
ranges::zip        MISSING  (a C++23 library feature)
```

The flag is an empty shell here. The C++26 features that would actually change
this codebase — **reflection (P2996)**, which could generate the entire
`transport/wire.cpp` from struct definitions the way Python's
`dataclasses.fields()` does — are not implemented in any shipping compiler.
Even C++23's library is incomplete in this libc++.

Decision: stay on **C++20**. `std::expected` would tidy the
`optional`-plus-error-string returns and is a reasonable later bump to C++23,
but it buys no performance, and the binding constraint is the production
toolchain, not this laptop. Revisit when reflection actually ships.

**Coroutines?** Measured, and the answer is "yes, but not everywhere":

| | ns |
|---|---|
| plain function chain (baseline) | 2.7 |
| coroutine, frame elided (HALO) | 2.7 |
| **coroutine, frame escapes** | **45** |
| bare `resume()`, frame hot | 0.5 |

When Heap Allocation eLision fires, a coroutine is free. When it cannot — which
is whenever the handle is stored, queued, or crosses an interface, i.e. any
real scheduler — the frame is a `malloc` and costs **45 ns and an unbounded
tail**.

And HALO cannot be relied on. The benchmark demonstrates this accidentally but
reproducibly: the pooled variant records **exactly 1000 allocations every run**
— the warmup loop — and **zero** in the measured loop. Same coroutine, same
lambda, two loops in one function, and the optimiser elided the frame in one
and not the other. Nothing in the source distinguishes them.

Decision: **no coroutines on the hot path** (recv → parse → decide → send). It
never actually suspends there — the data is already in the buffer — so the
machinery would be paid for nothing. **Coroutines for the cold path**: connect,
TLS handshake, WebSocket handshake, auth, subscribe, reconnect backoff. Those
are inherently sequential-with-waits and are where hand-rolled state machines
grow bugs. Give them a `promise_type` with a pooled `operator new` so the
allocation is bounded whether or not HALO fires, and drive them from the same
busy-poll loop rather than pulling in an `io_context`.

### The one optimisation measurement actually forced

The first WebSocket benchmark run said UTF-8 validation cost **99.8 ns** on a
288-byte payload — 18× the frame decode, and essentially the entire receive
path. Exchange feeds are JSON and effectively all ASCII, so the validator now
tests the high bits of eight bytes at a time and only falls back to per-byte
decoding when one is set:

```
validate UTF-8            99.78 ns  ->  12.42 ns
decode + assemble         96.89 ns  ->  16.25 ns
```

Correctness is unchanged — every overlong-encoding, surrogate, and truncation
test still passes, plus new ones that plant an invalid sequence at every offset
across the 8-byte stride, which is precisely where a careless fast path would
leak. This is the loop the whole project is built around: measure, find the one
thing that dominates, fix that, re-measure.

---

## Testing

383 tests. `ctest --test-dir build`.

Three that carry more weight than the rest:

- **`python_wire_compat`** — runs 41 canonical objects through the *real* Python
  `transport/serialization.py` and asserts Python reconstructs each dataclass
  and re-serialises it to the same bytes. Covers every enum spelling, null
  optionals, timestamps with and without fractions, and numeric edges from
  `1e-09` to `9999999.999999999`. This is the test that guarantees an existing
  Python strategy still works.
- **`ShmRing.TwoProcessesExchangeMessages`** — forks and passes 200 000 messages
  between two real processes, checking order and content. The only way to prove
  the atomics are genuinely address-free.
- **`SpscRing.ConcurrentProducerConsumerPreservesEveryItemInOrder`** — 2 M items
  across two threads, asserting nothing is lost, duplicated, reordered, or
  corrupted.
- **`test_ws_connection.cpp`** — a real WebSocket server in-process: real TCP on
  loopback, real TLS with a certificate generated at runtime, real framing.
  Certificate verification stays **on** and the client trusts only that
  generated certificate, so the suite includes tests that an untrusted
  certificate and a hostname mismatch are both **rejected**. Mocking the
  transport would only prove the mock and the client agree.
- **`WsFrame.DecodeNeverReadsPastTheBuffer`** — feeds every prefix of a large
  frame to the decoder under ASan. A one-byte overread here is what turns a
  malformed frame from a peer into a crash in production.
- **SHA-1 and base64 against RFC 3174 / RFC 4648 vectors**, including the
  55/56/63/64/119/120-byte lengths that exercise every branch of SHA-1's
  padding, and the RFC 6455 §1.3 handshake example end to end.

Sanitizers, both clean across the whole suite:

```
cmake --preset asan && ctest --preset asan     # 383/383
cmake --preset tsan && ctest --preset tsan     # 383/383
```

TSan on the ring buffers is not optional — it is the only real evidence the
memory ordering is correct.

### Three bugs the tests actually caught

Worth recording, because each was silent:

1. **`mul()` wrapped on overflow.** `100000 × 100000 = 1e10` exceeds the ±9.22e9
   range of `Decimal<1e9>` and wrapped to **−8.45e9** — the *sign flipped*. In a
   notional calculation that turns a long into a short with nothing downstream
   able to notice. Every operation now saturates; `checked_mul` / `checked_div`
   return `nullopt` for callers that must know rather than merely survive.
2. **`div()` rounded the wrong way for mixed signs.** `10 ÷ −4` gave
   `−2.499999999`. The rounding offset was applied in signed space; it now
   rounds the magnitude and reattaches the sign.
3. **`clock_info().resolution_ns` was derived from `CNTFRQ_EL0` and was wrong**
   by 41×, as described above.

---

## What is not here

Nothing below this line exists yet.

- **No exchange CONNECTIVITY.** The Deribit protocol layer exists and is
  tested, but nothing can reach the venue: there is no socket, no TLS, no
  request signing. Deribit's WebSocket auth uses plain client_id/client_secret
  so it needs no signing, but Binance, Bybit and OKX all sign requests with
  HMAC-SHA256, which arrives with OpenSSL alongside TLS.
- **Nothing has ever connected to an exchange.** The transport is proven
  against a real TLS server on loopback and the parsers against captured
  payloads, but neither is proof that a venue accepts what we send. That needs
  credentials and a testnet.
- **No REST client.** Binance needs one for its listenKey, and all three perps
  currently place orders over REST. Only the WebSocket path exists.
- **No reconnect logic.** A dropped connection stays dropped; there is no
  backoff, no resubscribe, no state recovery.
- **No WebSocket order entry on the perp venues.** See above; it is the largest
  remaining latency win.
- **No OMS/EMS.** No `OrderManager`, no reconcilers, no `PortfolioManager`.
- **No persistence.** No Postgres. The design is a dedicated writer thread
  behind an `SpscRing`, entirely off the hot path — `libpqxx` has no good async
  story and persistence is not latency-critical.
- **No ZMQ server.** `transport/wire.h` produces and consumes the bytes; nothing
  binds a socket yet.
- **No OMS/EMS wiring.** The venue layer emits `OrderUpdateMsg` and `FillMsg`;
  nothing consumes them yet. `OrderManager`, the reconcilers and
  `PortfolioManager` are unported.
- **No metrics or logging backend.** `prometheus-cpp` behind a project
  `MetricsClient` (hot path writes per-thread POD counters, a background thread
  aggregates), and Quill rather than spdlog for logging.
- **`Price` and `Qty` are the same type.** They are aliases, so the compiler will
  *not* catch passing one where the other is expected. Making them distinct
  needs a phantom tag plus a cross-tag multiply (price × qty → notional); worth
  doing once the order path exists and the meaningful combinations are known.

---

## Next steps, in order

1. **Network baseline.** Candidate regions × each venue, 24 h RTT p99 and
   jitter. This decides the deployment topology, which decides how `oms_service`
   and `ems_service` get split. Do it before writing more code.
2. ~~**WebSocket protocol layer.**~~ Done — `net/`.
3. ~~**Socket and TLS.**~~ Done — `net/tcp_socket.h`, `net/tls_stream.h`,
   `net/ws_connection.h`.
4. **One venue against testnet.** Everything needed is now present; what is
   missing is credentials. This is the step that turns "parses the shapes" into
   "works", and it should happen before any more code is written on top.
5. **ZMQ control plane server**, then cross-validate by pointing the existing
   Python `StrategyClient` and `tests/test_*.py` at the C++ engine.
6. **Freeze the hot path**: zero allocation, Quill, per-thread metrics, core
   pinning, `TCP_NODELAY` (forgetting this one costs tens of milliseconds and is
   the most common way to lose all of the above).
7. **Remaining three venues** — mechanical once the template exists.
8. Persistence, reconcilers, production hardening.

### Deployment notes (Linux)

Thread pinning, `isolcpus` / `nohz_full`, `mlockall`, and `SO_BUSY_POLL` are all
Linux-only. macOS has no real affinity API, so develop here but treat only Linux
numbers as real. Put the shm ring on `/dev/shm` (tmpfs) — a path on a real
filesystem lets the kernel try to write ring pages back to disk.

The shm ring requires the consumer to **busy-poll**. Blocking on a condition
variable to save a core hands back the 2–10 µs futex wakeup and leaves you worse
off than ZMQ ipc. 100 % CPU on the hot cores is the entry fee.
