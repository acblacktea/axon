// Latency benchmarks for the foundation layer.
//
// Two measurement modes, and picking the right one is the whole trick:
//
//   PER-ITERATION  time each call individually and build a distribution.
//                  Gives a real tail. Only valid when the operation costs more
//                  than the counter's resolution.
//
//   AMORTISED      time a batch and divide. Needed for anything below the
//                  counter resolution -- on Apple Silicon that is ~42ns, so a
//                  5ns register read timed individually reads as "0 or 42",
//                  which looks like instrumentation and is noise. The cost is
//                  that a per-batch tail is not a per-operation tail: one slow
//                  call in a batch of 1000 is invisible.
//
// Anything reported as amortised has no meaningful tail; read only its p50.
// Anything reported per-iteration has a real one.
//
// Read all of it as RELATIVE. An unpinned laptop with an active scheduler
// shows a tail a tuned, core-isolated production host would not. The
// comparisons that survive the move -- binary vs JSON, pool vs malloc, ring vs
// syscall -- are the point.

#include <atomic>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "calais/core/histogram.h"
#include "calais/core/latency.h"
#include "calais/core/object_pool.h"
#include "calais/core/shm_ring.h"
#include "calais/core/spsc_ring.h"
#include "calais/net/ws_frame.h"
#include "calais/net/ws_handshake.h"
#include "calais/net/ws_message.h"
#include "calais/venue/deribit/deribit_builder.h"
#include "calais/venue/binance/binance_parser.h"
#include "calais/venue/bybit/bybit_parser.h"
#include "calais/venue/deribit/deribit_parser.h"
#include "calais/venue/okx/okx_parser.h"
#include "calais/transport/hot_messages.h"
#include "calais/transport/wire.h"

using namespace calais;
using core::Histogram;
using core::now_ticks;
using core::Ticks;

namespace {

// Stops the optimiser deleting work whose result is unused, without the
// barrier itself costing anything measurable.
template <typename T>
void keep(T&& value) {
  __asm__ __volatile__("" : : "r,m"(value) : "memory");
}

void header() {
  std::printf("%-42s %9s %9s %9s %11s %10s\n", "", "p50", "p99", "p99.9", "max",
              "n");
}

// Per-iteration histograms hold TICKS.
void report(const char* name, const Histogram& h) {
  const double k = core::clock_info().ns_per_tick;
  std::printf("%-42s %9.1f %9.1f %9.1f %11.1f %10llu\n", name,
              static_cast<double>(h.p50()) * k, static_cast<double>(h.p99()) * k,
              static_cast<double>(h.p999()) * k,
              static_cast<double>(h.max()) * k,
              static_cast<unsigned long long>(h.count()));
}

// Amortised histograms hold PICOSECONDS per operation, so sub-nanosecond costs
// survive the integer bucket.
void report_amortised(const char* name, const Histogram& h) {
  std::printf("%-42s %9.2f %9s %9s %11s %10llu  [amortised]\n", name,
              static_cast<double>(h.p50()) / 1000.0, "-", "-", "-",
              static_cast<unsigned long long>(h.count()));
}

template <typename F>
Histogram measure(int iters, F&& body) {
  Histogram h(60'000'000'000ULL, 3);
  for (int i = 0; i < iters / 10 + 1000; ++i) {
    body();
  }
  for (int i = 0; i < iters; ++i) {
    const Ticks t0 = now_ticks();
    body();
    const Ticks t1 = now_ticks();
    h.record(t1 - t0);
  }
  return h;
}

template <typename F>
Histogram measure_amortised(int batches, int batch, F&& body) {
  Histogram h(60'000'000'000ULL, 3);
  const double ns_per_tick = core::clock_info().ns_per_tick;

  for (int i = 0; i < batch; ++i) {
    body();
  }
  for (int b = 0; b < batches; ++b) {
    const Ticks t0 = now_ticks();
    for (int i = 0; i < batch; ++i) {
      body();
    }
    const Ticks t1 = now_ticks();
    const double ps_per_op = static_cast<double>(t1 - t0) * ns_per_tick *
                             1000.0 / static_cast<double>(batch);
    h.record(static_cast<std::uint64_t>(ps_per_op));
  }
  return h;
}

void section(const char* title) {
  std::printf("\n=== %s ===\n", title);
  header();
}

// ---------------------------------------------------------------------------

void bench_clock() {
  section("clock");
  report_amortised("now_ticks()",
                   measure_amortised(2000, 1000, [] { keep(now_ticks()); }));
  report_amortised("now_ticks_serialized()",
                   measure_amortised(2000, 1000,
                                     [] { keep(core::now_ticks_serialized()); }));
  report_amortised("wall_clock_ns()  [vDSO]",
                   measure_amortised(2000, 1000,
                                     [] { keep(core::wall_clock_ns()); }));
}

void bench_decimal() {
  section("fixed point vs double");

  const std::string text = "64000.5";
  report_amortised("Price::from_string(\"64000.5\")",
                   measure_amortised(2000, 1000,
                                     [&] { keep(core::Price::from_string(text)); }));
  report_amortised("std::stod(\"64000.5\")",
                   measure_amortised(2000, 1000, [&] { keep(std::stod(text)); }));

  const core::Price p = *core::Price::from_string("64000.5");
  char buf[core::Price::kMaxChars];
  report_amortised("Price::write() -> chars",
                   measure_amortised(2000, 1000,
                                     [&] { keep(p.write(buf, sizeof(buf))); }));

  const double d = 64000.5;
  report_amortised("snprintf(\"%.9g\", double)",
                   measure_amortised(1000, 500, [&] {
                     keep(std::snprintf(buf, sizeof(buf), "%.9g", d));
                   }));

  const core::Price a = *core::Price::from_string("0.0345");
  const core::Price b = *core::Price::from_string("2.5");
  report_amortised("Price::mul() [__int128, saturating]",
                   measure_amortised(2000, 1000, [&] { keep(a.mul(b)); }));
}

void bench_histogram() {
  section("histogram");
  Histogram target(60'000'000'000ULL, 3);
  std::uint64_t v = 1;
  report_amortised("Histogram::record()", measure_amortised(2000, 1000, [&] {
                     v = v * 6364136223846793005ULL + 1442695040888963407ULL;
                     target.record(v % 1'000'000);
                   }));
}

void bench_object_pool() {
  section("object pool vs new/delete (128B object)");

  struct Node {
    std::uint64_t a = 0;
    std::uint64_t b = 0;
    char pad[112] = {};
  };

  core::ObjectPool<Node> pool(1024);
  report_amortised("ObjectPool acquire+release", measure_amortised(2000, 1000, [&] {
                     Node* n = pool.acquire();
                     keep(n);
                     pool.release(n);
                   }));
  report_amortised("ObjectPool acquire_dirty+release",
                   measure_amortised(2000, 1000, [&] {
                     Node* n = pool.acquire_dirty();
                     keep(n);
                     pool.release(n);
                   }));
  report_amortised("new + delete", measure_amortised(2000, 1000, [&] {
                     Node* n = new Node();
                     keep(n);
                     delete n;
                   }));
}

void bench_spsc_ring() {
  section("in-process SPSC ring");

  struct Item {
    std::uint64_t a;
    std::uint64_t b;
  };
  core::SpscRing<Item, 1024> ring;

  report_amortised("push + pop, single thread",
                   measure_amortised(2000, 1000, [&] {
                     Item in{1, 2};
                     ring.try_push(in);
                     Item out{};
                     ring.try_pop(out);
                     keep(out.a);
                   }));

  // Real two-thread round trip: A pings, B pongs, A measures. This one has a
  // genuine tail -- it crosses a cache line between cores.
  {
    core::SpscRing<Ticks, 1024> to_b;
    core::SpscRing<Ticks, 1024> to_a;
    std::atomic<bool> stop{false};

    std::thread responder([&] {
      Ticks t = 0;
      while (!stop.load(std::memory_order_relaxed)) {
        if (to_b.try_pop(t)) {
          while (!to_a.try_push(t)) {
            core::cpu_pause();
          }
        } else {
          core::cpu_pause();
        }
      }
    });

    Histogram rtt(60'000'000'000ULL, 3);
    for (int i = 0; i < 200'000; ++i) {
      const Ticks t0 = now_ticks();
      while (!to_b.try_push(t0)) {
        core::cpu_pause();
      }
      Ticks echoed = 0;
      while (!to_a.try_pop(echoed)) {
        core::cpu_pause();
      }
      rtt.record(now_ticks() - t0);
    }

    stop.store(true, std::memory_order_relaxed);
    responder.join();
    report("thread-to-thread round trip", rtt);
  }
}

void bench_shm_ring() {
  section("cross-process SPSC ring (shared memory)");

  const std::string path = "/tmp/calais_bench_ring";
  core::ShmRing ring =
      core::ShmRing::create(path, transport::kMaxHotMessageBytes, 1024);
  ring.set_unlink_on_destroy(true);

  transport::PlaceOrderMsg msg{};
  models::OrderRequest req;
  req.instrument = "BTC-27JUN25-100000-C";
  req.amount = *core::Qty::from_string("2.5");
  req.price = *core::Price::from_string("0.0345");
  req.internal_order_id = "0123456789abcdef0123456789abcdef";
  req.strategy_id = "alpha";
  transport::encode_place_order(req, "deribit", 1, msg);

  char buf[transport::kMaxHotMessageBytes];
  std::uint32_t len = 0;

  report_amortised("push + pop (216B msg)", measure_amortised(2000, 1000, [&] {
                     ring.try_push(&msg, sizeof(msg));
                     ring.try_pop(buf, sizeof(buf), len);
                     keep(len);
                   }));

  report_amortised("zero-copy acquire/commit", measure_amortised(2000, 1000, [&] {
                     void* slot = ring.try_acquire_write();
                     if (slot != nullptr) {
                       std::memcpy(slot, &msg, sizeof(msg));
                       ring.commit_write(sizeof(msg));
                     }
                     std::uint32_t rlen = 0;
                     if (ring.try_acquire_read(rlen) != nullptr) {
                       ring.commit_read();
                     }
                     keep(rlen);
                   }));
}

// The comparison the whole architecture rests on.
// The real receive path for one venue, end to end.
void bench_venue() {
  section("Deribit venue layer (real feed shapes)");

  const std::string order_frame =
      R"({"jsonrpc":"2.0","method":"subscription","params":{"channel":"user.orders.BTC-27JUN25-100000-C.raw",)"
      R"("data":{"order_id":"DERIBIT-12345","instrument_name":"BTC-27JUN25-100000-C",)"
      R"("direction":"buy","order_type":"limit","amount":2.5,"filled_amount":1.25,)"
      R"("price":0.0345,"average_price":0.0344,"order_state":"open","label":"chase-maker",)"
      R"("post_only":true,"reject_post_only":false,)"
      R"("creation_timestamp":1786106096123,"last_update_timestamp":1786106096456}}})";

  struct Sink {
    std::uint64_t orders = 0;
    std::uint64_t fills = 0;
    void on_order(const transport::OrderUpdateMsg& m) { orders += m.hdr.seq; }
    void on_fill(const transport::FillMsg& m) { fills += m.hdr.seq; }
    void on_account(const models::AccountSummary&) {}
  };

  // Padded exactly as the receive buffer will be.
  std::string padded(order_frame);
  const std::size_t len = padded.size();
  padded.resize(len + venue::kJsonPadding, '\0');

  venue::deribit::DeribitParser parser;
  Sink sink;
  report_amortised("parse order update -> OrderUpdateMsg",
                   measure_amortised(2000, 500, [&] {
                     keep(parser.parse(padded.data(), len, padded.size(), sink)
                              .emitted);
                   }));

  const std::string trade_frame =
      R"({"jsonrpc":"2.0","method":"subscription","params":{"channel":"user.trades.BTC-27JUN25-100000-C.raw",)"
      R"("data":[{"trade_id":"TRADE-9876","order_id":"DERIBIT-12345",)"
      R"("instrument_name":"BTC-27JUN25-100000-C","direction":"buy","amount":1.25,)"
      R"("price":0.0344,"fee":0.0000123,"fee_currency":"BTC","liquidity":"M",)"
      R"("timestamp":1786106096456,"index_price":64000.5,"mark_price":64000.6}]}})";
  std::string trade_padded(trade_frame);
  const std::size_t trade_len = trade_padded.size();
  trade_padded.resize(trade_len + venue::kJsonPadding, '\0');

  report_amortised("parse trade frame -> FillMsg",
                   measure_amortised(2000, 500, [&] {
                     keep(parser
                              .parse(trade_padded.data(), trade_len,
                                     trade_padded.size(), sink)
                              .emitted);
                   }));

  venue::deribit::DeribitBuilder builder;
  models::OrderRequest req;
  req.instrument = "BTC-27JUN25-100000-C";
  req.side = models::OrderSide::kBuy;
  req.amount = *core::Qty::from_string("2.5");
  req.order_type = models::OrderType::kLimit;
  req.price = *core::Price::from_string("0.0345");
  req.label = "chase-maker";
  req.post_only = true;

  char out[venue::deribit::kMaxRequestBytes];
  std::int64_t id = 0;
  report_amortised("build private/buy request",
                   measure_amortised(2000, 500, [&] {
                     keep(builder.place_order(out, sizeof(out), req, id));
                   }));

  report_amortised("build private/cancel request",
                   measure_amortised(2000, 500, [&] {
                     keep(builder.cancel_order(out, sizeof(out), "DERIBIT-12345",
                                               id));
                   }));

  const std::size_t n = builder.place_order(out, sizeof(out), req, id);
  std::printf("\norder request is %zu bytes; keep(sink)=%llu\n", n,
              static_cast<unsigned long long>(sink.orders + sink.fills));
}

// All four venues on their own real feed shape, so the numbers are comparable.
void bench_all_venues() {
  section("order-update parse, all four venues");

  struct Sink {
    std::uint64_t n = 0;
    void on_order(const transport::OrderUpdateMsg& m) { n += m.hdr.seq; }
    void on_fill(const transport::FillMsg& m) { n += m.hdr.seq; }
    void on_account(const models::AccountSummary&) {}
  };
  Sink sink;

  auto pad = [](std::string s) {
    const std::size_t len = s.size();
    s.resize(len + venue::kJsonPadding, '\0');
    return std::pair<std::string, std::size_t>{std::move(s), len};
  };

  auto [deribit_buf, deribit_len] = pad(
      R"({"jsonrpc":"2.0","method":"subscription","params":{"channel":"user.orders.BTC-27JUN25-100000-C.raw",)"
      R"("data":{"order_id":"DERIBIT-12345","instrument_name":"BTC-27JUN25-100000-C",)"
      R"("direction":"buy","order_type":"limit","amount":2.5,"filled_amount":1.25,)"
      R"("price":0.0345,"average_price":0.0344,"order_state":"open","label":"chase",)"
      R"("creation_timestamp":1786106096123,"last_update_timestamp":1786106096456}}})");

  auto [binance_buf, binance_len] = pad(
      R"({"e":"ORDER_TRADE_UPDATE","T":1786106096123,"E":1786106096124,)"
      R"("o":{"s":"BTCUSDT","c":"chase","S":"BUY","o":"LIMIT","f":"GTX","q":"2.5",)"
      R"("p":"64000.5","ap":"64000.25","x":"NEW","X":"NEW","i":12345678,"l":"0",)"
      R"("z":"1.25","L":"0","n":"0","N":"USDT","T":1786106096123,"t":0,"m":false}})");

  auto [bybit_buf, bybit_len] = pad(
      R"({"topic":"order","id":"x","creationTime":1786106096123,"data":[{)"
      R"("category":"linear","symbol":"BTCUSDT","orderId":"BYBIT-1","side":"Buy",)"
      R"("orderType":"Limit","qty":"2.5","price":"64000.5","cumExecQty":"1.25",)"
      R"("avgPrice":"64000.25","orderStatus":"PartiallyFilled","orderLinkId":"chase",)"
      R"("createdTime":"1786106096123","updatedTime":"1786106096456"}]})");

  auto [okx_buf, okx_len] = pad(
      R"({"arg":{"channel":"orders","instType":"SWAP"},"data":[{)"
      R"("instId":"BTC-USDT-SWAP","ordId":"OKX-1","clOrdId":"chase","side":"buy",)"
      R"("ordType":"limit","sz":"2.5","px":"64000.5","accFillSz":"1.25",)"
      R"("avgPx":"64000.25","state":"partially_filled","cTime":"1786106096123",)"
      R"("uTime":"1786106096456"}]})");

  venue::deribit::DeribitParser deribit;
  venue::binance::BinanceParser binance;
  venue::bybit::BybitParser bybit;
  venue::okx::OkxParser okx;

  report_amortised("deribit (options, bare JSON numbers)",
                   measure_amortised(2000, 500, [&] {
                     keep(deribit.parse(deribit_buf.data(), deribit_len,
                                        deribit_buf.size(), sink).emitted);
                   }));
  report_amortised("binance (perp, quoted numbers)",
                   measure_amortised(2000, 500, [&] {
                     keep(binance.parse(binance_buf.data(), binance_len,
                                        binance_buf.size(), sink).emitted);
                   }));
  report_amortised("bybit   (perp, quoted numbers + ts)",
                   measure_amortised(2000, 500, [&] {
                     keep(bybit.parse(bybit_buf.data(), bybit_len,
                                      bybit_buf.size(), sink).emitted);
                   }));
  report_amortised("okx     (perp, quoted numbers + ts)",
                   measure_amortised(2000, 500, [&] {
                     keep(okx.parse(okx_buf.data(), okx_len, okx_buf.size(),
                                    sink).emitted);
                   }));
  std::printf("\npayload sizes: deribit %zu, binance %zu, bybit %zu, okx %zu bytes\n",
              deribit_len, binance_len, bybit_len, okx_len);
}

void bench_hot_vs_json() {
  section("hot path (binary) vs control plane (JSON)");

  models::OrderRequest req;
  req.instrument = "BTC-27JUN25-100000-C";
  req.side = models::OrderSide::kBuy;
  req.amount = *core::Qty::from_string("2.5");
  req.order_type = models::OrderType::kLimit;
  req.price = *core::Price::from_string("0.0345");
  req.label = "chase-maker";
  req.internal_order_id = "0123456789abcdef0123456789abcdef";
  req.strategy_id = "alpha";

  transport::PlaceOrderMsg msg{};
  report_amortised("encode PlaceOrderMsg (binary)",
                   measure_amortised(2000, 1000, [&] {
                     keep(transport::encode_place_order(req, "deribit", 1, msg));
                   }));

  transport::encode_place_order(req, "deribit", 1, msg);
  report_amortised("consume PlaceOrderMsg (cast + read)",
                   measure_amortised(2000, 1000, [&] {
                     const auto* m =
                         reinterpret_cast<const transport::PlaceOrderMsg*>(&msg);
                     keep(m->price_raw);
                   }));
  report_amortised("decode PlaceOrderMsg -> model (allocs)",
                   measure_amortised(2000, 500,
                                     [&] { keep(transport::decode_place_order(msg)); }));

  // JSON is microseconds -- well above the counter resolution, so these get
  // real per-iteration tails.
  report("serialise OrderRequest (JSON)",
         measure(200'000, [&] { keep(transport::to_json(req).dump()); }));

  const std::string json_bytes = transport::to_json(req).dump();
  report("parse OrderRequest (JSON, DOM)", measure(200'000, [&] {
           keep(transport::order_request_from_json(
               transport::Json::parse(json_bytes)));
         }));

  models::Order order;
  order.order_id = "DERIBIT-12345";
  order.exchange = "deribit";
  order.instrument = "BTC-27JUN25-100000-C";
  order.amount = *core::Qty::from_string("2.5");
  order.price = *core::Price::from_string("0.0345");
  order.filled_amount = *core::Qty::from_string("1.25");
  order.strategy_id = "alpha";
  order.internal_order_id = "0123456789abcdef0123456789abcdef";

  const std::string order_json = transport::to_json(order).dump();
  report("parse Order (JSON, DOM)", measure(200'000, [&] {
           keep(transport::order_from_json(transport::Json::parse(order_json)));
         }));

  std::printf("\nwire size: PlaceOrderMsg %zu bytes, equivalent JSON %zu bytes\n",
              sizeof(transport::PlaceOrderMsg), json_bytes.size());
}

void bench_websocket() {
  section("websocket protocol layer");

  // A realistic exchange payload: a Deribit-shaped order update.
  const std::string json =
      R"({"jsonrpc":"2.0","method":"subscription","params":{"channel":"user.orders.BTC-27JUN25-100000-C.raw",)"
      R"("data":{"order_id":"DERIBIT-12345","instrument_name":"BTC-27JUN25-100000-C","order_state":"open",)"
      R"("price":0.0345,"amount":2.5,"filled_amount":1.25,"direction":"buy","label":"chase-maker"}}})";

  std::vector<std::byte> frame(json.size() + net::kMaxFrameHeaderSize);

  // Server-to-client frames are unmasked, which is what we actually decode.
  std::vector<std::byte> server_frame(json.size() + net::kMaxFrameHeaderSize);
  const std::size_t hdr_n =
      net::ws_encode_header(server_frame.data(), server_frame.size(),
                            net::WsOpcode::kText, true, json.size(), false, 0);
  std::memcpy(server_frame.data() + hdr_n, json.data(), json.size());
  const std::size_t server_frame_size = hdr_n + json.size();

  report_amortised("decode frame header (server->client)",
                   measure_amortised(2000, 1000, [&] {
                     keep(net::ws_decode_frame(server_frame.data(),
                                               server_frame_size));
                   }));

  report_amortised("encode frame (client->server, masked)",
                   measure_amortised(2000, 1000, [&] {
                     keep(net::ws_encode_frame(
                         frame.data(), frame.size(), net::WsOpcode::kText, true,
                         json.data(), json.size(), 0x37fa213d));
                   }));

  std::vector<std::byte> payload(json.size());
  std::memcpy(payload.data(), json.data(), json.size());
  report_amortised("mask payload (word-at-a-time)",
                   measure_amortised(2000, 1000, [&] {
                     net::ws_mask(payload.data(), payload.size(), 0x37fa213d);
                     keep(payload[0]);
                   }));

  report_amortised("validate UTF-8", measure_amortised(2000, 1000, [&] {
                     keep(net::ws_is_valid_utf8(payload.data(), payload.size()));
                   }));

  // The whole receive path a message takes before the JSON parser sees it.
  net::WsMessageAssembler assembler;
  report_amortised("decode + assemble (full rx path)",
                   measure_amortised(2000, 1000, [&] {
                     const auto r = net::ws_decode_frame(server_frame.data(),
                                                         server_frame_size);
                     const auto e = assembler.feed(r.header, r.payload);
                     keep(e.payload_size);
                   }));

  std::printf("\npayload %zu bytes, framed %zu bytes (%zu bytes of overhead)\n",
              json.size(), server_frame_size, server_frame_size - json.size());

  // The handshake runs once per connection, so its cost is irrelevant to the
  // steady state -- measured only to confirm it is not pathological.
  net::WsHandshakeRequest req;
  req.host = "www.deribit.com";
  req.target = "/ws/api/v2";
  report("build handshake (once per connection)",
         measure(20'000, [&] { keep(net::ws_build_handshake(req).request.size()); }));
}

}  // namespace

int main() {
  const auto& info = core::clock_info();
  std::printf("clock:   source=%s nominal=%lluHz\n", info.source,
              static_cast<unsigned long long>(info.hz));
  std::printf("         measured granularity=%llu ticks = %.2fns\n",
              static_cast<unsigned long long>(info.tick_granularity),
              info.resolution_ns);
  std::printf("machine: %d cores, %zu-byte cache line\n",
              core::online_core_count(), core::kCacheLineSize);

  const auto pin = core::pin_current_thread_to_core(0);
  std::printf("pinning: %s\n", pin.detail.c_str());
  if (!pin.ok) {
    std::printf(
        "\nNOTE: unpinned run. Tail percentiles include scheduler noise and\n"
        "      are not representative of a tuned production host.\n");
  }
  if (info.resolution_ns > 5.0) {
    std::printf(
        "NOTE: counter resolution is %.1fns, so operations faster than that\n"
        "      are measured in batches and reported as [amortised] -- p50\n"
        "      only, no meaningful tail.\n",
        info.resolution_ns);
  }
  std::printf("\nall figures in nanoseconds per operation\n");

  bench_clock();
  bench_decimal();
  bench_histogram();
  bench_object_pool();
  bench_spsc_ring();
  bench_shm_ring();
  bench_websocket();
  bench_venue();
  bench_all_venues();
  bench_hot_vs_json();

  std::printf("\n");
  return 0;
}
