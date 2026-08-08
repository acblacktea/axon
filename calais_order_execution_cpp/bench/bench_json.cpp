// JSON parser comparison, on the payload that actually matters.
//
// The question this answers: what should parse the EXCHANGE FEED? That is the
// only JSON on the hot path -- the ZMQ control plane is cold and its parser
// choice is irrelevant, and outbound order messages should not go through a
// JSON library at all (see the template benchmark at the bottom).
//
// Realistic workload, not a synthetic one:
//   * a Deribit-shaped order update, ~450 bytes, nested two levels
//   * we extract SIX fields, which is what an OMS actually needs -- order id,
//     state, price, amount, filled amount, direction. Nobody needs the whole
//     document, and a benchmark that materialises one measures the wrong thing.
//
// That last point is the crux. DOM parsers pay to build the entire tree
// whether you read one field or fifty. On-demand and SAX parsers do not. On a
// message where we want 6 of ~15 fields, that difference dominates any
// difference in raw parsing speed.

#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <nlohmann/json.hpp>

#include <rapidjson/document.h>
#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <simdjson.h>

#include "calais/core/clock.h"
#include "calais/core/decimal.h"
#include "calais/core/histogram.h"

using namespace calais;
using core::Histogram;
using core::now_ticks;
using core::Ticks;

namespace {

template <typename T>
void keep(T&& value) {
  __asm__ __volatile__("" : : "r,m"(value) : "memory");
}

// A Deribit user.orders subscription message.
const std::string kFeedMessage =
    R"({"jsonrpc":"2.0","method":"subscription","params":{"channel":"user.orders.BTC-27JUN25-100000-C.raw",)"
    R"("data":{"order_id":"DERIBIT-12345","instrument_name":"BTC-27JUN25-100000-C","order_state":"open",)"
    R"("price":0.0345,"amount":2.5,"filled_amount":1.25,"direction":"buy","label":"chase-maker",)"
    R"("order_type":"limit","post_only":true,"reduce_only":false,"time_in_force":"good_til_cancelled",)"
    R"("creation_timestamp":1786106096123,"last_update_timestamp":1786106096456}}})";

// What the OMS actually needs out of that message.
//
// Strings are copied into fixed inline storage rather than kept as views. Two
// reasons, and the second one cost me a bug:
//
//   1. It is what the real consumer does -- these fields end up in an
//      OrderUpdateMsg's FixedString members, so the copy is part of the work
//      and belongs in the measurement.
//
//   2. A string_view into a parser's output does not outlive the parser, and
//      the lifetimes differ per library in ways that are easy to get wrong.
//      nlohmann and rapidjson DOM views die with the Document. rapidjson's SAX
//      Reader hands the callback a pointer into an internal stack buffer that
//      is reused within the same parse. simdjson and the hand-written scanner
//      point into the caller's buffer and are fine. The first version of this
//      benchmark returned views from all of them and silently compared
//      garbage.
struct Field {
  char data[48] = {};
  std::uint8_t len = 0;

  void assign(std::string_view v) noexcept {
    len = static_cast<std::uint8_t>(v.size() < sizeof(data) ? v.size()
                                                            : sizeof(data));
    std::memcpy(data, v.data(), len);
  }
  std::string_view view() const noexcept { return {data, len}; }
  bool operator==(const Field& o) const noexcept { return view() == o.view(); }
};

struct Extracted {
  Field order_id;
  Field order_state;
  Field direction;
  double price = 0;
  double amount = 0;
  double filled_amount = 0;
};

void report(const char* name, const Histogram& h, const char* note = "") {
  const double k = core::clock_info().ns_per_tick;
  std::printf("%-44s %9.1f %9.1f %10.1f  %s\n", name,
              static_cast<double>(h.p50()) * k, static_cast<double>(h.p99()) * k,
              static_cast<double>(h.p999()) * k, note);
}

void report_amortised(const char* name, const Histogram& h,
                      const char* note = "") {
  std::printf("%-44s %9.2f %9s %10s  %s\n", name,
              static_cast<double>(h.p50()) / 1000.0, "-", "-", note);
}

template <typename F>
Histogram measure(int iters, F&& body) {
  Histogram h(60'000'000'000ULL, 3);
  for (int i = 0; i < iters / 10 + 100; ++i) {
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
    h.record(static_cast<std::uint64_t>(static_cast<double>(t1 - t0) *
                                        ns_per_tick * 1000.0 /
                                        static_cast<double>(batch)));
  }
  return h;
}

// ---------------------------------------------------------------------------
// nlohmann, DOM
// ---------------------------------------------------------------------------
Extracted parse_nlohmann(const std::string& s) {
  Extracted e;
  const auto doc = nlohmann::json::parse(s, nullptr, false);
  if (doc.is_discarded()) {
    return e;
  }
  const auto& d = doc["params"]["data"];
  e.order_id.assign(d["order_id"].get_ref<const std::string&>());
  e.order_state.assign(d["order_state"].get_ref<const std::string&>());
  e.direction.assign(d["direction"].get_ref<const std::string&>());
  e.price = d["price"].get<double>();
  e.amount = d["amount"].get<double>();
  e.filled_amount = d["filled_amount"].get<double>();
  return e;
}

// ---------------------------------------------------------------------------
// rapidjson, DOM (in-situ and copying variants)
// ---------------------------------------------------------------------------
Extracted parse_rapidjson_dom(const std::string& s) {
  Extracted e;
  rapidjson::Document doc;
  doc.Parse(s.c_str());
  if (doc.HasParseError()) {
    return e;
  }
  const auto& d = doc["params"]["data"];
  e.order_id.assign({d["order_id"].GetString(), d["order_id"].GetStringLength()});
  e.order_state.assign(
      {d["order_state"].GetString(), d["order_state"].GetStringLength()});
  e.direction.assign(
      {d["direction"].GetString(), d["direction"].GetStringLength()});
  e.price = d["price"].GetDouble();
  e.amount = d["amount"].GetDouble();
  e.filled_amount = d["filled_amount"].GetDouble();
  return e;
}

// In-situ parsing writes NULs into the buffer and points values at it, which
// removes the string copies. It destroys the input, so the caller needs a
// scratch copy -- which for us is free, since the frame is already in a buffer
// we own.
Extracted parse_rapidjson_insitu(char* mutable_json) {
  Extracted e;
  rapidjson::Document doc;
  doc.ParseInsitu(mutable_json);
  if (doc.HasParseError()) {
    return e;
  }
  const auto& d = doc["params"]["data"];
  e.order_id.assign({d["order_id"].GetString(), d["order_id"].GetStringLength()});
  e.order_state.assign(
      {d["order_state"].GetString(), d["order_state"].GetStringLength()});
  e.direction.assign(
      {d["direction"].GetString(), d["direction"].GetStringLength()});
  e.price = d["price"].GetDouble();
  e.amount = d["amount"].GetDouble();
  e.filled_amount = d["filled_amount"].GetDouble();
  return e;
}

// ---------------------------------------------------------------------------
// rapidjson, SAX -- no DOM is built at all.
// ---------------------------------------------------------------------------
struct SaxHandler : rapidjson::BaseReaderHandler<rapidjson::UTF8<>, SaxHandler> {
  Extracted out;
  // The KEY must be copied too, not just the value.
  //
  // rapidjson's Reader decodes strings into an internal stack buffer that it
  // reuses within a single parse. Holding a string_view to the key survives
  // until the next string is decoded -- which is the VALUE, clobbering it. The
  // symptom is beautifully misleading: numeric fields come out correct
  // (decoding a number writes nothing to the stack, so the key survives) while
  // every string field silently comes out empty.
  Field pending_key;
  int depth = 0;
  bool in_data = false;

  bool Key(const char* str, rapidjson::SizeType len, bool) {
    pending_key.assign({str, len});
    if (pending_key.view() == "data") {
      in_data = true;
    }
    return true;
  }
  bool String(const char* str, rapidjson::SizeType len, bool) {
    if (!in_data) {
      return true;
    }
    const std::string_view v(str, len);
    // Copy here, not later: rapidjson's Reader hands us a pointer into an
    // internal stack buffer that it reuses before the parse finishes.
    if (pending_key.view() == "order_id") {
      out.order_id.assign(v);
    } else if (pending_key.view() == "order_state") {
      out.order_state.assign(v);
    } else if (pending_key.view() == "direction") {
      out.direction.assign(v);
    }
    return true;
  }
  bool Double(double d) {
    if (!in_data) {
      return true;
    }
    if (pending_key.view() == "price") {
      out.price = d;
    } else if (pending_key.view() == "amount") {
      out.amount = d;
    } else if (pending_key.view() == "filled_amount") {
      out.filled_amount = d;
    }
    return true;
  }
  bool Uint(unsigned v) { return Double(v); }
  bool Int(int v) { return Double(v); }
  bool Uint64(std::uint64_t v) { return Double(static_cast<double>(v)); }
  bool Int64(std::int64_t v) { return Double(static_cast<double>(v)); }
  bool StartObject() {
    ++depth;
    return true;
  }
  bool EndObject(rapidjson::SizeType) {
    --depth;
    return true;
  }
};

Extracted parse_rapidjson_sax(const std::string& s) {
  SaxHandler handler;
  rapidjson::Reader reader;
  rapidjson::StringStream ss(s.c_str());
  reader.Parse(ss, handler);
  return handler.out;
}

// ---------------------------------------------------------------------------
// simdjson On-Demand -- lazily walks the document, touching only what is asked
// for. Requires the input to be padded, which is why the caller keeps a
// padded_string.
// ---------------------------------------------------------------------------
Extracted parse_simdjson(simdjson::ondemand::parser& parser,
                         simdjson::padded_string& padded) {
  Extracted e;
  auto doc = parser.iterate(padded);
  auto data = doc["params"]["data"];

  std::string_view sv;
  if (data["order_id"].get_string().get(sv) == simdjson::SUCCESS) {
    e.order_id.assign(sv);
  }
  if (data["order_state"].get_string().get(sv) == simdjson::SUCCESS) {
    e.order_state.assign(sv);
  }
  double d = 0;
  if (data["price"].get_double().get(d) == simdjson::SUCCESS) {
    e.price = d;
  }
  if (data["amount"].get_double().get(d) == simdjson::SUCCESS) {
    e.amount = d;
  }
  if (data["filled_amount"].get_double().get(d) == simdjson::SUCCESS) {
    e.filled_amount = d;
  }
  if (data["direction"].get_string().get(sv) == simdjson::SUCCESS) {
    e.direction.assign(sv);
  }
  return e;
}

// ---------------------------------------------------------------------------
// Hand-written scanner: memmem for each key, then read the value.
//
// The floor. No generality, no validation, no error recovery -- it assumes a
// message shape the venue has already agreed to. Included to show what the
// libraries are actually costing, not as a recommendation: an exchange that
// adds a field or reorders one breaks this silently, which is a bad trade for
// a few hundred nanoseconds unless you are certain.
// ---------------------------------------------------------------------------
// memmem is POSIX, not in namespace std.
const char* find_key(const char* hay, std::size_t hay_len, const char* key,
                     std::size_t key_len) {
  const void* p = ::memmem(hay, hay_len, key, key_len);
  return static_cast<const char*>(p);
}

std::string_view scan_string(const char* p) {
  // p points just past the key's closing quote and colon; expects a quote.
  while (*p != '"') {
    ++p;
  }
  const char* start = ++p;
  while (*p != '"') {
    ++p;
  }
  return std::string_view(start, static_cast<std::size_t>(p - start));
}

double scan_number(const char* p) {
  while (*p == ':' || *p == ' ') {
    ++p;
  }
  return std::strtod(p, nullptr);
}

Extracted parse_handwritten(const std::string& s) {
  Extracted e;
  const char* base = s.data();
  const std::size_t n = s.size();

  if (const char* p = find_key(base, n, "\"order_id\"", 10)) {
    e.order_id.assign(scan_string(p + 10));
  }
  if (const char* p = find_key(base, n, "\"order_state\"", 13)) {
    e.order_state.assign(scan_string(p + 13));
  }
  if (const char* p = find_key(base, n, "\"direction\"", 11)) {
    e.direction.assign(scan_string(p + 11));
  }
  if (const char* p = find_key(base, n, "\"price\"", 7)) {
    e.price = scan_number(p + 7);
  }
  if (const char* p = find_key(base, n, "\"amount\"", 8)) {
    e.amount = scan_number(p + 8);
  }
  if (const char* p = find_key(base, n, "\"filled_amount\"", 15)) {
    e.filled_amount = scan_number(p + 15);
  }
  return e;
}

// ---------------------------------------------------------------------------
void verify_all_agree() {
  const Extracted reference = parse_nlohmann(kFeedMessage);

  auto check = [&](const char* who, const Extracted& e) {
    const bool ok = e.order_id == reference.order_id &&
                    e.order_state == reference.order_state &&
                    e.direction == reference.direction &&
                    e.price == reference.price && e.amount == reference.amount &&
                    e.filled_amount == reference.filled_amount;
    std::printf("  %-24s %s\n", who, ok ? "agrees" : "*** DISAGREES ***");
    if (!ok) {
      std::printf("      got id=%.*s state=%.*s dir=%.*s price=%g amount=%g filled=%g\n",
                  static_cast<int>(e.order_id.len), e.order_id.data,
                  static_cast<int>(e.order_state.len), e.order_state.data,
                  static_cast<int>(e.direction.len), e.direction.data,
                  e.price, e.amount, e.filled_amount);
    }
  };

  std::printf("correctness cross-check (all parsers must extract the same 6 fields):\n");
  std::printf("  %-24s id=%.*s state=%.*s dir=%.*s price=%g amount=%g filled=%g\n",
              "nlohmann (reference)",
              static_cast<int>(reference.order_id.len), reference.order_id.data,
              static_cast<int>(reference.order_state.len),
              reference.order_state.data,
              static_cast<int>(reference.direction.len),
              reference.direction.data, reference.price, reference.amount,
              reference.filled_amount);

  check("rapidjson DOM", parse_rapidjson_dom(kFeedMessage));

  std::vector<char> scratch(kFeedMessage.begin(), kFeedMessage.end());
  scratch.push_back('\0');
  check("rapidjson in-situ", parse_rapidjson_insitu(scratch.data()));

  check("rapidjson SAX", parse_rapidjson_sax(kFeedMessage));

  simdjson::ondemand::parser parser;
  simdjson::padded_string padded(kFeedMessage);
  check("simdjson on-demand", parse_simdjson(parser, padded));

  check("hand-written scanner", parse_handwritten(kFeedMessage));
  std::printf("\n");
}

}  // namespace

int main() {
  const auto& info = core::clock_info();
  std::printf("clock: %s, granularity %.2fns\n", info.source, info.resolution_ns);
  std::printf("payload: %zu bytes, extracting 6 of ~15 fields\n\n",
              kFeedMessage.size());

  verify_all_agree();

  std::printf("%-44s %9s %9s %10s\n", "parse exchange feed message", "p50", "p99",
              "p99.9");
  std::printf("%s\n", std::string(76, '-').c_str());

  report("nlohmann DOM",
         measure(100'000, [&] { keep(parse_nlohmann(kFeedMessage).price); }));

  report("rapidjson DOM",
         measure(100'000, [&] { keep(parse_rapidjson_dom(kFeedMessage).price); }));

  {
    std::vector<char> scratch(kFeedMessage.size() + 1);
    report("rapidjson DOM in-situ", measure(100'000, [&] {
             std::memcpy(scratch.data(), kFeedMessage.data(), kFeedMessage.size());
             scratch[kFeedMessage.size()] = '\0';
             keep(parse_rapidjson_insitu(scratch.data()).price);
           }), "includes the required buffer copy");
  }

  report("rapidjson SAX (no DOM)",
         measure(100'000, [&] { keep(parse_rapidjson_sax(kFeedMessage).price); }));

  {
    simdjson::ondemand::parser parser;
    simdjson::padded_string padded(kFeedMessage);
    report("simdjson on-demand", measure(100'000, [&] {
             keep(parse_simdjson(parser, padded).price);
           }), "needs SIMDJSON_PADDING past the buffer");
  }

  report_amortised("hand-written scanner",
                   measure_amortised(2000, 500,
                                     [&] { keep(parse_handwritten(kFeedMessage).price); }),
                   "no validation, breaks silently on schema drift");

  // -------------------------------------------------------------------------
  std::printf("\n%-44s %9s %9s %10s\n", "build an order message (outbound)", "p50",
              "p99", "p99.9");
  std::printf("%s\n", std::string(76, '-').c_str());

  report("nlohmann dump", measure(100'000, [&] {
           nlohmann::json j;
           j["method"] = "private/buy";
           j["params"]["instrument_name"] = "BTC-27JUN25-100000-C";
           j["params"]["amount"] = 2.5;
           j["params"]["price"] = 0.0345;
           j["params"]["type"] = "limit";
           j["params"]["post_only"] = true;
           keep(j.dump().size());
         }));

  report("rapidjson Writer", measure(100'000, [&] {
           rapidjson::StringBuffer buf;
           rapidjson::Writer<rapidjson::StringBuffer> w(buf);
           w.StartObject();
           w.Key("method");
           w.String("private/buy");
           w.Key("params");
           w.StartObject();
           w.Key("instrument_name");
           w.String("BTC-27JUN25-100000-C");
           w.Key("amount");
           w.Double(2.5);
           w.Key("price");
           w.Double(0.0345);
           w.Key("type");
           w.String("limit");
           w.Key("post_only");
           w.Bool(true);
           w.EndObject();
           w.EndObject();
           keep(buf.GetSize());
         }));

  // The approach that should actually ship: a byte template with the varying
  // fields written in by hand. No library is involved, and the fixed-point
  // price never becomes a double.
  {
    char out[512];
    const core::Price price = *core::Price::from_string("0.0345");
    const core::Qty amount = *core::Qty::from_string("2.5");

    report_amortised("template + Decimal::write", measure_amortised(2000, 500, [&] {
                       char* p = out;
                       auto put = [&p](std::string_view s) {
                         std::memcpy(p, s.data(), s.size());
                         p += s.size();
                       };
                       put(R"({"method":"private/buy","params":{"instrument_name":")");
                       put("BTC-27JUN25-100000-C");
                       put(R"(","amount":)");
                       p += amount.write(p, 32);
                       put(R"(,"price":)");
                       p += price.write(p, 32);
                       put(R"(,"type":"limit","post_only":true}})");
                       keep(static_cast<std::size_t>(p - out));
                     }), "what should ship on the hot path");
  }

  std::printf("\n");
  return 0;
}
