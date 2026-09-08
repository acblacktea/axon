// Thin wrapper over simdjson for exchange feed parsing.
//
// WHY A WRAPPER, given that simdjson is already the fastest option: the same
// reason the Python side wraps prometheus_client in a MetricsClient. simdjson's
// On-Demand API is a value proposition with sharp edges -- forward-only
// iteration, error codes on every accessor, a padding requirement on the input
// buffer, and objects that are invalidated as soon as you move past them. Those
// constraints belong in one file, not sprinkled through four venue parsers
// where each one will get them subtly wrong.
//
// The wrapper is header-only and every method is a thin forwarding call, so it
// costs nothing at runtime. It exists to make the venue parsers read like the
// Python they are ported from.
//
// THREE SIMDJSON CONSTRAINTS THIS ENCAPSULATES
//
//   1. PADDING. On-Demand reads up to SIMDJSON_PADDING bytes past the end of
//      the document. Feeding it a buffer without that slack is a heap overread
//      -- the kind ASan catches and production does not. `required_padding()`
//      and the checks in Document::parse make that impossible to forget.
//
//   2. FORWARD-ONLY ITERATION. Requesting fields in a different order from the
//      document costs a rescan, turning an O(n) parse into O(n*fields). The
//      venue parsers therefore read fields in document order, and each one says
//      so where it matters.
//
//   3. LIFETIME. Every string_view points into the ORIGINAL buffer, not into
//      the parser. That is a feature -- it is why simdjson has none of the
//      dangling-view traps rapidjson's SAX interface has -- but it means the
//      receive buffer must outlive anything extracted from it. Callers copy
//      into fixed storage before the buffer is reused.
//
// NOT thread safe. One Document (and its parser) per connection thread.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include <simdjson.h>

#include "axon/core/decimal.h"
#include "axon/core/timestamp.h"
#include "axon/venue/json_padding.h"

namespace axon::venue {

// Bytes of readable slack simdjson requires past the end of a document.
// Receive buffers must reserve this beyond their usable capacity.
inline constexpr std::size_t kJsonPadding = simdjson::SIMDJSON_PADDING;

// net/json_padding.h duplicates this number so the network layer does not have
// to include simdjson. This keeps the two from drifting apart -- if simdjson
// ever raises its padding, the build breaks here rather than producing a heap
// overread at runtime.
static_assert(net::kJsonParserPadding >= simdjson::SIMDJSON_PADDING,
              "net::kJsonParserPadding is smaller than simdjson requires; "
              "receive buffers would be under-padded");

class Object;
class Array;

// A single JSON value. Every accessor returns nullopt rather than throwing:
// a malformed or unexpected field from an exchange is an ordinary event, and
// the receive loop must stay free of try/catch.
class Value {
 public:
  Value() = default;
  explicit Value(simdjson::ondemand::value v) noexcept : v_(v), valid_(true) {}

  bool valid() const noexcept { return valid_; }

  std::optional<std::string_view> as_string() noexcept {
    if (!valid_) {
      return std::nullopt;
    }
    std::string_view out;
    if (v_.get_string().get(out) != simdjson::SUCCESS) {
      return std::nullopt;
    }
    return out;
  }

  std::optional<double> as_double() noexcept {
    if (!valid_) {
      return std::nullopt;
    }
    double out = 0;
    if (v_.get_double().get(out) != simdjson::SUCCESS) {
      return std::nullopt;
    }
    return out;
  }

  // Accepts a JSON integer OR an integer quoted as a string.
  //
  // Not laxness for its own sake: Bybit and OKX quote every numeric field
  // including their millisecond timestamps ("createdTime":"1786106096123"),
  // while Deribit and Binance's event times are bare numbers. One accessor
  // that handles both is what lets four venue parsers share this file instead
  // of each inventing its own coercion.
  std::optional<std::int64_t> as_int() noexcept {
    if (!valid_) {
      return std::nullopt;
    }
    std::int64_t out = 0;
    if (v_.get_int64().get(out) == simdjson::SUCCESS) {
      return out;
    }
    std::string_view sv;
    if (v_.get_string().get(sv) != simdjson::SUCCESS) {
      return std::nullopt;
    }
    return parse_int(sv);
  }

  std::optional<bool> as_bool() noexcept {
    if (!valid_) {
      return std::nullopt;
    }
    bool out = false;
    if (v_.get_bool().get(out) != simdjson::SUCCESS) {
      return std::nullopt;
    }
    return out;
  }

  // Decimal from a JSON number, via its RAW TEXT rather than via a double.
  //
  // This is the whole reason prices go through here instead of as_double():
  // simdjson can hand back the number's original characters, so "0.0345"
  // becomes exactly 34500000 raw units with no float ever involved. Exchanges
  // that quote numbers as strings (Binance, Bybit) land on the same path.
  std::optional<core::Price> as_decimal() noexcept {
    if (!valid_) {
      return std::nullopt;
    }
    // A JSON string value: parse its contents directly.
    //
    // An EMPTY string is treated as absent, not as zero. OKX sends "px":"" and
    // "avgPx":"" for an order that has no price yet, and reading that as 0
    // would turn "unpriced" into "priced at zero" -- which downstream looks
    // like a real order at an impossible price.
    std::string_view sv;
    if (v_.get_string().get(sv) == simdjson::SUCCESS) {
      if (sv.empty()) {
        return std::nullopt;
      }
      return core::Price::from_string(sv);
    }
    // A JSON number: take the raw token text, still exact.
    const std::string_view raw = v_.raw_json_token();
    return core::Price::from_string(trim(raw));
  }

  // Milliseconds since epoch -- what every venue we support uses.
  std::optional<core::Timestamp> as_millis_timestamp() noexcept {
    const auto ms = as_int();
    if (!ms.has_value()) {
      return std::nullopt;
    }
    return core::Timestamp::from_millis(*ms);
  }

  bool is_null() noexcept { return valid_ && v_.is_null().value_unsafe(); }

  std::optional<Object> as_object() noexcept;
  std::optional<Array> as_array() noexcept;

 private:
  static std::optional<std::int64_t> parse_int(std::string_view s) noexcept {
    s = trim(s);
    if (s.empty()) {
      return std::nullopt;
    }
    std::size_t i = 0;
    bool negative = false;
    if (s[i] == '+' || s[i] == '-') {
      negative = (s[i] == '-');
      ++i;
    }
    if (i >= s.size()) {
      return std::nullopt;
    }
    std::int64_t v = 0;
    for (; i < s.size(); ++i) {
      if (s[i] < '0' || s[i] > '9') {
        return std::nullopt;
      }
      if (v > (INT64_MAX - 9) / 10) {
        return std::nullopt;
      }
      v = v * 10 + static_cast<std::int64_t>(s[i] - '0');
    }
    return negative ? -v : v;
  }

  // raw_json_token() can include trailing whitespace or a separator.
  static std::string_view trim(std::string_view s) noexcept {
    std::size_t b = 0;
    while (b < s.size() && (s[b] == ' ' || s[b] == '\t' || s[b] == '\n' ||
                            s[b] == '\r')) {
      ++b;
    }
    std::size_t e = s.size();
    while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\n' ||
                     s[e - 1] == '\r' || s[e - 1] == ',')) {
      --e;
    }
    return s.substr(b, e - b);
  }

  simdjson::ondemand::value v_{};
  bool valid_ = false;
};

class Object {
 public:
  Object() = default;
  explicit Object(simdjson::ondemand::object o) noexcept
      : o_(o), valid_(true) {}

  bool valid() const noexcept { return valid_; }

  // Look up a field. Absent or erroring fields yield an invalid Value, so a
  // parser can chain lookups and check once at the end.
  //
  // Ask for fields in DOCUMENT ORDER. On-Demand searches forward from the
  // current position and only rewinds when it has to; out-of-order access
  // silently degrades to a rescan per field.
  Value operator[](std::string_view key) noexcept {
    if (!valid_) {
      return {};
    }
    simdjson::ondemand::value v;
    if (o_[key].get(v) != simdjson::SUCCESS) {
      return {};
    }
    return Value{v};
  }

  simdjson::ondemand::object& raw() noexcept { return o_; }

 private:
  simdjson::ondemand::object o_{};
  bool valid_ = false;
};

class Array {
 public:
  Array() = default;
  explicit Array(simdjson::ondemand::array a) noexcept : a_(a), valid_(true) {}

  bool valid() const noexcept { return valid_; }

  // Calls `fn(Object&)` for each element that is an object. Returns the number
  // of elements visited. Iteration is single-pass; the array cannot be
  // revisited.
  template <typename Fn>
  std::size_t for_each_object(Fn&& fn) noexcept {
    if (!valid_) {
      return 0;
    }
    std::size_t n = 0;
    for (auto element : a_) {
      simdjson::ondemand::object obj;
      if (element.get_object().get(obj) != simdjson::SUCCESS) {
        continue;
      }
      Object wrapped{obj};
      fn(wrapped);
      ++n;
    }
    return n;
  }

 private:
  simdjson::ondemand::array a_{};
  bool valid_ = false;
};

inline std::optional<Object> Value::as_object() noexcept {
  if (!valid_) {
    return std::nullopt;
  }
  simdjson::ondemand::object o;
  if (v_.get_object().get(o) != simdjson::SUCCESS) {
    return std::nullopt;
  }
  return Object{o};
}

inline std::optional<Array> Value::as_array() noexcept {
  if (!valid_) {
    return std::nullopt;
  }
  simdjson::ondemand::array a;
  if (v_.get_array().get(a) != simdjson::SUCCESS) {
    return std::nullopt;
  }
  return Array{a};
}

// Owns the parser. Construct once per connection and reuse -- the parser holds
// the scratch buffers that make On-Demand allocation-free after the first
// document, so creating one per message would give back most of the win.
class Document {
 public:
  Document() = default;

  // Pre-size the parser's internal buffers so no message ever triggers a
  // reallocation mid-parse.
  explicit Document(std::size_t max_message_bytes) {
    // Ignoring the error: allocation failure here surfaces on the first
    // parse(), which is where the caller is already checking.
    static_cast<void>(parser_.allocate(max_message_bytes));
  }

  // Parses `json`, which MUST have at least kJsonPadding readable bytes past
  // its end. Returns nullopt on malformed input.
  //
  // The padded_string_view form is used rather than the copying one on
  // purpose: it parses straight out of the receive buffer.
  std::optional<Object> parse(const char* data, std::size_t len,
                              std::size_t capacity) noexcept {
    if (capacity < len + kJsonPadding) {
      return std::nullopt;
    }
    const simdjson::padded_string_view view(data, len, capacity);
    if (parser_.iterate(view).get(doc_) != simdjson::SUCCESS) {
      return std::nullopt;
    }
    simdjson::ondemand::object obj;
    if (doc_.get_object().get(obj) != simdjson::SUCCESS) {
      return std::nullopt;
    }
    return Object{obj};
  }

  // Convenience for tests and cold paths: copies into a padded buffer first.
  std::optional<Object> parse_copy(std::string_view json) {
    scratch_.assign(json.begin(), json.end());
    scratch_.resize(json.size() + kJsonPadding, '\0');
    return parse(scratch_.data(), json.size(), scratch_.size());
  }

 private:
  simdjson::ondemand::parser parser_;
  simdjson::ondemand::document doc_;
  std::string scratch_;
};

}  // namespace axon::venue
