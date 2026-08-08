// Bounds-checked byte writer shared by every venue builder.
//
// Outbound messages are assembled by copying literal runs and writing the
// varying fields directly -- 12ns against 250ns for rapidjson's Writer and
// 1458ns for nlohmann's dump(). The reason is not clever string handling: it
// is that a fixed-point price never becomes a double, so no shortest-round-trip
// float formatter ever runs.
//
// Every builder threads one Writer through and checks `ok` once at the end, so
// a truncated message can never be sent. Truncation is worse than failure: the
// venue would reject it at best and misparse it at worst.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "calais/core/decimal.h"

namespace calais::venue {

// ---------------------------------------------------------------------------
// Writer primitives, exposed for the other venue builders to reuse.
// ---------------------------------------------------------------------------
namespace detail {

// Bounds-checked append. Every builder threads one of these through and checks
// `ok` once at the end, so a truncated message can never be sent.
struct Writer {
  char* p;
  char* const end;
  bool ok = true;

  Writer(char* out, std::size_t cap) noexcept : p(out), end(out + cap) {}

  void raw(std::string_view s) noexcept {
    if (!ok || static_cast<std::size_t>(end - p) < s.size()) {
      ok = false;
      return;
    }
    std::memcpy(p, s.data(), s.size());
    p += s.size();
  }

  // A JSON string literal, with escaping. Labels are caller-controlled and an
  // unescaped quote would produce a malformed request -- or worse, a
  // well-formed one with injected fields.
  void json_string(std::string_view s) noexcept;

  void decimal(core::Price v) noexcept {
    if (!ok) {
      return;
    }
    const std::size_t n = v.write(p, static_cast<std::size_t>(end - p));
    if (n == 0) {
      ok = false;
      return;
    }
    p += n;
  }

  void integer(std::int64_t v) noexcept;

  std::size_t written(const char* origin) const noexcept {
    return ok ? static_cast<std::size_t>(p - origin) : 0;
  }
};

}  // namespace detail

}  // namespace calais::venue
