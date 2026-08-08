// UTC timestamp with nanosecond resolution.
//
// Internally: signed nanoseconds since the Unix epoch. Signed so pre-epoch
// values and timestamp differences share one type; nanoseconds because the
// binary hot path measures in nanoseconds even though every exchange feed and
// the JSON control plane are coarser.
//
// PRECISION CONTRACT -- important, and the reason this type is not just an
// int64 typedef:
//
//   exchange feeds  -> milliseconds  (all four venues we support)
//   JSON wire       -> microseconds  (Python datetime tops out there)
//   binary hot path -> nanoseconds   (full resolution preserved)
//
// So a Timestamp that round-trips through the JSON control plane loses
// sub-microsecond detail. That is deliberate: the control plane exists to stay
// byte-compatible with the Python implementation, and Python cannot represent
// nanoseconds in a datetime. Latency measurement must therefore never rely on
// timestamps that have been through JSON -- use core/latency.h, which keeps
// raw ticks end to end.
//
// ISO-8601 output matches Python's `datetime.isoformat()` byte for byte,
// including its rule of omitting the fractional part entirely when the
// microsecond field is zero. Anything else would break wire compatibility
// with transport/serialization.py.

#pragma once

#include <cstddef>
#include <cstdint>
#include <compare>
#include <optional>
#include <string>
#include <string_view>

#include "calais/core/clock.h"

namespace calais::core {

namespace detail {

// Howard Hinnant's civil-date algorithms. Valid for the full int64 range of
// days; branch-free apart from the era adjustment.
constexpr std::int64_t days_from_civil(std::int64_t y, std::int64_t m,
                                       std::int64_t d) noexcept {
  y -= (m <= 2) ? 1 : 0;
  const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
  const std::int64_t yoe = y - era * 400;                       // [0, 399]
  const std::int64_t doy =
      (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;           // [0, 365]
  const std::int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}

struct CivilDate {
  std::int64_t year;
  std::int64_t month;
  std::int64_t day;
};

constexpr CivilDate civil_from_days(std::int64_t z) noexcept {
  z += 719468;
  const std::int64_t era = (z >= 0 ? z : z - 146096) / 146097;
  const std::int64_t doe = z - era * 146097;                          // [0, 146096]
  const std::int64_t yoe =
      (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;          // [0, 399]
  const std::int64_t y = yoe + era * 400;
  const std::int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);   // [0, 365]
  const std::int64_t mp = (5 * doy + 2) / 153;                        // [0, 11]
  const std::int64_t d = doy - (153 * mp + 2) / 5 + 1;                // [1, 31]
  const std::int64_t m = mp + (mp < 10 ? 3 : -9);                     // [1, 12]
  return CivilDate{y + ((m <= 2) ? 1 : 0), m, d};
}

// Floor division / modulo, so pre-epoch instants behave (C++ truncates toward
// zero, which would put 1969-12-31T23:59:59 in the wrong day).
constexpr std::int64_t floor_div(std::int64_t a, std::int64_t b) noexcept {
  const std::int64_t q = a / b;
  return (a % b != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
}

constexpr std::int64_t floor_mod(std::int64_t a, std::int64_t b) noexcept {
  return a - floor_div(a, b) * b;
}

constexpr void write_padded(char* out, std::int64_t value, int width) noexcept {
  for (int i = width - 1; i >= 0; --i) {
    out[i] = static_cast<char>('0' + (value % 10));
    value /= 10;
  }
}

}  // namespace detail

class Timestamp {
 public:
  // "2026-08-07T12:34:56.123456" == 26 chars; +1 slack.
  static constexpr std::size_t kMaxChars = 32;

  constexpr Timestamp() noexcept = default;

  static constexpr Timestamp from_ns(std::int64_t ns) noexcept {
    Timestamp t;
    t.ns_ = ns;
    return t;
  }
  static constexpr Timestamp from_micros(std::int64_t us) noexcept {
    return from_ns(us * 1'000);
  }
  // The constructor exchange feeds actually use.
  static constexpr Timestamp from_millis(std::int64_t ms) noexcept {
    return from_ns(ms * 1'000'000);
  }
  static constexpr Timestamp from_seconds(std::int64_t s) noexcept {
    return from_ns(s * 1'000'000'000);
  }

  static Timestamp now() noexcept { return from_ns(wall_clock_ns()); }

  constexpr std::int64_t ns() const noexcept { return ns_; }
  constexpr std::int64_t micros() const noexcept {
    return detail::floor_div(ns_, 1'000);
  }
  constexpr std::int64_t millis() const noexcept {
    return detail::floor_div(ns_, 1'000'000);
  }
  constexpr std::int64_t seconds() const noexcept {
    return detail::floor_div(ns_, 1'000'000'000);
  }

  constexpr bool is_epoch() const noexcept { return ns_ == 0; }

  // Parses Python's isoformat output, plus a few tolerated variants:
  //   - 'T' or ' ' as the date/time separator
  //   - 1..9 fractional digits (truncated to nanoseconds)
  //   - optional 'Z' or +/-HH:MM[:SS] UTC offset
  // Returns nullopt on anything else.
  static constexpr std::optional<Timestamp> from_iso8601(
      std::string_view s) noexcept;

  // Emits exactly what Python's datetime.isoformat() would, for a naive UTC
  // datetime: no offset suffix, and no fractional part when microseconds are
  // zero. Returns bytes written, or 0 if cap is too small.
  constexpr std::size_t write_iso8601(char* out, std::size_t cap) const noexcept;

  std::string to_iso8601() const {
    char buf[kMaxChars];
    const std::size_t n = write_iso8601(buf, sizeof(buf));
    return std::string(buf, n);
  }

  friend constexpr bool operator==(Timestamp a, Timestamp b) noexcept {
    return a.ns_ == b.ns_;
  }
  friend constexpr std::strong_ordering operator<=>(Timestamp a,
                                                    Timestamp b) noexcept {
    return a.ns_ <=> b.ns_;
  }

  // Difference in nanoseconds. Not a Timestamp -- a duration is not an instant.
  friend constexpr std::int64_t operator-(Timestamp a, Timestamp b) noexcept {
    return a.ns_ - b.ns_;
  }

 private:
  std::int64_t ns_ = 0;
};

// ---------------------------------------------------------------------------
constexpr std::size_t Timestamp::write_iso8601(char* out,
                                               std::size_t cap) const noexcept {
  if (cap < kMaxChars) {
    return 0;
  }

  const std::int64_t days = detail::floor_div(ns_, 86'400'000'000'000LL);
  const std::int64_t rem_ns = detail::floor_mod(ns_, 86'400'000'000'000LL);

  const detail::CivilDate date = detail::civil_from_days(days);

  const std::int64_t sec_of_day = rem_ns / 1'000'000'000LL;
  const std::int64_t hour = sec_of_day / 3600;
  const std::int64_t minute = (sec_of_day / 60) % 60;
  const std::int64_t second = sec_of_day % 60;
  // Python's datetime holds microseconds, so we truncate here rather than
  // emitting nanoseconds Python could not parse back.
  const std::int64_t micro = (rem_ns % 1'000'000'000LL) / 1'000LL;

  std::size_t pos = 0;
  detail::write_padded(out + pos, date.year, 4);
  pos += 4;
  out[pos++] = '-';
  detail::write_padded(out + pos, date.month, 2);
  pos += 2;
  out[pos++] = '-';
  detail::write_padded(out + pos, date.day, 2);
  pos += 2;
  out[pos++] = 'T';
  detail::write_padded(out + pos, hour, 2);
  pos += 2;
  out[pos++] = ':';
  detail::write_padded(out + pos, minute, 2);
  pos += 2;
  out[pos++] = ':';
  detail::write_padded(out + pos, second, 2);
  pos += 2;

  // Python omits the fractional part entirely when microsecond == 0.
  if (micro != 0) {
    out[pos++] = '.';
    detail::write_padded(out + pos, micro, 6);
    pos += 6;
  }

  return pos;
}

// ---------------------------------------------------------------------------
constexpr std::optional<Timestamp> Timestamp::from_iso8601(
    std::string_view s) noexcept {
  auto digits = [&](std::size_t at, std::size_t count,
                    std::int64_t& out_value) constexpr -> bool {
    if (at + count > s.size()) {
      return false;
    }
    std::int64_t v = 0;
    for (std::size_t k = 0; k < count; ++k) {
      const char c = s[at + k];
      if (c < '0' || c > '9') {
        return false;
      }
      v = v * 10 + static_cast<std::int64_t>(c - '0');
    }
    out_value = v;
    return true;
  };

  // YYYY-MM-DD is the minimum accepted form.
  if (s.size() < 10) {
    return std::nullopt;
  }

  std::int64_t year = 0;
  std::int64_t month = 0;
  std::int64_t day = 0;
  if (!digits(0, 4, year) || s[4] != '-' || !digits(5, 2, month) ||
      s[7] != '-' || !digits(8, 2, day)) {
    return std::nullopt;
  }
  if (month < 1 || month > 12 || day < 1 || day > 31) {
    return std::nullopt;
  }

  std::int64_t hour = 0;
  std::int64_t minute = 0;
  std::int64_t second = 0;
  std::int64_t frac_ns = 0;
  std::int64_t offset_seconds = 0;

  std::size_t i = 10;
  if (i < s.size()) {
    if (s[i] != 'T' && s[i] != 't' && s[i] != ' ') {
      return std::nullopt;
    }
    ++i;

    if (!digits(i, 2, hour) || i + 2 >= s.size() || s[i + 2] != ':') {
      return std::nullopt;
    }
    i += 3;
    if (!digits(i, 2, minute)) {
      return std::nullopt;
    }
    i += 2;

    if (i < s.size() && s[i] == ':') {
      ++i;
      if (!digits(i, 2, second)) {
        return std::nullopt;
      }
      i += 2;
    }

    if (i < s.size() && (s[i] == '.' || s[i] == ',')) {
      ++i;
      std::size_t start = i;
      std::int64_t scale = 100'000'000;  // first digit is 1e8 ns
      while (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        if (scale > 0) {
          frac_ns += static_cast<std::int64_t>(s[i] - '0') * scale;
          scale /= 10;
        }
        ++i;
      }
      if (i == start) {
        return std::nullopt;  // '.' with no digits
      }
    }

    if (i < s.size() && (s[i] == 'Z' || s[i] == 'z')) {
      ++i;
    } else if (i < s.size() && (s[i] == '+' || s[i] == '-')) {
      const std::int64_t sign = (s[i] == '-') ? -1 : 1;
      ++i;
      std::int64_t oh = 0;
      std::int64_t om = 0;
      std::int64_t os = 0;
      if (!digits(i, 2, oh)) {
        return std::nullopt;
      }
      i += 2;
      if (i < s.size() && s[i] == ':') {
        ++i;
      }
      if (i < s.size() && s[i] >= '0' && s[i] <= '9') {
        if (!digits(i, 2, om)) {
          return std::nullopt;
        }
        i += 2;
        if (i < s.size() && s[i] == ':') {
          ++i;
          if (!digits(i, 2, os)) {
            return std::nullopt;
          }
          i += 2;
        }
      }
      offset_seconds = sign * (oh * 3600 + om * 60 + os);
    }
  }

  if (i != s.size()) {
    return std::nullopt;  // trailing garbage
  }
  if (hour > 23 || minute > 59 || second > 60) {
    return std::nullopt;
  }

  const std::int64_t days = detail::days_from_civil(year, month, day);
  const std::int64_t total_seconds =
      days * 86'400LL + hour * 3600 + minute * 60 + second - offset_seconds;

  return from_ns(total_seconds * 1'000'000'000LL + frac_ns);
}

}  // namespace calais::core
