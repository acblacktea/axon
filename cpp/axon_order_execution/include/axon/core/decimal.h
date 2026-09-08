// Fixed-point decimal for prices and quantities.
//
// Why not double: two reasons, and neither is about "float is inexact".
//
//  1. Formatting a double into JSON is the single most expensive step in
//     building an order message -- shortest-round-trip algorithms (Ryu/Grisu)
//     cost 20-50ns and allocate in most standard library implementations.
//     Formatting an int64 is a divide loop over ~10 digits, ~10ns, no alloc.
//
//  2. Accumulating fills in double lets rounding error drift into position
//     reconciliation, which then shows up as a phantom residual position that
//     the reconciler tries to flatten. Integers make fill accumulation exact.
//
// Representation: value = raw / Scale, Scale = 10^kDecimals.
// With Scale = 1e9 the representable range is +/- 9.22e9 with 9 decimal
// places, which covers every price and quantity we trade (BTC notional and
// option premia alike) with a wide margin.
//
// The range is NOT wide enough for arbitrary products, though: 1e5 * 1e5 = 1e10
// already exceeds it. Every operation saturates rather than wrapping (see the
// overflow policy on the arithmetic section below), and checked_mul/checked_div
// are there for callers that must detect it rather than survive it. If you are
// aggregating notionals across a whole book, accumulate in __int128 or a
// coarser scale rather than trusting this type.
//
// Parsing never goes through double: exchange JSON gives us decimal text, and
// text -> int64 is both exact and faster than text -> double -> int64.

#pragma once

#include <cstddef>
#include <cstdint>
#include <compare>
#include <optional>
#include <string>
#include <string_view>

#include "axon/core/platform.h"

namespace axon::core {

namespace detail {

constexpr int decimals_for(std::int64_t scale) noexcept {
  int d = 0;
  while (scale > 1) {
    scale /= 10;
    ++d;
  }
  return d;
}

constexpr std::int64_t kPow10[19] = {
    1LL,
    10LL,
    100LL,
    1'000LL,
    10'000LL,
    100'000LL,
    1'000'000LL,
    10'000'000LL,
    100'000'000LL,
    1'000'000'000LL,
    10'000'000'000LL,
    100'000'000'000LL,
    1'000'000'000'000LL,
    10'000'000'000'000LL,
    100'000'000'000'000LL,
    1'000'000'000'000'000LL,
    10'000'000'000'000'000LL,
    100'000'000'000'000'000LL,
    1'000'000'000'000'000'000LL,
};

}  // namespace detail

template <std::int64_t Scale>
class Decimal {
  static_assert(Scale > 0, "Scale must be positive");

 public:
  using raw_type = std::int64_t;

  static constexpr std::int64_t kScale = Scale;
  static constexpr int kDecimals = detail::decimals_for(Scale);

  // Longest output: '-' + 19 integer digits + '.' + kDecimals + NUL.
  static constexpr std::size_t kMaxChars =
      1 + 19 + 1 + static_cast<std::size_t>(kDecimals) + 1;

  constexpr Decimal() noexcept = default;

  static constexpr Decimal from_raw(std::int64_t raw) noexcept {
    Decimal d;
    d.raw_ = raw;
    return d;
  }

  static constexpr Decimal from_integer(std::int64_t units) noexcept {
    return from_raw(units * Scale);
  }

  // Lossy on purpose: only for bridging the Python control plane, which speaks
  // JSON numbers (i.e. doubles) already. Never call this on an exchange feed.
  static Decimal from_double(double v) noexcept {
    const double scaled = v * static_cast<double>(Scale);
    // Round half away from zero, matching from_string's rounding.
    const double rounded = scaled >= 0.0 ? scaled + 0.5 : scaled - 0.5;
    return from_raw(static_cast<std::int64_t>(rounded));
  }

  // Exact decimal-text parse. Accepts optional sign, digits, optional
  // fraction, and optional decimal exponent ("1e-8", "-0.0345", "12").
  // Returns nullopt on malformed input or on integer overflow.
  //
  // Digits beyond kDecimals are rounded half away from zero.
  static constexpr std::optional<Decimal> from_string(std::string_view s) noexcept;

  constexpr std::int64_t raw() const noexcept { return raw_; }

  double to_double() const noexcept {
    return static_cast<double>(raw_) / static_cast<double>(Scale);
  }

  // Writes the shortest exact decimal representation (trailing fractional
  // zeros trimmed) into `out`. Returns the number of bytes written, or 0 if
  // `cap` is too small. Does not NUL-terminate. Zero-allocation.
  constexpr std::size_t write(char* out, std::size_t cap) const noexcept;

  std::string to_string() const {
    char buf[kMaxChars];
    const std::size_t n = write(buf, sizeof(buf));
    return std::string(buf, n);
  }

  // --- arithmetic ---------------------------------------------------------
  //
  // OVERFLOW POLICY: every operation SATURATES at the representable range and
  // never wraps.
  //
  // This is not paranoia. With Scale = 1e9 the range is +/- 9.22e9, and
  // 100000 * 100000 = 1e10 is already outside it. Wrapping turns that into
  // -8.45e9 -- the SIGN FLIPS. A notional that silently changes sign turns a
  // long into a short in whatever consumes it, and nothing downstream has any
  // way to notice. Saturation at least preserves sign and ordering.
  //
  // Signed overflow is also undefined behaviour in C++, so plain `a + b` on
  // int64 would license the compiler to do anything at all. The checks below
  // compile to an add plus a not-taken conditional branch -- effectively free.
  //
  // When the caller must KNOW rather than merely survive, use the checked_*
  // variants, which return nullopt instead of saturating. Anything that sizes
  // a real order should use those.

  constexpr Decimal operator-() const noexcept {
    return from_raw(raw_ == INT64_MIN ? INT64_MAX : -raw_);
  }

  constexpr Decimal& operator+=(Decimal o) noexcept {
    *this = *this + o;
    return *this;
  }
  constexpr Decimal& operator-=(Decimal o) noexcept {
    *this = *this - o;
    return *this;
  }
  constexpr Decimal& operator*=(std::int64_t k) noexcept {
    *this = *this * k;
    return *this;
  }

  friend constexpr Decimal operator+(Decimal a, Decimal b) noexcept {
    std::int64_t r = 0;
    if (__builtin_add_overflow(a.raw_, b.raw_, &r)) {
      return from_raw(b.raw_ > 0 ? INT64_MAX : INT64_MIN);
    }
    return from_raw(r);
  }
  friend constexpr Decimal operator-(Decimal a, Decimal b) noexcept {
    std::int64_t r = 0;
    if (__builtin_sub_overflow(a.raw_, b.raw_, &r)) {
      return from_raw(b.raw_ < 0 ? INT64_MAX : INT64_MIN);
    }
    return from_raw(r);
  }
  friend constexpr Decimal operator*(Decimal a, std::int64_t k) noexcept {
    std::int64_t r = 0;
    if (__builtin_mul_overflow(a.raw_, k, &r)) {
      const bool negative = (a.raw_ < 0) != (k < 0);
      return from_raw(negative ? INT64_MIN : INT64_MAX);
    }
    return from_raw(r);
  }
  friend constexpr Decimal operator*(std::int64_t k, Decimal a) noexcept {
    return a * k;
  }

  // Full-precision multiply, rounded half away from zero, saturating.
  // The intermediate is __int128 so the product itself cannot overflow before
  // it is rescaled; only the final rescaled value can exceed the range.
  constexpr Decimal mul(Decimal o) const noexcept {
    return from_raw(saturate(mul_exact(raw_, o.raw_)));
  }

  // nullopt if the exact result is outside the representable range.
  constexpr std::optional<Decimal> checked_mul(Decimal o) const noexcept {
    const auto v = mul_exact(raw_, o.raw_);
    if (!fits_i64(v)) {
      return std::nullopt;
    }
    return from_raw(static_cast<std::int64_t>(v));
  }

  // Full-precision divide, rounded half away from zero, saturating.
  //
  // Divide by zero yields zero rather than trapping: on the hot path a zero
  // denominator is checked once where it is constructed (an empty book, a
  // zero position), not on every arithmetic operation. Use checked_div() if
  // the distinction between "zero" and "undefined" matters.
  constexpr Decimal div(Decimal o) const noexcept {
    if (o.raw_ == 0) {
      return Decimal{};
    }
    return from_raw(saturate(div_exact(raw_, o.raw_)));
  }

  constexpr std::optional<Decimal> checked_div(Decimal o) const noexcept {
    if (o.raw_ == 0) {
      return std::nullopt;
    }
    const auto v = div_exact(raw_, o.raw_);
    if (!fits_i64(v)) {
      return std::nullopt;
    }
    return from_raw(static_cast<std::int64_t>(v));
  }

  constexpr Decimal abs() const noexcept {
    if (raw_ >= 0) {
      return *this;
    }
    return from_raw(raw_ == INT64_MIN ? INT64_MAX : -raw_);
  }
  constexpr bool is_zero() const noexcept { return raw_ == 0; }
  constexpr int sign() const noexcept { return (raw_ > 0) - (raw_ < 0); }

  friend constexpr bool operator==(Decimal a, Decimal b) noexcept {
    return a.raw_ == b.raw_;
  }
  friend constexpr std::strong_ordering operator<=>(Decimal a, Decimal b) noexcept {
    return a.raw_ <=> b.raw_;
  }

 private:
  using i128 = __int128;

  static constexpr bool fits_i64(i128 v) noexcept {
    return v <= static_cast<i128>(INT64_MAX) && v >= static_cast<i128>(INT64_MIN);
  }

  static constexpr std::int64_t saturate(i128 v) noexcept {
    if (v > static_cast<i128>(INT64_MAX)) {
      return INT64_MAX;
    }
    if (v < static_cast<i128>(INT64_MIN)) {
      return INT64_MIN;
    }
    return static_cast<std::int64_t>(v);
  }

  // (a * b) / Scale, rounded half away from zero.
  //
  // Rounding is applied to the MAGNITUDE and the sign reattached afterwards.
  // Doing it in signed space needs the rounding offset to follow the sign of
  // the result, which is easy to get wrong -- and did get it wrong here for
  // mixed-sign division, where 10 / -4 came out as -2.499999999.
  static constexpr i128 mul_exact(std::int64_t a, std::int64_t b) noexcept {
    // |a|,|b| <= 2^63 so the product fits comfortably in 127 bits and the
    // negation below cannot overflow.
    const i128 p = static_cast<i128>(a) * static_cast<i128>(b);
    const bool negative = p < 0;
    const i128 magnitude = negative ? -p : p;
    const i128 scale = static_cast<i128>(Scale);
    const i128 q = (magnitude + scale / 2) / scale;
    return negative ? -q : q;
  }

  // (a * Scale) / b, rounded half away from zero. Caller guarantees b != 0.
  static constexpr i128 div_exact(std::int64_t a, std::int64_t b) noexcept {
    const i128 n = static_cast<i128>(a) * static_cast<i128>(Scale);
    const i128 d = static_cast<i128>(b);
    const bool negative = (n < 0) != (d < 0);
    const i128 an = n < 0 ? -n : n;
    const i128 ad = d < 0 ? -d : d;
    const i128 q = (an + ad / 2) / ad;
    return negative ? -q : q;
  }

  std::int64_t raw_ = 0;
};

// ---------------------------------------------------------------------------
// from_string
// ---------------------------------------------------------------------------
template <std::int64_t Scale>
constexpr std::optional<Decimal<Scale>> Decimal<Scale>::from_string(
    std::string_view s) noexcept {
  std::size_t i = 0;
  const std::size_t n = s.size();
  if (n == 0) {
    return std::nullopt;
  }

  bool negative = false;
  if (s[i] == '+' || s[i] == '-') {
    negative = (s[i] == '-');
    ++i;
  }

  // Guard so `mant * 10 + 9` cannot overflow.
  constexpr std::int64_t kMantLimit = (INT64_MAX - 9) / 10;

  std::int64_t mant = 0;
  int exp10 = 0;
  bool any_digit = false;

  // Integer part. Once the mantissa is saturated we keep consuming digits but
  // account for them in the exponent instead -- this loses low-order precision
  // on absurdly long inputs rather than silently wrapping.
  while (i < n && s[i] >= '0' && s[i] <= '9') {
    const std::int64_t d = static_cast<std::int64_t>(s[i] - '0');
    if (mant <= kMantLimit) {
      mant = mant * 10 + d;
    } else {
      ++exp10;
    }
    any_digit = true;
    ++i;
  }

  // Fractional part.
  if (i < n && s[i] == '.') {
    ++i;
    while (i < n && s[i] >= '0' && s[i] <= '9') {
      const std::int64_t d = static_cast<std::int64_t>(s[i] - '0');
      if (mant <= kMantLimit) {
        mant = mant * 10 + d;
        --exp10;
      }
      any_digit = true;
      ++i;
    }
  }

  if (!any_digit) {
    return std::nullopt;
  }

  // Scientific exponent.
  if (i < n && (s[i] == 'e' || s[i] == 'E')) {
    ++i;
    bool exp_negative = false;
    if (i < n && (s[i] == '+' || s[i] == '-')) {
      exp_negative = (s[i] == '-');
      ++i;
    }
    if (i >= n || s[i] < '0' || s[i] > '9') {
      return std::nullopt;
    }
    int e = 0;
    while (i < n && s[i] >= '0' && s[i] <= '9') {
      e = e * 10 + static_cast<int>(s[i] - '0');
      if (e > 1000) {  // far outside anything representable; clamp to bail out
        e = 1001;
        while (i < n && s[i] >= '0' && s[i] <= '9') {
          ++i;
        }
        break;
      }
      ++i;
    }
    exp10 += exp_negative ? -e : e;
  }

  if (i != n) {
    return std::nullopt;  // trailing garbage
  }

  // Rescale mantissa from 10^exp10 to 10^-kDecimals.
  int shift = exp10 + kDecimals;

  if (mant == 0) {
    return Decimal{};
  }

  if (shift > 0) {
    if (shift > 18) {
      return std::nullopt;  // overflow
    }
    const std::int64_t factor = detail::kPow10[shift];
    if (mant > INT64_MAX / factor) {
      return std::nullopt;  // overflow
    }
    mant *= factor;
  } else if (shift < 0) {
    const int drop = -shift;
    if (drop > 18) {
      return Decimal{};  // rounds to zero
    }
    const std::int64_t factor = detail::kPow10[drop];
    const std::int64_t q = mant / factor;
    const std::int64_t r = mant % factor;
    mant = q + ((r * 2 >= factor) ? 1 : 0);  // round half away from zero
  }

  return from_raw(negative ? -mant : mant);
}

// ---------------------------------------------------------------------------
// write
// ---------------------------------------------------------------------------
template <std::int64_t Scale>
constexpr std::size_t Decimal<Scale>::write(char* out, std::size_t cap) const noexcept {
  if (cap < kMaxChars) {
    return 0;
  }

  std::size_t pos = 0;
  std::uint64_t magnitude;
  if (raw_ < 0) {
    out[pos++] = '-';
    // Negate in unsigned space so INT64_MIN is handled.
    magnitude = static_cast<std::uint64_t>(-(raw_ + 1)) + 1U;
  } else {
    magnitude = static_cast<std::uint64_t>(raw_);
  }

  const std::uint64_t scale = static_cast<std::uint64_t>(Scale);
  const std::uint64_t int_part = magnitude / scale;
  std::uint64_t frac_part = magnitude % scale;

  // Integer part, written backwards into a scratch buffer then reversed.
  char scratch[20];
  std::size_t len = 0;
  if (int_part == 0) {
    scratch[len++] = '0';
  } else {
    std::uint64_t v = int_part;
    while (v > 0) {
      scratch[len++] = static_cast<char>('0' + (v % 10));
      v /= 10;
    }
  }
  while (len > 0) {
    out[pos++] = scratch[--len];
  }

  if (frac_part != 0) {
    out[pos++] = '.';
    // Emit exactly kDecimals digits, then trim trailing zeros. Trimming first
    // would need a divide loop of unknown length; this is a fixed cost.
    std::size_t frac_start = pos;
    for (int d = kDecimals - 1; d >= 0; --d) {
      const std::uint64_t p = static_cast<std::uint64_t>(detail::kPow10[d]);
      out[pos++] = static_cast<char>('0' + ((frac_part / p) % 10));
    }
    while (pos > frac_start && out[pos - 1] == '0') {
      --pos;
    }
  }

  return pos;
}

// ---------------------------------------------------------------------------
// Domain aliases
//
// These are aliases, not distinct types -- `Price` and `Qty` are the same type
// and the compiler will NOT catch passing one where the other is expected.
// Making them distinct needs a phantom tag parameter plus a cross-tag multiply
// (price x qty -> notional); worth doing once the order path exists and we know
// which combinations are actually meaningful. Tracked in README "Follow-ups".
// ---------------------------------------------------------------------------
inline constexpr std::int64_t kDefaultScale = 1'000'000'000LL;  // 9 decimals

using Price = Decimal<kDefaultScale>;
using Qty = Decimal<kDefaultScale>;
using Notional = Decimal<kDefaultScale>;

}  // namespace axon::core
