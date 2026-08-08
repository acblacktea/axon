#include "calais/core/decimal.h"

#include <gtest/gtest.h>

#include <limits>
#include <random>
#include <string>

using calais::core::Decimal;
using calais::core::Price;
using calais::core::Qty;

namespace {

Price P(std::string_view s) {
  const auto v = Price::from_string(s);
  EXPECT_TRUE(v.has_value()) << "failed to parse: " << s;
  return v.value_or(Price{});
}

}  // namespace

TEST(Decimal, DefaultIsZero) {
  Price p;
  EXPECT_EQ(p.raw(), 0);
  EXPECT_TRUE(p.is_zero());
  EXPECT_EQ(p.to_string(), "0");
}

TEST(Decimal, ScaleAndDecimals) {
  EXPECT_EQ(Price::kScale, 1'000'000'000LL);
  EXPECT_EQ(Price::kDecimals, 9);
}

TEST(Decimal, ParseIntegers) {
  EXPECT_EQ(P("0").raw(), 0);
  EXPECT_EQ(P("1").raw(), 1'000'000'000LL);
  EXPECT_EQ(P("42").raw(), 42'000'000'000LL);
  EXPECT_EQ(P("-7").raw(), -7'000'000'000LL);
  EXPECT_EQ(P("+7").raw(), 7'000'000'000LL);
}

TEST(Decimal, ParseFractions) {
  EXPECT_EQ(P("0.1").raw(), 100'000'000LL);
  EXPECT_EQ(P("0.0345").raw(), 34'500'000LL);
  EXPECT_EQ(P("0.000000001").raw(), 1LL);
  EXPECT_EQ(P("-0.5").raw(), -500'000'000LL);
  EXPECT_EQ(P(".5").raw(), 500'000'000LL);
  EXPECT_EQ(P("1.").raw(), 1'000'000'000LL);
}

TEST(Decimal, ParseScientific) {
  EXPECT_EQ(P("1e-8").raw(), 10LL);
  EXPECT_EQ(P("1E-9").raw(), 1LL);
  EXPECT_EQ(P("1.5e2").raw(), 150'000'000'000LL);
  EXPECT_EQ(P("-2.5e-3").raw(), -2'500'000LL);
  EXPECT_EQ(P("1e+3").raw(), 1'000'000'000'000LL);
}

TEST(Decimal, ParseRoundsHalfAwayFromZero) {
  // 10 decimals: the last digit must round, not truncate.
  EXPECT_EQ(P("0.0000000004").raw(), 0LL);
  EXPECT_EQ(P("0.0000000005").raw(), 1LL);
  EXPECT_EQ(P("0.0000000006").raw(), 1LL);
  EXPECT_EQ(P("-0.0000000005").raw(), -1LL);
  EXPECT_EQ(P("-0.0000000004").raw(), 0LL);
}

TEST(Decimal, ParseUnderflowsToZero) {
  EXPECT_EQ(P("1e-30").raw(), 0LL);
}

TEST(Decimal, ParseRejectsGarbage) {
  EXPECT_FALSE(Price::from_string("").has_value());
  EXPECT_FALSE(Price::from_string("abc").has_value());
  EXPECT_FALSE(Price::from_string("1.2.3").has_value());
  EXPECT_FALSE(Price::from_string("1x").has_value());
  EXPECT_FALSE(Price::from_string("-").has_value());
  EXPECT_FALSE(Price::from_string(".").has_value());
  EXPECT_FALSE(Price::from_string("1e").has_value());
  EXPECT_FALSE(Price::from_string("1e+").has_value());
  EXPECT_FALSE(Price::from_string(" 1").has_value());
  EXPECT_FALSE(Price::from_string("1 ").has_value());
  EXPECT_FALSE(Price::from_string("nan").has_value());
  EXPECT_FALSE(Price::from_string("inf").has_value());
}

TEST(Decimal, ParseRejectsOverflow) {
  // 1e30 cannot be scaled by 1e9 into an int64.
  EXPECT_FALSE(Price::from_string("1e30").has_value());
  EXPECT_FALSE(Price::from_string("99999999999999999999").has_value());
}

TEST(Decimal, FormatTrimsTrailingZeros) {
  EXPECT_EQ(P("0.1").to_string(), "0.1");
  EXPECT_EQ(P("0.100000000").to_string(), "0.1");
  EXPECT_EQ(P("1.000000000").to_string(), "1");
  EXPECT_EQ(P("0.0345").to_string(), "0.0345");
  EXPECT_EQ(P("0.000000001").to_string(), "0.000000001");
  EXPECT_EQ(P("-0.5").to_string(), "-0.5");
  EXPECT_EQ(P("123456.789").to_string(), "123456.789");
}

TEST(Decimal, FormatHandlesExtremes) {
  EXPECT_EQ(Price::from_raw(std::numeric_limits<std::int64_t>::min()).to_string(),
            "-9223372036.854775808");
  EXPECT_EQ(Price::from_raw(std::numeric_limits<std::int64_t>::max()).to_string(),
            "9223372036.854775807");
}

TEST(Decimal, FormatRejectsUndersizedBuffer) {
  char buf[4];
  EXPECT_EQ(P("1.5").write(buf, sizeof(buf)), 0u);
}

TEST(Decimal, RoundTripsThroughText) {
  // Every value expressible in 9 decimals must survive format -> parse.
  std::mt19937_64 rng(0xC0FFEE);
  std::uniform_int_distribution<std::int64_t> dist(-1'000'000'000'000'000LL,
                                                   1'000'000'000'000'000LL);
  for (int i = 0; i < 20000; ++i) {
    const Price a = Price::from_raw(dist(rng));
    const auto b = Price::from_string(a.to_string());
    ASSERT_TRUE(b.has_value()) << a.to_string();
    EXPECT_EQ(a.raw(), b->raw()) << a.to_string();
  }
}

TEST(Decimal, Arithmetic) {
  EXPECT_EQ((P("1.5") + P("2.25")).to_string(), "3.75");
  EXPECT_EQ((P("1.5") - P("2.25")).to_string(), "-0.75");
  EXPECT_EQ((P("1.5") * 3).to_string(), "4.5");
  EXPECT_EQ((3 * P("1.5")).to_string(), "4.5");
  EXPECT_EQ((-P("1.5")).to_string(), "-1.5");
  EXPECT_EQ(P("-1.5").abs().to_string(), "1.5");

  Price acc = P("1");
  acc += P("0.5");
  EXPECT_EQ(acc.to_string(), "1.5");
  acc -= P("2");
  EXPECT_EQ(acc.to_string(), "-0.5");
}

TEST(Decimal, MultiplyUsesFullPrecision) {
  // 0.1 * 0.1 == 0.01 exactly, which is the classic double failure.
  EXPECT_EQ(P("0.1").mul(P("0.1")).to_string(), "0.01");
  EXPECT_EQ(P("1000").mul(P("0.001")).to_string(), "1");
  EXPECT_EQ(P("-2.5").mul(P("4")).to_string(), "-10");

  // The intermediate here is 1e9 * 1e9 * 1e9 = 1e27, far past int64. It only
  // survives because the product is computed in __int128 before rescaling.
  EXPECT_EQ(P("100000").mul(P("90000")).to_string(), "9000000000");
}

TEST(Decimal, SaturatesInsteadOfWrapping) {
  // 100000 * 100000 = 1e10, outside the +/- 9.22e9 range of Decimal<1e9>.
  // Wrapping would produce -8446744073.709551616 -- a SIGN FLIP, which in a
  // notional calculation silently turns a long into a short. Saturating keeps
  // the sign and the ordering.
  const Price big = P("100000").mul(P("100000"));
  EXPECT_GT(big.raw(), 0) << "overflow must never flip the sign";
  EXPECT_EQ(big.raw(), std::numeric_limits<std::int64_t>::max());

  const Price negative_big = P("-100000").mul(P("100000"));
  EXPECT_LT(negative_big.raw(), 0);
  EXPECT_EQ(negative_big.raw(), std::numeric_limits<std::int64_t>::min());

  // Addition and subtraction saturate too -- plain int64 overflow would be
  // undefined behaviour, not merely wrong.
  const Price max = Price::from_raw(std::numeric_limits<std::int64_t>::max());
  EXPECT_EQ((max + P("1")).raw(), std::numeric_limits<std::int64_t>::max());
  const Price min = Price::from_raw(std::numeric_limits<std::int64_t>::min());
  EXPECT_EQ((min - P("1")).raw(), std::numeric_limits<std::int64_t>::min());
  EXPECT_EQ((max * 2).raw(), std::numeric_limits<std::int64_t>::max());
  EXPECT_EQ((max * -2).raw(), std::numeric_limits<std::int64_t>::min());

  // Negation and abs of INT64_MIN have no true value; they must not be UB.
  EXPECT_EQ((-min).raw(), std::numeric_limits<std::int64_t>::max());
  EXPECT_EQ(min.abs().raw(), std::numeric_limits<std::int64_t>::max());
}

TEST(Decimal, CheckedOperationsReportOverflowInsteadOfSaturating) {
  // Anything sizing a real order should use these: saturating to 9.22e9 is
  // survivable, but silently sending it as a quantity would not be.
  EXPECT_FALSE(P("100000").checked_mul(P("100000")).has_value());
  EXPECT_TRUE(P("100000").checked_mul(P("90000")).has_value());
  EXPECT_EQ(P("0.1").checked_mul(P("0.1"))->to_string(), "0.01");

  EXPECT_FALSE(P("1").checked_div(Price{}).has_value());
  EXPECT_EQ(P("10").checked_div(P("4"))->to_string(), "2.5");
  // 9.22e9 / 1e-9 is ~9.2e18 scaled by 1e9 -- far outside the range.
  EXPECT_FALSE(P("1000000").checked_div(Price::from_raw(1)).has_value());
}

TEST(Decimal, MultiplyRoundsHalfAwayFromZero) {
  // 1e-9 * 0.5 = 5e-10, which rounds to 1e-9.
  EXPECT_EQ(P("0.000000001").mul(P("0.5")).raw(), 1);
  EXPECT_EQ(P("-0.000000001").mul(P("0.5")).raw(), -1);
  // 1e-9 * 0.4 = 4e-10, rounds to 0.
  EXPECT_EQ(P("0.000000001").mul(P("0.4")).raw(), 0);
}

TEST(Decimal, Divide) {
  EXPECT_EQ(P("1").div(P("4")).to_string(), "0.25");
  EXPECT_EQ(P("10").div(P("4")).to_string(), "2.5");
  EXPECT_EQ(P("-10").div(P("4")).to_string(), "-2.5");
  EXPECT_EQ(P("10").div(P("-4")).to_string(), "-2.5");
  // 1/3 is not representable; it rounds at the 9th decimal.
  EXPECT_EQ(P("1").div(P("3")).to_string(), "0.333333333");
  EXPECT_EQ(P("2").div(P("3")).to_string(), "0.666666667");
}

TEST(Decimal, DivideByZeroYieldsZeroRatherThanTrapping) {
  EXPECT_EQ(P("1").div(Price{}).raw(), 0);
}

TEST(Decimal, Comparisons) {
  EXPECT_TRUE(P("1.5") == P("1.50"));
  EXPECT_TRUE(P("1.5") < P("1.6"));
  EXPECT_TRUE(P("-1.5") < P("1.5"));
  EXPECT_TRUE(P("1.5") >= P("1.5"));
  EXPECT_NE(P("1.5"), P("1.6"));

  EXPECT_EQ(P("1.5").sign(), 1);
  EXPECT_EQ(P("-1.5").sign(), -1);
  EXPECT_EQ(Price{}.sign(), 0);
}

TEST(Decimal, FromDoubleRoundTripsTypicalMarketValues) {
  // Bridging the Python control plane goes through double. Values with <= 9
  // decimals and a realistic magnitude must survive intact.
  const char* values[] = {"0.1",  "0.0345", "100000.5", "0.000000001",
                          "-1.5", "68.5",   "123456.789"};
  for (const char* s : values) {
    const Price exact = P(s);
    const Price viaDouble = Price::from_double(exact.to_double());
    EXPECT_EQ(exact.raw(), viaDouble.raw()) << s;
  }
}

TEST(Decimal, FromIntegerAndFromRaw) {
  EXPECT_EQ(Price::from_integer(5).to_string(), "5");
  EXPECT_EQ(Price::from_integer(-5).to_string(), "-5");
  EXPECT_EQ(Price::from_raw(1).to_string(), "0.000000001");
}

TEST(Decimal, FillAccumulationStaysExact) {
  // The motivating case: summing many partial fills must not drift. With
  // doubles, 0.1 added 10 times is 0.9999999999999999.
  Qty position;
  for (int i = 0; i < 10; ++i) {
    position += P("0.1");
  }
  EXPECT_EQ(position.raw(), Price::from_integer(1).raw());
  EXPECT_EQ(position.to_string(), "1");

  // And 1000 fills of 1e-9 is exactly 1e-6.
  Qty tiny;
  for (int i = 0; i < 1000; ++i) {
    tiny += Price::from_raw(1);
  }
  EXPECT_EQ(tiny.to_string(), "0.000001");
}

TEST(Decimal, AlternateScaleWorks) {
  using Cents = Decimal<100>;
  EXPECT_EQ(Cents::kDecimals, 2);
  const auto v = Cents::from_string("12.345");
  ASSERT_TRUE(v.has_value());
  EXPECT_EQ(v->raw(), 1235);  // rounds half away from zero
  EXPECT_EQ(v->to_string(), "12.35");
}

TEST(Decimal, ConstexprUsable) {
  constexpr Price a = Price::from_raw(1'500'000'000LL);
  constexpr Price b = Price::from_integer(2);
  constexpr Price c = a + b;
  static_assert(c.raw() == 3'500'000'000LL);
  EXPECT_EQ(c.to_string(), "3.5");
}
