#include "axon/core/timestamp.h"

#include <gtest/gtest.h>

#include <random>
#include <string>

using axon::core::Timestamp;

namespace {

Timestamp TS(std::string_view s) {
  const auto v = Timestamp::from_iso8601(s);
  EXPECT_TRUE(v.has_value()) << "failed to parse: " << s;
  return v.value_or(Timestamp{});
}

}  // namespace

TEST(Timestamp, EpochIsZero) {
  Timestamp t;
  EXPECT_EQ(t.ns(), 0);
  EXPECT_TRUE(t.is_epoch());
  EXPECT_EQ(t.to_iso8601(), "1970-01-01T00:00:00");
}

TEST(Timestamp, Constructors) {
  EXPECT_EQ(Timestamp::from_millis(1500).ns(), 1'500'000'000LL);
  EXPECT_EQ(Timestamp::from_micros(1500).ns(), 1'500'000LL);
  EXPECT_EQ(Timestamp::from_seconds(2).ns(), 2'000'000'000LL);
  EXPECT_EQ(Timestamp::from_ns(1234).ns(), 1234);
}

TEST(Timestamp, UnitAccessorsFloorTowardsNegativeInfinity) {
  // Truncation toward zero would put -1ns in the wrong millisecond.
  EXPECT_EQ(Timestamp::from_ns(1'999'999).millis(), 1);
  EXPECT_EQ(Timestamp::from_ns(-1).millis(), -1);
  EXPECT_EQ(Timestamp::from_ns(-1).micros(), -1);
  EXPECT_EQ(Timestamp::from_ns(-1).seconds(), -1);
}

// ---------------------------------------------------------------------------
// Formatting -- these strings are the Python compatibility contract.
// ---------------------------------------------------------------------------

TEST(Timestamp, FormatMatchesPythonIsoformat) {
  EXPECT_EQ(TS("2026-08-07T12:34:56").to_iso8601(), "2026-08-07T12:34:56");
  EXPECT_EQ(TS("2026-08-07T12:34:56.123456").to_iso8601(),
            "2026-08-07T12:34:56.123456");
  EXPECT_EQ(TS("2026-01-01T00:00:00").to_iso8601(), "2026-01-01T00:00:00");
  EXPECT_EQ(TS("1999-12-31T23:59:59.999999").to_iso8601(),
            "1999-12-31T23:59:59.999999");
}

TEST(Timestamp, FormatOmitsFractionWhenMicrosecondsAreZero) {
  // datetime.isoformat() drops ".000000" entirely. Emitting it would produce a
  // string Python never produces, and the cross-implementation test would fail.
  EXPECT_EQ(Timestamp::from_seconds(0).to_iso8601(), "1970-01-01T00:00:00");
  EXPECT_EQ(TS("2026-08-07T12:34:56.000000").to_iso8601(),
            "2026-08-07T12:34:56");
}

TEST(Timestamp, FormatPadsFractionToSixDigits) {
  EXPECT_EQ(Timestamp::from_micros(1).to_iso8601(), "1970-01-01T00:00:00.000001");
  EXPECT_EQ(Timestamp::from_micros(100'000).to_iso8601(),
            "1970-01-01T00:00:00.100000");
}

TEST(Timestamp, FormatTruncatesNanosecondsToMicroseconds) {
  // Python's datetime cannot hold nanoseconds. Truncating (rather than
  // rounding) matches what the Python side would store.
  EXPECT_EQ(Timestamp::from_ns(1999).to_iso8601(), "1970-01-01T00:00:00.000001");
}

TEST(Timestamp, FormatRejectsUndersizedBuffer) {
  char buf[8];
  EXPECT_EQ(Timestamp::now().write_iso8601(buf, sizeof(buf)), 0u);
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

TEST(Timestamp, ParsesLeapYears) {
  EXPECT_EQ(TS("2024-02-29T00:00:00").to_iso8601(), "2024-02-29T00:00:00");
  EXPECT_EQ(TS("2000-02-29T00:00:00").to_iso8601(), "2000-02-29T00:00:00");
}

TEST(Timestamp, ParsesKnownEpochOffsets) {
  EXPECT_EQ(TS("1970-01-01T00:00:00").ns(), 0);
  EXPECT_EQ(TS("1970-01-02T00:00:00").ns(), 86'400'000'000'000LL);
  EXPECT_EQ(TS("2000-01-01T00:00:00").seconds(), 946'684'800LL);
  EXPECT_EQ(TS("2026-08-07T00:00:00").seconds(), 1'786'060'800LL);
}

TEST(Timestamp, ParsesPreEpoch) {
  const Timestamp t = TS("1969-12-31T23:59:59");
  EXPECT_EQ(t.ns(), -1'000'000'000LL);
  EXPECT_EQ(t.to_iso8601(), "1969-12-31T23:59:59");
}

TEST(Timestamp, ParsesToleratedVariants) {
  EXPECT_EQ(TS("2026-08-07 12:34:56").to_iso8601(), "2026-08-07T12:34:56");
  EXPECT_EQ(TS("2026-08-07T12:34").to_iso8601(), "2026-08-07T12:34:00");
  EXPECT_EQ(TS("2026-08-07").to_iso8601(), "2026-08-07T00:00:00");
  EXPECT_EQ(TS("2026-08-07T12:34:56Z").to_iso8601(), "2026-08-07T12:34:56");
  EXPECT_EQ(TS("2026-08-07T12:34:56.5").to_iso8601(), "2026-08-07T12:34:56.500000");
}

TEST(Timestamp, ParsesUtcOffsets) {
  // +02:00 means the instant is two hours earlier in UTC.
  EXPECT_EQ(TS("2026-08-07T12:34:56+02:00").to_iso8601(), "2026-08-07T10:34:56");
  EXPECT_EQ(TS("2026-08-07T12:34:56-05:00").to_iso8601(), "2026-08-07T17:34:56");
  EXPECT_EQ(TS("2026-08-07T12:34:56+0200").to_iso8601(), "2026-08-07T10:34:56");
}

TEST(Timestamp, ParsesSubMicrosecondFractions) {
  EXPECT_EQ(TS("1970-01-01T00:00:00.123456789").ns(), 123'456'789LL);
}

TEST(Timestamp, ParseRejectsGarbage) {
  EXPECT_FALSE(Timestamp::from_iso8601("").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("not-a-date").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026-08").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026/08/07").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026-13-01T00:00:00").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026-08-32T00:00:00").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026-08-07T25:00:00").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026-08-07T12:61:00").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026-08-07T12:34:56.").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026-08-07T12:34:56 trailing").has_value());
  EXPECT_FALSE(Timestamp::from_iso8601("2026-08-07X12:34:56").has_value());
}

TEST(Timestamp, RoundTripsAcrossManyInstants) {
  std::mt19937_64 rng(0xBEEF);
  // Roughly 1970..2100, at microsecond granularity (what the format carries).
  std::uniform_int_distribution<std::int64_t> dist(0, 4'100'000'000'000'000LL);
  for (int i = 0; i < 20000; ++i) {
    const Timestamp a = Timestamp::from_micros(dist(rng));
    const auto b = Timestamp::from_iso8601(a.to_iso8601());
    ASSERT_TRUE(b.has_value()) << a.to_iso8601();
    EXPECT_EQ(a.ns(), b->ns()) << a.to_iso8601();
  }
}

TEST(Timestamp, Comparisons) {
  const Timestamp a = Timestamp::from_seconds(100);
  const Timestamp b = Timestamp::from_seconds(200);
  EXPECT_TRUE(a < b);
  EXPECT_TRUE(b > a);
  EXPECT_TRUE(a == Timestamp::from_seconds(100));
  EXPECT_NE(a, b);
  EXPECT_EQ(b - a, 100'000'000'000LL);
  EXPECT_EQ(a - b, -100'000'000'000LL);
}

TEST(Timestamp, NowIsPlausible) {
  const Timestamp t = Timestamp::now();
  // Between 2020-01-01 and 2100-01-01. Catches a clock source that returns 0
  // or a unit mix-up (seconds where nanoseconds were expected).
  EXPECT_GT(t.seconds(), 1'577'836'800LL);
  EXPECT_LT(t.seconds(), 4'102'444'800LL);
}

TEST(Timestamp, ConstexprUsable) {
  constexpr auto t = Timestamp::from_iso8601("2026-08-07T12:34:56");
  static_assert(t.has_value());
  static_assert(t->seconds() == 1'786'106'096LL);
  EXPECT_EQ(t->to_iso8601(), "2026-08-07T12:34:56");
}
