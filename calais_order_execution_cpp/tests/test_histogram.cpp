#include "calais/core/histogram.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

using calais::core::Histogram;

namespace {

// The exact percentile of a sorted sample, using the same "round to nearest
// rank" convention the histogram uses.
std::uint64_t exact_percentile(std::vector<std::uint64_t> values, double p) {
  std::sort(values.begin(), values.end());
  auto rank = static_cast<std::size_t>(
      (p / 100.0) * static_cast<double>(values.size()) + 0.5);
  if (rank == 0) {
    rank = 1;
  }
  if (rank > values.size()) {
    rank = values.size();
  }
  return values[rank - 1];
}

}  // namespace

TEST(Histogram, EmptyReportsZeros) {
  Histogram h;
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.min(), 0u);
  EXPECT_EQ(h.max(), 0u);
  EXPECT_EQ(h.p50(), 0u);
  EXPECT_EQ(h.p99(), 0u);
  EXPECT_DOUBLE_EQ(h.mean(), 0.0);
  EXPECT_DOUBLE_EQ(h.stddev(), 0.0);
}

TEST(Histogram, RejectsBadParameters) {
  EXPECT_THROW(Histogram(1000, 0), std::invalid_argument);
  EXPECT_THROW(Histogram(1000, 6), std::invalid_argument);
  EXPECT_THROW(Histogram(1, 3), std::invalid_argument);
}

TEST(Histogram, SingleValue) {
  Histogram h;
  h.record(1234);
  EXPECT_EQ(h.count(), 1u);
  EXPECT_EQ(h.min(), 1234u);
  EXPECT_EQ(h.max(), 1234u);
  EXPECT_DOUBLE_EQ(h.mean(), 1234.0);
  // Percentiles land in the same bucket as the recorded value.
  EXPECT_GE(h.p50(), h.lowest_equivalent(1234));
  EXPECT_LE(h.p50(), h.highest_equivalent(1234));
}

TEST(Histogram, ZeroIsRecordable) {
  Histogram h;
  h.record(0);
  EXPECT_EQ(h.count(), 1u);
  EXPECT_EQ(h.min(), 0u);
  EXPECT_EQ(h.p50(), 0u);
}

TEST(Histogram, BucketingIsWithinPrecisionAcrossTheWholeRange) {
  // The core promise: a value recorded anywhere in the range is recoverable to
  // within 10^-significant_figures relative error.
  constexpr int kSigFigs = 3;
  Histogram h(60'000'000'000ULL, kSigFigs);
  const double tolerance = 1.0 / std::pow(10.0, kSigFigs);

  for (std::uint64_t v = 1; v < 50'000'000'000ULL; v = v + 1 + v / 7) {
    const std::uint64_t lo = h.lowest_equivalent(v);
    const std::uint64_t hi = h.highest_equivalent(v);
    ASSERT_LE(lo, v) << "value " << v;
    ASSERT_GE(hi, v) << "value " << v;

    const double relative_error =
        static_cast<double>(hi - lo) / static_cast<double>(v);
    ASSERT_LT(relative_error, tolerance * 2.0)
        << "value " << v << " bucket [" << lo << ", " << hi << "]";
  }
}

TEST(Histogram, PercentilesMatchExactWithinPrecision) {
  Histogram h(60'000'000'000ULL, 3);
  std::vector<std::uint64_t> raw;

  // A realistic latency shape: a tight body plus a heavy tail.
  std::mt19937_64 rng(1234);
  std::lognormal_distribution<double> body(std::log(50000.0), 0.4);
  std::uniform_real_distribution<double> pick(0.0, 1.0);
  std::lognormal_distribution<double> tail(std::log(2'000'000.0), 0.8);

  for (int i = 0; i < 200000; ++i) {
    const double v = (pick(rng) < 0.005) ? tail(rng) : body(rng);
    const auto value = static_cast<std::uint64_t>(v);
    raw.push_back(value);
    h.record(value);
  }

  EXPECT_EQ(h.count(), raw.size());

  for (const double p : {50.0, 90.0, 99.0, 99.9, 99.99}) {
    const std::uint64_t expected = exact_percentile(raw, p);
    const std::uint64_t got = h.value_at_percentile(p);
    // Must be in the same bucket as the true value, and never below it.
    EXPECT_GE(got, h.lowest_equivalent(expected)) << "p" << p;
    EXPECT_LE(got, h.highest_equivalent(expected) + 1) << "p" << p;
  }
}

TEST(Histogram, MeanAndStddevAreAccurate) {
  Histogram h;
  double sum = 0.0;
  double sum_sq = 0.0;
  constexpr int kN = 10000;
  for (int i = 1; i <= kN; ++i) {
    const auto v = static_cast<std::uint64_t>(i);
    h.record(v);
    sum += static_cast<double>(v);
    sum_sq += static_cast<double>(v) * static_cast<double>(v);
  }
  const double expected_mean = sum / kN;
  const double expected_var = sum_sq / kN - expected_mean * expected_mean;

  EXPECT_NEAR(h.mean(), expected_mean, expected_mean * 1e-9);
  EXPECT_NEAR(h.stddev(), std::sqrt(expected_var), std::sqrt(expected_var) * 1e-9);
}

TEST(Histogram, ClampsRatherThanDropsOverflow) {
  // An outlier that vanished from the histogram would make the tail look
  // healthy, which is the exact failure this design must avoid.
  Histogram h(1'000'000ULL, 3);
  h.record(500'000);
  h.record(999'999'999'999ULL);

  EXPECT_EQ(h.count(), 2u);
  EXPECT_EQ(h.overflow_count(), 1u);
  EXPECT_EQ(h.max(), 1'000'000ULL);
  EXPECT_GE(h.p99(), 1'000'000ULL - 1000);
}

TEST(Histogram, P999IsSensitiveToATinyTail) {
  // 9980 fast samples and 20 slow ones (0.2%): the mean barely moves, but
  // p99.9 must land squarely in the slow group. This is the property that
  // makes the histogram worth having -- the mean here is ~20us, which looks
  // fine, while one request in 500 takes 10ms.
  Histogram h;
  for (int i = 0; i < 9980; ++i) {
    h.record(100);
  }
  for (int i = 0; i < 20; ++i) {
    h.record(10'000'000);
  }

  EXPECT_LT(h.p50(), 200u);
  EXPECT_LT(h.p99(), 200u);
  EXPECT_GE(h.p999(), 9'000'000u);
  EXPECT_EQ(h.max(), 10'000'000u);
  EXPECT_LT(h.mean(), 25'000.0) << "the mean hides what p99.9 exposes";
}

TEST(Histogram, PercentileUsesNearestRank) {
  // Boundary behaviour worth pinning down so nobody "fixes" it later: with
  // exactly 1 slow sample in 1000, the 99.9th percentile by nearest rank is
  // the 999th value -- a FAST one. The outlier shows up in max(), not p99.9.
  // This matches HdrHistogram's convention.
  Histogram h;
  for (int i = 0; i < 999; ++i) {
    h.record(100);
  }
  h.record(10'000'000);

  EXPECT_LT(h.p999(), 200u) << "1 in 1000 sits exactly on the rank boundary";
  EXPECT_EQ(h.max(), 10'000'000u);
  EXPECT_EQ(h.value_at_percentile(100.0), h.highest_equivalent(10'000'000));
}

TEST(Histogram, RecordManyMatchesRepeatedRecord) {
  Histogram a;
  Histogram b;
  for (int i = 0; i < 500; ++i) {
    a.record(777);
  }
  b.record_many(777, 500);

  EXPECT_EQ(a.count(), b.count());
  EXPECT_EQ(a.p50(), b.p50());
  EXPECT_EQ(a.max(), b.max());
  EXPECT_DOUBLE_EQ(a.mean(), b.mean());
}

TEST(Histogram, RecordManyOfZeroCountIsNoOp) {
  Histogram h;
  h.record_many(100, 0);
  EXPECT_EQ(h.count(), 0u);
}

TEST(Histogram, Reset) {
  Histogram h;
  for (int i = 0; i < 1000; ++i) {
    h.record(static_cast<std::uint64_t>(i + 1));
  }
  h.reset();
  EXPECT_EQ(h.count(), 0u);
  EXPECT_EQ(h.max(), 0u);
  EXPECT_EQ(h.min(), 0u);
  EXPECT_EQ(h.p99(), 0u);
  EXPECT_DOUBLE_EQ(h.mean(), 0.0);
}

TEST(Histogram, Merge) {
  Histogram a;
  Histogram b;
  Histogram combined;

  for (int i = 1; i <= 1000; ++i) {
    a.record(static_cast<std::uint64_t>(i));
    combined.record(static_cast<std::uint64_t>(i));
  }
  for (int i = 1001; i <= 2000; ++i) {
    b.record(static_cast<std::uint64_t>(i));
    combined.record(static_cast<std::uint64_t>(i));
  }

  a.merge(b);
  EXPECT_EQ(a.count(), combined.count());
  EXPECT_EQ(a.min(), combined.min());
  EXPECT_EQ(a.max(), combined.max());
  EXPECT_EQ(a.p50(), combined.p50());
  EXPECT_EQ(a.p99(), combined.p99());
}

TEST(Histogram, MergeRejectsMismatchedParameters) {
  Histogram a(1'000'000, 3);
  Histogram b(1'000'000, 2);
  EXPECT_THROW(a.merge(b), std::invalid_argument);
}

TEST(Histogram, PercentileArgumentIsClamped) {
  Histogram h;
  for (int i = 1; i <= 100; ++i) {
    h.record(static_cast<std::uint64_t>(i));
  }
  EXPECT_EQ(h.value_at_percentile(-5.0), h.value_at_percentile(0.0));
  EXPECT_EQ(h.value_at_percentile(150.0), h.value_at_percentile(100.0));
  EXPECT_GE(h.value_at_percentile(100.0), 100u);
}

TEST(Histogram, MemoryStaysBounded) {
  // 3 significant figures over a 60s range must not need a giant table.
  Histogram h(60'000'000'000ULL, 3);
  EXPECT_LT(h.bucket_count(), 100000u);
  EXPECT_GT(h.bucket_count(), 1000u);
}

TEST(Histogram, SummaryIsNonEmpty) {
  Histogram h;
  h.record(42);
  const std::string s = h.summary("ns");
  EXPECT_NE(s.find("n=1"), std::string::npos);
  EXPECT_NE(s.find("(ns)"), std::string::npos);
}
