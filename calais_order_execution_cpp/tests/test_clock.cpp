#include "calais/core/clock.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdio>
#include <thread>

using calais::core::clock_info;
using calais::core::monotonic_ns;
using calais::core::now_ticks;
using calais::core::now_ticks_serialized;
using calais::core::Ticks;
using calais::core::ticks_to_ns;
using calais::core::wall_clock_ns;

TEST(Clock, InfoIsSane) {
  const auto& info = clock_info();
  std::printf(
      "[clock] source=%s hz=%llu ns_per_tick=%.4f granularity=%llu ticks "
      "(%.2fns) exact_rate=%d\n",
      info.source, static_cast<unsigned long long>(info.hz), info.ns_per_tick,
      static_cast<unsigned long long>(info.tick_granularity), info.resolution_ns,
      info.exact ? 1 : 0);

  EXPECT_GT(info.hz, 1'000'000ULL) << "counter slower than 1MHz is unusable";
  EXPECT_GT(info.ns_per_tick, 0.0);
  EXPECT_LT(info.ns_per_tick, 1000.0);
  EXPECT_NEAR(info.ns_per_tick * info.ticks_per_ns, 1.0, 1e-9);
}

TEST(Clock, GranularityIsMeasuredNotAssumed) {
  // The nominal frequency lies about granularity on some platforms -- Apple
  // Silicon reports 1GHz while the counter steps ~42ns at a time. Trusting
  // CNTFRQ would make every sub-42ns measurement read as 0 or 42, which looks
  // like working instrumentation and is not. So this must be measured.
  const auto& info = clock_info();

  EXPECT_GE(info.tick_granularity, 1u);
  EXPECT_GT(info.resolution_ns, 0.0);
  EXPECT_LT(info.resolution_ns, 10'000.0) << "clock too coarse to be useful";

  // Independently confirm it: consecutive differing reads must never be closer
  // together than the reported granularity.
  std::uint64_t smallest = UINT64_MAX;
  for (int trial = 0; trial < 64; ++trial) {
    const Ticks a = now_ticks();
    Ticks b = a;
    int spins = 0;
    while (b == a && spins < 1'000'000) {
      b = now_ticks();
      ++spins;
    }
    if (b > a) {
      smallest = std::min(smallest, b - a);
    }
  }
  ASSERT_NE(smallest, UINT64_MAX) << "counter never advanced";
  EXPECT_GE(smallest, info.tick_granularity)
      << "observed a step finer than the reported granularity";
}

TEST(Clock, InfoIsStableAcrossCalls) {
  // Magic-static initialisation must happen exactly once; a second call
  // returning different numbers would mean every conversion is inconsistent.
  const auto& a = clock_info();
  const auto& b = clock_info();
  EXPECT_EQ(&a, &b);
  EXPECT_EQ(a.hz, b.hz);
}

TEST(Clock, TicksAdvanceMonotonically) {
  Ticks prev = now_ticks();
  for (int i = 0; i < 100000; ++i) {
    const Ticks t = now_ticks();
    ASSERT_GE(t, prev);
    prev = t;
  }
}

TEST(Clock, SerializedTicksAdvanceMonotonically) {
  Ticks prev = now_ticks_serialized();
  for (int i = 0; i < 10000; ++i) {
    const Ticks t = now_ticks_serialized();
    ASSERT_GE(t, prev);
    prev = t;
  }
}

TEST(Clock, TicksAgreeWithMonotonicClock) {
  // The whole point of the calibration: a tick delta converted to nanoseconds
  // must match an independent clock. 10% tolerance absorbs scheduling noise on
  // a shared dev machine.
  const std::int64_t t0 = monotonic_ns();
  const Ticks c0 = now_ticks_serialized();

  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  const Ticks c1 = now_ticks_serialized();
  const std::int64_t t1 = monotonic_ns();

  const double from_ticks = ticks_to_ns(c1 - c0);
  const double from_clock = static_cast<double>(t1 - t0);

  EXPECT_NEAR(from_ticks, from_clock, from_clock * 0.10)
      << "tick rate calibration is off: " << from_ticks << "ns vs "
      << from_clock << "ns";
}

TEST(Clock, WallClockIsPlausible) {
  const std::int64_t ns = wall_clock_ns();
  const std::int64_t seconds = ns / 1'000'000'000LL;
  EXPECT_GT(seconds, 1'577'836'800LL);  // after 2020-01-01
  EXPECT_LT(seconds, 4'102'444'800LL);  // before 2100-01-01
}

TEST(Clock, WallAndMonotonicAdvanceTogether) {
  const std::int64_t w0 = wall_clock_ns();
  const std::int64_t m0 = monotonic_ns();
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const std::int64_t w1 = wall_clock_ns();
  const std::int64_t m1 = monotonic_ns();

  EXPECT_GT(w1, w0);
  EXPECT_GT(m1, m0);
  EXPECT_NEAR(static_cast<double>(w1 - w0), static_cast<double>(m1 - m0),
              5'000'000.0);  // 5ms
}

TEST(Clock, ReadIsCheapEnoughForTheHotPath) {
  // Not a benchmark assertion so much as a tripwire: if now_ticks() ever
  // becomes a syscall on some platform, this catches it immediately.
  constexpr int kIters = 200000;
  const std::int64_t t0 = monotonic_ns();
  Ticks sink = 0;
  for (int i = 0; i < kIters; ++i) {
    sink += now_ticks();
  }
  const std::int64_t t1 = monotonic_ns();
  EXPECT_NE(sink, 0u);

  const double ns_each = static_cast<double>(t1 - t0) / kIters;
  std::printf("[clock] now_ticks() ~%.1fns per call\n", ns_each);
  EXPECT_LT(ns_each, 100.0) << "now_ticks() is far too expensive for a poll loop";
}
