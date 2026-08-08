#include "calais/core/clock.h"

#include <time.h>

#include <algorithm>
#include <array>
#include <thread>

namespace calais::core {
namespace {

std::int64_t clock_ns(clockid_t id) noexcept {
  struct timespec ts {};
  ::clock_gettime(id, &ts);
  return static_cast<std::int64_t>(ts.tv_sec) * 1'000'000'000LL +
         static_cast<std::int64_t>(ts.tv_nsec);
}

#if defined(__aarch64__)
std::uint64_t read_cntfrq() noexcept {
  std::uint64_t v;
  __asm__ __volatile__("mrs %0, cntfrq_el0" : "=r"(v));
  return v;
}
#endif

// Calibrate the counter against CLOCK_MONOTONIC.
//
// Run several short trials and keep the one with the smallest elapsed wall
// time for a fixed tick budget -- i.e. the trial least disturbed by
// preemption. Taking a mean here would bake in scheduler noise; taking the
// minimum-disturbance sample is the standard trick.
double calibrate_ns_per_tick() noexcept {
  constexpr int kTrials = 5;
  constexpr std::int64_t kTrialNs = 20'000'000;  // 20ms each

  double best_ns_per_tick = 0.0;
  double best_quality = 1e300;

  for (int trial = 0; trial < kTrials; ++trial) {
    const std::int64_t t0 = clock_ns(CLOCK_MONOTONIC);
    const Ticks c0 = now_ticks_serialized();

    std::int64_t t1 = t0;
    while (t1 - t0 < kTrialNs) {
      t1 = clock_ns(CLOCK_MONOTONIC);
    }

    const Ticks c1 = now_ticks_serialized();
    const std::int64_t elapsed_ns = t1 - t0;
    const std::uint64_t elapsed_ticks = c1 - c0;

    if (elapsed_ticks == 0) {
      continue;
    }

    const double ns_per_tick =
        static_cast<double>(elapsed_ns) / static_cast<double>(elapsed_ticks);

    // Overshoot past the trial budget is our proxy for "this sample got
    // descheduled". Smaller is better.
    const double quality = static_cast<double>(elapsed_ns - kTrialNs);
    if (quality < best_quality) {
      best_quality = quality;
      best_ns_per_tick = ns_per_tick;
    }
  }

  return best_ns_per_tick > 0.0 ? best_ns_per_tick : 1.0;
}

// Measure how far the counter actually jumps when it moves.
//
// The nominal frequency is not the granularity. Apple Silicon reports 1GHz via
// CNTFRQ_EL0 while the counter advances in steps of 40 ticks (~42ns), and x86
// TSC on some virtualised hosts is similarly coarse. Read repeatedly until the
// value changes and keep the smallest step seen.
std::uint64_t measure_tick_granularity() noexcept {
  constexpr int kTrials = 32;
  constexpr int kSpinLimit = 10'000'000;

  std::uint64_t smallest = UINT64_MAX;
  for (int trial = 0; trial < kTrials; ++trial) {
    const Ticks a = now_ticks();
    Ticks b = a;
    int spins = 0;
    while (b == a && spins < kSpinLimit) {
      b = now_ticks();
      ++spins;
    }
    if (b > a) {
      const std::uint64_t delta = b - a;
      if (delta < smallest) {
        smallest = delta;
      }
    }
  }
  return smallest == UINT64_MAX ? 1 : smallest;
}

ClockInfo build_clock_info() noexcept {
  ClockInfo info;

#if defined(__aarch64__)
  const std::uint64_t hz = read_cntfrq();
  if (hz > 0) {
    info.hz = hz;
    info.ns_per_tick = 1e9 / static_cast<double>(hz);
    info.ticks_per_ns = static_cast<double>(hz) / 1e9;
    info.source = "cntfrq_el0";
    info.exact = true;
    info.tick_granularity = measure_tick_granularity();
    info.resolution_ns =
        static_cast<double>(info.tick_granularity) * info.ns_per_tick;
    return info;
  }
#endif

  const double ns_per_tick = calibrate_ns_per_tick();
  info.ns_per_tick = ns_per_tick;
  info.ticks_per_ns = 1.0 / ns_per_tick;
  info.hz = static_cast<std::uint64_t>(1e9 / ns_per_tick + 0.5);
  info.source = "calibrated-vs-CLOCK_MONOTONIC";
  info.exact = false;
  info.tick_granularity = measure_tick_granularity();
  info.resolution_ns =
      static_cast<double>(info.tick_granularity) * info.ns_per_tick;
  return info;
}

}  // namespace

const ClockInfo& clock_info() noexcept {
  // Magic static: thread-safe, initialised exactly once, and immune to static
  // initialisation order across translation units.
  static const ClockInfo info = build_clock_info();
  return info;
}

std::int64_t wall_clock_ns() noexcept { return clock_ns(CLOCK_REALTIME); }

std::int64_t monotonic_ns() noexcept { return clock_ns(CLOCK_MONOTONIC); }

}  // namespace calais::core
