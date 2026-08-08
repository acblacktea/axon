// Cycle-counter clock.
//
// Design rule that shapes this whole file: THE HOT PATH RECORDS RAW TICKS AND
// NEVER CONVERTS. `now_ticks()` is a single register read (~5ns, no syscall, no
// memory barrier, no vDSO call). Conversion to nanoseconds is a floating-point
// multiply done by the reporting thread, long after the trade decision was
// made. Never call ticks_to_ns() inside a busy-poll loop.
//
// Counter source:
//   x86_64  -- RDTSC. Invariant TSC is assumed (every server CPU since Nehalem;
//              check `constant_tsc`+`nonstop_tsc` in /proc/cpuinfo). The
//              tick rate is not architecturally discoverable, so we calibrate
//              against CLOCK_MONOTONIC at startup.
//   aarch64 -- CNTVCT_EL0, with the rate from CNTFRQ_EL0.
//
// CNTFRQ_EL0 CANNOT BE TRUSTED FOR GRANULARITY. On Apple Silicon it reports
// 1GHz, but the counter actually advances in steps of ~42ns -- the frequency
// is scaled, the underlying tick is not 1ns. Believing CNTFRQ would make every
// sub-42ns measurement read as either 0 or 42, which looks like working
// instrumentation and is not.
//
// So the granularity is MEASURED at startup, not derived, and reported as
// clock_info().resolution_ns. Always check it before trusting a sub-100ns
// number on a new machine; if an operation costs less than the resolution,
// time a batch of them and divide instead.
//
// RDTSC is not a serialising instruction; the CPU may reorder it against
// surrounding loads/stores. For end-to-end latency over microseconds that skew
// is irrelevant. If you ever need to time an instruction sequence of a few
// nanoseconds, use now_ticks_serialized().

#pragma once

#include <cstdint>

#include "calais/core/platform.h"

#if defined(__x86_64__) || defined(__i386__)
#include <x86intrin.h>
#endif

namespace calais::core {

using Ticks = std::uint64_t;

// Single register read. This is the function that runs on the hot path.
CALAIS_ALWAYS_INLINE Ticks now_ticks() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  return __rdtsc();
#elif defined(__aarch64__)
  std::uint64_t v;
  __asm__ __volatile__("mrs %0, cntvct_el0" : "=r"(v));
  return v;
#else
  return 0;
#endif
}

// Ordered variant: prevents the counter read from floating across neighbouring
// memory operations. Costs noticeably more than now_ticks(); use only when
// timing very short instruction sequences.
CALAIS_ALWAYS_INLINE Ticks now_ticks_serialized() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  unsigned aux;
  return __rdtscp(&aux);
#elif defined(__aarch64__)
  std::uint64_t v;
  __asm__ __volatile__("isb\n\tmrs %0, cntvct_el0" : "=r"(v)::"memory");
  return v;
#else
  return 0;
#endif
}

struct ClockInfo {
  double ns_per_tick = 1.0;
  double ticks_per_ns = 1.0;
  std::uint64_t hz = 1'000'000'000;

  // Smallest observed non-zero step of the counter, in ticks. Measured, not
  // derived from the nominal frequency.
  std::uint64_t tick_granularity = 1;

  // tick_granularity converted to nanoseconds: the finest interval this clock
  // can actually distinguish. If this is 42, a "37ns" measurement is noise and
  // a "0ns" measurement means "below 42ns", not "free".
  double resolution_ns = 1.0;

  // "cntfrq_el0" (nominal rate is exact) or "calibrated-vs-CLOCK_MONOTONIC".
  const char* source = "unknown";
  // Whether the RATE is exact. Says nothing about the granularity.
  bool exact = false;
};

// Lazily initialised on first call, thread-safe. Not for the hot path.
const ClockInfo& clock_info() noexcept;

inline double ticks_to_ns(Ticks t) noexcept {
  return static_cast<double>(t) * clock_info().ns_per_tick;
}

inline std::uint64_t ticks_to_ns_u64(Ticks t) noexcept {
  return static_cast<std::uint64_t>(ticks_to_ns(t) + 0.5);
}

inline Ticks ns_to_ticks(double ns) noexcept {
  return static_cast<Ticks>(ns * clock_info().ticks_per_ns + 0.5);
}

// Nanoseconds since the Unix epoch (CLOCK_REALTIME). Involves a vDSO call
// (~20-25ns) -- fine for stamping an order once, too expensive for a poll
// loop. Use now_ticks() there and anchor to wall time once per batch.
std::int64_t wall_clock_ns() noexcept;

// Monotonic nanoseconds (CLOCK_MONOTONIC). Used to calibrate the tick rate and
// for timeouts that must survive a wall-clock step.
std::int64_t monotonic_ns() noexcept;

}  // namespace calais::core
