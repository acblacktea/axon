// End-to-end latency instrumentation.
//
// The point of this file is to make "where did the time go" answerable without
// guessing. A single total-latency number tells you the system is slow; a
// per-stage breakdown tells you which stage to fix. Stamping is cheap enough
// (~5ns per stamp) to leave on in production, and it must be: a latency
// problem that only appears under real load cannot be reproduced on a bench.
//
// Split of responsibilities, which is the whole design:
//
//   HOT PATH   fills a Timeline with raw ticks and pushes it into an SpscRing.
//              No division, no floating point, no histogram, no formatting.
//
//   REPORTING  pops Timelines, converts ticks to nanoseconds, and folds them
//   THREAD     into per-stage histograms.
//
// Doing the conversion on the hot path would add a floating-point multiply and
// a dependent load of clock_info() to every message, to produce a number
// nobody reads for another second.

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "calais/core/clock.h"
#include "calais/core/histogram.h"
#include "calais/core/platform.h"

namespace calais::core {

// A fixed set of stamp points for one message's journey through the system.
// `N` is the number of stages, known at compile time so the whole thing is a
// flat array with no indirection.
template <std::size_t N>
class Timeline {
  static_assert(N >= 2, "a timeline needs at least a start and an end");

 public:
  static constexpr std::size_t kStages = N;

  CALAIS_ALWAYS_INLINE void stamp(std::size_t stage) noexcept {
    stamps_[stage] = now_ticks();
  }

  // For a stage whose time was captured elsewhere, e.g. a kernel receive
  // timestamp from SO_TIMESTAMPING.
  CALAIS_ALWAYS_INLINE void set(std::size_t stage, Ticks t) noexcept {
    stamps_[stage] = t;
  }

  CALAIS_ALWAYS_INLINE Ticks at(std::size_t stage) const noexcept {
    return stamps_[stage];
  }

  // Ticks between two stages. Unsigned subtraction, so an out-of-order pair
  // yields a huge number rather than a negative one -- check ordering in the
  // caller if the stages can legitimately be skipped.
  CALAIS_ALWAYS_INLINE Ticks delta(std::size_t from, std::size_t to) const noexcept {
    return stamps_[to] - stamps_[from];
  }

  CALAIS_ALWAYS_INLINE Ticks total() const noexcept { return delta(0, N - 1); }

  void reset() noexcept { stamps_.fill(0); }

  const Ticks* data() const noexcept { return stamps_.data(); }

 private:
  std::array<Ticks, N> stamps_{};
};

// Per-stage histograms plus a total. Owned and driven by the reporting thread.
class StageLatency {
 public:
  // `stage_names` must have at least two entries. Produces
  // stage_names.size() - 1 segment histograms plus one total histogram.
  explicit StageLatency(std::vector<std::string> stage_names,
                        std::uint64_t highest_ns = 60'000'000'000ULL,
                        int significant_figures = 3);

  // Folds one journey into the histograms. `stamps` must have
  // stage_count() entries. Converts ticks to nanoseconds here -- reporting
  // thread only, never the hot path.
  void record(const Ticks* stamps, std::size_t count);

  template <std::size_t N>
  void record(const Timeline<N>& tl) {
    record(tl.data(), N);
  }

  std::size_t stage_count() const noexcept { return stage_names_.size(); }
  std::size_t segment_count() const noexcept { return segments_.size(); }

  const std::string& stage_name(std::size_t i) const { return stage_names_[i]; }

  // Histogram of the time from stage `i` to stage `i+1`, in nanoseconds.
  const Histogram& segment(std::size_t i) const { return segments_[i]; }
  const Histogram& total() const noexcept { return total_; }

  std::uint64_t dropped() const noexcept { return dropped_; }

  void reset();

  // Multi-line table: one row per segment plus a total row.
  std::string report() const;

 private:
  std::vector<std::string> stage_names_;
  std::vector<Histogram> segments_;
  Histogram total_;
  // Journeys rejected because stamps went backwards (a stage was skipped, or
  // the counter was read across a core migration on a machine without
  // invariant TSC).
  std::uint64_t dropped_ = 0;
};

}  // namespace calais::core
