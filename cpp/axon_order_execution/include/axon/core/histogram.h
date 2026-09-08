// HDR-style latency histogram.
//
// Latency work is decided by tails, not averages. A mean of 40us tells you
// nothing when p99.9 is 8ms -- and it is the 8ms fills that cost money. This
// records at constant cost so you can afford to measure every message, and
// reports the percentiles that actually matter.
//
// Structure: values are bucketed by binary exponent, and each exponent is
// subdivided into a fixed number of linear sub-buckets. That gives constant
// RELATIVE precision across the whole range -- 3 significant figures means a
// recorded 1us is accurate to ~1ns and a recorded 1s is accurate to ~1ms, in
// bounded memory. A plain linear histogram covering 1ns..60s at 1ns resolution
// would need 60 billion buckets; this needs ~50k.
//
// Cost of record(): a leading-zero count, a couple of shifts, one increment.
// ~5ns, no branches on the common path, no allocation ever after construction.
//
// THREADING: single-writer. Give each hot-path thread its own histogram and
// have the reporting thread merge snapshots. Counters are plain (non-atomic)
// integers on purpose -- making them atomic would cost more than the
// measurement itself. Reading from another thread while recording is racy but
// benign: you may see a slightly stale count, never a corrupt one.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace axon::core {

class Histogram {
 public:
  // `highest_trackable` sets the range; values above it are clamped into the
  // top bucket and counted in overflow_count(). `significant_figures` in 1..5
  // trades memory for precision; 3 is the usual choice.
  explicit Histogram(std::uint64_t highest_trackable = 60'000'000'000ULL,  // 60s in ns
                     int significant_figures = 3);

  // Constant-time. Values of 0 are recorded as 0 (a legitimate sub-tick
  // measurement), values above the range are clamped and counted as overflow.
  void record(std::uint64_t value) noexcept;

  // Record `count` occurrences of the same value. Used when replaying or
  // merging.
  void record_many(std::uint64_t value, std::uint64_t count) noexcept;

  void reset() noexcept;

  // Adds another histogram's counts into this one. Both must have identical
  // parameters.
  void merge(const Histogram& other);

  std::uint64_t count() const noexcept { return total_count_; }
  std::uint64_t min() const noexcept { return total_count_ == 0 ? 0 : min_; }
  std::uint64_t max() const noexcept { return max_; }
  std::uint64_t overflow_count() const noexcept { return overflow_count_; }
  double mean() const noexcept;
  double stddev() const noexcept;

  // `percentile` in [0, 100]. Returns the highest value in the bucket at that
  // percentile, so the true value is guaranteed <= the returned number.
  std::uint64_t value_at_percentile(double percentile) const noexcept;

  // Convenience for the numbers we actually report on.
  std::uint64_t p50() const noexcept { return value_at_percentile(50.0); }
  std::uint64_t p90() const noexcept { return value_at_percentile(90.0); }
  std::uint64_t p99() const noexcept { return value_at_percentile(99.0); }
  std::uint64_t p999() const noexcept { return value_at_percentile(99.9); }
  std::uint64_t p9999() const noexcept { return value_at_percentile(99.99); }

  // Single-line summary, e.g.
  //   n=100000 min=41 p50=83 p99=210 p99.9=1204 max=8801 (ns)
  std::string summary(const char* unit = "ns") const;

  int significant_figures() const noexcept { return significant_figures_; }
  std::uint64_t highest_trackable() const noexcept { return highest_trackable_; }
  std::size_t bucket_count() const noexcept { return counts_.size(); }

  // Smallest and largest values that land in the same bucket as `value`.
  // Useful when asserting that a reported percentile is "equal" to an expected
  // value within the histogram's precision.
  std::uint64_t lowest_equivalent(std::uint64_t value) const noexcept;
  std::uint64_t highest_equivalent(std::uint64_t value) const noexcept;

 private:
  std::size_t counts_index(std::uint64_t value) const noexcept;
  std::uint64_t value_from_index(std::size_t index) const noexcept;
  int bucket_index_of(std::uint64_t value) const noexcept;
  std::uint64_t size_of_equivalent_range(std::uint64_t value) const noexcept;

  std::uint64_t highest_trackable_;
  int significant_figures_;

  int sub_bucket_half_count_magnitude_;
  std::uint32_t sub_bucket_count_;
  std::uint32_t sub_bucket_half_count_;
  std::uint64_t sub_bucket_mask_;
  int leading_zero_count_base_;
  int bucket_count_;

  std::vector<std::uint64_t> counts_;
  std::uint64_t total_count_ = 0;
  std::uint64_t overflow_count_ = 0;
  std::uint64_t min_ = UINT64_MAX;
  std::uint64_t max_ = 0;

  // Kept in double for the mean/stddev; the sums would overflow uint64 for
  // long runs of large values and exactness is not required for a summary
  // statistic.
  double sum_ = 0.0;
  double sum_squares_ = 0.0;
};

}  // namespace axon::core
