#include "axon/core/histogram.h"

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <stdexcept>

namespace axon::core {
namespace {

int ceil_log2(std::uint64_t v) noexcept {
  int m = 0;
  while ((1ULL << m) < v) {
    ++m;
  }
  return m;
}

std::uint64_t pow10_u64(int n) noexcept {
  std::uint64_t r = 1;
  for (int i = 0; i < n; ++i) {
    r *= 10;
  }
  return r;
}

}  // namespace

Histogram::Histogram(std::uint64_t highest_trackable, int significant_figures)
    : highest_trackable_(highest_trackable),
      significant_figures_(significant_figures) {
  if (significant_figures < 1 || significant_figures > 5) {
    throw std::invalid_argument("significant_figures must be in [1, 5]");
  }
  if (highest_trackable < 2) {
    throw std::invalid_argument("highest_trackable must be >= 2");
  }

  // Enough linear sub-buckets per binary bucket to resolve the requested
  // number of significant figures, rounded up to a power of two so the index
  // arithmetic is shifts rather than divides.
  const std::uint64_t largest_value_with_single_unit_resolution =
      2 * pow10_u64(significant_figures);
  const int sub_bucket_count_magnitude =
      ceil_log2(largest_value_with_single_unit_resolution);

  sub_bucket_half_count_magnitude_ =
      sub_bucket_count_magnitude > 1 ? sub_bucket_count_magnitude - 1 : 0;
  sub_bucket_count_ = 1U << (sub_bucket_half_count_magnitude_ + 1);
  sub_bucket_half_count_ = sub_bucket_count_ / 2;
  sub_bucket_mask_ = static_cast<std::uint64_t>(sub_bucket_count_) - 1;

  // unit_magnitude is fixed at 0: the smallest distinguishable value is 1,
  // which for a nanosecond-denominated latency histogram is exactly right.
  leading_zero_count_base_ = 64 - sub_bucket_half_count_magnitude_ - 1;

  // Grow buckets until the top of the range is covered.
  std::uint64_t smallest_untrackable = sub_bucket_count_;
  int buckets = 1;
  while (smallest_untrackable <= highest_trackable_) {
    if (smallest_untrackable > (UINT64_MAX / 2)) {
      ++buckets;
      break;
    }
    smallest_untrackable <<= 1;
    ++buckets;
  }
  bucket_count_ = buckets;

  const std::size_t len = static_cast<std::size_t>(bucket_count_ + 1) *
                          static_cast<std::size_t>(sub_bucket_half_count_);
  counts_.assign(len, 0);
}

int Histogram::bucket_index_of(std::uint64_t value) const noexcept {
  // ORing in the mask guarantees a non-zero operand for clz and puts every
  // value below sub_bucket_count into bucket 0.
  const int clz = __builtin_clzll(value | sub_bucket_mask_);
  return leading_zero_count_base_ - clz;
}

std::size_t Histogram::counts_index(std::uint64_t value) const noexcept {
  const int bucket_index = bucket_index_of(value);
  const std::uint64_t sub_bucket_index = value >> bucket_index;
  const std::size_t bucket_base =
      static_cast<std::size_t>(bucket_index + 1)
      << sub_bucket_half_count_magnitude_;
  const std::size_t offset =
      static_cast<std::size_t>(sub_bucket_index) - sub_bucket_half_count_;
  return bucket_base + offset;
}

std::uint64_t Histogram::value_from_index(std::size_t index) const noexcept {
  int bucket_index =
      static_cast<int>(index >> sub_bucket_half_count_magnitude_) - 1;
  std::uint64_t sub_bucket_index =
      (index & (static_cast<std::size_t>(sub_bucket_half_count_) - 1)) +
      sub_bucket_half_count_;
  if (bucket_index < 0) {
    sub_bucket_index -= sub_bucket_half_count_;
    bucket_index = 0;
  }
  return sub_bucket_index << bucket_index;
}

std::uint64_t Histogram::size_of_equivalent_range(
    std::uint64_t value) const noexcept {
  return 1ULL << bucket_index_of(value);
}

std::uint64_t Histogram::lowest_equivalent(std::uint64_t value) const noexcept {
  const int bucket_index = bucket_index_of(value);
  const std::uint64_t sub_bucket_index = value >> bucket_index;
  return sub_bucket_index << bucket_index;
}

std::uint64_t Histogram::highest_equivalent(std::uint64_t value) const noexcept {
  return lowest_equivalent(value) + size_of_equivalent_range(value) - 1;
}

void Histogram::record(std::uint64_t value) noexcept { record_many(value, 1); }

void Histogram::record_many(std::uint64_t value, std::uint64_t count) noexcept {
  if (count == 0) {
    return;
  }

  std::uint64_t v = value;
  if (v > highest_trackable_) {
    // Clamp rather than drop: an outlier that vanishes from the histogram is
    // worse than one reported as "at least the ceiling", because the tail
    // percentiles would then look healthy.
    overflow_count_ += count;
    v = highest_trackable_;
  }

  const std::size_t idx = counts_index(v);
  if (idx >= counts_.size()) {
    overflow_count_ += count;
    return;
  }

  counts_[idx] += count;
  total_count_ += count;

  const double dv = static_cast<double>(v);
  sum_ += dv * static_cast<double>(count);
  sum_squares_ += dv * dv * static_cast<double>(count);

  if (v < min_) {
    min_ = v;
  }
  if (v > max_) {
    max_ = v;
  }
}

void Histogram::reset() noexcept {
  std::fill(counts_.begin(), counts_.end(), 0ULL);
  total_count_ = 0;
  overflow_count_ = 0;
  min_ = UINT64_MAX;
  max_ = 0;
  sum_ = 0.0;
  sum_squares_ = 0.0;
}

void Histogram::merge(const Histogram& other) {
  if (other.counts_.size() != counts_.size() ||
      other.significant_figures_ != significant_figures_) {
    throw std::invalid_argument(
        "Histogram::merge requires identical parameters");
  }
  for (std::size_t i = 0; i < counts_.size(); ++i) {
    counts_[i] += other.counts_[i];
  }
  total_count_ += other.total_count_;
  overflow_count_ += other.overflow_count_;
  sum_ += other.sum_;
  sum_squares_ += other.sum_squares_;
  if (other.total_count_ > 0) {
    min_ = std::min(min_, other.min_);
    max_ = std::max(max_, other.max_);
  }
}

double Histogram::mean() const noexcept {
  if (total_count_ == 0) {
    return 0.0;
  }
  return sum_ / static_cast<double>(total_count_);
}

double Histogram::stddev() const noexcept {
  if (total_count_ < 2) {
    return 0.0;
  }
  const double n = static_cast<double>(total_count_);
  const double m = sum_ / n;
  const double variance = (sum_squares_ / n) - (m * m);
  return variance > 0.0 ? std::sqrt(variance) : 0.0;
}

std::uint64_t Histogram::value_at_percentile(double percentile) const noexcept {
  if (total_count_ == 0) {
    return 0;
  }

  const double p = std::clamp(percentile, 0.0, 100.0);

  // Round up so that, e.g., p99 over 100 samples selects the 99th sample
  // rather than the 98th.
  std::uint64_t target =
      static_cast<std::uint64_t>((p / 100.0) * static_cast<double>(total_count_) + 0.5);
  if (target == 0) {
    target = 1;
  }
  if (target > total_count_) {
    target = total_count_;
  }

  std::uint64_t running = 0;
  for (std::size_t i = 0; i < counts_.size(); ++i) {
    running += counts_[i];
    if (running >= target) {
      return highest_equivalent(value_from_index(i));
    }
  }
  return max_;
}

std::string Histogram::summary(const char* unit) const {
  char buf[256];
  const int n = std::snprintf(
      buf, sizeof(buf),
      "n=%llu min=%llu p50=%llu p90=%llu p99=%llu p99.9=%llu max=%llu mean=%.1f (%s)",
      static_cast<unsigned long long>(count()),
      static_cast<unsigned long long>(min()),
      static_cast<unsigned long long>(p50()),
      static_cast<unsigned long long>(p90()),
      static_cast<unsigned long long>(p99()),
      static_cast<unsigned long long>(p999()),
      static_cast<unsigned long long>(max()), mean(), unit);
  return n > 0 ? std::string(buf, static_cast<std::size_t>(n)) : std::string();
}

}  // namespace axon::core
