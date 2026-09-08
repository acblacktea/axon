#include "axon/core/latency.h"

#include <cstdio>
#include <stdexcept>

namespace axon::core {

StageLatency::StageLatency(std::vector<std::string> stage_names,
                           std::uint64_t highest_ns, int significant_figures)
    : stage_names_(std::move(stage_names)),
      total_(highest_ns, significant_figures) {
  if (stage_names_.size() < 2) {
    throw std::invalid_argument("StageLatency needs at least two stages");
  }
  segments_.reserve(stage_names_.size() - 1);
  for (std::size_t i = 0; i + 1 < stage_names_.size(); ++i) {
    segments_.emplace_back(highest_ns, significant_figures);
  }
}

void StageLatency::record(const Ticks* stamps, std::size_t count) {
  if (count != stage_names_.size()) {
    throw std::invalid_argument("stamp count does not match stage count");
  }

  // Reject the whole journey if any stamp is missing or out of order. A
  // partially-valid timeline would quietly poison the percentiles, and a
  // dropped-sample counter is far easier to notice than a skewed p99.
  for (std::size_t i = 0; i + 1 < count; ++i) {
    if (stamps[i] == 0 || stamps[i + 1] < stamps[i]) {
      ++dropped_;
      return;
    }
  }
  if (stamps[count - 1] == 0) {
    ++dropped_;
    return;
  }

  const double ns_per_tick = clock_info().ns_per_tick;

  for (std::size_t i = 0; i + 1 < count; ++i) {
    const Ticks d = stamps[i + 1] - stamps[i];
    segments_[i].record(
        static_cast<std::uint64_t>(static_cast<double>(d) * ns_per_tick + 0.5));
  }

  const Ticks t = stamps[count - 1] - stamps[0];
  total_.record(
      static_cast<std::uint64_t>(static_cast<double>(t) * ns_per_tick + 0.5));
}

void StageLatency::reset() {
  for (auto& h : segments_) {
    h.reset();
  }
  total_.reset();
  dropped_ = 0;
}

std::string StageLatency::report() const {
  std::string out;
  char line[512];

  std::snprintf(line, sizeof(line), "%-34s %10s %10s %10s %10s %10s\n",
                "segment", "n", "p50", "p99", "p99.9", "max");
  out += line;

  for (std::size_t i = 0; i < segments_.size(); ++i) {
    const std::string label = stage_names_[i] + " -> " + stage_names_[i + 1];
    const Histogram& h = segments_[i];
    std::snprintf(line, sizeof(line), "%-34s %10llu %10llu %10llu %10llu %10llu\n",
                  label.c_str(), static_cast<unsigned long long>(h.count()),
                  static_cast<unsigned long long>(h.p50()),
                  static_cast<unsigned long long>(h.p99()),
                  static_cast<unsigned long long>(h.p999()),
                  static_cast<unsigned long long>(h.max()));
    out += line;
  }

  const std::string total_label =
      stage_names_.front() + " -> " + stage_names_.back() + " (TOTAL)";
  std::snprintf(line, sizeof(line), "%-34s %10llu %10llu %10llu %10llu %10llu\n",
                total_label.c_str(),
                static_cast<unsigned long long>(total_.count()),
                static_cast<unsigned long long>(total_.p50()),
                static_cast<unsigned long long>(total_.p99()),
                static_cast<unsigned long long>(total_.p999()),
                static_cast<unsigned long long>(total_.max()));
  out += line;

  if (dropped_ > 0) {
    std::snprintf(line, sizeof(line), "dropped (out-of-order stamps): %llu\n",
                  static_cast<unsigned long long>(dropped_));
    out += line;
  }

  out += "all values in nanoseconds; clock source: ";
  out += clock_info().source;
  out += "\n";
  return out;
}

}  // namespace axon::core
