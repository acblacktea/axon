#include "axon/core/latency.h"

#include <gtest/gtest.h>

#include <cstdio>
#include <string>
#include <thread>
#include <vector>

using axon::core::clock_info;
using axon::core::StageLatency;
using axon::core::Ticks;
using axon::core::Timeline;

namespace {

std::vector<std::string> stages() {
  return {"wire_in", "parsed", "decided", "wire_out"};
}

// Build a synthetic journey with known nanosecond gaps.
std::vector<Ticks> journey(Ticks start, const std::vector<double>& gaps_ns) {
  const double ticks_per_ns = clock_info().ticks_per_ns;
  std::vector<Ticks> out;
  out.push_back(start);
  Ticks cur = start;
  for (const double g : gaps_ns) {
    cur += static_cast<Ticks>(g * ticks_per_ns);
    out.push_back(cur);
  }
  return out;
}

}  // namespace

TEST(Timeline, StampsAdvance) {
  Timeline<4> tl;
  for (std::size_t i = 0; i < 4; ++i) {
    tl.stamp(i);
  }
  EXPECT_GT(tl.at(0), 0u);
  EXPECT_GE(tl.at(1), tl.at(0));
  EXPECT_GE(tl.at(3), tl.at(0));
  EXPECT_EQ(tl.total(), tl.delta(0, 3));
}

TEST(Timeline, SetAcceptsExternalStamps) {
  Timeline<3> tl;
  tl.set(0, 1000);
  tl.set(1, 1500);
  tl.set(2, 3000);
  EXPECT_EQ(tl.delta(0, 1), 500u);
  EXPECT_EQ(tl.delta(1, 2), 1500u);
  EXPECT_EQ(tl.total(), 2000u);
}

TEST(Timeline, ResetClears) {
  Timeline<3> tl;
  tl.stamp(0);
  tl.reset();
  EXPECT_EQ(tl.at(0), 0u);
}

TEST(StageLatency, RejectsTooFewStages) {
  EXPECT_THROW(StageLatency({"only_one"}), std::invalid_argument);
  EXPECT_THROW(StageLatency({}), std::invalid_argument);
}

TEST(StageLatency, ShapeMatchesStageNames) {
  StageLatency sl(stages());
  EXPECT_EQ(sl.stage_count(), 4u);
  EXPECT_EQ(sl.segment_count(), 3u);
  EXPECT_EQ(sl.stage_name(0), "wire_in");
  EXPECT_EQ(sl.stage_name(3), "wire_out");
}

TEST(StageLatency, RejectsWrongStampCount) {
  StageLatency sl(stages());
  const Ticks stamps[2] = {1, 2};
  EXPECT_THROW(sl.record(stamps, 2), std::invalid_argument);
}

TEST(StageLatency, AttributesTimeToTheRightSegment) {
  StageLatency sl(stages());

  // 1us in parse, 10us in decide, 2us to write out.
  for (int i = 0; i < 1000; ++i) {
    const auto j = journey(1'000'000, {1000.0, 10000.0, 2000.0});
    sl.record(j.data(), j.size());
  }

  EXPECT_EQ(sl.total().count(), 1000u);
  EXPECT_EQ(sl.dropped(), 0u);

  // 5% tolerance absorbs the tick-to-ns rounding at the histogram's precision.
  EXPECT_NEAR(static_cast<double>(sl.segment(0).p50()), 1000.0, 50.0);
  EXPECT_NEAR(static_cast<double>(sl.segment(1).p50()), 10000.0, 500.0);
  EXPECT_NEAR(static_cast<double>(sl.segment(2).p50()), 2000.0, 100.0);
  EXPECT_NEAR(static_cast<double>(sl.total().p50()), 13000.0, 650.0);
}

TEST(StageLatency, DropsOutOfOrderJourneysInsteadOfPoisoningPercentiles) {
  // A timeline where a stage was skipped would produce a nonsense delta. It
  // must be counted and discarded, not folded in -- a skewed p99 is much
  // harder to notice than a dropped-sample counter.
  StageLatency sl(stages());

  Ticks bad[4] = {1000, 900, 1100, 1200};  // stage 1 before stage 0
  sl.record(bad, 4);
  EXPECT_EQ(sl.dropped(), 1u);
  EXPECT_EQ(sl.total().count(), 0u);

  Ticks missing[4] = {1000, 0, 1100, 1200};  // stage 1 never stamped
  sl.record(missing, 4);
  EXPECT_EQ(sl.dropped(), 2u);
  EXPECT_EQ(sl.total().count(), 0u);

  Ticks missing_last[4] = {1000, 1050, 1100, 0};
  sl.record(missing_last, 4);
  EXPECT_EQ(sl.dropped(), 3u);
  EXPECT_EQ(sl.total().count(), 0u);
}

TEST(StageLatency, AcceptsAZeroLengthSegment) {
  // Two stamps landing in the same tick is normal on a coarse counter and
  // must not be mistaken for out-of-order.
  StageLatency sl(stages());
  Ticks flat[4] = {1000, 1000, 1000, 1000};
  sl.record(flat, 4);
  EXPECT_EQ(sl.dropped(), 0u);
  EXPECT_EQ(sl.total().count(), 1u);
  EXPECT_EQ(sl.total().p50(), 0u);
}

TEST(StageLatency, RecordsFromATimeline) {
  StageLatency sl(stages());
  Timeline<4> tl;
  tl.stamp(0);
  tl.stamp(1);
  tl.stamp(2);
  tl.stamp(3);
  sl.record(tl);
  EXPECT_EQ(sl.total().count(), 1u);
  EXPECT_EQ(sl.dropped(), 0u);
}

TEST(StageLatency, Reset) {
  StageLatency sl(stages());
  const auto j = journey(1000, {100.0, 100.0, 100.0});
  sl.record(j.data(), j.size());
  Ticks bad[4] = {10, 5, 6, 7};
  sl.record(bad, 4);

  sl.reset();
  EXPECT_EQ(sl.total().count(), 0u);
  EXPECT_EQ(sl.segment(0).count(), 0u);
  EXPECT_EQ(sl.dropped(), 0u);
}

TEST(StageLatency, ReportMentionsEverySegment) {
  StageLatency sl(stages());
  const auto j = journey(1000, {500.0, 500.0, 500.0});
  sl.record(j.data(), j.size());

  const std::string r = sl.report();
  EXPECT_NE(r.find("wire_in -> parsed"), std::string::npos);
  EXPECT_NE(r.find("parsed -> decided"), std::string::npos);
  EXPECT_NE(r.find("decided -> wire_out"), std::string::npos);
  EXPECT_NE(r.find("TOTAL"), std::string::npos);
  EXPECT_NE(r.find("nanoseconds"), std::string::npos);
}

// A realistic end-to-end shape: stamp around actual work and confirm the
// instrumentation attributes the sleep to the right segment.
TEST(StageLatency, MeasuresRealWork) {
  StageLatency sl({"start", "after_sleep", "end"});

  for (int i = 0; i < 20; ++i) {
    Timeline<3> tl;
    tl.stamp(0);
    std::this_thread::sleep_for(std::chrono::microseconds(500));
    tl.stamp(1);
    tl.stamp(2);
    sl.record(tl);
  }

  EXPECT_EQ(sl.total().count(), 20u);
  EXPECT_EQ(sl.dropped(), 0u);
  // Sleep overshoots but never undershoots meaningfully.
  EXPECT_GE(sl.segment(0).p50(), 400'000u);
  EXPECT_LT(sl.segment(1).p50(), 100'000u);

  std::printf("%s", sl.report().c_str());
}
