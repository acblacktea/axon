// The silence watchdog on a live exchange session.
//
// It used to condemn any connection quiet for three heartbeat periods. Binance's
// user data stream is quiet by design on an idle account -- it pushes only when
// an order or balance changes, and pings every three minutes -- so the engine
// tore it down and rebuilt it every 30 seconds, with a window each time in which
// an order update could be lost. Now silence earns a ping first, and only a
// peer that does not answer is dead.

#include <gtest/gtest.h>

#include "axon/oms/detail/session_util.h"

using axon::oms::detail::check_liveness;
using axon::oms::detail::Liveness;

namespace {
constexpr double kHeartbeat = 10.0;
}

TEST(SessionLiveness, RecentTrafficIsFine) {
  EXPECT_EQ(check_liveness(105.0, 100.0, 0.0, kHeartbeat), Liveness::kFine);
}

TEST(SessionLiveness, OneQuietPeriodEarnsAProbe) {
  EXPECT_EQ(check_liveness(111.0, 100.0, 0.0, kHeartbeat), Liveness::kProbe);
}

TEST(SessionLiveness, ProbesAreSpacedByTheHeartbeat) {
  // Probed at 111; still quiet at 115, so no second probe yet.
  EXPECT_EQ(check_liveness(115.0, 100.0, 111.0, kHeartbeat), Liveness::kFine);
  EXPECT_EQ(check_liveness(121.0, 100.0, 111.0, kHeartbeat), Liveness::kProbe);
}

TEST(SessionLiveness, NoAnswerForThreePeriodsIsDead) {
  EXPECT_EQ(check_liveness(131.0, 100.0, 121.0, kHeartbeat), Liveness::kDead);
}

// The Binance case: an idle account whose only traffic is the pong to our
// probe. Each pong refreshes last_activity, so the session lives indefinitely.
TEST(SessionLiveness, AnAnsweredProbeKeepsAQuietStreamAlive) {
  double last_activity = 0.0;
  double last_probe = 0.0;
  for (double now = 0.0; now < 600.0; now += 1.0) {
    const Liveness l = check_liveness(now, last_activity, last_probe, kHeartbeat);
    ASSERT_NE(l, Liveness::kDead) << "condemned at t=" << now;
    if (l == Liveness::kProbe) {
      last_probe = now;
      last_activity = now + 0.05;  // the pong, one round trip later
    }
  }
}
