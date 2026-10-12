#include "axon/ems/rate_limiter.h"

#include <algorithm>
#include <cmath>

#include <fmt/format.h>

namespace axon::ems {
namespace {

// A throttle reply that does not say for how long still means stop. One
// second is long enough for a sliding window to drain a little and short
// enough not to strand a strategy.
constexpr double kDefaultPauseSeconds = 1.0;

}  // namespace

RateLimiter::RateLimiter(std::vector<RateLimitRule> rules, double headroom)
    : headroom_(headroom) {
  for (const auto& rule : rules) {
    windows_.push_back(Window{rule, {}, 0, -1.0});
  }
}

void RateLimiter::expire(Window& w, double now) {
  while (!w.sent.empty() && now - w.sent.front() >= w.rule.window_seconds) {
    w.sent.pop_front();
  }
}

int RateLimiter::used(Window& w, double now) {
  expire(w, now);
  int count = static_cast<int>(w.sent.size());
  // A report is good for one window. Requests we sent since it was taken are
  // in our own count, so the larger of the two is the safe reading.
  if (w.reported_at >= 0.0 && now - w.reported_at < w.rule.window_seconds) {
    count = std::max(count, w.reported_used);
  }
  return count;
}

std::optional<std::string> RateLimiter::check(Kind kind, double now) {
  if (paused(now)) {
    return fmt::format("the exchange throttled this account; paused for another {:.1f}s",
                       paused_until_ - now);
  }
  for (auto& w : windows_) {
    const int n = used(w, now);
    const double share = kind == Kind::kCancel ? 1.0 : headroom_;
    const int allowed = static_cast<int>(std::floor(w.rule.limit * share));
    if (n >= allowed) {
      return fmt::format("{} requests in the last {:g}s against a limit of {}{}", n,
                         w.rule.window_seconds, w.rule.limit,
                         kind == Kind::kCancel ? "" : " (new orders stop at the headroom)");
    }
  }
  return std::nullopt;
}

void RateLimiter::record(double now) {
  for (auto& w : windows_) {
    expire(w, now);
    w.sent.push_back(now);
  }
}

void RateLimiter::observe(const RateLimitFeedback& feedback, double now) {
  for (const auto& u : feedback.usage) {
    for (auto& w : windows_) {
      // Matched on the window length: that is the one thing both sides
      // describe the same way.
      if (std::abs(w.rule.window_seconds - u.window_seconds) > 1e-9) {
        continue;
      }
      w.reported_used = u.used;
      w.reported_at = now;
      // The exchange's number is the real one: an account tier above the
      // configured default is not refused, and one below is not overrun.
      if (u.limit > 0) {
        w.rule.limit = u.limit;
      }
    }
  }
  if (feedback.throttled) {
    const double pause =
        feedback.retry_after_seconds > 0.0 ? feedback.retry_after_seconds : kDefaultPauseSeconds;
    paused_until_ = std::max(paused_until_, now + pause);
  }
}

double RateLimiter::utilisation(double now) {
  double worst = 0.0;
  for (auto& w : windows_) {
    if (w.rule.limit > 0) {
      worst = std::max(worst, static_cast<double>(used(w, now)) / w.rule.limit);
    }
  }
  return worst;
}

}  // namespace axon::ems
