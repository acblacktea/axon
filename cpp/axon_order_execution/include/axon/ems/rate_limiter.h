// The exchange's own request limits, counted on our side of the wire.
//
// Every exchange caps how many orders an account may send per window, and the
// penalty for exceeding it escalates: a rejected request, then a temporary IP
// ban (Binance's 418), then an account review. A strategy that bursts -- a
// market maker requoting on every tick does -- finds the limit long before the
// exchange tells it anything useful. So the EMS counts every request it sends
// against the exchange's published windows and refuses locally first.
//
// Two sources, the stricter one wins:
//   * our own count of what we sent, per configured window;
//   * what the exchange reports, when it reports anything (Binance puts its
//     counters on every order-entry reply; Bybit sends a remaining-quota
//     header). That covers requests this process did not send -- another
//     process on the same account, or one that just restarted.
//
// New orders and amends stop at a headroom below the limit; cancels may use it
// all. When the budget is tight, getting out must still be possible -- a kill
// switch that cannot cancel because quoting spent the budget is no kill switch.
//
// Single-threaded, like the EMS that owns it.

#pragma once

#include <cstddef>
#include <deque>
#include <optional>
#include <string>
#include <vector>

#include "axon/config.h"

namespace axon::ems {

// What one reply said about the account's usage, in a form every exchange can
// fill. Windows the exchange did not mention are simply absent.
struct RateLimitFeedback {
  struct Usage {
    double window_seconds = 0.0;
    int used = 0;
    int limit = 0;
  };
  std::vector<Usage> usage;
  // The exchange said we are over: stop sending until `retry_after_seconds`
  // from now (0 = the exchange did not say; a default pause applies).
  bool throttled = false;
  double retry_after_seconds = 0.0;
};

class RateLimiter {
 public:
  enum class Kind {
    kNewOrder,  // place and amend: subject to the headroom
    kCancel,    // may spend the whole budget
  };

  RateLimiter() = default;
  // `headroom` is the fraction of each limit new orders may use.
  explicit RateLimiter(std::vector<RateLimitRule> rules, double headroom = 0.8);

  // nullopt if a request of this kind may go out now; otherwise why not.
  std::optional<std::string> check(Kind kind, double now);
  // A request went out.
  void record(double now);
  // What the exchange said on a reply.
  void observe(const RateLimitFeedback& feedback, double now);

  bool paused(double now) const noexcept { return now < paused_until_; }
  // The highest usage across windows, as a fraction of its limit. For metrics
  // and the heartbeat: how close the account is to being refused.
  double utilisation(double now);

 private:
  struct Window {
    RateLimitRule rule;
    std::deque<double> sent;  // timestamps inside the window, oldest first
    // The exchange's last word on this window.
    int reported_used = 0;
    double reported_at = -1.0;
  };
  void expire(Window& w, double now);
  int used(Window& w, double now);

  std::vector<Window> windows_;
  double headroom_ = 0.8;
  double paused_until_ = 0.0;
};

}  // namespace axon::ems
