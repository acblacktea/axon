// Small helpers shared by the exchange sessions in exchanges/<exchange>/. Internal to oms/.

#pragma once

#include <cstdint>
#include <optional>

#include "axon/core/clock.h"
#include "axon/exchanges/common/json_view.h"

namespace axon::oms::detail {

inline double monotonic_seconds() {
  return static_cast<double>(core::monotonic_ns()) / 1e9;
}

// What the silence watchdog should do with a live session.
//
// Silence alone does not mean the peer is gone: Binance's user data stream
// says nothing at all on an idle account, and its own pings come every three
// minutes. So one quiet heartbeat period earns a WebSocket ping -- which the
// peer must answer -- and only three without ANY inbound frame, the probe's
// pong included, condemn the connection.
enum class Liveness { kFine, kProbe, kDead };

inline Liveness check_liveness(double now, double last_activity, double last_probe,
                               double heartbeat_interval) noexcept {
  const double silence = now - last_activity;
  if (silence > heartbeat_interval * 3.0) {
    return Liveness::kDead;
  }
  if (silence > heartbeat_interval && now - last_probe >= heartbeat_interval) {
    return Liveness::kProbe;
  }
  return Liveness::kFine;
}

// Correlates on `reqId` (Bybit) or `id` (Binance, OKX), STRINGS carrying
// digits. The EMS keys its pending map on the integer.
inline std::optional<std::int64_t> parse_string_id(exchanges::Value v) noexcept {
  const auto text = v.as_string();
  if (!text.has_value() || text->empty()) {
    return std::nullopt;
  }
  std::int64_t out = 0;
  for (const char c : *text) {
    if (c < '0' || c > '9') {
      return std::nullopt;
    }
    out = out * 10 + (c - '0');
  }
  return out;
}

}  // namespace axon::oms::detail
