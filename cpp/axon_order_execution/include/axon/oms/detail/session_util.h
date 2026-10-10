// Small helpers shared by the venue sessions in exchanges/<venue>/. Internal to oms/.

#pragma once

#include <cstdint>
#include <optional>

#include "axon/core/clock.h"
#include "axon/venue/json_view.h"

namespace axon::oms::detail {

inline double monotonic_seconds() {
  return static_cast<double>(core::monotonic_ns()) / 1e9;
}

// Correlates on `reqId` (Bybit) or `id` (Binance, OKX), STRINGS carrying
// digits. The EMS keys its pending map on the integer.
inline std::optional<std::int64_t> parse_string_id(venue::Value v) noexcept {
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
