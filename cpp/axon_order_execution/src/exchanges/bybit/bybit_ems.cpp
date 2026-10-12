#include "axon/exchanges/bybit/bybit_ems.h"

#include <algorithm>
#include <string>

#include "axon/exchanges/common/json_view.h"

namespace axon::exchanges::bybit {

using namespace axon::ems;
namespace {

std::size_t build_place(char* buf, std::size_t cap,
                        const models::OrderRequest& request,
                        const BuildContext& ctx, std::int64_t& id) {
  return ctx.state->bybit.ws_place_order(buf, cap, request, ctx.now_ms, id);
}

std::size_t build_cancel(char* buf, std::size_t cap, std::string_view symbol,
                         std::string_view order_id, const BuildContext& ctx,
                         std::int64_t& id) {
  return ctx.state->bybit.ws_cancel_order(buf, cap, std::string(symbol),
                                   std::string(order_id), ctx.now_ms, id);
}

std::size_t build_modify(char* buf, std::size_t cap,
                         const models::Order& existing,
                         std::optional<core::Qty> amount,
                         std::optional<core::Price> price,
                         const BuildContext& ctx, std::int64_t& id) {
  return ctx.state->bybit.ws_amend_order(buf, cap, existing.instrument,
                                  existing.order_id, amount, price, ctx.now_ms,
                                  id);
}

// Acceptance only. The authoritative order state arrives on the feed a moment
// later, so no Order is constructed here.
OrderResult interpret_reply(std::string_view, const models::OrderRequest&, ExchangeState&) {
  return OrderResult{true, std::nullopt, {}};
}

// Bybit's default for order.create on linear contracts is 10 requests per
// second per UID. The trade stream's reply header carries the real figure.
constexpr RateLimitRule kDefaultRateLimits[] = {{1.0, 10}};

// Each /v5/trade reply carries the endpoint's quota in a header object:
//   "header":{"X-Bapi-Limit":"10","X-Bapi-Limit-Status":"9",
//             "X-Bapi-Limit-Reset-Timestamp":"1711001595207",..}
// -- the limit, what REMAINS of it, and when it refills. retCode 10006 is
// "too many visits".
void read_rate_limits(std::string_view payload, ExchangeState& state, std::int64_t now_ms,
                      RateLimitFeedback& out) {
  auto root = state.reply_doc.parse_copy(payload);
  if (!root.has_value()) {
    return;
  }
  if ((*root)["retCode"].as_int().value_or(0) == 10006) {
    out.throttled = true;
  }
  auto header = (*root)["header"].as_object();
  if (!header.has_value()) {
    return;
  }
  const auto limit = (*header)["X-Bapi-Limit"].as_int();
  const auto remaining = (*header)["X-Bapi-Limit-Status"].as_int();
  const auto reset_ms = (*header)["X-Bapi-Limit-Reset-Timestamp"].as_int();
  if (limit.has_value() && remaining.has_value() && *limit > 0) {
    RateLimitFeedback::Usage u;
    u.window_seconds = 1.0;
    u.limit = static_cast<int>(*limit);
    u.used = static_cast<int>(*limit - *remaining);
    out.usage.push_back(u);
  }
  if (out.throttled && reset_ms.has_value()) {
    out.retry_after_seconds =
        static_cast<double>(std::max<std::int64_t>(*reset_ms - now_ms, 0)) / 1000.0;
  }
}

void configure(ExchangeState& state, const ExchangeConfig& config) {
  state.bybit.set_self_trade_prevention(config.self_trade_prevention);
}

}  // namespace

const ExchangeOps& ems_ops() {
  static const ExchangeOps kOps{
      "bybit",
      /*own_trade_connection=*/true,  // /v5/trade, with its own op:"auth"
      /*needs_symbol=*/true,
      build_place, build_cancel, build_modify, interpret_reply,
      kDefaultRateLimits, read_rate_limits, configure,
  };
  return kOps;
}

}  // namespace axon::exchanges::bybit
