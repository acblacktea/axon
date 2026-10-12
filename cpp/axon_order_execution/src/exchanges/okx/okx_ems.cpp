#include "axon/exchanges/okx/okx_ems.h"

#include <string>

#include "axon/util/metrics.h"
#include "axon/exchanges/common/json_view.h"

namespace axon::exchanges::okx {

using namespace axon::ems;
namespace {

// OKX reports TWO outcomes per request: a top-level `code` for whether the
// request was processed, and an `sCode` PER ORDER for whether that order was
// accepted. The session checks the first; this checks the second.
//
// Missing this is how a rejected order reads as a successful one -- top-level
// code "0" with sCode "51008" (insufficient balance) is a perfectly normal
// shape.
std::optional<std::string> order_rejection(std::string_view payload,
                                           exchanges::Document& doc) {
  auto root = doc.parse_copy(payload);
  if (!root.has_value()) {
    return std::nullopt;
  }
  auto data = (*root)["data"].as_array();
  if (!data.has_value()) {
    return std::nullopt;
  }
  std::optional<std::string> rejection;
  data->for_each_object([&](exchanges::Object& entry) {
    if (rejection.has_value()) {
      return;
    }
    const auto s_code = entry["sCode"].as_string().value_or("0");
    if (s_code == "0") {
      return;
    }
    const auto s_msg = entry["sMsg"].as_string().value_or("");
    rejection = "okx rejected the order (sCode " + std::string(s_code) + "): " +
                std::string(s_msg);
  });
  return rejection;
}

std::size_t build_place(char* buf, std::size_t cap,
                        const models::OrderRequest& request, const BuildContext& ctx,
                        std::int64_t& id) {
  return ctx.state->okx.ws_place_order(buf, cap, request, id);
}

std::size_t build_cancel(char* buf, std::size_t cap, std::string_view symbol,
                         std::string_view order_id, const BuildContext& ctx,
                         std::int64_t& id) {
  return ctx.state->okx.ws_cancel_order(buf, cap, std::string(symbol),
                                   std::string(order_id), id);
}

std::size_t build_modify(char* buf, std::size_t cap,
                         const models::Order& existing,
                         std::optional<core::Qty> amount,
                         std::optional<core::Price> price, const BuildContext& ctx,
                         std::int64_t& id) {
  return ctx.state->okx.ws_amend_order(buf, cap, existing.instrument,
                                  existing.order_id, amount, price, id);
}

// Acceptance of the REQUEST is not acceptance of the ORDER, so a reply OKX
// called successful still has to be inspected.
OrderResult interpret_reply(std::string_view payload,
                            const models::OrderRequest&, ExchangeState& state) {
  if (const auto rejection = order_rejection(payload, state.reply_doc);
      rejection.has_value()) {
    util::get_metrics().inc_order_rejected("okx", "exchange");
    return OrderResult{false, std::nullopt, *rejection};
  }
  return OrderResult{true, std::nullopt, {}};
}

// OKX limits order placement to 60 requests per 2 seconds -- per instrument,
// and 1000 per 2 seconds per sub-account across all of them. Counting every
// instrument against the per-instrument figure is stricter than OKX is, and
// stays correct for an engine trading a handful of instruments.
constexpr RateLimitRule kDefaultRateLimits[] = {{2.0, 60}};

// OKX reports no usage, only the refusal: code 50011 ("rate limit reached")
// or 50061 (sub-account limit), at the top level or per order in sCode.
bool is_throttle_code(std::string_view code) { return code == "50011" || code == "50061"; }

void read_rate_limits(std::string_view payload, ExchangeState& state, std::int64_t,
                      RateLimitFeedback& out) {
  auto root = state.reply_doc.parse_copy(payload);
  if (!root.has_value()) {
    return;
  }
  if (is_throttle_code((*root)["code"].as_string().value_or("0"))) {
    out.throttled = true;
    return;
  }
  if (auto data = (*root)["data"].as_array(); data.has_value()) {
    data->for_each_object([&](exchanges::Object& entry) {
      if (is_throttle_code(entry["sCode"].as_string().value_or("0"))) {
        out.throttled = true;
      }
    });
  }
}

void configure(ExchangeState& state, const ExchangeConfig& config) {
  state.okx.set_self_trade_prevention(config.self_trade_prevention);
}

}  // namespace

const ExchangeOps& ems_ops() {
  static const ExchangeOps kOps{
      "okx",
      /*own_trade_connection=*/false,  // orders ride the private stream
      /*needs_symbol=*/true,
      build_place, build_cancel, build_modify, interpret_reply,
      kDefaultRateLimits, read_rate_limits, configure,
  };
  return kOps;
}

}  // namespace axon::exchanges::okx
