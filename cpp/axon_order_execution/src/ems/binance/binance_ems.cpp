#include "axon/ems/binance/binance_ems.h"

#include <string>

namespace axon::ems::binance {
namespace {

venue::binance::BinanceBuilder& builder() {
  static thread_local venue::binance::BinanceBuilder b;
  return b;
}

// Binance is the only venue with no session login: every request carries its
// own apiKey and signature, so a missing credential is not a configuration
// warning here, it is an unbuildable request.
std::size_t build_place(char* buf, std::size_t cap,
                        const models::OrderRequest& request,
                        const BuildContext& ctx, std::int64_t& id) {
  if (ctx.config == nullptr) {
    return 0;
  }
  return builder().ws_place_order(buf, cap, request, ctx.config->api_key,
                                  ctx.config->api_secret, ctx.now_ms, id);
}

std::size_t build_cancel(char* buf, std::size_t cap, std::string_view symbol,
                         std::string_view order_id, const BuildContext& ctx,
                         std::int64_t& id) {
  if (ctx.config == nullptr) {
    return 0;
  }
  return builder().ws_cancel_order(buf, cap, std::string(symbol),
                                   std::string(order_id), ctx.config->api_key,
                                   ctx.config->api_secret, ctx.now_ms, id);
}

std::size_t build_modify(char* buf, std::size_t cap,
                         const models::Order& existing,
                         std::optional<core::Qty> amount,
                         std::optional<core::Price> price,
                         const BuildContext& ctx, std::int64_t& id) {
  if (ctx.config == nullptr) {
    return 0;
  }
  // Binance will not accept a partial amend, so whatever the caller left out
  // comes from the order we already have -- same rule as the REST path.
  const core::Qty final_amount = amount.value_or(existing.amount);
  if (!price.has_value() && !existing.price.has_value()) {
    return 0;
  }
  const core::Price final_price =
      price.value_or(existing.price.value_or(core::Price{}));
  return builder().ws_modify_order(buf, cap, existing.instrument,
                                   existing.order_id, existing.side,
                                   final_amount, final_price,
                                   ctx.config->api_key, ctx.config->api_secret,
                                   ctx.now_ms, id);
}

// Acceptance only; the feed carries the real state.
OrderResult interpret_reply(std::string_view, const models::OrderRequest&) {
  return OrderResult{true, std::nullopt, {}};
}

}  // namespace

const VenueOps& ops() {
  static const VenueOps kOps{
      "binance",
      /*own_trade_connection=*/true,  // ws-fapi, no session login
      /*needs_symbol=*/true,
      build_place, build_cancel, build_modify, interpret_reply,
  };
  return kOps;
}

}  // namespace axon::ems::binance
