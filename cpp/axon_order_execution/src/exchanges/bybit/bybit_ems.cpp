#include "axon/exchanges/bybit/bybit_ems.h"

#include <string>

namespace axon::ems::bybit {
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
OrderResult interpret_reply(std::string_view, const models::OrderRequest&, VenueState&) {
  return OrderResult{true, std::nullopt, {}};
}

}  // namespace

const VenueOps& ops() {
  static const VenueOps kOps{
      "bybit",
      /*own_trade_connection=*/true,  // /v5/trade, with its own op:"auth"
      /*needs_symbol=*/true,
      build_place, build_cancel, build_modify, interpret_reply,
  };
  return kOps;
}

}  // namespace axon::ems::bybit
