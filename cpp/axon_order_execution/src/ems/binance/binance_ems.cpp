#include "axon/ems/binance/binance_ems.h"

#include <string>

#include "axon/venue/binance/binance_parser.h"
#include "axon/venue/json_view.h"

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

// ws-fapi answers order.place with the order as the venue recorded it:
//   {"id":"7","status":200,"result":{"orderId":..,"symbol":..,"status":"NEW",
//    "clientOrderId":..,"price":..,"origQty":..,"executedQty":..,"avgPrice":..,
//    "type":..,"side":..,"updateTime":..},"rateLimits":[..]}
// Returning it is what gives a strategy the venue order id for what it just
// placed. The feed still carries the authoritative state afterwards; a reply
// that cannot be read is accepted without an order, as before.
OrderResult interpret_reply(std::string_view payload, const models::OrderRequest& request) {
  static thread_local venue::Document doc(64 * 1024);
  auto root = doc.parse_copy(payload);
  auto result = root.has_value() ? (*root)["result"].as_object() : std::nullopt;
  if (!result.has_value()) {
    return OrderResult{true, std::nullopt, {}};
  }
  auto& r = *result;
  const auto order_id = r["orderId"].as_int();
  const auto symbol = r["symbol"].as_string();
  if (!order_id.has_value() || !symbol.has_value()) {
    return OrderResult{true, std::nullopt, {}};
  }

  models::Order o;
  o.order_id = std::to_string(*order_id);
  o.exchange = "binance";
  o.instrument = std::string(*symbol);
  o.status = venue::binance::map_order_status(r["status"].as_string().value_or("NEW")).status;
  const auto price = r["price"].as_decimal();
  if (price.has_value() && !price->is_zero()) {
    o.price = price;
  }
  o.amount = r["origQty"].as_decimal().value_or(request.amount);
  o.filled_amount = r["executedQty"].as_decimal().value_or(core::Qty{});
  const auto avg = r["avgPrice"].as_decimal();
  if (avg.has_value() && !avg->is_zero()) {
    o.average_price = avg;
  }
  o.side = request.side;
  o.order_type = request.order_type;
  o.post_only = request.post_only;
  o.label = request.label;
  o.internal_order_id = request.internal_order_id;
  o.strategy_id = request.strategy_id;
  const auto updated = r["updateTime"].as_int();
  o.created_at = core::Timestamp::from_millis(updated.value_or(0));
  o.updated_at = o.created_at;
  return OrderResult{true, std::move(o), {}};
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
