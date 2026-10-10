#include "axon/exchanges/deribit/deribit_ems.h"

#include "axon/exchanges/deribit/deribit_parser.h"
#include "axon/venue/json_view.h"

namespace axon::ems::deribit {
namespace {

// Deribit answers private/buy with {"result":{"order":{...}}}. Parsed with the
// same on-demand parser the feed uses.
std::optional<models::Order> parse_order_reply(std::string_view payload,
                                               venue::Document& doc) {
  auto root = doc.parse_copy(payload);
  if (!root.has_value()) {
    return std::nullopt;
  }
  auto result = (*root)["result"].as_object();
  if (!result.has_value()) {
    return std::nullopt;
  }
  auto order_obj = (*result)["order"].as_object();
  if (!order_obj.has_value()) {
    return std::nullopt;
  }

  models::Order order;
  order.exchange = "deribit";
  const auto id = (*order_obj)["order_id"].as_string();
  const auto instrument = (*order_obj)["instrument_name"].as_string();
  if (!id.has_value() || !instrument.has_value()) {
    return std::nullopt;
  }
  order.order_id = std::string(*id);
  order.instrument = std::string(*instrument);

  const auto direction = (*order_obj)["direction"].as_string();
  order.side = venue::deribit::map_direction(direction.value_or("buy"));
  const auto order_type = (*order_obj)["order_type"].as_string();
  order.order_type = venue::deribit::map_order_type(order_type.value_or("limit"));

  const auto amount = (*order_obj)["amount"].as_decimal();
  order.amount = amount.value_or(core::Qty{});
  const auto filled = (*order_obj)["filled_amount"].as_decimal();
  order.filled_amount = filled.value_or(core::Qty{});
  order.price = (*order_obj)["price"].as_decimal();
  order.average_price = (*order_obj)["average_price"].as_decimal();

  const auto state = (*order_obj)["order_state"].as_string();
  const auto mapping = venue::deribit::map_order_state(state.value_or("open"));
  order.status = venue::deribit::refine_with_fill(
      mapping.status, order.filled_amount.raw() > 0);

  const auto created = (*order_obj)["creation_timestamp"].as_int();
  const auto updated = (*order_obj)["last_update_timestamp"].as_int();
  order.created_at = core::Timestamp::from_millis(created.value_or(0));
  order.updated_at = core::Timestamp::from_millis(updated.value_or(created.value_or(0)));
  return order;
}

std::size_t build_place(char* buf, std::size_t cap,
                        const models::OrderRequest& request, const BuildContext& ctx,
                        std::int64_t& id) {
  return ctx.state->deribit.place_order(buf, cap, request, id);
}

// Deribit is the one venue that cancels by order id alone, so the symbol
// argument every other venue needs is unused here.
std::size_t build_cancel(char* buf, std::size_t cap, std::string_view,
                         std::string_view order_id, const BuildContext& ctx,
                         std::int64_t& id) {
  return ctx.state->deribit.cancel_order(buf, cap, std::string(order_id), id);
}

std::size_t build_modify(char* buf, std::size_t cap,
                         const models::Order& existing,
                         std::optional<core::Qty> amount,
                         std::optional<core::Price> price, const BuildContext& ctx,
                         std::int64_t& id) {
  return ctx.state->deribit.modify_order(buf, cap, existing.order_id, amount, price, id);
}

// The only venue that returns the whole order on the reply, so the strategy
// gets it without waiting for the feed.
OrderResult interpret_reply(std::string_view payload,
                            const models::OrderRequest& request, VenueState& state) {
  auto order = parse_order_reply(payload, state.reply_doc);
  if (!order.has_value()) {
    return OrderResult{false, std::nullopt, "could not parse the venue's reply"};
  }
  // The internal id is ours, not the venue's; carry it onto the order so the
  // OMS can correlate what a strategy asked for with what came back.
  order->internal_order_id = request.internal_order_id;
  order->strategy_id = request.strategy_id;
  return OrderResult{true, std::move(order), {}};
}

}  // namespace

const VenueOps& ops() {
  static const VenueOps kOps{
      "deribit",
      /*own_trade_connection=*/false,  // orders ride the private stream
      /*needs_symbol=*/false,          // cancels and amends by id alone
      build_place, build_cancel, build_modify, interpret_reply,
  };
  return kOps;
}

}  // namespace axon::ems::deribit
