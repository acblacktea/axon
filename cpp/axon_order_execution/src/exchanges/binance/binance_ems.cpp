#include "axon/exchanges/binance/binance_ems.h"

#include <algorithm>
#include <string>

#include "axon/exchanges/binance/binance_parser.h"
#include "axon/exchanges/common/json_view.h"

namespace axon::exchanges::binance {

using namespace axon::ems;
namespace {

// Binance is the only exchange with no session login: every request carries its
// own apiKey and signature, so a missing credential is not a configuration
// warning here, it is an unbuildable request.
std::size_t build_place(char* buf, std::size_t cap,
                        const models::OrderRequest& request,
                        const BuildContext& ctx, std::int64_t& id) {
  if (ctx.config == nullptr) {
    return 0;
  }
  return ctx.state->binance.ws_place_order(buf, cap, request, ctx.config->api_key,
                                  ctx.config->api_secret, ctx.now_ms, id);
}

std::size_t build_cancel(char* buf, std::size_t cap, std::string_view symbol,
                         std::string_view order_id, const BuildContext& ctx,
                         std::int64_t& id) {
  if (ctx.config == nullptr) {
    return 0;
  }
  return ctx.state->binance.ws_cancel_order(buf, cap, std::string(symbol),
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
  return ctx.state->binance.ws_modify_order(buf, cap, existing.instrument,
                                   existing.order_id, existing.side,
                                   final_amount, final_price,
                                   ctx.config->api_key, ctx.config->api_secret,
                                   ctx.now_ms, id);
}

// ws-fapi answers order.place with the order as the exchange recorded it:
//   {"id":"7","status":200,"result":{"orderId":..,"symbol":..,"status":"NEW",
//    "clientOrderId":..,"price":..,"origQty":..,"executedQty":..,"avgPrice":..,
//    "type":..,"side":..,"updateTime":..},"rateLimits":[..]}
// Returning it is what gives a strategy the exchange order id for what it just
// placed. The feed still carries the authoritative state afterwards; a reply
// that cannot be read is accepted without an order, as before.
OrderResult interpret_reply(std::string_view payload, const models::OrderRequest& request,
                            ExchangeState& state) {
  auto root = state.reply_doc.parse_copy(payload);
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
  o.status = exchanges::binance::map_order_status(r["status"].as_string().value_or("NEW")).status;
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

// USDT-M futures order limits per account: 300 orders per 10 seconds and 1200
// per minute. The ORDERS counters on every ws-fapi reply override these.
constexpr RateLimitRule kDefaultRateLimits[] = {{10.0, 300}, {60.0, 1200}};

// Every ws-fapi reply, error or not, ends in the account's counters:
//   "rateLimits":[{"rateLimitType":"ORDERS","interval":"SECOND","intervalNum":10,
//                  "limit":300,"count":12}, ..]
// REQUEST_WEIGHT is a different budget (REST and queries) and is skipped: its
// minute window would otherwise be read as the order minute. Over the limit
// is status 429 -- or 418, an IP ban -- with error.data.retryAfter, epoch ms.
void read_rate_limits(std::string_view payload, ExchangeState& state, std::int64_t now_ms,
                      RateLimitFeedback& out) {
  auto root = state.reply_doc.parse_copy(payload);
  if (!root.has_value()) {
    return;
  }
  const auto status = (*root)["status"].as_int().value_or(200);
  if (status == 429 || status == 418) {
    out.throttled = true;
    if (auto err = (*root)["error"].as_object(); err.has_value()) {
      if (auto data = (*err)["data"].as_object(); data.has_value()) {
        if (const auto until = (*data)["retryAfter"].as_int(); until.has_value()) {
          out.retry_after_seconds =
              static_cast<double>(std::max<std::int64_t>(*until - now_ms, 0)) / 1000.0;
        }
      }
    }
  }
  if (auto limits = (*root)["rateLimits"].as_array(); limits.has_value()) {
    limits->for_each_object([&](exchanges::Object& l) {
      if (l["rateLimitType"].as_string().value_or("") != "ORDERS") {
        return;
      }
      const auto interval = l["interval"].as_string().value_or("");
      const auto num = static_cast<double>(l["intervalNum"].as_int().value_or(1));
      const double unit = interval == "SECOND" ? 1.0
                          : interval == "MINUTE" ? 60.0
                          : interval == "DAY"    ? 86400.0
                                                 : 0.0;
      if (unit == 0.0) {
        return;
      }
      RateLimitFeedback::Usage u;
      u.window_seconds = unit * num;
      u.limit = static_cast<int>(l["limit"].as_int().value_or(0));
      u.used = static_cast<int>(l["count"].as_int().value_or(0));
      out.usage.push_back(u);
    });
  }
}

void configure(ExchangeState& state, const ExchangeConfig& config) {
  state.binance.set_self_trade_prevention(config.self_trade_prevention);
}

}  // namespace

const ExchangeOps& ems_ops() {
  static const ExchangeOps kOps{
      "binance",
      /*own_trade_connection=*/true,  // ws-fapi, no session login
      /*needs_symbol=*/true,
      build_place, build_cancel, build_modify, interpret_reply,
      kDefaultRateLimits, read_rate_limits, configure,
  };
  return kOps;
}

}  // namespace axon::exchanges::binance
