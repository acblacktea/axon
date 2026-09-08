// Deribit inbound message parser.
//
// Turns a raw WebSocket text frame into hot-path messages, using simdjson
// On-Demand. Nothing here allocates once the parser is constructed, and every
// string lands in fixed inline storage rather than a std::string.
//
// Shape, ported from _handle_message in oms/deribit/deribit_ws.py:
//
//   {"method":"heartbeat", "params":{"type":"test_request"}}   -> must answer
//   {"method":"subscription", "params":{"channel":..., "data":...}}
//        channel contains "user.orders"     -> one order object
//        channel contains "user.trades"     -> an ARRAY of trades
//        channel contains "user.portfolio"  -> one account object
//   {"id":N, "result":...}                  -> RPC reply
//   {"id":N, "error":{...}}                 -> RPC failure
//
// FIELD ORDER MATTERS. simdjson On-Demand searches forward and only rewinds
// when it must, so reading fields out of document order silently turns one
// pass into one pass per field. The reads below follow Deribit's actual field
// order; if the venue reorders them the parser still works, just slower.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

#include "axon/core/timestamp.h"
#include "axon/models/portfolio.h"
#include "axon/transport/hot_messages.h"
#include "axon/venue/deribit/deribit_protocol.h"
#include "axon/venue/json_view.h"
#include "axon/venue/venue_events.h"

namespace axon::venue::deribit {

// Deribit's own names for the shared result types. The four venues report
// through one shape (see venue/venue_events.h) so the OMS above them does not
// need to know which protocol produced a message.
using MessageKind = VenueMessageKind;
using ParseResult = VenueParseResult;

class DeribitParser {
 public:
  explicit DeribitParser(std::size_t max_message_bytes = 64u * 1024u)
      : doc_(max_message_bytes) {}

  // Handler must provide:
  //   void on_order(const transport::OrderUpdateMsg&)
  //   void on_fill(const transport::FillMsg&)
  //   void on_account(const models::AccountSummary&)
  //
  // Taken as a template rather than std::function so the calls inline and
  // nothing allocates. `capacity` must be at least len + kJsonPadding.
  template <typename Handler>
  ParseResult parse(const char* data, std::size_t len, std::size_t capacity,
                    Handler& handler) {
    ParseResult result;

    auto root_opt = doc_.parse(data, len, capacity);
    if (!root_opt.has_value()) {
      result.kind = MessageKind::kParseError;
      result.error = "malformed JSON";
      return result;
    }
    Object root = *root_opt;

    // An RPC reply carries "id"; a subscription push does not.
    if (auto id = root["id"].as_int(); id.has_value()) {
      result.rpc_id = *id;
      if (auto err = root["error"]; err.valid() && !err.is_null()) {
        result.kind = MessageKind::kRpcError;
        if (auto obj = err.as_object(); obj.has_value()) {
          if (auto msg = (*obj)["message"].as_string(); msg.has_value()) {
            result.rpc_error_text = *msg;
          }
        }
        return result;
      }
      result.kind = MessageKind::kRpcResult;
      return result;
    }

    const auto method = root["method"].as_string();
    if (!method.has_value()) {
      result.kind = MessageKind::kUnknown;
      return result;
    }

    if (*method == "heartbeat") {
      result.kind = MessageKind::kHeartbeat;
      auto params = root["params"].as_object();
      if (params.has_value()) {
        if (auto type = (*params)["type"].as_string();
            type.has_value() && *type == "test_request") {
          result.kind = MessageKind::kHeartbeatTestRequest;
        }
      }
      return result;
    }

    if (*method != "subscription") {
      result.kind = MessageKind::kUnknown;
      return result;
    }

    auto params_opt = root["params"].as_object();
    if (!params_opt.has_value()) {
      result.kind = MessageKind::kParseError;
      result.error = "subscription without params";
      return result;
    }
    Object params = *params_opt;

    const auto channel = params["channel"].as_string();
    if (!channel.has_value()) {
      result.kind = MessageKind::kParseError;
      result.error = "subscription without channel";
      return result;
    }
    result.channel = *channel;

    Value data_value = params["data"];
    if (!data_value.valid()) {
      result.kind = MessageKind::kParseError;
      result.error = "subscription without data";
      return result;
    }

    // Channel dispatch by substring, exactly as the Python does. Deribit
    // channels are "user.orders.<instrument>.raw" and friends, so an exact
    // match would need the instrument.
    if (channel->find("user.orders") != std::string_view::npos) {
      result.kind = MessageKind::kOrderUpdate;
      auto obj = data_value.as_object();
      if (!obj.has_value()) {
        result.kind = MessageKind::kParseError;
        result.error = "user.orders data is not an object";
        return result;
      }
      transport::OrderUpdateMsg msg{};
      bool unknown_state = false;
      if (!parse_order(*obj, next_seq(), msg, unknown_state)) {
        result.kind = MessageKind::kParseError;
        result.error = "incomplete order object";
        return result;
      }
      result.unknown_order_state = unknown_state;
      handler.on_order(msg);
      result.emitted = 1;
      return result;
    }

    if (channel->find("user.trades") != std::string_view::npos) {
      result.kind = MessageKind::kTradeUpdate;
      // Deribit delivers trades as an array, even when there is only one.
      auto arr = data_value.as_array();
      if (!arr.has_value()) {
        result.kind = MessageKind::kParseError;
        result.error = "user.trades data is not an array";
        return result;
      }
      std::size_t emitted = 0;
      arr->for_each_object([&](Object& trade) {
        transport::FillMsg msg{};
        if (parse_fill(trade, next_seq(), msg)) {
          handler.on_fill(msg);
          ++emitted;
        }
      });
      result.emitted = emitted;
      return result;
    }

    if (channel->find("user.portfolio") != std::string_view::npos) {
      result.kind = MessageKind::kPortfolioUpdate;
      auto obj = data_value.as_object();
      if (!obj.has_value()) {
        result.kind = MessageKind::kParseError;
        result.error = "user.portfolio data is not an object";
        return result;
      }
      models::AccountSummary account;
      parse_account(*obj, account);
      handler.on_account(account);
      result.emitted = 1;
      return result;
    }

    result.kind = MessageKind::kUnknown;
    return result;
  }

  std::uint64_t sequence() const noexcept { return seq_; }

 private:
  std::uint64_t next_seq() noexcept { return ++seq_; }

  // Ported field-for-field from _parse_order.
  static bool parse_order(Object& d, std::uint64_t seq,
                          transport::OrderUpdateMsg& out,
                          bool& unknown_state) noexcept {
    out.hdr.init(transport::HotMsgType::kOrderUpdate,
                 sizeof(transport::OrderUpdateMsg), seq);

    if (!out.exchange.assign(kExchangeName)) {
      return false;
    }

    const auto order_id = d["order_id"].as_string();
    const auto instrument = d["instrument_name"].as_string();
    const auto direction = d["direction"].as_string();
    if (!order_id.has_value() || !instrument.has_value() ||
        !direction.has_value()) {
      return false;
    }
    if (!out.order_id.assign(*order_id) || !out.instrument.assign(*instrument)) {
      return false;
    }
    out.side = static_cast<std::uint8_t>(map_direction(*direction));

    const auto order_type = d["order_type"].as_string();
    out.order_type = static_cast<std::uint8_t>(
        map_order_type(order_type.value_or(std::string_view{})));

    // Missing amount/filled_amount default to 0, matching data.get(..., 0).
    const auto amount = d["amount"].as_decimal();
    out.amount_raw = amount.has_value() ? amount->raw() : 0;

    const auto filled = d["filled_amount"].as_decimal();
    out.filled_amount_raw = filled.has_value() ? filled->raw() : 0;

    const auto price = d["price"].as_decimal();
    out.has_price = price.has_value() ? 1 : 0;
    out.price_raw = price.has_value() ? price->raw() : 0;

    const auto average = d["average_price"].as_decimal();
    out.has_average_price = average.has_value() ? 1 : 0;
    out.average_price_raw = average.has_value() ? average->raw() : 0;

    const auto state = d["order_state"].as_string();
    // data.get("order_state", "open") -- a missing state is treated as open
    // and is NOT flagged as unknown, since that is the documented default.
    const StatusMapping mapping =
        state.has_value() ? map_order_state(*state)
                          : StatusMapping{models::OrderStatus::kOpen, true};
    unknown_state = !mapping.recognised;
    out.status = static_cast<std::uint8_t>(
        refine_with_fill(mapping.status, out.filled_amount_raw > 0));

    // Deribit's order feed never reports liquidity; it arrives on the trade
    // feed. Default to maker, matching the Order model's default, and let the
    // trade handler upgrade it.
    out.liquidity = static_cast<std::uint8_t>(models::Liquidity::kMaker);

    // creation_timestamp is REQUIRED (the Python indexes it directly and would
    // raise); last_update_timestamp falls back to it.
    const auto creation = d["creation_timestamp"].as_int();
    if (!creation.has_value()) {
      return false;
    }
    const auto last_update = d["last_update_timestamp"].as_int();
    out.exchange_ts_ns =
        core::Timestamp::from_millis(last_update.value_or(*creation)).ns();

    // internal_order_id and strategy_id are ours, not Deribit's -- the OMS
    // restores them from the existing order, as _handle_order_update does.
    out.internal_order_id.clear();
    out.strategy_id.clear();
    std::memset(out.reserved, 0, sizeof(out.reserved));
    return true;
  }

  // Ported field-for-field from _parse_fill.
  static bool parse_fill(Object& d, std::uint64_t seq,
                         transport::FillMsg& out) noexcept {
    out.hdr.init(transport::HotMsgType::kFill, sizeof(transport::FillMsg), seq);

    if (!out.exchange.assign(kExchangeName)) {
      return false;
    }

    const auto trade_id = d["trade_id"].as_string();
    const auto order_id = d["order_id"].as_string();
    const auto instrument = d["instrument_name"].as_string();
    const auto direction = d["direction"].as_string();
    if (!trade_id.has_value() || !order_id.has_value() ||
        !instrument.has_value() || !direction.has_value()) {
      return false;
    }
    if (!out.trade_id.assign(*trade_id) || !out.order_id.assign(*order_id) ||
        !out.instrument.assign(*instrument)) {
      return false;
    }

    out.side = static_cast<std::uint8_t>(map_direction(*direction));

    const auto amount = d["amount"].as_decimal();
    out.amount_raw = amount.has_value() ? amount->raw() : 0;
    const auto price = d["price"].as_decimal();
    out.price_raw = price.has_value() ? price->raw() : 0;
    const auto fee = d["fee"].as_decimal();
    out.fee_raw = fee.has_value() ? fee->raw() : 0;

    const auto fee_currency = d["fee_currency"].as_string();
    if (!out.fee_currency.assign(fee_currency.value_or(std::string_view{}))) {
      return false;
    }

    const auto liquidity = d["liquidity"].as_string();
    out.liquidity = static_cast<std::uint8_t>(
        map_liquidity(liquidity.value_or(std::string_view{})));

    // DIVERGENCE from the Python, deliberately.
    //
    // _parse_fill falls back to datetime.utcnow() when "timestamp" is absent,
    // which stamps a fill with the moment we happened to parse it. That makes
    // an exchange-side delay indistinguishable from a local one and quietly
    // corrupts any latency measurement built on it. Here a missing timestamp
    // stays at the epoch, which is obviously wrong and therefore visible.
    const auto ts = d["timestamp"].as_int();
    out.exchange_ts_ns =
        ts.has_value() ? core::Timestamp::from_millis(*ts).ns() : 0;

    out.strategy_id.clear();
    std::memset(out.reserved, 0, sizeof(out.reserved));
    return true;
  }

  // Ported from _handle_portfolio_update.
  //
  // Not a hot path: Deribit pushes portfolio updates at a low rate, and the
  // consumer wants a full AccountSummary rather than a fixed-layout message.
  // std::string allocation here is acceptable for that reason and no other.
  static void parse_account(Object& d, models::AccountSummary& out) {
    const auto currency = d["currency"].as_string();
    out.currency = std::string(currency.value_or(std::string_view{}));
    out.exchange = std::string(kExchangeName);

    auto dec = [&d](std::string_view key) {
      const auto v = d[key].as_decimal();
      return v.value_or(core::Price{});
    };

    out.equity = dec("equity");
    out.balance = dec("balance");
    out.available_funds = dec("available_funds");
    out.initial_margin = dec("initial_margin");
    out.maintenance_margin = dec("maintenance_margin");
    out.margin_balance = dec("margin_balance");
    out.delta_total = dec("delta_total");
    out.options_delta = dec("options_delta");
    out.options_gamma = dec("options_gamma");
    out.options_vega = dec("options_vega");
    out.options_theta = dec("options_theta");
    out.futures_pl = dec("futures_pl");
    out.options_pl = dec("options_pl");
    out.total_pl = dec("total_pl");

    // DIVERGENCE from the Python, deliberately: it stamps this with
    // datetime.utcnow(), i.e. local receive time rather than anything the
    // venue said. Deribit's portfolio payload carries no timestamp, so
    // receive time is the only option -- but it is recorded as such by the
    // caller, not baked in here where it would look like venue data.
    out.timestamp = core::Timestamp{};
  }

  Document doc_;
  std::uint64_t seq_ = 0;
};

}  // namespace axon::venue::deribit
