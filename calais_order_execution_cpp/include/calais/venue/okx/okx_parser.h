// OKX v5 perpetual swap: private channel parser.
//
// Ported from oms/okx/okx_ws.py.
//
//   * ARG/DATA SHAPE. {"arg":{"channel":"orders",...},"data":[...]}.
//     Anything carrying "event" is a login/subscribe acknowledgement, not a
//     data push.
//
//   * THE HEARTBEAT IS NOT JSON. OKX expects the literal text "ping" and
//     replies with the literal text "pong". Neither reaches this parser --
//     the WebSocket layer handles them, which is exactly what the odd-looking
//     "some exchanges send plain text" branch in the Python receive loop is
//     for. `is_plain_text_heartbeat` below is provided so the caller can
//     recognise them before attempting a parse.
//
//   * EMPTY STRING MEANS ABSENT. OKX sends "px":"" and "avgPx":"" rather than
//     omitting them. json_view treats an empty string as nullopt precisely
//     because of this; reading it as zero would create an order priced at 0.
//
//   * FEES ARE NEGATIVE. OKX reports a fee paid as a negative number. The
//     Python takes abs(); so does this, so that fee always means "cost".
//
//   * post_only IS AN ORDER TYPE, not a flag: ordType is "post_only" rather
//     than "limit". Both map to a limit order.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "calais/core/timestamp.h"
#include "calais/models/portfolio.h"
#include "calais/transport/hot_messages.h"
#include "calais/venue/json_view.h"
#include "calais/venue/venue_events.h"

namespace calais::venue::okx {

inline constexpr std::string_view kExchangeName = "okx";

inline constexpr std::string_view kProductionUrl = "wss://ws.okx.com:8443/ws/v5/private";
inline constexpr std::string_view kTestnetUrl =
    "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999";
inline constexpr std::string_view kProductionHost = "ws.okx.com";
inline constexpr std::string_view kTestnetHost = "wspap.okx.com";

using MessageKind = VenueMessageKind;
using ParseResult = VenueParseResult;

// OKX's liveness exchange is plain text, not JSON. Check this before parsing.
constexpr bool is_plain_text_heartbeat(std::string_view frame) noexcept {
  return frame == "ping" || frame == "pong";
}

struct StatusMapping {
  models::OrderStatus status;
  bool recognised;
};

// From _parse_order. mmp_canceled is a market-maker-protection cancel; from
// our side it is simply cancelled.
constexpr StatusMapping map_order_status(std::string_view s) noexcept {
  if (s == "live") {
    return {models::OrderStatus::kOpen, true};
  }
  if (s == "partially_filled") {
    return {models::OrderStatus::kPartiallyFilled, true};
  }
  if (s == "filled") {
    return {models::OrderStatus::kFilled, true};
  }
  if (s == "canceled") {  // one L, OKX's spelling
    return {models::OrderStatus::kCancelled, true};
  }
  if (s == "mmp_canceled") {
    return {models::OrderStatus::kCancelled, true};
  }
  return {models::OrderStatus::kOpen, false};
}

constexpr models::OrderType map_order_type(std::string_view s) noexcept {
  return (s == "limit" || s == "post_only") ? models::OrderType::kLimit
                                            : models::OrderType::kMarket;
}

class OkxParser {
 public:
  explicit OkxParser(std::size_t max_message_bytes = 64u * 1024u)
      : doc_(max_message_bytes) {}

  template <typename Handler>
  ParseResult parse(const char* data, std::size_t len, std::size_t capacity,
                    Handler& handler) {
    ParseResult result;

    // The plain-text heartbeat never reaches a JSON parser.
    if (is_plain_text_heartbeat(std::string_view(data, len))) {
      result.kind = MessageKind::kHeartbeat;
      return result;
    }

    auto root_opt = doc_.parse(data, len, capacity);
    if (!root_opt.has_value()) {
      result.kind = MessageKind::kParseError;
      result.error = "malformed JSON";
      return result;
    }
    Object root = *root_opt;

    // "event" marks a login/subscribe/error acknowledgement.
    if (auto event = root["event"].as_string(); event.has_value()) {
      if (*event == "error") {
        result.kind = MessageKind::kRpcError;
        if (auto msg = root["msg"].as_string(); msg.has_value()) {
          result.rpc_error_text = *msg;
        }
        if (auto code = root["code"].as_int(); code.has_value()) {
          result.rpc_id = *code;
        }
      } else {
        result.kind = MessageKind::kSessionEvent;
        result.channel = *event;
      }
      return result;
    }

    auto arg = root["arg"].as_object();
    if (!arg.has_value()) {
      result.kind = MessageKind::kUnknown;
      return result;
    }
    const auto channel = (*arg)["channel"].as_string();
    if (!channel.has_value()) {
      result.kind = MessageKind::kUnknown;
      return result;
    }
    result.channel = *channel;

    auto arr = root["data"].as_array();
    if (!arr.has_value()) {
      result.kind = MessageKind::kParseError;
      result.error = "channel payload is not an array";
      return result;
    }

    if (*channel == "orders") {
      result.kind = MessageKind::kOrderUpdate;
      std::size_t emitted = 0;
      bool unknown = false;
      arr->for_each_object([&](Object& d) {
        transport::OrderUpdateMsg msg{};
        bool unknown_state = false;
        if (parse_order(d, next_seq(), msg, unknown_state)) {
          unknown = unknown || unknown_state;
          handler.on_order(msg);
          ++emitted;
        }
      });
      result.emitted = emitted;
      result.unknown_order_state = unknown;
      return result;
    }

    if (*channel == "fills") {
      result.kind = MessageKind::kTradeUpdate;
      std::size_t emitted = 0;
      arr->for_each_object([&](Object& d) {
        transport::FillMsg msg{};
        if (parse_fill(d, next_seq(), msg)) {
          handler.on_fill(msg);
          ++emitted;
        }
      });
      result.emitted = emitted;
      return result;
    }

    if (*channel == "account") {
      result.kind = MessageKind::kPortfolioUpdate;
      std::size_t emitted = 0;
      arr->for_each_object([&](Object& d) {
        models::AccountSummary account;
        parse_account(d, account);
        handler.on_account(account);
        ++emitted;
      });
      result.emitted = emitted;
      return result;
    }

    result.kind = MessageKind::kUnknown;
    return result;
  }

  std::uint64_t sequence() const noexcept { return seq_; }

 private:
  std::uint64_t next_seq() noexcept { return ++seq_; }

  static bool parse_order(Object& d, std::uint64_t seq,
                          transport::OrderUpdateMsg& out,
                          bool& unknown_state) noexcept {
    out.hdr.init(transport::HotMsgType::kOrderUpdate,
                 sizeof(transport::OrderUpdateMsg), seq);
    if (!out.exchange.assign(kExchangeName)) {
      return false;
    }

    const auto inst = d["instId"].as_string();
    const auto ord_id = d["ordId"].as_string();
    const auto side = d["side"].as_string();
    if (!inst.has_value() || !ord_id.has_value() || !side.has_value()) {
      return false;
    }
    if (!out.order_id.assign(*ord_id) || !out.instrument.assign(*inst)) {
      return false;
    }

    out.side = static_cast<std::uint8_t>(*side == "buy"
                                             ? models::OrderSide::kBuy
                                             : models::OrderSide::kSell);

    const auto ord_type = d["ordType"].as_string();
    out.order_type = static_cast<std::uint8_t>(
        map_order_type(ord_type.value_or("limit")));

    const auto sz = d["sz"].as_decimal();
    out.amount_raw = sz.has_value() ? sz->raw() : 0;

    // "" is absent, not zero. json_view enforces that.
    const auto px = d["px"].as_decimal();
    out.has_price = px.has_value() ? 1 : 0;
    out.price_raw = px.has_value() ? px->raw() : 0;

    const auto filled = d["accFillSz"].as_decimal();
    out.filled_amount_raw = filled.has_value() ? filled->raw() : 0;

    const auto avg = d["avgPx"].as_decimal();
    out.has_average_price = avg.has_value() ? 1 : 0;
    out.average_price_raw = avg.has_value() ? avg->raw() : 0;

    const auto state = d["state"].as_string();
    const StatusMapping mapping =
        state.has_value() ? map_order_status(*state)
                          : StatusMapping{models::OrderStatus::kOpen, true};
    unknown_state = !mapping.recognised;
    out.status = static_cast<std::uint8_t>(mapping.status);

    out.liquidity = static_cast<std::uint8_t>(models::Liquidity::kMaker);

    const auto created = d["cTime"].as_int();
    const auto updated = d["uTime"].as_int();
    const std::int64_t ms = updated.value_or(created.value_or(0));
    out.exchange_ts_ns = ms != 0 ? core::Timestamp::from_millis(ms).ns() : 0;

    out.internal_order_id.clear();
    out.strategy_id.clear();
    std::memset(out.reserved, 0, sizeof(out.reserved));
    return true;
  }

  static bool parse_fill(Object& d, std::uint64_t seq,
                         transport::FillMsg& out) noexcept {
    out.hdr.init(transport::HotMsgType::kFill, sizeof(transport::FillMsg), seq);
    if (!out.exchange.assign(kExchangeName)) {
      return false;
    }

    const auto inst = d["instId"].as_string();
    const auto ord_id = d["ordId"].as_string();
    const auto side = d["side"].as_string();
    if (!inst.has_value() || !ord_id.has_value() || !side.has_value()) {
      return false;
    }

    // tradeId, falling back to billId as the Python does.
    auto trade_id = d["tradeId"].as_string();
    if (!trade_id.has_value() || trade_id->empty()) {
      trade_id = d["billId"].as_string();
    }
    if (!trade_id.has_value() || trade_id->empty()) {
      return false;
    }

    if (!out.trade_id.assign(*trade_id) || !out.order_id.assign(*ord_id) ||
        !out.instrument.assign(*inst)) {
      return false;
    }

    out.side = static_cast<std::uint8_t>(*side == "buy"
                                             ? models::OrderSide::kBuy
                                             : models::OrderSide::kSell);

    const auto sz = d["fillSz"].as_decimal();
    out.amount_raw = sz.has_value() ? sz->raw() : 0;
    const auto px = d["fillPx"].as_decimal();
    out.price_raw = px.has_value() ? px->raw() : 0;

    // OKX reports a fee paid as NEGATIVE. Normalise to "cost", matching abs()
    // in the Python -- otherwise fee accounting flips sign against the other
    // three venues.
    const auto fee = d["fee"].as_decimal();
    out.fee_raw = fee.has_value() ? fee->abs().raw() : 0;

    const auto fee_ccy = d["feeCcy"].as_string();
    if (!out.fee_currency.assign(fee_ccy.value_or("USDT"))) {
      return false;
    }

    const auto exec_type = d["execType"].as_string();
    out.liquidity = static_cast<std::uint8_t>(
        exec_type.value_or(std::string_view{}) == "M" ? models::Liquidity::kMaker
                                                      : models::Liquidity::kTaker);

    auto ts = d["ts"].as_int();
    if (!ts.has_value()) {
      ts = d["fillTime"].as_int();
    }
    out.exchange_ts_ns =
        ts.has_value() && *ts != 0 ? core::Timestamp::from_millis(*ts).ns() : 0;

    out.strategy_id.clear();
    std::memset(out.reserved, 0, sizeof(out.reserved));
    return true;
  }

  static void parse_account(Object& d, models::AccountSummary& out) {
    out.exchange = std::string(kExchangeName);
    // The account channel reports a portfolio total plus a per-currency
    // breakdown; the summary here is the total, keyed by the reporting
    // currency OKX uses for it.
    out.currency = "USD";
    const auto equity = d["totalEq"].as_decimal();
    out.equity = equity.value_or(core::Price{});
    const auto iso = d["isoEq"].as_decimal();
    out.initial_margin = iso.value_or(core::Price{});
    const auto avail = d["adjEq"].as_decimal();
    out.available_funds = avail.value_or(core::Price{});
    const auto mgn = d["mgnRatio"].as_decimal();
    out.maintenance_margin = mgn.value_or(core::Price{});
    out.balance = out.equity;
    out.margin_balance = out.equity;
    out.timestamp = core::Timestamp{};
  }

  Document doc_;
  std::uint64_t seq_ = 0;
};

}  // namespace calais::venue::okx
