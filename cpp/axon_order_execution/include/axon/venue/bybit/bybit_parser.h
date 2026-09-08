// Bybit v5 USDT perpetual: private stream parser.
//
// Ported from oms/bybit/bybit_ws.py.
//
//   * TOPIC/DATA SHAPE. {"topic":"order","data":[...]} -- every payload is an
//     array, even for a single object.
//
//   * CATEGORY FILTERING IS MANDATORY. One private stream carries linear
//     (USDT perp), inverse, option and spot. Anything whose `category` is not
//     "linear" belongs to a different product and must be dropped, or a spot
//     fill would be applied to a perp position. The Python does this and so
//     does this; frames filtered out are reported with emitted == 0.
//
//   * EVERYTHING IS A STRING, including createdTime/updatedTime/execTime,
//     which are millisecond epochs quoted as text.
//
//   * PRICE "0" MEANS UNSET. A market order arrives with price "0"; treating
//     that as a real price would produce an order at zero.

#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

#include "axon/core/timestamp.h"
#include "axon/models/portfolio.h"
#include "axon/transport/hot_messages.h"
#include "axon/venue/json_view.h"
#include "axon/venue/venue_events.h"

namespace axon::venue::bybit {

inline constexpr std::string_view kExchangeName = "bybit";

inline constexpr std::string_view kProductionUrl = "wss://stream.bybit.com/v5/private";
inline constexpr std::string_view kTestnetUrl =
    "wss://stream-testnet.bybit.com/v5/private";
inline constexpr std::string_view kProductionHost = "stream.bybit.com";
inline constexpr std::string_view kTestnetHost = "stream-testnet.bybit.com";
inline constexpr std::string_view kPath = "/v5/private";

// The only product category this engine trades. See the note above.
inline constexpr std::string_view kCategory = "linear";

using MessageKind = VenueMessageKind;
using ParseResult = VenueParseResult;

struct StatusMapping {
  models::OrderStatus status;
  bool recognised;
};

// From _parse_order. Bybit has more states than the model does:
// Deactivated collapses to cancelled (a conditional order that will never
// trigger is not live), and Triggered becomes open (it just became a real
// resting order).
constexpr StatusMapping map_order_status(std::string_view s) noexcept {
  if (s == "New") {
    return {models::OrderStatus::kOpen, true};
  }
  if (s == "PartiallyFilled") {
    return {models::OrderStatus::kPartiallyFilled, true};
  }
  if (s == "Filled") {
    return {models::OrderStatus::kFilled, true};
  }
  if (s == "Cancelled") {
    return {models::OrderStatus::kCancelled, true};
  }
  if (s == "Rejected") {
    return {models::OrderStatus::kRejected, true};
  }
  if (s == "Deactivated") {
    return {models::OrderStatus::kCancelled, true};
  }
  if (s == "Untriggered") {
    return {models::OrderStatus::kPending, true};
  }
  if (s == "Triggered") {
    return {models::OrderStatus::kOpen, true};
  }
  return {models::OrderStatus::kOpen, false};
}

class BybitParser {
 public:
  explicit BybitParser(std::size_t max_message_bytes = 64u * 1024u)
      : doc_(max_message_bytes) {}

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

    // Operation replies (auth/subscribe/pong) carry "op" rather than "topic".
    if (auto op = root["op"].as_string(); op.has_value()) {
      if (*op == "pong" || *op == "ping") {
        result.kind = MessageKind::kHeartbeat;
        return result;
      }
      result.kind = MessageKind::kSessionEvent;
      if (auto success = root["success"].as_bool();
          success.has_value() && !*success) {
        result.kind = MessageKind::kRpcError;
        if (auto msg = root["ret_msg"].as_string(); msg.has_value()) {
          result.rpc_error_text = *msg;
        }
      }
      if (auto req_id = root["req_id"].as_int(); req_id.has_value()) {
        result.rpc_id = *req_id;
      }
      return result;
    }

    const auto topic = root["topic"].as_string();
    if (!topic.has_value()) {
      result.kind = MessageKind::kUnknown;
      return result;
    }
    result.channel = *topic;

    auto arr = root["data"].as_array();
    if (!arr.has_value()) {
      result.kind = MessageKind::kParseError;
      result.error = "topic payload is not an array";
      return result;
    }

    if (*topic == "order") {
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

    if (*topic == "execution") {
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

    if (*topic == "wallet") {
      result.kind = MessageKind::kPortfolioUpdate;
      std::size_t emitted = 0;
      arr->for_each_object([&](Object& d) {
        models::AccountSummary account;
        parse_wallet(d, account);
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
    // Drop anything that is not a USDT perp before doing any other work.
    const auto category = d["category"].as_string();
    if (!category.has_value() || *category != kCategory) {
      return false;
    }

    out.hdr.init(transport::HotMsgType::kOrderUpdate,
                 sizeof(transport::OrderUpdateMsg), seq);
    if (!out.exchange.assign(kExchangeName)) {
      return false;
    }

    const auto symbol = d["symbol"].as_string();
    const auto order_id = d["orderId"].as_string();
    const auto side = d["side"].as_string();
    if (!symbol.has_value() || !order_id.has_value() || !side.has_value()) {
      return false;
    }
    if (!out.order_id.assign(*order_id) || !out.instrument.assign(*symbol)) {
      return false;
    }

    out.side = static_cast<std::uint8_t>(*side == "Buy"
                                             ? models::OrderSide::kBuy
                                             : models::OrderSide::kSell);

    const auto order_type = d["orderType"].as_string();
    out.order_type = static_cast<std::uint8_t>(
        order_type.value_or(std::string_view{}) == "Limit"
            ? models::OrderType::kLimit
            : models::OrderType::kMarket);

    const auto qty = d["qty"].as_decimal();
    out.amount_raw = qty.has_value() ? qty->raw() : 0;

    // "0" means unset, not zero -- see the header note.
    const auto price = d["price"].as_decimal();
    out.has_price = (price.has_value() && !price->is_zero()) ? 1 : 0;
    out.price_raw = out.has_price != 0 ? price->raw() : 0;

    const auto cum = d["cumExecQty"].as_decimal();
    out.filled_amount_raw = cum.has_value() ? cum->raw() : 0;

    const auto avg = d["avgPrice"].as_decimal();
    out.has_average_price = (avg.has_value() && !avg->is_zero()) ? 1 : 0;
    out.average_price_raw = out.has_average_price != 0 ? avg->raw() : 0;

    const auto status = d["orderStatus"].as_string();
    const StatusMapping mapping =
        status.has_value() ? map_order_status(*status)
                           : StatusMapping{models::OrderStatus::kOpen, true};
    unknown_state = !mapping.recognised;
    out.status = static_cast<std::uint8_t>(mapping.status);

    out.liquidity = static_cast<std::uint8_t>(models::Liquidity::kMaker);

    // Quoted millisecond epochs. updatedTime falls back to createdTime.
    const auto created = d["createdTime"].as_int();
    const auto updated = d["updatedTime"].as_int();
    const std::int64_t ms = updated.value_or(created.value_or(0));
    out.exchange_ts_ns = ms != 0 ? core::Timestamp::from_millis(ms).ns() : 0;

    out.internal_order_id.clear();
    out.strategy_id.clear();
    std::memset(out.reserved, 0, sizeof(out.reserved));
    return true;
  }

  static bool parse_fill(Object& d, std::uint64_t seq,
                         transport::FillMsg& out) noexcept {
    const auto category = d["category"].as_string();
    if (!category.has_value() || *category != kCategory) {
      return false;
    }

    out.hdr.init(transport::HotMsgType::kFill, sizeof(transport::FillMsg), seq);
    if (!out.exchange.assign(kExchangeName)) {
      return false;
    }

    const auto exec_id = d["execId"].as_string();
    const auto order_id = d["orderId"].as_string();
    const auto symbol = d["symbol"].as_string();
    const auto side = d["side"].as_string();
    if (!exec_id.has_value() || !order_id.has_value() || !symbol.has_value() ||
        !side.has_value()) {
      return false;
    }
    if (!out.trade_id.assign(*exec_id) || !out.order_id.assign(*order_id) ||
        !out.instrument.assign(*symbol)) {
      return false;
    }

    out.side = static_cast<std::uint8_t>(*side == "Buy"
                                             ? models::OrderSide::kBuy
                                             : models::OrderSide::kSell);

    const auto qty = d["execQty"].as_decimal();
    out.amount_raw = qty.has_value() ? qty->raw() : 0;
    const auto price = d["execPrice"].as_decimal();
    out.price_raw = price.has_value() ? price->raw() : 0;
    const auto fee = d["execFee"].as_decimal();
    out.fee_raw = fee.has_value() ? fee->raw() : 0;

    const auto fee_ccy = d["feeCurrency"].as_string();
    if (!out.fee_currency.assign(fee_ccy.value_or("USDT"))) {
      return false;
    }

    // isMaker arrives as a JSON boolean on some paths and the string "true" on
    // others; the Python accepts both and so does this.
    bool maker = false;
    if (auto b = d["isMaker"].as_bool(); b.has_value()) {
      maker = *b;
    } else if (auto s = d["isMaker"].as_string(); s.has_value()) {
      maker = (*s == "true");
    }
    out.liquidity = static_cast<std::uint8_t>(maker ? models::Liquidity::kMaker
                                                    : models::Liquidity::kTaker);

    const auto ts = d["execTime"].as_int();
    out.exchange_ts_ns =
        ts.has_value() && *ts != 0 ? core::Timestamp::from_millis(*ts).ns() : 0;

    out.strategy_id.clear();
    std::memset(out.reserved, 0, sizeof(out.reserved));
    return true;
  }

  static void parse_wallet(Object& d, models::AccountSummary& out) {
    out.exchange = std::string(kExchangeName);
    const auto ccy = d["accountType"].as_string();
    out.currency = std::string(ccy.value_or(std::string_view{}));

    const auto equity = d["totalEquity"].as_decimal();
    out.equity = equity.value_or(core::Price{});
    const auto available = d["totalAvailableBalance"].as_decimal();
    out.available_funds = available.value_or(core::Price{});
    const auto margin_balance = d["totalMarginBalance"].as_decimal();
    out.margin_balance = margin_balance.value_or(core::Price{});
    out.balance = out.margin_balance;
    const auto im = d["totalInitialMargin"].as_decimal();
    out.initial_margin = im.value_or(core::Price{});
    const auto mm = d["totalMaintenanceMargin"].as_decimal();
    out.maintenance_margin = mm.value_or(core::Price{});
    const auto pnl = d["totalPerpUPL"].as_decimal();
    out.total_pl = pnl.value_or(core::Price{});
    out.futures_pl = out.total_pl;
    out.timestamp = core::Timestamp{};
  }

  Document doc_;
  std::uint64_t seq_ = 0;
};

}  // namespace axon::venue::bybit
