// Binance USDT-M perpetual futures: user data stream parser.
//
// Ported from oms/binance/binance_ws.py. Binance is the odd one of the four:
//
//   * NO SUBSCRIPTION. The stream is keyed by a listenKey obtained over REST
//     and appended to the URL; events then arrive unbidden. There is nothing
//     to subscribe to and no WebSocket-level auth.
//
//   * THE LISTENKEY EXPIRES. It must be renewed over REST every 30 minutes,
//     and when it lapses the venue sends `listenKeyExpired` and stops. That is
//     reported as kSessionExpired, because the correct response is to
//     re-establish the session, not to log and carry on.
//
//   * ONE EVENT CARRIES BOTH. ORDER_TRADE_UPDATE is an order update AND, when
//     its execution type is TRADE, a fill. Both are emitted from the single
//     frame, which is why the order handler runs before the fill handler here
//     -- the fill references an order the consumer must already have seen.
//
//   * SINGLE-LETTER FIELD NAMES, including `o` for both the payload object and
//     the order type inside it. Every one is annotated below; do not "tidy"
//     them.
//
// All numeric fields arrive as JSON strings, so they route through the exact
// string path and never touch a double.

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

namespace axon::venue::binance {

inline constexpr std::string_view kExchangeName = "binance";

inline constexpr std::string_view kProductionWsUrl = "wss://fstream.binance.com/ws";
inline constexpr std::string_view kTestnetWsUrl =
    "wss://stream.binancefuture.com/ws";
inline constexpr std::string_view kProductionWsHost = "fstream.binance.com";
inline constexpr std::string_view kTestnetWsHost = "stream.binancefuture.com";

using MessageKind = VenueMessageKind;
using ParseResult = VenueParseResult;

// From _parse_order. EXPIRED and EXPIRED_IN_MATCH both map to cancelled: from
// our side an expired order is simply no longer live, and collapsing them
// keeps the state machine at five states across four venues.
struct StatusMapping {
  models::OrderStatus status;
  bool recognised;
};

constexpr StatusMapping map_order_status(std::string_view s) noexcept {
  if (s == "NEW") {
    return {models::OrderStatus::kOpen, true};
  }
  if (s == "PARTIALLY_FILLED") {
    return {models::OrderStatus::kPartiallyFilled, true};
  }
  if (s == "FILLED") {
    return {models::OrderStatus::kFilled, true};
  }
  if (s == "CANCELED") {  // one L, Binance's spelling
    return {models::OrderStatus::kCancelled, true};
  }
  if (s == "REJECTED") {
    return {models::OrderStatus::kRejected, true};
  }
  if (s == "EXPIRED" || s == "EXPIRED_IN_MATCH") {
    return {models::OrderStatus::kCancelled, true};
  }
  return {models::OrderStatus::kOpen, false};
}

class BinanceParser {
 public:
  explicit BinanceParser(std::size_t max_message_bytes = 64u * 1024u)
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

    const auto event = root["e"].as_string();
    if (!event.has_value()) {
      result.kind = MessageKind::kUnknown;
      return result;
    }
    result.channel = *event;

    if (*event == "listenKeyExpired") {
      result.kind = MessageKind::kSessionExpired;
      return result;
    }

    if (*event == "ACCOUNT_UPDATE") {
      result.kind = MessageKind::kPortfolioUpdate;
      auto a = root["a"].as_object();
      if (!a.has_value()) {
        result.kind = MessageKind::kParseError;
        result.error = "ACCOUNT_UPDATE without 'a'";
        return result;
      }
      // "B" is the balances array; one AccountSummary per asset, matching
      // _handle_account_update.
      auto balances = (*a)["B"].as_array();
      if (!balances.has_value()) {
        return result;
      }
      std::size_t emitted = 0;
      balances->for_each_object([&](Object& b) {
        models::AccountSummary account;
        parse_balance(b, account);
        handler.on_account(account);
        ++emitted;
      });
      result.emitted = emitted;
      return result;
    }

    if (*event != "ORDER_TRADE_UPDATE") {
      result.kind = MessageKind::kUnknown;
      return result;
    }

    result.kind = MessageKind::kOrderUpdate;
    auto o = root["o"].as_object();
    if (!o.has_value()) {
      result.kind = MessageKind::kParseError;
      result.error = "ORDER_TRADE_UPDATE without 'o'";
      return result;
    }
    Object payload = *o;

    // The order and the fill are read from the SAME object, so they are
    // extracted together in one forward pass rather than by revisiting it --
    // simdjson On-Demand would have to rescan otherwise.
    transport::OrderUpdateMsg order{};
    transport::FillMsg fill{};
    bool unknown_state = false;
    bool has_fill = false;
    if (!parse_payload(payload, next_seq(), order, fill, unknown_state,
                       has_fill)) {
      result.kind = MessageKind::kParseError;
      result.error = "incomplete ORDER_TRADE_UPDATE payload";
      return result;
    }

    result.unknown_order_state = unknown_state;
    handler.on_order(order);
    result.emitted = 1;

    if (has_fill) {
      // Order first, then fill: the fill refers to an order the consumer must
      // already know about.
      fill.hdr.seq = next_seq();
      handler.on_fill(fill);
      result.kind = MessageKind::kTradeUpdate;
      ++result.emitted;
    }
    return result;
  }

  std::uint64_t sequence() const noexcept { return seq_; }

 private:
  std::uint64_t next_seq() noexcept { return ++seq_; }

  static bool parse_payload(Object& d, std::uint64_t seq,
                            transport::OrderUpdateMsg& order,
                            transport::FillMsg& fill, bool& unknown_state,
                            bool& has_fill) noexcept {
    order.hdr.init(transport::HotMsgType::kOrderUpdate,
                   sizeof(transport::OrderUpdateMsg), seq);
    fill.hdr.init(transport::HotMsgType::kFill, sizeof(transport::FillMsg), seq);

    if (!order.exchange.assign(kExchangeName) ||
        !fill.exchange.assign(kExchangeName)) {
      return false;
    }

    // s = symbol, c = client order id, S = side, o = order type,
    // f = time in force, q = original quantity, p = original price,
    // x = execution type, X = order status, i = order id, l = last filled qty,
    // z = cumulative filled qty, L = last filled price, N = commission asset,
    // n = commission, T = trade/transaction time, t = trade id,
    // m = is maker side, ap = average price.
    // READ IN BINANCE'S FIELD ORDER. simdjson On-Demand scans forward and only
    // rewinds when it must, so a lookup that goes backwards costs a full
    // rescan of the object. Reordering these lines is a silent slowdown, not a
    // correctness change -- which is exactly why it is easy to do by accident.
    const auto symbol = d["s"].as_string();
    const auto client_id = d["c"].as_string();
    const auto side = d["S"].as_string();
    const auto order_type = d["o"].as_string();
    const auto tif = d["f"].as_string();
    const auto qty = d["q"].as_decimal();
    const auto price = d["p"].as_decimal();
    const auto avg_price = d["ap"].as_decimal();
    const auto exec_type = d["x"].as_string();
    const auto status = d["X"].as_string();
    const auto order_id = d["i"].as_int();
    const auto last_qty = d["l"].as_decimal();
    const auto cum_qty = d["z"].as_decimal();
    const auto last_price = d["L"].as_decimal();
    const auto commission = d["n"].as_decimal();
    const auto commission_asset = d["N"].as_string();
    const auto event_time = d["T"].as_int();
    const auto trade_id = d["t"].as_int();
    const auto is_maker = d["m"].as_bool();

    if (!symbol.has_value() || !side.has_value() || !order_id.has_value()) {
      return false;
    }

    // The order id is a number on the wire; the model carries it as text.
    char id_buf[24];
    const std::size_t id_len = write_int(id_buf, sizeof(id_buf), *order_id);
    if (!order.order_id.assign(std::string_view(id_buf, id_len)) ||
        !order.instrument.assign(*symbol)) {
      return false;
    }

    order.side = static_cast<std::uint8_t>(*side == "BUY"
                                               ? models::OrderSide::kBuy
                                               : models::OrderSide::kSell);
    order.order_type = static_cast<std::uint8_t>(
        order_type.value_or(std::string_view{}) == "LIMIT"
            ? models::OrderType::kLimit
            : models::OrderType::kMarket);

    order.amount_raw = qty.has_value() ? qty->raw() : 0;
    order.filled_amount_raw = cum_qty.has_value() ? cum_qty->raw() : 0;

    // Binance sends "0" rather than omitting an unset price; treat it as absent
    // exactly as the Python does.
    order.has_price = (price.has_value() && !price->is_zero()) ? 1 : 0;
    order.price_raw = order.has_price != 0 ? price->raw() : 0;
    order.has_average_price =
        (avg_price.has_value() && !avg_price->is_zero()) ? 1 : 0;
    order.average_price_raw =
        order.has_average_price != 0 ? avg_price->raw() : 0;

    const StatusMapping mapping =
        status.has_value() ? map_order_status(*status)
                           : StatusMapping{models::OrderStatus::kOpen, true};
    unknown_state = !mapping.recognised;
    order.status = static_cast<std::uint8_t>(mapping.status);

    // GTX is Binance's post-only time-in-force. OrderUpdateMsg has no
    // post_only field -- it is an attribute of the REQUEST, and the consumer
    // already knows what it sent -- so this is read only to keep the field
    // walk in document order for simdjson.
    static_cast<void>(tif);

    // Liquidity is not knowable from an order update on any venue; it arrives
    // with the fill. Default to maker and let the fill upgrade it, matching
    // every other parser here.
    order.liquidity = static_cast<std::uint8_t>(models::Liquidity::kMaker);

    order.exchange_ts_ns =
        event_time.has_value() ? core::Timestamp::from_millis(*event_time).ns()
                               : 0;
    order.internal_order_id.clear();
    order.strategy_id.clear();
    std::memset(order.reserved, 0, sizeof(order.reserved));

    // A fill only exists when the execution type says so.
    has_fill = exec_type.has_value() && *exec_type == "TRADE";
    if (!has_fill) {
      return true;
    }

    char trade_buf[24];
    const std::size_t trade_len =
        write_int(trade_buf, sizeof(trade_buf), trade_id.value_or(0));
    if (!fill.trade_id.assign(std::string_view(trade_buf, trade_len)) ||
        !fill.order_id.assign(std::string_view(id_buf, id_len)) ||
        !fill.instrument.assign(*symbol)) {
      has_fill = false;
      return true;
    }

    fill.side = order.side;
    fill.amount_raw = last_qty.has_value() ? last_qty->raw() : 0;
    fill.price_raw = last_price.has_value() ? last_price->raw() : 0;
    fill.fee_raw = commission.has_value() ? commission->raw() : 0;
    // Default USDT, matching data.get("N", "USDT").
    if (!fill.fee_currency.assign(commission_asset.value_or("USDT"))) {
      has_fill = false;
      return true;
    }
    // `m` is "was the buyer the maker" -- true means OUR side was the maker.
    fill.liquidity = static_cast<std::uint8_t>(is_maker.value_or(false)
                                                   ? models::Liquidity::kMaker
                                                   : models::Liquidity::kTaker);
    fill.exchange_ts_ns = order.exchange_ts_ns;
    fill.strategy_id.clear();
    std::memset(fill.reserved, 0, sizeof(fill.reserved));

    // The client order id is our label; keep it out of the hot message but
    // note it exists. (Consumers recover it from their own order book.)
    static_cast<void>(client_id);
    return true;
  }

  static void parse_balance(Object& b, models::AccountSummary& out) {
    // a = asset, wb = wallet balance, cw = cross wallet balance.
    const auto asset = b["a"].as_string();
    out.currency = std::string(asset.value_or(std::string_view{}));
    out.exchange = std::string(kExchangeName);
    const auto wallet = b["wb"].as_decimal();
    out.balance = wallet.value_or(core::Price{});
    out.equity = out.balance;
    const auto cross = b["cw"].as_decimal();
    out.available_funds = cross.value_or(core::Price{});
    out.timestamp = core::Timestamp{};
  }

  static std::size_t write_int(char* out, std::size_t cap,
                               std::int64_t v) noexcept {
    char scratch[24];
    std::size_t len = 0;
    bool negative = v < 0;
    std::uint64_t magnitude =
        negative ? static_cast<std::uint64_t>(-(v + 1)) + 1U
                 : static_cast<std::uint64_t>(v);
    if (magnitude == 0) {
      scratch[len++] = '0';
    }
    while (magnitude > 0) {
      scratch[len++] = static_cast<char>('0' + (magnitude % 10));
      magnitude /= 10;
    }
    std::size_t pos = 0;
    if (negative && pos < cap) {
      out[pos++] = '-';
    }
    while (len > 0 && pos < cap) {
      out[pos++] = scratch[--len];
    }
    return pos;
  }

  Document doc_;
  std::uint64_t seq_ = 0;
};

}  // namespace axon::venue::binance
