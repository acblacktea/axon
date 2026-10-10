// OKX: everything the OMS needs from this venue.
//
// OKX v5 perpetual swaps: one private connection carries the feed AND order
// entry (op:"order"), logged in once with a signed, passphrase-bearing login.
//
// Moved out of venue_session.cpp and venue_rest.cpp so each venue's
// protocol lives in one place, as it does on the EMS side. Only venue() is
// visible outside this file.

#include "axon/exchanges/okx/okx_oms.h"

#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "axon/core/clock.h"
#include "axon/net/crypto_lite.h"
#include "axon/oms/detail/rest_json.h"
#include "axon/oms/detail/session_util.h"
#include "axon/util/logging.h"
#include "axon/util/metrics.h"
#include "axon/exchanges/okx/okx_builder.h"
#include "axon/exchanges/okx/okx_parser.h"
#include "axon/venue/json_view.h"

namespace axon::oms::okx {
namespace {

using detail::doc;
using detail::first_of_array_at;
using detail::http_error;
using detail::monotonic_seconds;
using detail::now_ms;
using detail::parse_array_at;
using detail::parse_object_at;
using detail::parse_string_id;
using detail::url_encode;

// ===========================================================================
// The private feed session
// ===========================================================================
// ===========================================================================
// OKX
// ===========================================================================
class OkxSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::okx::kTestnetHost
                                              : venue::okx::kProductionHost);
  }
  std::string path() const override { return "/ws/v5/private"; }

  bool send_authentication() override {
    char buf[venue::okx::kMaxRequestBytes];
    // SECONDS, not milliseconds. OKX rejects a millisecond timestamp here even
    // though its REST signing uses them, with an error that says nothing.
    const std::int64_t seconds = core::wall_clock_ns() / 1'000'000'000LL;
    const std::size_t n = venue::okx::OkxBuilder::ws_login(
        buf, sizeof(buf), exchange_.api_key, exchange_.passphrase,
        exchange_.api_secret, seconds);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool send_subscriptions() override {
    const std::string_view channels[] = {venue::okx::kOrdersChannel,
                                         venue::okx::kFillsChannel,
                                         venue::okx::kAccountChannel};
    char buf[venue::okx::kMaxRequestBytes];
    const std::size_t n =
        venue::okx::OkxBuilder::ws_subscribe(buf, sizeof(buf), channels, 3);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    const std::string_view frame(reinterpret_cast<const char*>(data), len);
    // OKX's liveness exchange is plain text, not JSON.
    if (frame == "ping") {
      send_raw("pong");
      return true;
    }
    if (frame == "pong") {
      return true;
    }

    // An order-entry reply, which on OKX arrives on this same connection:
    // {"id":"<id>","op":"order","data":[...],"code":"0"}. It is claimed here
    // before the feed parser sees it -- the parser is built for channel
    // messages and an `op` reply is not one.
    if (frame.size() > 7 && frame.compare(0, 7, R"({"id":")") == 0) {
      auto reply = reply_doc_.parse(frame.data(), len, capacity);
      if (reply.has_value()) {
        const auto id = parse_string_id((*reply)["id"]);
        if (id.has_value() && handlers_.on_rpc_reply) {
          // `code` is the REQUEST's outcome. Whether the ORDER was accepted is
          // in each element's sCode, which the EMS checks -- it knows the
          // result shape, this does not.
          const auto code = (*reply)["code"].as_string().value_or("0");
          const auto msg = (*reply)["msg"].as_string().value_or("");
          handlers_.on_rpc_reply(*id, code == "0", frame, msg);
          return true;
        }
      }
    }

    Sink sink{this};
    const auto result = parser_.parse(reinterpret_cast<const char*>(data), len,
                                      capacity, sink);
    if (result.kind == venue::VenueMessageKind::kSessionEvent) {
      if (result.channel == "login") {
        on_auth_reply(true, {});
      } else if (result.channel == "subscribe") {
        // OKX acknowledges each channel separately; the first is enough to
        // call the session live.
        on_subscribe_reply(true, {});
      }
    } else if (result.kind == venue::VenueMessageKind::kRpcError) {
      const std::string text(result.rpc_error_text);
      fail_session("okx error: " + text, /*fatal=*/text.find("Login") != std::string::npos);
    }
    return true;
  }

 private:
  struct Sink {
    OkxSession* self;
    void on_order(const transport::OrderUpdateMsg& m) {
      if (self->handlers_.on_order) self->handlers_.on_order(m);
    }
    void on_fill(const transport::FillMsg& m) {
      if (self->handlers_.on_fill) self->handlers_.on_fill(m);
    }
    void on_account(const models::AccountSummary& a) {
      if (self->handlers_.on_account) self->handlers_.on_account(a);
    }
  };

  venue::okx::OkxParser parser_;
  // A second parser, because the feed parser's document is mid-walk when an
  // order reply arrives and On-Demand cannot revisit one.
  venue::Document reply_doc_{64 * 1024};
};

// ===========================================================================
// REST snapshots: reconciliation, positions, tickers
// ===========================================================================
// ===========================================================================
// OKX v5
// ===========================================================================
class OkxRestImpl final : public VenueRest {
 public:
  OkxRestImpl(ExchangeConfig exchange, net::HttpClient* http)
      : exchange_(std::move(exchange)), http_(http) {}

  const std::string& exchange_name() const override { return exchange_.name; }

  void get_ticker(const std::string& instrument, TickerCallback cb) override {
    send("/api/v5/market/ticker?instId=" + url_encode(instrument), "GET", {}, false,
         [cb, instrument](const net::HttpResponse& r) {
           if (!r.error.empty() || !r.ok()) {
             cb(std::nullopt, http_error(r));
             return;
           }
           auto root = doc().parse_copy(r.body);
           auto data = root.has_value() ? (*root)["data"].as_array() : std::nullopt;
           if (!data.has_value()) {
             cb(std::nullopt, "no ticker data");
             return;
           }
           std::optional<models::Ticker> ticker;
           data->for_each_object([&](venue::Object& d) {
             if (ticker.has_value()) {
               return;
             }
             models::Ticker t;
             t.instrument = instrument;
             t.best_bid_price = d["bidPx"].as_decimal().value_or(core::Price{});
             t.best_bid_amount = d["bidSz"].as_decimal().value_or(core::Qty{});
             t.best_ask_price = d["askPx"].as_decimal().value_or(core::Price{});
             t.best_ask_amount = d["askSz"].as_decimal().value_or(core::Qty{});
             t.last_price = d["last"].as_decimal();
             ticker = t;
           });
           if (!ticker.has_value()) {
             cb(std::nullopt, "ticker not found");
             return;
           }
           cb(ticker, {});
         });
  }

  void get_open_orders(const std::string&, OrdersCallback cb) override {
    send("/api/v5/trade/orders-pending?instType=SWAP", "GET", {}, true,
         [cb](const net::HttpResponse& r) {
           parse_array_at<models::Order>(r, {"data"}, parse_order, cb);
         });
  }

  void get_order(const models::Order& order, OrderCallback cb) override {
    send("/api/v5/trade/order?instId=" + url_encode(order.instrument) +
             "&ordId=" + url_encode(order.order_id),
         "GET", {}, true, [cb](const net::HttpResponse& r) {
           first_of_array_at<models::Order>(r, {"data"}, parse_order, cb);
         });
  }

  void get_positions(const std::string&, PositionsCallback cb) override {
    send("/api/v5/account/positions?instType=SWAP", "GET", {}, true,
         [cb](const net::HttpResponse& r) {
           parse_array_at<models::Position>(r, {"data"}, parse_position, cb);
         });
  }

  void get_user_trades(const std::string&, std::int64_t start_ms,
                       std::int64_t end_ms, FillsCallback cb) override {
    send("/api/v5/trade/fills?instType=SWAP&begin=" + std::to_string(start_ms) +
             "&end=" + std::to_string(end_ms) + "&limit=100",
         "GET", {}, true, [cb](const net::HttpResponse& r) {
           parse_array_at<models::Fill>(r, {"data"}, parse_fill, cb);
         });
  }

 private:
  static std::optional<models::Order> parse_order(venue::Object& d);
  static std::optional<models::Fill> parse_fill(venue::Object& d);
  static std::optional<models::Position> parse_position(venue::Object& d);

  void send(const std::string& path, const std::string& method,
            const std::string& body, bool authenticated,
            std::function<void(const net::HttpResponse&)> handler) {
    net::HttpRequest req;
    req.method = method;
    req.host = std::string(venue::okx::kProductionRestHost);
    req.path = path;
    req.body = body;
    if (authenticated) {
      // The SAME timestamp string must go in the header and into the
      // signature. Generating it twice risks a mismatch across a millisecond
      // boundary, which fails with an unhelpful error.
      const std::string timestamp =
          core::Timestamp::from_millis(now_ms()).to_iso8601() + "Z";
      const std::string pre = timestamp + method + path + body;
      req.headers.push_back(
          {"OK-ACCESS-SIGN",
           net::base64_encode(net::hmac_sha256(exchange_.api_secret, pre).data(),
                              32)});
      req.headers.push_back({"OK-ACCESS-KEY", exchange_.api_key});
      req.headers.push_back({"OK-ACCESS-TIMESTAMP", timestamp});
      req.headers.push_back({"OK-ACCESS-PASSPHRASE", exchange_.passphrase});
      if (exchange_.is_testnet()) {
        req.headers.push_back({"x-simulated-trading", "1"});
      }
    }
    if (!body.empty()) {
      req.headers.push_back({"Content-Type", "application/json"});
    }
    http_->submit(std::move(req), std::move(handler));
  }

  ExchangeConfig exchange_;
  net::HttpClient* http_;
};

std::optional<models::Order> OkxRestImpl::parse_order(venue::Object& d) {
  models::Order o;
  o.exchange = "okx";
  const auto id = d["ordId"].as_string();
  const auto inst = d["instId"].as_string();
  if (!id.has_value() || !inst.has_value()) {
    return std::nullopt;
  }
  o.order_id = std::string(*id);
  o.instrument = std::string(*inst);
  o.side = d["side"].as_string().value_or("buy") == "buy" ? models::OrderSide::kBuy
                                                          : models::OrderSide::kSell;
  o.order_type = venue::okx::map_order_type(d["ordType"].as_string().value_or("limit"));
  o.amount = d["sz"].as_decimal().value_or(core::Qty{});
  o.filled_amount = d["accFillSz"].as_decimal().value_or(core::Qty{});
  o.price = d["px"].as_decimal();
  o.average_price = d["avgPx"].as_decimal();
  o.status = venue::okx::map_order_status(d["state"].as_string().value_or("live")).status;
  const auto created = d["cTime"].as_int();
  const auto updated = d["uTime"].as_int();
  o.created_at = core::Timestamp::from_millis(created.value_or(0));
  o.updated_at = core::Timestamp::from_millis(updated.value_or(created.value_or(0)));
  // The client order id we sent: our internal_order_id, or the strategy's
  // label. It is what lets a snapshot resolve an order whose placement reply
  // was lost.
  if (const auto cid = d["clOrdId"].as_string(); cid.has_value() && !cid->empty()) {
    o.internal_order_id = std::string(*cid);
  }
  return o;
}

std::optional<models::Fill> OkxRestImpl::parse_fill(venue::Object& d) {
  models::Fill f;
  f.exchange = "okx";
  auto trade_id = d["tradeId"].as_string();
  if (!trade_id.has_value() || trade_id->empty()) {
    trade_id = d["billId"].as_string();
  }
  const auto order_id = d["ordId"].as_string();
  const auto inst = d["instId"].as_string();
  if (!trade_id.has_value() || !order_id.has_value() || !inst.has_value()) {
    return std::nullopt;
  }
  f.trade_id = std::string(*trade_id);
  f.order_id = std::string(*order_id);
  f.instrument = std::string(*inst);
  f.side = d["side"].as_string().value_or("buy") == "buy" ? models::OrderSide::kBuy
                                                          : models::OrderSide::kSell;
  f.amount = d["fillSz"].as_decimal().value_or(core::Qty{});
  f.price = d["fillPx"].as_decimal().value_or(core::Price{});
  // OKX reports a fee paid as negative; normalise to "cost" like the feed does.
  f.fee = d["fee"].as_decimal().value_or(core::Price{}).abs();
  f.fee_currency = std::string(d["feeCcy"].as_string().value_or("USDT"));
  f.liquidity = d["execType"].as_string().value_or("T") == "M"
                    ? models::Liquidity::kMaker
                    : models::Liquidity::kTaker;
  f.timestamp = core::Timestamp::from_millis(d["ts"].as_int().value_or(0));
  return f;
}

std::optional<models::Position> OkxRestImpl::parse_position(venue::Object& d) {
  models::Position p;
  p.exchange = "okx";
  const auto inst = d["instId"].as_string();
  if (!inst.has_value()) {
    return std::nullopt;
  }
  p.instrument = std::string(*inst);
  p.kind = "swap";
  const auto pos = d["pos"].as_decimal().value_or(core::Qty{});
  p.size = pos.abs();
  p.direction = pos.raw() > 0 ? "buy" : (pos.raw() < 0 ? "sell" : "zero");
  p.average_price = d["avgPx"].as_decimal().value_or(core::Price{});
  p.mark_price = d["markPx"].as_decimal().value_or(core::Price{});
  p.floating_profit_loss = d["upl"].as_decimal().value_or(core::Price{});
  p.total_profit_loss = p.floating_profit_loss;
  p.timestamp = core::Timestamp::now();
  return p;
}

std::unique_ptr<VenueSession> make_session(const ExchangeConfig& exchange,
                                           const WebSocketConfig& ws_config,
                                           const RuntimeConfig& runtime,
                                           const net::TlsContext* tls, VenueRest*,
                                           VenueSessionHandlers handlers) {
  return std::make_unique<OkxSession>(exchange, ws_config, runtime, tls,
                                                  std::move(handlers));
}

std::unique_ptr<VenueRest> make_rest(const ExchangeConfig& exchange, net::HttpClient* http) {
  return std::make_unique<OkxRestImpl>(exchange, http);
}

}  // namespace

const OmsVenue& venue() {
  static const OmsVenue kVenue{
      "okx",
      make_session,
      nullptr,  // no order-entry session: orders ride the feed session
      make_rest,
  };
  return kVenue;
}

}  // namespace axon::oms::okx
