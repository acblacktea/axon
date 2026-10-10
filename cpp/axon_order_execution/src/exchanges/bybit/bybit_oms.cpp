// Bybit: everything the OMS needs from this venue.
//
// Bybit V5 linear perpetuals: the private stream for the feed, and a second
// connection to /v5/trade -- with its own op:"auth" -- for order entry.
//
// Moved out of venue_session.cpp and venue_rest.cpp so each venue's
// protocol lives in one place, as it does on the EMS side. Only venue() is
// visible outside this file.

#include "axon/exchanges/bybit/bybit_oms.h"

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
#include "axon/exchanges/bybit/bybit_builder.h"
#include "axon/exchanges/bybit/bybit_parser.h"
#include "axon/venue/json_view.h"

namespace axon::oms::bybit {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.bybit");
  return logger;
}

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
// Bybit
// ===========================================================================
class BybitSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::bybit::kTestnetHost
                                              : venue::bybit::kProductionHost);
  }
  std::string path() const override { return std::string(venue::bybit::kPath); }

  bool send_authentication() override {
    char buf[venue::bybit::kMaxRequestBytes];
    // Expiry a few seconds out. Bybit rejects a signature whose expiry has
    // passed, and clock skew is the usual cause of a login that works locally
    // and fails in production.
    const std::int64_t expires = core::wall_clock_ns() / 1'000'000LL + 10'000LL;
    const std::size_t n =
        builder_.ws_auth(buf, sizeof(buf), exchange_.api_key,
                         exchange_.api_secret, expires, auth_id_);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool send_subscriptions() override {
    const std::string_view topics[] = {venue::bybit::kOrderTopic,
                                       venue::bybit::kExecutionTopic,
                                       venue::bybit::kWalletTopic};
    char buf[venue::bybit::kMaxRequestBytes];
    std::int64_t id = 0;
    const std::size_t n = builder_.ws_subscribe(buf, sizeof(buf), topics, 3, id);
    subscribe_id_ = id;
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    Sink sink{this};
    const auto result = parser_.parse(reinterpret_cast<const char*>(data), len,
                                      capacity, sink);
    if (result.kind == venue::VenueMessageKind::kSessionEvent) {
      // Bybit acknowledges auth and subscribe with the same shape; the req_id
      // tells them apart.
      if (result.rpc_id == auth_id_) {
        on_auth_reply(true, {});
      } else if (result.rpc_id == subscribe_id_) {
        on_subscribe_reply(true, {});
      }
    } else if (result.kind == venue::VenueMessageKind::kRpcError) {
      const std::string text(result.rpc_error_text);
      if (result.rpc_id == auth_id_) {
        on_auth_reply(false, text);
      } else {
        on_subscribe_reply(false, text);
      }
    }
    return true;
  }

 private:
  struct Sink {
    BybitSession* self;
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

  venue::bybit::BybitParser parser_;
  venue::bybit::BybitBuilder builder_;
  std::int64_t auth_id_ = -1;
  std::int64_t subscribe_id_ = -1;
};

// ===========================================================================
// Trade sessions -- the second connection, for WebSocket order entry.
//
// Structurally these are the same state machine with two of its stages
// hollowed out: there is nothing to subscribe to, and for Binance nothing to
// log in with either. What they add is a keepalive, because a connection that
// only carries order-entry replies is SILENT whenever nobody is trading, and
// the base class would read that silence as a dead socket and reconnect every
// few seconds.
//
// Everything inbound is a reply to something we sent, so handle_frame extracts
// the correlation id and hands the raw bytes to on_rpc_reply. The EMS knows
// each venue's result shape; the session does not and should not.
// ===========================================================================

// ---------------------------------------------------------------------------
// Bybit /v5/trade
// ---------------------------------------------------------------------------
class BybitTradeSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::bybit::kTestnetHost
                                              : venue::bybit::kProductionHost);
  }
  std::string path() const override {
    return std::string(venue::bybit::kTradePath);
  }

  bool send_authentication() override {
    char buf[venue::bybit::kMaxRequestBytes];
    const std::int64_t expires = core::wall_clock_ns() / 1'000'000LL + 10'000LL;
    const std::size_t n =
        builder_.ws_auth(buf, sizeof(buf), exchange_.api_key,
                         exchange_.api_secret, expires, auth_id_);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  // Nothing to subscribe to: this connection carries only replies to requests
  // we send.
  bool send_subscriptions() override {
    note_subscribed();
    return true;
  }

  void on_poll() override {
    if (!live()) {
      return;
    }
    const double now =
        static_cast<double>(core::monotonic_ns()) / 1e9;
    if (now - last_ping_ < ws_config_.heartbeat_interval_seconds) {
      return;
    }
    last_ping_ = now;
    // The venue's reply is what actually proves the socket is alive; sending
    // alone would not, which is why the watchdog measures inbound silence.
    char buf[64];
    const std::size_t n = venue::bybit::BybitBuilder::ws_ping(buf, sizeof(buf));
    if (n > 0) {
      send_raw(std::string_view(buf, n));
    }
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    const std::string_view frame(reinterpret_cast<const char*>(data), len);
    auto root = doc_.parse(frame.data(), len, capacity);
    if (!root.has_value()) {
      AXON_LOG_WARN(log(), "[{}] unparseable trade reply", exchange_.name);
      return true;
    }

    // Fields in DOCUMENT ORDER -- reqId, retCode, retMsg, op -- so On-Demand
    // walks forward once instead of rescanning per field.
    const auto id = parse_string_id((*root)["reqId"]);
    const auto ret_code = (*root)["retCode"].as_int().value_or(-1);
    const auto ret_msg = (*root)["retMsg"].as_string().value_or("");
    const auto op = (*root)["op"].as_string().value_or("");
    const bool ok = ret_code == 0;

    if (op == "pong" || op == "ping") {
      return true;
    }
    if (op == "auth") {
      on_auth_reply(ok, std::string(ret_msg));
      return true;
    }
    if (!id.has_value()) {
      return true;
    }
    if (handlers_.on_rpc_reply) {
      handlers_.on_rpc_reply(*id, ok, frame, ok ? std::string_view{} : ret_msg);
    }
    return true;
  }

 private:
  venue::Document doc_{64 * 1024};
  venue::bybit::BybitBuilder builder_;
  std::int64_t auth_id_ = -1;
  double last_ping_ = 0.0;
};

// ===========================================================================
// REST snapshots: reconciliation, positions, tickers
// ===========================================================================
// ===========================================================================
// Bybit v5
// ===========================================================================
class BybitRestImpl final : public VenueRest {
 public:
  BybitRestImpl(ExchangeConfig exchange, net::HttpClient* http)
      : exchange_(std::move(exchange)), http_(http) {}

  const std::string& exchange_name() const override { return exchange_.name; }

  void get_ticker(const std::string& instrument, TickerCallback cb) override {
    send("/v5/market/tickers?category=linear&symbol=" + url_encode(instrument), "",
         false, "GET", {}, [cb, instrument](const net::HttpResponse& r) {
           if (!r.error.empty() || !r.ok()) {
             cb(std::nullopt, http_error(r));
             return;
           }
           auto root = doc().parse_copy(r.body);
           auto result = root.has_value() ? (*root)["result"].as_object() : std::nullopt;
           auto list = result.has_value() ? (*result)["list"].as_array() : std::nullopt;
           if (!list.has_value()) {
             cb(std::nullopt, "no ticker list");
             return;
           }
           std::optional<models::Ticker> ticker;
           list->for_each_object([&](venue::Object& d) {
             if (ticker.has_value()) {
               return;
             }
             models::Ticker t;
             t.instrument = instrument;
             t.best_bid_price = d["bid1Price"].as_decimal().value_or(core::Price{});
             t.best_bid_amount = d["bid1Size"].as_decimal().value_or(core::Qty{});
             t.best_ask_price = d["ask1Price"].as_decimal().value_or(core::Price{});
             t.best_ask_amount = d["ask1Size"].as_decimal().value_or(core::Qty{});
             t.last_price = d["lastPrice"].as_decimal();
             t.mark_price = d["markPrice"].as_decimal();
             ticker = t;
           });
           if (!ticker.has_value()) {
             cb(std::nullopt, "ticker not found");
             return;
           }
           cb(ticker, {});
         });
  }

  void get_open_orders(const std::string& currency, OrdersCallback cb) override {
    const std::string query =
        "category=linear&settleCoin=" + url_encode(currency.empty() ? "USDT" : currency);
    send("/v5/order/realtime?" + query, query, true, "GET", {},
         [cb](const net::HttpResponse& r) {
           parse_array_at<models::Order>(r, {"result", "list"}, parse_order, cb);
         });
  }

  void get_order(const models::Order& order, OrderCallback cb) override {
    // /realtime also returns recently CLOSED orders when asked by id, which is
    // the case this exists for -- the same endpoint the Python get_order uses.
    const std::string query = "category=linear&symbol=" + url_encode(order.instrument) +
                              "&orderId=" + url_encode(order.order_id);
    send("/v5/order/realtime?" + query, query, true, "GET", {},
         [cb](const net::HttpResponse& r) {
           first_of_array_at<models::Order>(r, {"result", "list"}, parse_order, cb);
         });
  }

  void get_positions(const std::string& currency, PositionsCallback cb) override {
    const std::string query =
        "category=linear&settleCoin=" + url_encode(currency.empty() ? "USDT" : currency);
    send("/v5/position/list?" + query, query, true, "GET", {},
         [cb](const net::HttpResponse& r) {
           parse_array_at<models::Position>(r, {"result", "list"}, parse_position, cb);
         });
  }

  void get_user_trades(const std::string& currency, std::int64_t start_ms,
                       std::int64_t end_ms, FillsCallback cb) override {
    const std::string query =
        "category=linear&startTime=" + std::to_string(start_ms) +
        "&endTime=" + std::to_string(end_ms) + "&limit=100";
    static_cast<void>(currency);
    send("/v5/execution/list?" + query, query, true, "GET", {},
         [cb](const net::HttpResponse& r) {
           parse_array_at<models::Fill>(r, {"result", "list"}, parse_fill, cb);
         });
  }

 private:
  static std::optional<models::Order> parse_order(venue::Object& d);
  static std::optional<models::Fill> parse_fill(venue::Object& d);
  static std::optional<models::Position> parse_position(venue::Object& d);

  // `signed_payload` is the query string for a GET and the body for a POST --
  // Bybit signs whichever the request carries, and mixing them up produces a
  // signature error that says nothing useful.
  void send(const std::string& path, const std::string& signed_payload,
            bool authenticated, const std::string& method, const std::string& body,
            std::function<void(const net::HttpResponse&)> handler) {
    net::HttpRequest req;
    req.method = method;
    req.host = std::string(exchange_.is_testnet() ? venue::bybit::kTestnetRestHost
                                                  : venue::bybit::kProductionRestHost);
    req.path = path;
    req.body = body;
    if (authenticated) {
      const std::int64_t ts = now_ms();
      constexpr int kWindow = 5000;
      const std::string pre = std::to_string(ts) + exchange_.api_key +
                              std::to_string(kWindow) + signed_payload;
      req.headers.push_back(
          {"X-BAPI-SIGN", net::to_hex(net::hmac_sha256(exchange_.api_secret, pre))});
      req.headers.push_back({"X-BAPI-API-KEY", exchange_.api_key});
      req.headers.push_back({"X-BAPI-TIMESTAMP", std::to_string(ts)});
      req.headers.push_back({"X-BAPI-RECV-WINDOW", std::to_string(kWindow)});
    }
    if (!body.empty()) {
      req.headers.push_back({"Content-Type", "application/json"});
    }
    http_->submit(std::move(req), std::move(handler));
  }

  ExchangeConfig exchange_;
  net::HttpClient* http_;
};

std::optional<models::Order> BybitRestImpl::parse_order(venue::Object& d) {
  models::Order o;
  o.exchange = "bybit";
  const auto id = d["orderId"].as_string();
  const auto symbol = d["symbol"].as_string();
  if (!id.has_value() || !symbol.has_value()) {
    return std::nullopt;
  }
  o.order_id = std::string(*id);
  o.instrument = std::string(*symbol);
  o.side = d["side"].as_string().value_or("Buy") == "Buy" ? models::OrderSide::kBuy
                                                          : models::OrderSide::kSell;
  o.order_type = d["orderType"].as_string().value_or("Limit") == "Limit"
                     ? models::OrderType::kLimit
                     : models::OrderType::kMarket;
  o.amount = d["qty"].as_decimal().value_or(core::Qty{});
  o.filled_amount = d["cumExecQty"].as_decimal().value_or(core::Qty{});
  const auto price = d["price"].as_decimal();
  if (price.has_value() && !price->is_zero()) {
    o.price = price;
  }
  o.status =
      venue::bybit::map_order_status(d["orderStatus"].as_string().value_or("New")).status;
  const auto created = d["createdTime"].as_int();
  const auto updated = d["updatedTime"].as_int();
  o.created_at = core::Timestamp::from_millis(created.value_or(0));
  o.updated_at = core::Timestamp::from_millis(updated.value_or(created.value_or(0)));
  // The client order id we sent: our internal_order_id, or the strategy's
  // label. It is what lets a snapshot resolve an order whose placement reply
  // was lost.
  if (const auto cid = d["orderLinkId"].as_string(); cid.has_value() && !cid->empty()) {
    o.internal_order_id = std::string(*cid);
  }
  return o;
}

std::optional<models::Fill> BybitRestImpl::parse_fill(venue::Object& d) {
  models::Fill f;
  f.exchange = "bybit";
  const auto id = d["execId"].as_string();
  const auto order_id = d["orderId"].as_string();
  const auto symbol = d["symbol"].as_string();
  if (!id.has_value() || !order_id.has_value() || !symbol.has_value()) {
    return std::nullopt;
  }
  f.trade_id = std::string(*id);
  f.order_id = std::string(*order_id);
  f.instrument = std::string(*symbol);
  f.side = d["side"].as_string().value_or("Buy") == "Buy" ? models::OrderSide::kBuy
                                                          : models::OrderSide::kSell;
  f.amount = d["execQty"].as_decimal().value_or(core::Qty{});
  f.price = d["execPrice"].as_decimal().value_or(core::Price{});
  f.fee = d["execFee"].as_decimal().value_or(core::Price{});
  f.fee_currency = std::string(d["feeCurrency"].as_string().value_or("USDT"));
  bool maker = false;
  if (auto b = d["isMaker"].as_bool(); b.has_value()) {
    maker = *b;
  } else if (auto s = d["isMaker"].as_string(); s.has_value()) {
    maker = (*s == "true");
  }
  f.liquidity = maker ? models::Liquidity::kMaker : models::Liquidity::kTaker;
  f.timestamp = core::Timestamp::from_millis(d["execTime"].as_int().value_or(0));
  return f;
}

std::optional<models::Position> BybitRestImpl::parse_position(venue::Object& d) {
  models::Position p;
  p.exchange = "bybit";
  const auto symbol = d["symbol"].as_string();
  if (!symbol.has_value()) {
    return std::nullopt;
  }
  p.instrument = std::string(*symbol);
  p.kind = "future";
  p.size = d["size"].as_decimal().value_or(core::Qty{});
  const auto side = d["side"].as_string().value_or("None");
  p.direction = side == "Buy" ? "buy" : (side == "Sell" ? "sell" : "zero");
  p.average_price = d["avgPrice"].as_decimal().value_or(core::Price{});
  p.mark_price = d["markPrice"].as_decimal().value_or(core::Price{});
  p.floating_profit_loss = d["unrealisedPnl"].as_decimal().value_or(core::Price{});
  p.realized_profit_loss = d["curRealisedPnl"].as_decimal().value_or(core::Price{});
  p.total_profit_loss = p.floating_profit_loss + p.realized_profit_loss;
  p.timestamp = core::Timestamp::now();
  return p;
}

std::unique_ptr<VenueSession> make_session(const ExchangeConfig& exchange,
                                           const WebSocketConfig& ws_config,
                                           const RuntimeConfig& runtime,
                                           const net::TlsContext* tls, VenueRest*,
                                           VenueSessionHandlers handlers) {
  return std::make_unique<BybitSession>(exchange, ws_config, runtime, tls,
                                                  std::move(handlers));
}

std::unique_ptr<VenueSession> make_trade_session(const ExchangeConfig& exchange,
                                                 const WebSocketConfig& ws_config,
                                                 const RuntimeConfig& runtime,
                                                 const net::TlsContext* tls,
                                                 VenueSessionHandlers handlers) {
  return std::make_unique<BybitTradeSession>(exchange, ws_config, runtime, tls,
                                                  std::move(handlers));
}

std::unique_ptr<VenueRest> make_rest(const ExchangeConfig& exchange, net::HttpClient* http) {
  return std::make_unique<BybitRestImpl>(exchange, http);
}

}  // namespace

const OmsVenue& venue() {
  static const OmsVenue kVenue{
      "bybit",
      make_session,
      make_trade_session,
      make_rest,
  };
  return kVenue;
}

}  // namespace axon::oms::bybit
