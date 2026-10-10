// Binance: everything the OMS needs from this venue.
//
// Binance USDT-M perpetuals: a user data stream keyed by a REST listenKey for
// the feed, and a second connection to ws-fapi for order entry, where every
// request carries its own signature.
//
// Moved out of venue_session.cpp and venue_rest.cpp so each venue's
// protocol lives in one place, as it does on the EMS side. Only venue() is
// visible outside this file.

#include "axon/exchanges/binance/binance_oms.h"

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
#include "axon/exchanges/binance/binance_builder.h"
#include "axon/exchanges/binance/binance_parser.h"
#include "axon/venue/json_view.h"

namespace axon::oms::binance {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.binance");
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
// Binance USDT-M
//
// The odd one: no WebSocket authentication at all. The stream is identified by
// a listenKey fetched over REST and appended to the URL, and that key EXPIRES
// after 60 minutes unless renewed. Renewal is a REST PUT every 30 minutes; if
// it lapses the venue sends listenKeyExpired and stops delivering, which the
// parser reports as kSessionExpired.
// ===========================================================================
class BinanceSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

  void set_rest(VenueRest* rest) { rest_ = rest; }

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::binance::kTestnetWsHost
                                              : venue::binance::kProductionWsHost);
  }
  // The key is part of the path, so this is only valid once we have one.
  std::string path() const override { return "/ws/" + listen_key_; }

  bool prepare_connect() override {
    if (!listen_key_.empty()) {
      return true;
    }
    if (rest_ == nullptr) {
      fail_session("binance needs a REST client for its listenKey", /*fatal=*/true);
      return false;
    }
    if (listen_key_pending_) {
      return false;  // already asked; wait for the reply
    }
    listen_key_pending_ = true;
    rest_->create_listen_key([this](std::string key, const std::string& error) {
      listen_key_pending_ = false;
      if (!error.empty() || key.empty()) {
        // A bad API key fails here rather than at a WebSocket auth step,
        // because there is no WebSocket auth step.
        fail_session("could not obtain a listenKey: " + error);
        return;
      }
      listen_key_ = std::move(key);
      last_keepalive_ = monotonic_seconds();
      AXON_LOG_INFO(log(), "[binance] listenKey obtained ({}...)",
                      listen_key_.substr(0, 8));
    });
    return false;
  }

  void on_poll() override {
    if (listen_key_.empty() || rest_ == nullptr) {
      return;
    }
    // Renew at 30 minutes against a 60-minute expiry. Halving the interval is
    // deliberate: one missed renewal must not cost the stream.
    constexpr double kRenewInterval = 30.0 * 60.0;
    if (monotonic_seconds() - last_keepalive_ < kRenewInterval) {
      return;
    }
    last_keepalive_ = monotonic_seconds();
    rest_->keepalive_listen_key(listen_key_, [](const std::string&,
                                                const std::string& error) {
      if (!error.empty()) {
        AXON_LOG_WARN(log(), "[binance] listenKey keepalive failed: {}", error);
      }
    });
  }

  bool send_authentication() override {
    // Nothing to authenticate: possessing the listenKey IS the authentication.
    note_authenticated();
    return true;
  }

  bool send_subscriptions() override {
    // Nothing to subscribe to either -- ORDER_TRADE_UPDATE and ACCOUNT_UPDATE
    // arrive unbidden on a user data stream.
    note_subscribed();
    return true;
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    Sink sink{this};
    const auto result = parser_.parse(reinterpret_cast<const char*>(data), len,
                                      capacity, sink);
    if (result.kind == venue::VenueMessageKind::kSessionExpired) {
      AXON_LOG_WARN(log(), "[binance] listenKey expired; re-establishing");
      listen_key_.clear();  // force a fresh key on the next connect
      fail_session("listenKey expired");
      return false;
    }
    return true;
  }

 private:
  struct Sink {
    BinanceSession* self;
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

  venue::binance::BinanceParser parser_;
  VenueRest* rest_ = nullptr;
  std::string listen_key_;
  bool listen_key_pending_ = false;
  double last_keepalive_ = 0.0;
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
// Binance ws-fapi
//
// The odd one again: NO session login. Every request carries its own apiKey
// and signature, so the connection is usable the moment it opens.
// ---------------------------------------------------------------------------
class BinanceTradeSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet()
                           ? venue::binance::kWsApiTestnetHost
                           : venue::binance::kWsApiProductionHost);
  }
  std::string path() const override {
    return std::string(venue::binance::kWsApiPath);
  }

  // Nothing to authenticate. Each request signs itself, so the session is
  // ready as soon as the socket is open.
  bool send_authentication() override {
    note_authenticated();
    return true;
  }

  bool send_subscriptions() override {
    note_subscribed();
    return true;
  }

  void on_poll() override {
    if (!live()) {
      return;
    }
    const double now = static_cast<double>(core::monotonic_ns()) / 1e9;
    if (now - last_ping_ < ws_config_.heartbeat_interval_seconds) {
      return;
    }
    last_ping_ = now;
    // Binance's WebSocket API answers an unsigned `ping` method, which keeps
    // the watchdog satisfied without spending a signature. Id 0 is reserved
    // for it and is never handed out by the builder, whose ids start high.
    send_raw(R"({"id":"0","method":"ping"})");
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    const std::string_view frame(reinterpret_cast<const char*>(data), len);
    auto root = doc_.parse(frame.data(), len, capacity);
    if (!root.has_value()) {
      AXON_LOG_WARN(log(), "[{}] unparseable trade reply", exchange_.name);
      return true;
    }

    const auto id = parse_string_id((*root)["id"]);
    if (!id.has_value()) {
      return true;  // the keepalive reply, or something we did not send
    }
    if (*id == 0) {
      return true;  // our own ping
    }

    const auto status = (*root)["status"].as_int().value_or(0);
    const bool ok = status == 200;
    std::string_view error;
    if (!ok) {
      if (auto err = (*root)["error"].as_object(); err.has_value()) {
        error = (*err)["msg"].as_string().value_or("");
      }
    }
    if (handlers_.on_rpc_reply) {
      handlers_.on_rpc_reply(*id, ok, frame, error);
    }
    return true;
  }

 private:
  venue::Document doc_{64 * 1024};
  double last_ping_ = 0.0;
};

// ===========================================================================
// REST snapshots: reconciliation, positions, tickers
// ===========================================================================
// ===========================================================================
// Binance USDT-M
// ===========================================================================
class BinanceRestImpl final : public VenueRest {
 public:
  BinanceRestImpl(ExchangeConfig exchange, net::HttpClient* http)
      : exchange_(std::move(exchange)), http_(http) {}

  const std::string& exchange_name() const override { return exchange_.name; }

  void get_ticker(const std::string& instrument, TickerCallback cb) override {
    send("/fapi/v1/ticker/bookTicker?symbol=" + url_encode(instrument), false, "GET",
         {}, [cb, instrument](const net::HttpResponse& r) {
           if (!r.error.empty() || !r.ok()) {
             cb(std::nullopt, http_error(r));
             return;
           }
           auto root = doc().parse_copy(r.body);
           if (!root.has_value()) {
             cb(std::nullopt, "malformed ticker");
             return;
           }
           models::Ticker t;
           t.instrument = instrument;
           t.best_bid_price = (*root)["bidPrice"].as_decimal().value_or(core::Price{});
           t.best_bid_amount = (*root)["bidQty"].as_decimal().value_or(core::Qty{});
           t.best_ask_price = (*root)["askPrice"].as_decimal().value_or(core::Price{});
           t.best_ask_amount = (*root)["askQty"].as_decimal().value_or(core::Qty{});
           cb(t, {});
         });
  }

  void get_open_orders(const std::string&, OrdersCallback cb) override {
    // Binance is symbol-scoped, not currency-scoped: omitting the symbol
    // returns every open order, which is what a backstop wants.
    send(signed_path("/fapi/v1/openOrders", ""), true, "GET", {},
         [cb](const net::HttpResponse& r) {
           parse_array_at<models::Order>(r, {}, parse_order, cb);
         });
  }

  void get_order(const models::Order& order, OrderCallback cb) override {
    send(signed_path("/fapi/v1/order", "symbol=" + url_encode(order.instrument) +
                                           "&orderId=" + url_encode(order.order_id)),
         true, "GET", {}, [cb](const net::HttpResponse& r) {
           parse_object_at<models::Order>(r, {}, parse_order, cb);
         });
  }

  void get_positions(const std::string&, PositionsCallback cb) override {
    send(signed_path("/fapi/v2/positionRisk", ""), true, "GET", {},
         [cb](const net::HttpResponse& r) {
           parse_array_at<models::Position>(r, {}, parse_position, cb);
         });
  }

  void get_user_trades(const std::string& currency, std::int64_t start_ms,
                       std::int64_t end_ms, FillsCallback cb) override {
    // userTrades REQUIRES a symbol, so `currency` is used as one here rather
    // than as an asset. A caller passing "BTC" gets nothing; it must pass
    // "BTCUSDT". Documented rather than papered over, because guessing the
    // quote asset would be wrong for half the book.
    send(signed_path("/fapi/v1/userTrades",
                     "symbol=" + url_encode(currency) +
                         "&startTime=" + std::to_string(start_ms) +
                         "&endTime=" + std::to_string(end_ms) + "&limit=1000"),
         true, "GET", {}, [cb](const net::HttpResponse& r) {
           parse_array_at<models::Fill>(r, {}, parse_fill, cb);
         });
  }

  void create_listen_key(StringCallback cb) override {
    // Creating a listen key needs the API key header but NO signature -- one of
    // the few Binance endpoints that works that way.
    send("/fapi/v1/listenKey", true, "POST", {}, [cb](const net::HttpResponse& r) {
      if (!r.ok()) {
        cb({}, http_error(r));
        return;
      }
      auto root = doc().parse_copy(r.body);
      const auto key = root.has_value() ? (*root)["listenKey"].as_string()
                                        : std::nullopt;
      if (!key.has_value()) {
        cb({}, "listenKey missing from the response");
        return;
      }
      cb(std::string(*key), {});
    });
  }

  void keepalive_listen_key(const std::string&, StringCallback cb) override {
    send("/fapi/v1/listenKey", true, "PUT", {}, [cb](const net::HttpResponse& r) {
      cb(r.body, r.ok() ? std::string{} : http_error(r));
    });
  }

 private:
  static std::optional<models::Order> parse_order(venue::Object& d);
  static std::optional<models::Fill> parse_fill(venue::Object& d);
  static std::optional<models::Position> parse_position(venue::Object& d);

  // Appends timestamp+recvWindow, signs the whole query, returns path?query.
  std::string signed_path(const std::string& path, const std::string& query) const {
    std::string q = query;
    if (!q.empty()) {
      q += "&";
    }
    q += "recvWindow=5000&timestamp=" + std::to_string(now_ms());
    const std::string signature =
        net::to_hex(net::hmac_sha256(exchange_.api_secret, q));
    return path + "?" + q + "&signature=" + signature;
  }

  void send(const std::string& path, bool authenticated, const std::string& method,
            const std::string& body,
            std::function<void(const net::HttpResponse&)> handler) {
    net::HttpRequest req;
    req.method = method;
    req.host = std::string(exchange_.is_testnet()
                               ? venue::binance::kTestnetRestHost
                               : venue::binance::kProductionRestHost);
    req.path = path;
    req.body = body;
    if (authenticated) {
      req.headers.push_back({"X-MBX-APIKEY", exchange_.api_key});
    }
    http_->submit(std::move(req), std::move(handler));
  }

  ExchangeConfig exchange_;
  net::HttpClient* http_;
};

std::optional<models::Order> BinanceRestImpl::parse_order(venue::Object& d) {
  models::Order o;
  o.exchange = "binance";
  // REST spells these out, unlike the single-letter stream payload.
  const auto symbol = d["symbol"].as_string();
  const auto id = d["orderId"].as_int();
  if (!symbol.has_value() || !id.has_value()) {
    return std::nullopt;
  }
  o.instrument = std::string(*symbol);
  o.order_id = std::to_string(*id);
  o.side = d["side"].as_string().value_or("BUY") == "BUY" ? models::OrderSide::kBuy
                                                          : models::OrderSide::kSell;
  o.order_type = d["type"].as_string().value_or("LIMIT") == "LIMIT"
                     ? models::OrderType::kLimit
                     : models::OrderType::kMarket;
  o.amount = d["origQty"].as_decimal().value_or(core::Qty{});
  o.filled_amount = d["executedQty"].as_decimal().value_or(core::Qty{});
  const auto price = d["price"].as_decimal();
  if (price.has_value() && !price->is_zero()) {
    o.price = price;
  }
  const auto mapping =
      venue::binance::map_order_status(d["status"].as_string().value_or("NEW"));
  o.status = mapping.status;
  const auto updated = d["updateTime"].as_int();
  const auto created = d["time"].as_int();
  o.created_at = core::Timestamp::from_millis(created.value_or(0));
  o.updated_at = core::Timestamp::from_millis(updated.value_or(created.value_or(0)));
  // The client order id we sent: our internal_order_id, or the strategy's
  // label. It is what lets a snapshot resolve an order whose placement reply
  // was lost.
  if (const auto cid = d["clientOrderId"].as_string(); cid.has_value() && !cid->empty()) {
    o.internal_order_id = std::string(*cid);
  }
  return o;
}

std::optional<models::Fill> BinanceRestImpl::parse_fill(venue::Object& d) {
  models::Fill f;
  f.exchange = "binance";
  const auto id = d["id"].as_int();
  const auto order_id = d["orderId"].as_int();
  const auto symbol = d["symbol"].as_string();
  if (!id.has_value() || !order_id.has_value() || !symbol.has_value()) {
    return std::nullopt;
  }
  f.trade_id = std::to_string(*id);
  f.order_id = std::to_string(*order_id);
  f.instrument = std::string(*symbol);
  f.side = d["side"].as_string().value_or("BUY") == "BUY" ? models::OrderSide::kBuy
                                                          : models::OrderSide::kSell;
  f.amount = d["qty"].as_decimal().value_or(core::Qty{});
  f.price = d["price"].as_decimal().value_or(core::Price{});
  f.fee = d["commission"].as_decimal().value_or(core::Price{});
  f.fee_currency = std::string(d["commissionAsset"].as_string().value_or("USDT"));
  f.liquidity = d["maker"].as_bool().value_or(false) ? models::Liquidity::kMaker
                                                     : models::Liquidity::kTaker;
  f.timestamp = core::Timestamp::from_millis(d["time"].as_int().value_or(0));
  return f;
}

std::optional<models::Position> BinanceRestImpl::parse_position(venue::Object& d) {
  models::Position p;
  p.exchange = "binance";
  const auto symbol = d["symbol"].as_string();
  if (!symbol.has_value()) {
    return std::nullopt;
  }
  p.instrument = std::string(*symbol);
  p.kind = "future";
  const auto amt = d["positionAmt"].as_decimal().value_or(core::Qty{});
  p.size = amt.abs();
  p.direction = amt.raw() > 0 ? "buy" : (amt.raw() < 0 ? "sell" : "zero");
  p.average_price = d["entryPrice"].as_decimal().value_or(core::Price{});
  p.mark_price = d["markPrice"].as_decimal().value_or(core::Price{});
  p.floating_profit_loss = d["unRealizedProfit"].as_decimal().value_or(core::Price{});
  p.total_profit_loss = p.floating_profit_loss;
  p.timestamp = core::Timestamp::now();
  return p;
}

std::unique_ptr<VenueSession> make_session(const ExchangeConfig& exchange,
                                           const WebSocketConfig& ws_config,
                                           const RuntimeConfig& runtime,
                                           const net::TlsContext* tls, VenueRest* rest,
                                           VenueSessionHandlers handlers) {
  // The user data stream is keyed by a listenKey obtained over REST.
  auto session = std::make_unique<BinanceSession>(exchange, ws_config, runtime, tls,
                                                  std::move(handlers));
  session->set_rest(rest);
  return session;
}

std::unique_ptr<VenueSession> make_trade_session(const ExchangeConfig& exchange,
                                                 const WebSocketConfig& ws_config,
                                                 const RuntimeConfig& runtime,
                                                 const net::TlsContext* tls,
                                                 VenueSessionHandlers handlers) {
  return std::make_unique<BinanceTradeSession>(exchange, ws_config, runtime, tls,
                                                  std::move(handlers));
}

std::unique_ptr<VenueRest> make_rest(const ExchangeConfig& exchange, net::HttpClient* http) {
  return std::make_unique<BinanceRestImpl>(exchange, http);
}

}  // namespace

const OmsVenue& venue() {
  static const OmsVenue kVenue{
      "binance",
      make_session,
      make_trade_session,
      make_rest,
  };
  return kVenue;
}

}  // namespace axon::oms::binance
