// Deribit: everything the OMS needs from this venue.
//
// Deribit options: one JSON-RPC connection carries the feed AND order entry,
// authenticated once with client credentials.
//
// Moved out of venue_session.cpp and venue_rest.cpp so each venue's
// protocol lives in one place, as it does on the EMS side. Only venue() is
// visible outside this file.

#include "axon/exchanges/deribit/deribit_oms.h"

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
#include "axon/exchanges/deribit/deribit_builder.h"
#include "axon/exchanges/deribit/deribit_parser.h"
#include "axon/exchanges/deribit/deribit_protocol.h"
#include "axon/venue/json_view.h"

namespace axon::oms::deribit {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.deribit");
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
// Deribit
// ===========================================================================

class DeribitSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::deribit::kTestnetHost
                                              : venue::deribit::kProductionHost);
  }
  std::string path() const override { return std::string(venue::deribit::kPath); }

  bool send_authentication() override {
    char buf[venue::deribit::kMaxRequestBytes];
    const std::size_t n = builder_.authenticate(buf, sizeof(buf), exchange_.api_key,
                                                exchange_.api_secret, auth_id_);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool send_subscriptions() override {
    char buf[venue::deribit::kMaxRequestBytes];
    std::int64_t id = 0;

    // Heartbeat first: Deribit will not send test_requests until asked, and
    // without them a dead connection is only noticed by the silence timer.
    std::size_t n = builder_.set_heartbeat(
        buf, sizeof(buf), ws_config_.heartbeat_interval_seconds, id);
    if (n == 0 || !send_raw(std::string_view(buf, n))) {
      return false;
    }

    std::vector<std::string> channel_storage{
        std::string(venue::deribit::kAllOrdersChannel),
        std::string(venue::deribit::kAllTradesChannel)};
    for (const auto& currency : currencies_) {
      channel_storage.push_back("user.portfolio." + currency);
    }
    std::vector<std::string_view> channels;
    channels.reserve(channel_storage.size());
    for (const auto& c : channel_storage) {
      channels.emplace_back(c);
    }

    n = builder_.subscribe(buf, sizeof(buf), channels.data(), channels.size(),
                           subscribe_id_);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    Sink sink{this};
    const auto result = parser_.parse(reinterpret_cast<const char*>(data), len,
                                      capacity, sink);

    switch (result.kind) {
      case venue::VenueMessageKind::kHeartbeatTestRequest: {
        // Deribit drops the connection if this goes unanswered, so it is
        // answered here rather than queued behind anything else.
        char buf[venue::deribit::kMaxRequestBytes];
        const std::size_t n =
            venue::deribit::DeribitBuilder::test_response(buf, sizeof(buf));
        send_raw(std::string_view(buf, n));
        break;
      }
      case venue::VenueMessageKind::kRpcResult:
        if (result.rpc_id == auth_id_) {
          on_auth_reply(true, {});
        } else if (result.rpc_id == subscribe_id_) {
          on_subscribe_reply(true, {});
        } else if (handlers_.on_rpc_reply) {
          // Not ours: an order-entry reply. Hand over the RAW frame -- the EMS
          // knows Deribit's result shape, the session does not and should not.
          handlers_.on_rpc_reply(result.rpc_id, true,
                                 std::string_view(reinterpret_cast<const char*>(data), len),
                                 {});
        }
        break;
      case venue::VenueMessageKind::kRpcError:
        if (result.rpc_id == auth_id_) {
          on_auth_reply(false, std::string(result.rpc_error_text));
        } else if (result.rpc_id == subscribe_id_) {
          on_subscribe_reply(false, std::string(result.rpc_error_text));
        } else if (handlers_.on_rpc_reply) {
          handlers_.on_rpc_reply(result.rpc_id, false,
                                 std::string_view(reinterpret_cast<const char*>(data), len),
                                 result.rpc_error_text);
        }
        break;
      case venue::VenueMessageKind::kParseError:
        // A single malformed frame is not worth dropping a session over.
        AXON_LOG_WARN(log(), "[deribit] parse error: {}",
                        result.error != nullptr ? result.error : "?");
        break;
      default:
        break;
    }

    if (result.unknown_order_state) {
      AXON_LOG_WARN(log(), "[deribit] unrecognised order_state; mapped to open");
    }
    return true;
  }

 private:
  struct Sink {
    DeribitSession* self;
    void on_order(const transport::OrderUpdateMsg& m) {
      if (self->handlers_.on_order) {
        self->handlers_.on_order(m);
      }
    }
    void on_fill(const transport::FillMsg& m) {
      if (self->handlers_.on_fill) {
        self->handlers_.on_fill(m);
      }
    }
    void on_account(const models::AccountSummary& a) {
      if (self->handlers_.on_account) {
        self->handlers_.on_account(a);
      }
    }
  };

  venue::deribit::DeribitParser parser_;
  venue::deribit::DeribitBuilder builder_;
  std::int64_t auth_id_ = -1;
  std::int64_t subscribe_id_ = -1;
  std::vector<std::string> currencies_{"BTC"};
};

// ===========================================================================
// REST snapshots: reconciliation, positions, tickers
// ===========================================================================
// ===========================================================================
// Deribit
// ===========================================================================
class DeribitRestImpl final : public VenueRest {
 public:
  DeribitRestImpl(ExchangeConfig exchange, net::HttpClient* http)
      : exchange_(std::move(exchange)), http_(http) {
    // Deribit accepts HTTP Basic, which avoids the token lifecycle entirely.
    basic_ = "Basic " + net::base64_encode(exchange_.api_key + ":" +
                                           exchange_.api_secret);
  }

  const std::string& exchange_name() const override { return exchange_.name; }

  void get_ticker(const std::string& instrument, TickerCallback cb) override {
    send("/api/v2/public/ticker?instrument_name=" + url_encode(instrument), false,
         "GET", {}, [cb, instrument](const net::HttpResponse& r) {
           if (!r.error.empty() || !r.ok()) {
             cb(std::nullopt, http_error(r));
             return;
           }
           auto root = doc().parse_copy(r.body);
           auto result = root.has_value() ? (*root)["result"].as_object()
                                          : std::nullopt;
           if (!result.has_value()) {
             cb(std::nullopt, "ticker response has no result");
             return;
           }
           models::Ticker t;
           t.instrument = instrument;
           t.best_bid_price =
               (*result)["best_bid_price"].as_decimal().value_or(core::Price{});
           t.best_bid_amount =
               (*result)["best_bid_amount"].as_decimal().value_or(core::Qty{});
           t.best_ask_price =
               (*result)["best_ask_price"].as_decimal().value_or(core::Price{});
           t.best_ask_amount =
               (*result)["best_ask_amount"].as_decimal().value_or(core::Qty{});
           t.last_price = (*result)["last_price"].as_decimal();
           t.mark_price = (*result)["mark_price"].as_decimal();
           if (const auto ts = (*result)["timestamp"].as_int(); ts.has_value()) {
             t.timestamp = core::Timestamp::from_millis(*ts);
           }
           cb(t, {});
         });
  }

  void get_open_orders(const std::string& currency, OrdersCallback cb) override {
    send("/api/v2/private/get_open_orders_by_currency?currency=" +
             url_encode(currency) + "&kind=any",
         true, "GET", {}, [cb](const net::HttpResponse& r) {
           parse_array_at<models::Order>(r, {"result"}, parse_order, cb);
         });
  }

  void get_order(const models::Order& order, OrderCallback cb) override {
    send("/api/v2/private/get_order_state?order_id=" + url_encode(order.order_id),
         true, "GET", {}, [cb](const net::HttpResponse& r) {
           parse_object_at<models::Order>(r, {"result"}, parse_order, cb);
         });
  }

  void get_positions(const std::string& currency, PositionsCallback cb) override {
    send("/api/v2/private/get_positions?currency=" + url_encode(currency) +
             "&kind=any",
         true, "GET", {}, [cb](const net::HttpResponse& r) {
           parse_array_at<models::Position>(r, {"result"}, parse_position, cb);
         });
  }

  void get_user_trades(const std::string& currency, std::int64_t start_ms,
                       std::int64_t end_ms, FillsCallback cb) override {
    send("/api/v2/private/get_user_trades_by_currency_and_time?currency=" +
             url_encode(currency) + "&start_timestamp=" + std::to_string(start_ms) +
             "&end_timestamp=" + std::to_string(end_ms) + "&count=1000",
         true, "GET", {}, [cb](const net::HttpResponse& r) {
           // Nested one level deeper than the others: result.trades.
           parse_array_at<models::Fill>(r, {"result", "trades"}, parse_fill, cb);
         });
  }

 private:
  static std::optional<models::Order> parse_order(venue::Object& d);
  static std::optional<models::Fill> parse_fill(venue::Object& d);
  static std::optional<models::Position> parse_position(venue::Object& d);

  void send(const std::string& path, bool authenticated, const std::string& method,
            const std::string& body,
            std::function<void(const net::HttpResponse&)> handler) {
    if (http_ == nullptr) {
      net::HttpResponse r;
      r.error = "no HTTP client";
      handler(r);
      return;
    }
    net::HttpRequest req;
    req.method = method;
    req.host = std::string(exchange_.is_testnet() ? venue::deribit::kTestnetHost
                                                  : venue::deribit::kProductionHost);
    req.path = path;
    req.body = body;
    if (authenticated) {
      req.headers.push_back({"Authorization", basic_});
    }
    http_->submit(std::move(req), std::move(handler));
  }

  ExchangeConfig exchange_;
  net::HttpClient* http_;
  std::string basic_;
};

std::optional<models::Order> DeribitRestImpl::parse_order(venue::Object& d) {
  models::Order o;
  o.exchange = "deribit";
  const auto id = d["order_id"].as_string();
  const auto inst = d["instrument_name"].as_string();
  const auto dir = d["direction"].as_string();
  if (!id.has_value() || !inst.has_value() || !dir.has_value()) {
    return std::nullopt;
  }
  o.order_id = std::string(*id);
  o.instrument = std::string(*inst);
  o.side = venue::deribit::map_direction(*dir);
  o.order_type =
      venue::deribit::map_order_type(d["order_type"].as_string().value_or("limit"));
  o.amount = d["amount"].as_decimal().value_or(core::Qty{});
  o.filled_amount = d["filled_amount"].as_decimal().value_or(core::Qty{});
  o.price = d["price"].as_decimal();
  o.average_price = d["average_price"].as_decimal();
  const auto mapping =
      venue::deribit::map_order_state(d["order_state"].as_string().value_or("open"));
  o.status =
      venue::deribit::refine_with_fill(mapping.status, o.filled_amount.raw() > 0);
  const auto created = d["creation_timestamp"].as_int();
  const auto updated = d["last_update_timestamp"].as_int();
  o.created_at = core::Timestamp::from_millis(created.value_or(0));
  o.updated_at = core::Timestamp::from_millis(updated.value_or(created.value_or(0)));
  // The client order id we sent: our internal_order_id, or the strategy's
  // label. It is what lets a snapshot resolve an order whose placement reply
  // was lost.
  if (const auto cid = d["label"].as_string(); cid.has_value() && !cid->empty()) {
    o.internal_order_id = std::string(*cid);
  }
  return o;
}

std::optional<models::Fill> DeribitRestImpl::parse_fill(venue::Object& d) {
  models::Fill f;
  f.exchange = "deribit";
  const auto tid = d["trade_id"].as_string();
  const auto oid = d["order_id"].as_string();
  const auto inst = d["instrument_name"].as_string();
  const auto dir = d["direction"].as_string();
  if (!tid.has_value() || !oid.has_value() || !inst.has_value() || !dir.has_value()) {
    return std::nullopt;
  }
  f.trade_id = std::string(*tid);
  f.order_id = std::string(*oid);
  f.instrument = std::string(*inst);
  f.side = venue::deribit::map_direction(*dir);
  f.amount = d["amount"].as_decimal().value_or(core::Qty{});
  f.price = d["price"].as_decimal().value_or(core::Price{});
  f.fee = d["fee"].as_decimal().value_or(core::Price{});
  f.fee_currency = std::string(d["fee_currency"].as_string().value_or(""));
  f.liquidity = venue::deribit::map_liquidity(d["liquidity"].as_string().value_or("M"));
  f.timestamp = core::Timestamp::from_millis(d["timestamp"].as_int().value_or(0));
  return f;
}

std::optional<models::Position> DeribitRestImpl::parse_position(venue::Object& d) {
  models::Position p;
  p.exchange = "deribit";
  const auto inst = d["instrument_name"].as_string();
  if (!inst.has_value()) {
    return std::nullopt;
  }
  p.instrument = std::string(*inst);
  p.kind = std::string(d["kind"].as_string().value_or(""));
  p.direction = std::string(d["direction"].as_string().value_or("zero"));
  p.size = d["size"].as_decimal().value_or(core::Qty{});
  p.average_price = d["average_price"].as_decimal().value_or(core::Price{});
  p.mark_price = d["mark_price"].as_decimal().value_or(core::Price{});
  p.index_price = d["index_price"].as_decimal().value_or(core::Price{});
  p.delta = d["delta"].as_decimal().value_or(core::Price{});
  p.total_profit_loss = d["total_profit_loss"].as_decimal().value_or(core::Price{});
  p.timestamp = core::Timestamp::now();
  return p;
}

std::unique_ptr<VenueSession> make_session(const ExchangeConfig& exchange,
                                           const WebSocketConfig& ws_config,
                                           const RuntimeConfig& runtime,
                                           const net::TlsContext* tls, VenueRest*,
                                           VenueSessionHandlers handlers) {
  return std::make_unique<DeribitSession>(exchange, ws_config, runtime, tls,
                                                  std::move(handlers));
}

std::unique_ptr<VenueRest> make_rest(const ExchangeConfig& exchange, net::HttpClient* http) {
  return std::make_unique<DeribitRestImpl>(exchange, http);
}

}  // namespace

const OmsVenue& venue() {
  static const OmsVenue kVenue{
      "deribit",
      make_session,
      nullptr,  // no order-entry session: orders ride the feed session
      make_rest,
  };
  return kVenue;
}

}  // namespace axon::oms::deribit
