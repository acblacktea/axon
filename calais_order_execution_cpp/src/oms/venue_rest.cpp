#include "calais/oms/venue_rest.h"

#include "calais/core/clock.h"
#include "calais/net/crypto_lite.h"
#include "calais/util/logging.h"
#include "calais/venue/binance/binance_builder.h"
#include "calais/venue/binance/binance_parser.h"
#include "calais/venue/bybit/bybit_builder.h"
#include "calais/venue/bybit/bybit_parser.h"
#include "calais/venue/deribit/deribit_protocol.h"
#include "calais/venue/json_view.h"
#include "calais/venue/okx/okx_builder.h"
#include "calais/venue/okx/okx_parser.h"

namespace calais::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.rest");
  return logger;
}

std::int64_t now_ms() { return core::wall_clock_ns() / 1'000'000LL; }

std::string url_encode(std::string_view s) {
  static constexpr char kHex[] = "0123456789ABCDEF";
  std::string out;
  for (char c : s) {
    const auto u = static_cast<unsigned char>(c);
    if ((u >= 'A' && u <= 'Z') || (u >= 'a' && u <= 'z') || (u >= '0' && u <= '9') ||
        c == '-' || c == '.' || c == '_' || c == '~') {
      out.push_back(c);
    } else {
      out.push_back('%');
      out.push_back(kHex[u >> 4]);
      out.push_back(kHex[u & 0xF]);
    }
  }
  return out;
}

venue::Document& doc() {
  static thread_local venue::Document d(1u << 20);
  return d;
}

std::string http_error(const net::HttpResponse& r) {
  if (!r.error.empty()) {
    return r.error;
  }
  return "HTTP " + std::to_string(r.status) + ": " + r.body;
}

// Walks a JSON array at `path` (a chain of object keys) and parses each element.
template <typename Item, typename Parse, typename Callback>
void parse_array_at(const net::HttpResponse& r,
                    const std::vector<std::string>& path, Parse parse,
                    Callback callback) {
  if (!r.error.empty() || !r.ok()) {
    callback(std::vector<Item>{}, http_error(r));
    return;
  }
  auto root = doc().parse_copy(r.body);
  if (!root.has_value()) {
    callback(std::vector<Item>{}, "malformed response");
    return;
  }

  std::optional<venue::Array> array;
  if (path.empty()) {
    // The whole document is the array. simdjson needs it asked for as one.
    auto reparsed = doc().parse_copy("{\"_\":" + r.body + "}");
    if (!reparsed.has_value()) {
      callback(std::vector<Item>{}, "malformed array response");
      return;
    }
    array = (*reparsed)["_"].as_array();
  } else {
    venue::Object current = *root;
    for (std::size_t i = 0; i + 1 < path.size(); ++i) {
      auto next = current[path[i]].as_object();
      if (!next.has_value()) {
        callback(std::vector<Item>{}, "response is missing '" + path[i] + "'");
        return;
      }
      current = *next;
    }
    array = current[path.back()].as_array();
  }

  if (!array.has_value()) {
    // An empty or absent list is not an error: no open orders is a normal
    // answer and must not look like a failed snapshot.
    callback(std::vector<Item>{}, {});
    return;
  }
  std::vector<Item> items;
  array->for_each_object([&](venue::Object& obj) {
    if (auto item = parse(obj); item.has_value()) {
      items.push_back(std::move(*item));
    }
  });
  callback(std::move(items), {});
}

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

  void cancel_order(const std::string&, const std::string& order_id,
                    StringCallback cb) override {
    // Deribit needs no symbol -- the order id is enough.
    send("/api/v2/private/cancel?order_id=" + url_encode(order_id), true, "GET", {},
         [cb](const net::HttpResponse& r) {
           cb(r.body, r.ok() ? std::string{} : http_error(r));
         });
  }

  void amend_order(const std::string&, const std::string& order_id,
                   std::optional<core::Qty> amount, std::optional<core::Price> price,
                   StringCallback cb) override {
    std::string path = "/api/v2/private/edit?order_id=" + url_encode(order_id);
    if (amount.has_value()) {
      path += "&amount=" + amount->to_string();
    }
    if (price.has_value()) {
      path += "&price=" + price->to_string();
    }
    send(path, true, "GET", {}, [cb](const net::HttpResponse& r) {
      cb(r.body, r.ok() ? std::string{} : http_error(r));
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

  void cancel_order(const std::string& symbol, const std::string& order_id,
                    StringCallback cb) override {
    send(signed_path("/fapi/v1/order",
                     "symbol=" + url_encode(symbol) + "&orderId=" + url_encode(order_id)),
         true, "DELETE", {}, [cb](const net::HttpResponse& r) {
           cb(r.body, r.ok() ? std::string{} : http_error(r));
         });
  }

  void amend_order(const std::string& symbol, const std::string& order_id,
                   std::optional<core::Qty> amount, std::optional<core::Price> price,
                   StringCallback cb) override {
    // Binance's amend needs side, quantity AND price -- it will not accept a
    // partial change. The caller must supply both, which is why the EMS looks
    // the order up first.
    if (!amount.has_value() || !price.has_value()) {
      cb({}, "Binance requires both quantity and price on an amend");
      return;
    }
    send(signed_path("/fapi/v1/order",
                     "symbol=" + url_encode(symbol) +
                         "&orderId=" + url_encode(order_id) +
                         "&quantity=" + amount->to_string() +
                         "&price=" + price->to_string()),
         true, "PUT", {}, [cb](const net::HttpResponse& r) {
           cb(r.body, r.ok() ? std::string{} : http_error(r));
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

  void cancel_order(const std::string& symbol, const std::string& order_id,
                    StringCallback cb) override {
    const std::string body = R"({"category":"linear","symbol":")" + symbol +
                             R"(","orderId":")" + order_id + R"("})";
    send("/v5/order/cancel", body, true, "POST", body,
         [cb](const net::HttpResponse& r) {
           cb(r.body, r.ok() ? std::string{} : http_error(r));
         });
  }

  void amend_order(const std::string& symbol, const std::string& order_id,
                   std::optional<core::Qty> amount, std::optional<core::Price> price,
                   StringCallback cb) override {
    std::string body = R"({"category":"linear","symbol":")" + symbol +
                       R"(","orderId":")" + order_id + R"(")";
    if (amount.has_value()) {
      body += R"(,"qty":")" + amount->to_string() + R"(")";
    }
    if (price.has_value()) {
      body += R"(,"price":")" + price->to_string() + R"(")";
    }
    body += "}";
    send("/v5/order/amend", body, true, "POST", body,
         [cb](const net::HttpResponse& r) {
           cb(r.body, r.ok() ? std::string{} : http_error(r));
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

  void cancel_order(const std::string& symbol, const std::string& order_id,
                    StringCallback cb) override {
    const std::string body =
        R"({"instId":")" + symbol + R"(","ordId":")" + order_id + R"("})";
    send("/api/v5/trade/cancel-order", "POST", body, true,
         [cb](const net::HttpResponse& r) {
           cb(r.body, r.ok() ? std::string{} : http_error(r));
         });
  }

  void amend_order(const std::string& symbol, const std::string& order_id,
                   std::optional<core::Qty> amount, std::optional<core::Price> price,
                   StringCallback cb) override {
    std::string body =
        R"({"instId":")" + symbol + R"(","ordId":")" + order_id + R"(")";
    if (amount.has_value()) {
      body += R"(,"newSz":")" + amount->to_string() + R"(")";
    }
    if (price.has_value()) {
      body += R"(,"newPx":")" + price->to_string() + R"(")";
    }
    body += "}";
    send("/api/v5/trade/amend-order", "POST", body, true,
         [cb](const net::HttpResponse& r) {
           cb(r.body, r.ok() ? std::string{} : http_error(r));
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

}  // namespace

std::unique_ptr<VenueRest> make_venue_rest(const ExchangeConfig& exchange,
                                           net::HttpClient* http) {
  if (exchange.name == "deribit") {
    return std::make_unique<DeribitRestImpl>(exchange, http);
  }
  if (exchange.name == "binance") {
    return std::make_unique<BinanceRestImpl>(exchange, http);
  }
  if (exchange.name == "bybit") {
    return std::make_unique<BybitRestImpl>(exchange, http);
  }
  if (exchange.name == "okx") {
    return std::make_unique<OkxRestImpl>(exchange, http);
  }
  CALAIS_LOG_ERROR(log(), "no REST implementation for exchange '{}'", exchange.name);
  return nullptr;
}

}  // namespace calais::oms
