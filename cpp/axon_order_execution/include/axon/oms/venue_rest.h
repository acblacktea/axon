// Per-venue REST snapshots, behind one interface.
//
// Four venues, four unrelated REST APIs, one shape the reconcilers can use.
// Each implementation signs its own requests -- and each signs differently,
// which is exactly why this is an interface rather than a switch statement:
//
//   Deribit   HTTP Basic, no signature at all
//   Binance   HMAC-SHA256 over the query string, hex, in the query
//   Bybit     HMAC-SHA256 over timestamp+key+window+query, hex, in a header
//   OKX       HMAC-SHA256 over an ISO timestamp+method+path+body, BASE64,
//             in a header, plus a passphrase
//
// Everything is asynchronous. A snapshot is a round trip and must never be
// taken on the engine thread inline.
//
// THERE IS NO ORDER ENTRY HERE, and that is now literal rather than a matter
// of layering: every order, cancel and amend goes over a WebSocket session in
// the EMS. What remains is the backstop READS that run on a timer --
// reconciliation snapshots, position refreshes, and Binance's listenKey.
//
// So an HTTP request in this process is never on a critical path. If one shows
// up in a latency profile, something has been added in the wrong place.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "axon/config.h"
#include "axon/models/fill.h"
#include "axon/models/order.h"
#include "axon/models/portfolio.h"
#include "axon/net/http_client.h"

namespace axon::oms {

class VenueRest {
 public:
  using TickerCallback =
      std::function<void(std::optional<models::Ticker>, const std::string& error)>;
  using OrdersCallback =
      std::function<void(std::vector<models::Order>, const std::string& error)>;
  using FillsCallback =
      std::function<void(std::vector<models::Fill>, const std::string& error)>;
  using PositionsCallback =
      std::function<void(std::vector<models::Position>, const std::string& error)>;
  using StringCallback =
      std::function<void(std::string value, const std::string& error)>;

  virtual ~VenueRest() = default;

  virtual void get_ticker(const std::string& instrument, TickerCallback cb) = 0;
  virtual void get_open_orders(const std::string& currency, OrdersCallback cb) = 0;
  virtual void get_positions(const std::string& currency, PositionsCallback cb) = 0;
  virtual void get_user_trades(const std::string& currency, std::int64_t start_ms,
                               std::int64_t end_ms, FillsCallback cb) = 0;

  // Binance only: create and renew the user data stream key. Every other venue
  // returns "not supported" -- their private streams authenticate over the
  // socket rather than through a URL token.
  virtual void create_listen_key(StringCallback cb) {
    cb({}, "listen keys are Binance-only");
  }
  virtual void keepalive_listen_key(const std::string&, StringCallback cb) {
    cb({}, "listen keys are Binance-only");
  }

  virtual const std::string& exchange_name() const = 0;
};

std::unique_ptr<VenueRest> make_venue_rest(const ExchangeConfig& exchange,
                                           net::HttpClient* http);

}  // namespace axon::oms
