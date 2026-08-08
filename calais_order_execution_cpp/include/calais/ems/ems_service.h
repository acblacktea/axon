// Execution Management: order entry. Port of ems/ems_service.py plus the four
// per-venue EMS classes.
//
// TWO TRANSPORTS, and the split is a latency decision rather than a stylistic
// one:
//
//   DERIBIT goes over the WEBSOCKET. private/buy, private/cancel and
//   private/edit ride the connection that is already open and already
//   authenticated, so an order costs one round trip and no handshake. This is
//   the fast path and it is what the whole design is aimed at.
//
//   BINANCE / BYBIT / OKX go over REST, because that is what their proven
//   Python implementations do and there is no verified WebSocket order-entry
//   mapping to port. Each of those orders pays an HTTP round trip on top --
//   1-2 RTTs, which on a colocated link is most of the latency budget. Moving
//   them to their WebSocket trade endpoints is the single largest remaining
//   win in the system.
//
// EVERYTHING IS ASYNCHRONOUS. place_order takes a callback and returns
// immediately; the reply arrives from poll(). Blocking would stall every venue
// feed for an internet round trip, which is the one thing a single-threaded
// busy-poll engine must never do.

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>

#include "calais/config.h"
#include "calais/models/order.h"
#include "calais/net/http_client.h"
#include "calais/oms/venue_rest.h"
#include "calais/oms/venue_session.h"

namespace calais::ems {

struct OrderResult {
  bool success = false;
  std::optional<models::Order> order;
  std::string error;
};

using OrderCallback = std::function<void(const OrderResult&)>;
using BoolCallback = std::function<void(bool success, const std::string& error)>;

class EmsService {
 public:
  EmsService(const Config& config, net::HttpClient* http);
  ~EmsService();

  // Sessions are owned by the OMS; the EMS borrows them to send order-entry
  // messages on an already-authenticated connection.
  void register_session(const std::string& exchange, oms::VenueSession* session);
  void register_rest(const std::string& exchange, oms::VenueRest* rest);

  // Binance, Bybit and OKX all require the SYMBOL to cancel or amend, and a
  // strategy only holds an order id. This looks it up from the order store --
  // which is why cancel on those venues fails cleanly if the order is not
  // known locally, rather than sending a request the venue will reject.
  using OrderLookup =
      std::function<std::optional<models::Order>(const std::string& order_id)>;
  void set_order_lookup(OrderLookup lookup) { lookup_ = std::move(lookup); }

  // Routes a venue reply that the session did not claim. Wired to
  // VenueSessionHandlers::on_rpc_reply.
  void on_rpc_reply(const std::string& exchange, std::int64_t id, bool success,
                    std::string_view payload, std::string_view error);

  void place_order(const std::string& exchange, const models::OrderRequest& request,
                   OrderCallback callback);
  void cancel_order(const std::string& exchange, const std::string& order_id,
                    BoolCallback callback);
  void modify_order(const std::string& exchange, const std::string& order_id,
                    std::optional<core::Qty> amount,
                    std::optional<core::Price> price, OrderCallback callback);

  // Times out requests whose reply never came. Call from the engine loop.
  void poll();

  std::size_t in_flight() const noexcept { return pending_.size(); }

 private:
  struct Pending {
    std::string exchange;
    OrderCallback order_callback;
    BoolCallback bool_callback;
    models::OrderRequest request;
    double deadline = 0.0;
  };

  bool place_via_websocket(const std::string& exchange,
                           const models::OrderRequest& request,
                           OrderCallback callback);
  void place_via_rest(const std::string& exchange,
                      const models::OrderRequest& request, OrderCallback callback);

  const ExchangeConfig* exchange_config(const std::string& name) const;

  Config config_;
  net::HttpClient* http_ = nullptr;
  std::map<std::string, oms::VenueSession*> sessions_;
  std::map<std::string, oms::VenueRest*> rest_;
  OrderLookup lookup_;
  // Keyed by "exchange:rpc_id" so two venues cannot collide on an id.
  std::map<std::string, Pending> pending_;
};

}  // namespace calais::ems
