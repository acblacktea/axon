// Execution Management: order entry. Port of ems/ems_service.py plus the four
// per-venue EMS classes.
//
// EVERY ORDER GOES OVER A WEBSOCKET. There is no REST order path and no
// fallback: an order rides a connection that is already open and already
// authenticated, so it costs one round trip and no handshake.
//
// Which connection depends on the venue:
//
//   DERIBIT   private/buy, private/cancel, private/edit on its one connection.
//   OKX       op:"order" on the private stream it already has.
//   BINANCE   a second connection to ws-fapi. No session login -- every
//             request carries its own apiKey and signature.
//   BYBIT     a second connection to /v5/trade, with its own op:"auth".
//
// WHAT THIS COSTS, and it is worth being explicit about it: a venue whose
// session is not live CANNOT TRADE. Order entry is exactly as available as the
// session underneath it. Before, the perpetual venues could fall back to an
// HTTP request that stood on nothing but credentials; now a session in backoff
// means orders are rejected until it comes back.
//
// That is the deliberate trade for removing 1-2 RTTs per order -- most of a
// colocated latency budget. It also means SESSION HEALTH IS NOW ORDER-ENTRY
// HEALTH: watch axon_ws_connected and the reconnect counters, because they
// no longer merely describe the feed.
//
// AND THE MAPPINGS ON THE THREE PERPETUAL VENUES ARE UNVERIFIED. They were
// transcribed from those venues' documentation, not from the Python that has
// been reconciling against them for a long time. There is no longer a proven
// path beside them to fall back to, so validating each against its testnet is
// a prerequisite to trading, not a follow-up.
//
// EVERYTHING IS ASYNCHRONOUS. place_order takes a callback and returns
// immediately; the reply arrives from poll(). Blocking would stall every venue
// feed for an internet round trip, which is the one thing a single-threaded
// busy-poll engine must never do.

#pragma once

#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_set>

#include "axon/config.h"
#include "axon/models/order.h"
#include "axon/net/http_client.h"
#include "axon/risk/risk_manager.h"
#include "axon/oms/venue_rest.h"
#include "axon/oms/venue_session.h"

namespace axon::ems {

// Everything the four venues do differently on the order path, as one row per
// venue. Defined in ems_service.cpp; only ever passed around by reference.
struct VenueOps;
struct VenueState;

struct OrderResult {
  bool success = false;
  std::optional<models::Order> order;
  std::string error;
  // The request reached the venue but no verdict came back in time: the order
  // may be live. NOT a rejection, and the caller must not resubmit as if it
  // were -- that is how a timeout becomes a doubled position.
  bool outcome_unknown = false;
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
  // The SECOND connection Binance and Bybit need for order entry. OKX and
  // Deribit send orders on the session registered above and never have one.
  void register_trade_session(const std::string& exchange,
                              oms::VenueSession* session);

  // Binance, Bybit and OKX all require the SYMBOL to cancel or amend, and a
  // strategy only holds an order id. This looks it up from the order store --
  // which is why cancel on those venues fails cleanly if the order is not
  // known locally, rather than sending a request the venue will reject.
  using OrderLookup =
      std::function<std::optional<models::Order>(const std::string& order_id)>;
  void set_order_lookup(OrderLookup lookup) { lookup_ = std::move(lookup); }

  // Pre-trade risk. Every place and amend is checked before it is built; a
  // refusal is answered locally and never reaches the venue. Cancels are
  // never checked -- reducing risk must always be possible, kill switch or
  // not. Null runs without a risk layer, which the engine warns about.
  void set_risk_manager(risk::RiskManager* risk) { risk_ = risk; }

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
    bool is_place = false;
    std::string exchange;
    OrderCallback order_callback;
    BoolCallback bool_callback;
    models::OrderRequest request;
    double deadline = 0.0;
  };

  // The connection order entry for `exchange` should go out on, or nullptr if
  // there is none or it is not live. Never invokes a callback -- deciding is
  // separate from reporting, so the caller owns the error message.
  oms::VenueSession* ws_entry_session(const std::string& exchange,
                                      const VenueOps& ops) const;
  // Why order entry is unavailable, naming the session state. "Session is
  // backoff" and "no session" send an operator to completely different places.
  std::string order_entry_unavailable(const std::string& exchange,
                                      const VenueOps& ops) const;

  // Each returns true if the request was handed to the socket. On false the
  // callback has NOT been invoked and the caller reports the failure.
  //
  // `ops` is resolved once by the public entry point and threaded through, so
  // no step below ever asks which venue this is a second time.
  bool place_via_websocket(const std::string& exchange, const VenueOps& ops,
                           const models::OrderRequest& request,
                           const OrderCallback& callback);
  bool cancel_via_websocket(const std::string& exchange, const VenueOps& ops,
                            const std::string& order_id,
                            const std::string& symbol,
                            const BoolCallback& callback);
  bool modify_via_websocket(const std::string& exchange, const VenueOps& ops,
                            const models::Order& existing,
                            std::optional<core::Qty> amount,
                            std::optional<core::Price> price,
                            const OrderCallback& callback);

  const ExchangeConfig* exchange_config(const std::string& name) const;

  Config config_;
  net::HttpClient* http_ = nullptr;
  std::map<std::string, oms::VenueSession*> sessions_;
  std::map<std::string, oms::VenueSession*> trade_sessions_;
  OrderLookup lookup_;
  risk::RiskManager* risk_ = nullptr;
  // Request builders and reply scratch for every venue -- this EMS's own, so
  // the id counters live and die with `pending_` below. Heap-held because
  // VenueState is only complete in venue_ops.h.
  std::unique_ptr<VenueState> venues_;
  // Keyed by "exchange:rpc_id" so two venues cannot collide on an id.
  std::map<std::string, Pending> pending_;

  // internal_order_ids handed to a venue, so a resubmission is refused here
  // rather than becoming a second order. Removed again when the venue
  // definitively rejects one -- a rejected order may be retried. Bounded,
  // oldest first; the venue's own duplicate-client-id check backs it up.
  std::unordered_set<std::string> submitted_;
  std::deque<std::string> submitted_order_;
  static constexpr std::size_t kMaxRememberedSubmissions = 100'000;
  void remember_submission(const std::string& key);
};

}  // namespace axon::ems
