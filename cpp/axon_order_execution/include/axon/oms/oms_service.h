// Order management: the engine's record of every order, fill, account and
// position, and the backstop that keeps it true. Port of oms/oms_service.py.
//
// WHAT IT OWNS
//   * the order, fill and portfolio stores, and their persistence;
//   * a reconciler per venue -- open orders, missed fills, positions;
//   * the processing of every feed event, in the order that matters:
//       order update -> resolve to its request -> publish the raw bytes ->
//       decode -> update the store -> publish the change;
//       fill -> attribute to its order's strategy -> mark seen -> deduplicate
//       -> publish;
//   * the bookkeeping around an order placement, so a reply that is lost or
//     late still finds its way back to the request.
//
// WHAT IT DOES NOT
//   * Talk to the venues: VenueConnections holds the connections, and feeds
//     this class through VenueEventSink.
//   * Publish: it decides WHAT is published and to WHOM, and tells its
//     OmsListeners; the engine turns that into shared-memory and ZMQ traffic.
//     That keeps transport out of here, and this class testable without it.
//   * Place orders: that is the EMS. Risk is separate too, and listens.
//
// Single-threaded, like the rest of the engine. Persistence, when configured,
// writes on its own thread behind a ring, never blocking this one.

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "axon/config.h"
#include "axon/models/fill.h"
#include "axon/models/order.h"
#include "axon/models/portfolio.h"
#include "axon/oms/oms_listener.h"
#include "axon/oms/order_store.h"
#include "axon/oms/portfolio_store.h"
#include "axon/oms/reconciler.h"
#include "axon/oms/venue_connections.h"
#include "axon/oms/venue_rest.h"
#include "axon/transport/hot_messages.h"

namespace axon::repository {
class PostgresWriter;
}

namespace axon::oms {

class OmsService final : public VenueEventSink {
 public:
  // With config.database, the stores write through a Postgres writer thread
  // (started here; throws if it cannot connect). Without one they keep
  // history in memory only, and say so.
  explicit OmsService(const Config& config);
  ~OmsService() override;

  OmsService(const OmsService&) = delete;
  OmsService& operator=(const OmsService&) = delete;

  // One reconciler per venue that has a REST client. A venue without one is
  // logged: a message its feed drops will never be recovered.
  void start(const std::map<std::string, VenueRest*>& rests);
  // Fires whichever reconciliation passes are due. Non-blocking.
  void poll();
  // Drops the reconcilers -- before the REST clients they borrow go away --
  // and flushes persistence. Safe to call twice.
  void stop();

  // Listeners are not owned and must outlive the service.
  void add_listener(OmsListener* listener) { listeners_.push_back(listener); }

  // --- VenueEventSink: the feed ------------------------------------------------
  void on_order_update(const std::string& exchange,
                       const transport::OrderUpdateMsg& msg) override;
  void on_fill(const std::string& exchange, const transport::FillMsg& msg) override;
  void on_account(const std::string& exchange,
                  const models::AccountSummary& account) override;
  void on_session_live(const std::string& exchange) override;

  // --- around an EMS placement -------------------------------------------------
  // Before the request is handed to the EMS: registers its client order id,
  // so the venue's updates -- even with the placement reply lost -- map back
  // to it and its strategy.
  void before_place(const std::string& exchange, const models::OrderRequest& request);
  // With the EMS's verdict, passed as plain values so the OMS does not
  // depend on the EMS. Accepted with an order: recorded. Refused: the
  // registration is dropped, since nothing will ever report on it. Unknown
  // (sent, no verdict): kept, because the order may be live and its updates
  // must still route.
  enum class PlaceOutcome { kAccepted, kRefused, kUnknown };
  void after_place(const std::string& exchange, const models::OrderRequest& request,
                   PlaceOutcome outcome, const std::optional<models::Order>& order);

  // --- queries -------------------------------------------------------------------
  std::optional<models::Order> get_order(const std::string& order_id) {
    return orders_.get_order(order_id);
  }
  std::vector<models::Order> active_orders() const { return orders_.active_orders(); }
  std::vector<models::Order> all_orders() const { return orders_.all_orders(); }
  std::vector<models::Fill> fills_by_order(const std::string& order_id) {
    return fills_.by_order(order_id);
  }
  std::vector<models::Fill> fills_by_strategy(const std::string& strategy_id) {
    return fills_.by_strategy(strategy_id);
  }
  std::optional<models::AccountSummary> account(const std::string& exchange,
                                                const std::string& currency) const {
    return portfolio_.account(exchange, currency);
  }
  std::vector<models::Position> positions(const std::string& exchange) const {
    return portfolio_.positions(exchange);
  }

  // --- for the heartbeat ---------------------------------------------------------
  std::size_t cached_order_count() const noexcept { return orders_.cached_order_count(); }
  std::uint64_t fills_accepted() const noexcept { return fills_.accepted(); }
  std::uint64_t fills_duplicate() const noexcept { return fills_.duplicates(); }
  const Reconciler* reconciler(const std::string& exchange) const;

 private:
  void record_recovered_fill(const models::Fill& fill);
  void publish_fill(const transport::FillMsg* msg, const models::Fill& fill, bool recovered);

  const Config& config_;
  std::unique_ptr<repository::PostgresWriter> writer_;
  OrderStore orders_;
  FillStore fills_;
  PortfolioStore portfolio_;
  std::map<std::string, std::unique_ptr<Reconciler>> reconcilers_;
  std::vector<OmsListener*> listeners_;
  // Sequence for hot messages this process originates: only recovered fills.
  // Venue-sourced messages carry their parser's numbering.
  std::uint64_t hot_seq_ = 0;
};

}  // namespace axon::oms
