// What the OMS publishes, as an interface: the engine's transports (shared
// memory, ZMQ) implement it to reach strategies, and the risk layer implements
// it to stay current. Its own header so a listener need not pull in the OMS.

#pragma once

#include <string>
#include <vector>

#include "axon/models/fill.h"
#include "axon/models/order.h"
#include "axon/models/portfolio.h"
#include "axon/transport/hot_messages.h"

namespace axon::oms {

// What the OMS tells the rest of the engine. Every method has an empty
// default; a listener overrides what it needs. Called on the engine thread,
// in the order listeners were added.
class OmsListener {
 public:
  virtual ~OmsListener() = default;

  // A venue order update, already resolved to the request that produced it
  // (internal id and strategy filled in). Called FIRST, before anything is
  // decoded or allocated: this is where the shared-memory hot path publishes,
  // and nothing on the control-plane side may delay it.
  virtual void on_order_message(const transport::OrderUpdateMsg&) {}

  // The same update, decoded, before the store applies it. Every update,
  // including ones that change nothing the store tracks (an amend's price).
  virtual void on_order_update(const models::Order&) {}

  // An order's state -- status or fill -- changed in the store. From the feed
  // or from reconciliation; at most once per change.
  virtual void on_order_changed(const models::Order&) {}

  // A fill, attributed to its order's strategy and deduplicated: each trade
  // reaches a listener once, whether the feed or the reconciler found it.
  // `msg` is the venue's own bytes for a feed fill and an encoding of `fill`
  // for a recovered one -- or null when a recovered fill does not fit the
  // fixed-width hot message, in which case only the decoded form exists.
  virtual void on_fill(const transport::FillMsg* msg, const models::Fill& fill,
                       bool recovered) {
    static_cast<void>(msg);
    static_cast<void>(fill);
    static_cast<void>(recovered);
  }

  // The venue's positions for one exchange, replacing the previous snapshot.
  virtual void on_positions(const std::string& exchange,
                            const std::vector<models::Position>& positions) {
    static_cast<void>(exchange);
    static_cast<void>(positions);
  }
};

}  // namespace axon::oms
