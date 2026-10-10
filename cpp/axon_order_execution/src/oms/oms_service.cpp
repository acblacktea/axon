#include "axon/oms/oms_service.h"

#include "axon/repository/postgres.h"
#include "axon/util/logging.h"
#include "axon/util/metrics.h"

namespace axon::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms");
  return logger;
}

}  // namespace

OmsService::OmsService(const Config& config) : config_(config) {
  // Every store takes a repository. With a database it writes through to the
  // writer thread; without one it is an in-memory twin. The stores cannot
  // tell the difference, which is the point: no `if (writer_)` anywhere else.
  if (config_.database.has_value()) {
    writer_ = std::make_unique<repository::PostgresWriter>();
    writer_->start(*config_.database);
    // Reads still come from the in-memory cache, never from a blocking SELECT
    // on the engine thread.
    orders_ = OrderStore(std::make_shared<repository::PostgresOrderRepository>(writer_.get()));
    fills_ = FillStore(std::make_shared<repository::PostgresFillRepository>(writer_.get()));
    portfolio_ = PortfolioStore(
        std::make_shared<repository::PostgresAccountRepository>(writer_.get()),
        std::make_shared<repository::PostgresPositionRepository>(writer_.get()));
    AXON_LOG_INFO(log(), "persistence enabled");
  } else {
    AXON_LOG_WARN(log(), "no database configured: order and fill history lives only in "
                         "memory and is lost on restart");
  }

  // A state change -- from the feed or from reconciliation -- goes out once.
  orders_.register_update_callback([this](const models::Order& order) {
    for (auto* l : listeners_) l->on_order_changed(order);
  });
}

OmsService::~OmsService() { stop(); }

void OmsService::start(const std::map<std::string, VenueRest*>& rests) {
  for (const auto& [venue, exchange] : config_.exchanges) {
    const auto it = rests.find(venue);
    if (it == rests.end() || it->second == nullptr) {
      AXON_LOG_WARN(log(), "[{}] no reconciler: a dropped feed message will not be "
                           "recovered on this venue", venue);
      continue;
    }
    ReconcilerCallbacks rc;
    rc.on_recovered_fill = [this](const models::Fill& fill) { record_recovered_fill(fill); };
    rc.on_positions = [this, venue = venue](const std::vector<models::Position>& positions) {
      portfolio_.update_positions(venue, positions);
      for (auto* l : listeners_) l->on_positions(venue, positions);
    };
    reconcilers_[venue] =
        std::make_unique<Reconciler>(config_, exchange, it->second, &orders_, std::move(rc));
  }
}

void OmsService::poll() {
  for (auto& [_, reconciler] : reconcilers_) reconciler->poll();
}

void OmsService::stop() {
  // Reconcilers first: they borrow REST clients that VenueConnections is
  // about to tear down.
  reconcilers_.clear();
  if (writer_) {
    // Flush what is queued before the process exits; the writer drains on stop.
    writer_->stop();
    writer_.reset();
  }
}

const Reconciler* OmsService::reconciler(const std::string& exchange) const {
  const auto it = reconcilers_.find(exchange);
  return it == reconcilers_.end() ? nullptr : it->second.get();
}

// ---------------------------------------------------------------------------
void OmsService::on_order_update(const std::string& exchange,
                                 const transport::OrderUpdateMsg& venue_msg) {
  // The venue echoes the client order id we sent, which the parser put in
  // internal_order_id. Map it back to the request that produced it -- its real
  // internal id and, above all, its strategy -- before anything is published,
  // or the update goes to every strategy and to none in particular.
  transport::OrderUpdateMsg msg = venue_msg;
  if (const auto sub = orders_.find_submission(exchange, msg.internal_order_id.view())) {
    static_cast<void>(msg.internal_order_id.assign(sub->internal_order_id));
    static_cast<void>(msg.strategy_id.assign(sub->strategy_id.value_or(std::string())));
  }

  // FAST PATH FIRST. The bytes go out before anything below allocates: the
  // decode and the store are the control-plane copy, and none of it is on a
  // co-located strategy's critical path.
  for (auto* l : listeners_) l->on_order_message(msg);

  // decode_order_update allocates. A known compromise: the store is built
  // around the domain Order the Python uses, and converting to it here is
  // what keeps the two implementations behaviourally identical.
  auto order = transport::decode_order_update(msg);
  for (auto* l : listeners_) l->on_order_update(order);
  orders_.update_from_ws(std::move(order));
}

void OmsService::on_fill(const std::string& exchange, const transport::FillMsg& venue_msg) {
  // A venue fill carries no strategy; the order it belongs to does. The order
  // update for it is always handled first (see the parsers), so it is known.
  transport::FillMsg msg = venue_msg;
  if (msg.strategy_id.empty()) {
    if (const auto order = orders_.get_order(std::string(msg.order_id.view()));
        order.has_value() && order->strategy_id.has_value()) {
      static_cast<void>(msg.strategy_id.assign(*order->strategy_id));
    }
  }
  const auto fill = transport::decode_fill(msg);

  // Tell the reconciler this trade arrived normally, so its next pass does not
  // "recover" it and count it twice.
  if (const auto it = reconcilers_.find(exchange); it != reconcilers_.end()) {
    it->second->note_fill(fill.trade_id);
  }
  // Deduplicate BEFORE publishing. A trade a strategy has acted on must not
  // reach it twice; that guarantee is what lets the feed and the reconciler
  // both report fills without coordinating.
  if (!fills_.add_fill(fill)) {
    return;
  }
  publish_fill(&msg, fill, /*recovered=*/false);
}

// A fill the venue had but the feed never delivered. Same idempotency check,
// so a reconciler pass that re-reports a known trade does nothing.
void OmsService::record_recovered_fill(const models::Fill& venue_fill) {
  // A REST fill carries no strategy either; attribute it like a feed fill, or
  // it would go out to every strategy instead of the one that traded.
  models::Fill fill = venue_fill;
  if (!fill.strategy_id.has_value()) {
    if (const auto order = orders_.get_order(fill.order_id);
        order.has_value() && order->strategy_id.has_value()) {
      fill.strategy_id = order->strategy_id;
    }
  }
  if (!fills_.add_fill(fill)) {
    return;
  }
  AXON_LOG_WARN(log(), "recovered fill {} for order {}", fill.trade_id, fill.order_id);
  transport::FillMsg msg{};
  const bool encoded = transport::encode_fill(fill, ++hot_seq_, msg);
  publish_fill(encoded ? &msg : nullptr, fill, /*recovered=*/true);
}

void OmsService::publish_fill(const transport::FillMsg* msg, const models::Fill& fill,
                              bool recovered) {
  for (auto* l : listeners_) l->on_fill(msg, fill, recovered);
}

void OmsService::on_account(const std::string&, const models::AccountSummary& account) {
  portfolio_.update_account(account);
  // Margin ratio is the one risk number worth a gauge: it is what an alert
  // fires on before a liquidation, not after.
  if (!account.equity.is_zero()) {
    util::get_metrics().set_account_margin_ratio(
        account.exchange, account.currency,
        account.maintenance_margin.to_double() / account.equity.to_double());
  }
}

void OmsService::on_session_live(const std::string& exchange) {
  // Everything that happened while disconnected was missed; reconcile now
  // rather than waiting for the next tick.
  if (const auto it = reconcilers_.find(exchange); it != reconcilers_.end()) {
    it->second->on_session_live();
  }
}

// ---------------------------------------------------------------------------
void OmsService::before_place(const std::string& exchange,
                              const models::OrderRequest& request) {
  orders_.register_submission(exchange, request);
}

void OmsService::after_place(const std::string& exchange, const models::OrderRequest& request,
                             PlaceOutcome outcome, const std::optional<models::Order>& order) {
  switch (outcome) {
    case PlaceOutcome::kAccepted:
      if (order.has_value()) {
        orders_.add_order(*order);
      }
      return;
    case PlaceOutcome::kRefused:
      orders_.forget_submission(exchange, request);
      return;
    case PlaceOutcome::kUnknown:
      // The order may be live: keep the registration so its updates route.
      return;
  }
}

}  // namespace axon::oms
