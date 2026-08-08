// Order state. Port of oms/order_manager.py.
//
// Single-threaded by construction. The Python holds an asyncio.Lock around
// every mutation because its callbacks can interleave at await points; here
// the engine loop owns this object outright and nothing else touches it, so
// there is no lock and no way to contend for one. Anything on another thread
// reaches it through an SpscRing, not through a mutex.
//
// FOUR BEHAVIOURS PORTED EXACTLY, each of which exists because of a specific
// failure:
//
//   1. STALE-UPDATE GUARD. An update whose updated_at predates what we already
//      have is dropped. A REST reconciliation response can arrive after a
//      newer WebSocket push and would otherwise roll the order backwards.
//
//   2. add_order FORCES PENDING. Whatever status the placement response
//      carried is discarded and the order is stored as pending with zero
//      filled. Otherwise a market order that comes back already FILLED looks
//      unchanged when the first WebSocket push says FILLED, and the fill
//      callback never fires.
//
//   3. TERMINAL NOTIFICATIONS FIRE ONCE. An order that has been reported
//      filled/cancelled/rejected is remembered for five minutes, so a stale
//      read that slips past the state-changed check cannot double-report a
//      fill to a strategy.
//
//   4. TERMINAL ORDERS LEAVE THE CACHE. Without eviction the map grows for the
//      life of the process.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "calais/models/order.h"

namespace calais::oms {

// Persistence seam. The in-memory implementation is the default and is what
// runs when no database is configured, matching the Python.
class OrderRepository {
 public:
  virtual ~OrderRepository() = default;
  virtual void save(const models::Order& order) = 0;
  virtual void update(const models::Order& order) = 0;
  virtual std::optional<models::Order> get(const std::string& order_id) = 0;
  virtual std::vector<models::Order> all() = 0;
};

class InMemoryOrderRepository final : public OrderRepository {
 public:
  void save(const models::Order& order) override { orders_[order.order_id] = order; }
  void update(const models::Order& order) override { orders_[order.order_id] = order; }
  std::optional<models::Order> get(const std::string& order_id) override {
    const auto it = orders_.find(order_id);
    return it == orders_.end() ? std::nullopt : std::optional<models::Order>(it->second);
  }
  std::vector<models::Order> all() override {
    std::vector<models::Order> out;
    out.reserve(orders_.size());
    for (const auto& [_, o] : orders_) {
      out.push_back(o);
    }
    return out;
  }

 private:
  std::unordered_map<std::string, models::Order> orders_;
};

class OrderStore {
 public:
  using UpdateCallback = std::function<void(const models::Order&)>;

  explicit OrderStore(std::shared_ptr<OrderRepository> repository = nullptr);

  // Registers a newly placed order. Stored as PENDING with zero filled --
  // see (2) above. Does NOT notify: order state is driven entirely by venue
  // pushes and by reconciliation, never by our own placement response.
  void add_order(models::Order order);

  // Applies an update from any source. Notifies subscribers only on a real
  // state change.
  void update_order(models::Order order);

  // Same as update_order, but records the ack latency on the first venue push
  // for an order we placed.
  void update_from_ws(models::Order order);

  std::optional<models::Order> get_order(const std::string& order_id);
  std::vector<models::Order> all_orders() const;
  std::vector<models::Order> active_orders() const;

  // Overwrites local state from an authoritative venue snapshot. Differences
  // are logged before being applied -- a silent correction here means the
  // WebSocket feed lost something, which is worth knowing about.
  void reconcile(const std::vector<models::Order>& orders);

  void register_update_callback(UpdateCallback callback);
  void clear_callbacks();

  std::size_t cached_order_count() const noexcept { return cache_.size(); }

 private:
  void notify(const models::Order& order);
  void record_terminal_latency(const models::Order& order);
  void purge_expired_terminals(double now_seconds);

  std::shared_ptr<OrderRepository> repository_;
  std::unordered_map<std::string, models::Order> cache_;
  std::vector<UpdateCallback> callbacks_;

  // order_id -> monotonic seconds when a terminal state was first reported.
  std::unordered_map<std::string, double> notified_terminal_;
  static constexpr double kTerminalTtlSeconds = 300.0;

  // Latency bookkeeping, keyed by order_id.
  std::unordered_map<std::string, double> submit_monotonic_;
  std::unordered_set<std::string> first_ws_seen_;
};

}  // namespace calais::oms
