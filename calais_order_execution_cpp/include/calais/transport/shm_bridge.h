// The hot path: shared-memory rings between the engine and co-located
// strategies, carrying fixed-layout binary messages.
//
// THIS IS THE PATH THE WHOLE DESIGN EXISTS FOR. The ZMQ control plane is for
// queries, subscriptions and Python strategies; it costs ~30-60us per round
// trip and builds JSON at both ends. This costs ~0.1-0.3us one way and copies
// a POD struct. Measured comparison in bench/bench_main.cpp.
//
// ONE RING PAIR PER STRATEGY, because the rings are SPSC:
//
//   <dir>/calais.<strategy>.events    engine -> strategy  (orders, fills)
//   <dir>/calais.<strategy>.commands  strategy -> engine  (place, cancel)
//
// A broadcast ring shared by several readers would need MPMC and would lose
// the property that makes this fast. Per-strategy rings also mean one slow
// strategy cannot delay another.
//
// THE CONSUMER MUST BUSY-POLL. Blocking on a condition variable to save a core
// hands back the 2-10us futex wakeup and leaves you worse off than ZMQ ipc.
// That is the entry fee, and it is why this is opt-in per strategy rather than
// the default.
//
// BACKPRESSURE IS A DROP, NOT A BLOCK. If a strategy stops reading, its event
// ring fills and further events are dropped with a counter. Blocking the
// engine loop on a wedged strategy would take down every venue feed.
//
// Put `dir` on /dev/shm on Linux (tmpfs). A path on a real filesystem lets the
// kernel try to write ring pages back to disk.

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "calais/core/histogram.h"
#include "calais/core/latency.h"
#include "calais/core/shm_ring.h"
#include "calais/transport/hot_messages.h"

namespace calais::transport {

struct ShmBridgeStats {
  std::uint64_t events_published = 0;
  std::uint64_t events_dropped = 0;
  std::uint64_t commands_received = 0;
};

class ShmBridge {
 public:
  // Called for each command a strategy pushes. Runs on the engine thread.
  using PlaceHandler = std::function<void(const std::string& strategy,
                                          const PlaceOrderMsg&)>;
  using CancelHandler = std::function<void(const std::string& strategy,
                                           const CancelOrderMsg&)>;

  ShmBridge() = default;

  // Creates the ring pair for each strategy. Throws if a ring cannot be
  // mapped -- a strategy that thinks it has a fast path but does not would
  // silently fall back to nothing.
  void start(const std::string& directory,
             const std::vector<std::string>& strategies,
             std::uint32_t slot_count = 4096);
  void stop();

  void set_handlers(PlaceHandler place, CancelHandler cancel) {
    on_place_ = std::move(place);
    on_cancel_ = std::move(cancel);
  }

  // Drains every strategy's command ring. Returns how many were handled.
  std::size_t poll();

  // Publishes to one strategy, or to all when `strategy` is empty -- an order
  // update with no strategy id belongs to whoever is listening.
  void publish_order(const OrderUpdateMsg& msg, std::string_view strategy);
  void publish_fill(const FillMsg& msg, std::string_view strategy);

  bool active() const noexcept { return !rings_.empty(); }
  ShmBridgeStats stats() const noexcept { return stats_; }

  // Per-stage latency of everything that came through the command ring.
  // Populated from the stamps each message carries, so it measures the real
  // path rather than a benchmark of it.
  const core::StageLatency& command_latency() const noexcept { return latency_; }
  std::string latency_report() const { return latency_.report(); }

 private:
  struct Pair {
    std::unique_ptr<core::ShmRing> events;    // engine -> strategy
    std::unique_ptr<core::ShmRing> commands;  // strategy -> engine
  };

  template <typename Msg>
  void publish(const Msg& msg, std::string_view strategy, HotMsgType type);

  std::map<std::string, Pair> rings_;
  PlaceHandler on_place_;
  CancelHandler on_cancel_;
  ShmBridgeStats stats_;
  core::StageLatency latency_{{"origin", "enqueued", "dequeued", "handled"}};
};

}  // namespace calais::transport
