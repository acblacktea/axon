// Strategy-side client. Port of client/strategy_client.py, plus the fast path
// the Python has no equivalent for.
//
// TWO TRANSPORTS, and picking between them is the whole point:
//
//   FAST PATH  shared-memory rings. place_order and cancel_order become a
//              struct copy into a ring -- ~0.1-0.3us one way, no JSON, no
//              syscall. Order and fill events arrive the same way. Requires
//              the strategy to run on the same host as the engine and to
//              busy-poll.
//
//   CONTROL    ZMQ DEALER/SUB, byte-compatible with the Python client. Every
//   PLANE      query, and every command when the fast path is unavailable.
//              ~30-60us per round trip.
//
// The client uses the fast path when one is configured and falls back to ZMQ
// otherwise, so a strategy is written once and gets whichever is available.
//
// FIRE-AND-FORGET ON THE FAST PATH. A ring push has no reply: the venue's
// answer arrives later as an order update, exactly as it would from the
// WebSocket. A strategy that needs to know its order id synchronously must use
// the control plane -- which is a real trade, not an oversight, because
// waiting for a reply is what makes the slow path slow.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "calais/core/shm_ring.h"
#include "calais/models/fill.h"
#include "calais/models/order.h"
#include "calais/models/portfolio.h"
#include "calais/transport/hot_messages.h"
#include "calais/transport/messages.h"

namespace calais::client {

struct StrategyClientConfig {
  std::string strategy_id;
  std::string router_endpoint = "tcp://localhost:5555";
  std::string pub_endpoint = "tcp://localhost:5556";

  // Empty disables the fast path and routes everything through ZMQ.
  std::string shm_directory;

  // How long a control-plane request waits before failing.
  int request_timeout_ms = 5000;
};

struct StrategyEvents {
  std::function<void(const models::Order&)> on_order;
  std::function<void(const models::Fill&)> on_fill;
  // Raw hot messages, delivered from the fast path with their latency stamps
  // intact. A strategy that cares about the last microsecond uses these; one
  // that does not uses the decoded callbacks above.
  std::function<void(const transport::OrderUpdateMsg&)> on_order_fast;
  std::function<void(const transport::FillMsg&)> on_fill_fast;
};

class StrategyClient {
 public:
  StrategyClient();
  ~StrategyClient();

  StrategyClient(const StrategyClient&) = delete;
  StrategyClient& operator=(const StrategyClient&) = delete;

  void connect(const StrategyClientConfig& config, StrategyEvents events = {});
  void close();

  bool fast_path_available() const noexcept;

  // Drains both transports and dispatches events. Call in a busy loop.
  std::size_t poll();

  // --- order entry -------------------------------------------------------

  // Fast path when available: a struct copy, no reply. Returns false only if
  // the ring is full or the request is malformed.
  bool place_order_fast(const std::string& exchange,
                        const models::OrderRequest& request);
  bool cancel_order_fast(const std::string& exchange, const std::string& instrument,
                         const std::string& order_id);

  // Control plane, blocking until the engine answers or the timeout expires.
  // These are the ones that return the venue's order back.
  std::optional<models::Order> place_order(const std::string& exchange,
                                           const models::OrderRequest& request,
                                           std::string& error);
  bool cancel_order(const std::string& exchange, const std::string& order_id,
                    std::string& error);
  std::optional<models::Order> modify_order(const std::string& exchange,
                                            const std::string& order_id,
                                            std::optional<core::Qty> amount,
                                            std::optional<core::Price> price,
                                            std::string& error);

  // --- queries -----------------------------------------------------------
  std::optional<models::Order> get_order(const std::string& order_id,
                                         std::string& error);
  std::vector<models::Order> get_active_orders(std::string& error);
  std::optional<models::Ticker> get_ticker(const std::string& exchange,
                                           const std::string& instrument,
                                           std::string& error);
  std::vector<models::Position> get_positions(const std::string& exchange,
                                              std::string& error);
  std::optional<models::AccountSummary> get_account_summary(
      const std::string& exchange, const std::string& currency, std::string& error);
  std::vector<models::Fill> get_fills_by_order(const std::string& order_id,
                                               std::string& error);

  struct Stats {
    std::uint64_t fast_sent = 0;
    std::uint64_t fast_rejected = 0;
    std::uint64_t fast_events = 0;
    std::uint64_t requests = 0;
    std::uint64_t request_failures = 0;
  };
  const Stats& stats() const noexcept { return stats_; }

 private:
  // One request/response round trip on the DEALER socket.
  std::optional<transport::Json> request(const std::string& command_type,
                                         const transport::Json& payload,
                                         std::string& error);

  struct Impl;
  std::unique_ptr<Impl> impl_;
  StrategyClientConfig config_;
  StrategyEvents events_;
  Stats stats_;
};

}  // namespace calais::client
