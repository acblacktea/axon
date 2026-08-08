// ZMQ control plane. Port of transport/server.py.
//
// ROUTER on 5555 takes commands from strategy DEALER sockets; PUB on 5556
// broadcasts order/fill/account events with strategy_id as the topic so a
// strategy only receives its own.
//
// THE WIRE FORMAT IS THE COMPATIBILITY CONTRACT. Frames, JSON shape and topic
// are identical to the Python server's, so an existing Python StrategyClient
// connects to this engine unmodified. That is what makes the port incremental
// and what makes cross-checking possible at all.
//
// POLLED, NOT BLOCKING. The Python runs a coroutine per command; this is
// drained from the engine loop with a zero-timeout poll. Commands are handled
// inline and in order, which is simpler and, for a control plane running at
// human rates, strictly better: no task ordering to reason about.
//
// NOT THE HOT PATH. Order entry that matters for latency goes through the
// shared-memory ring in core/shm_ring.h. This carries queries, subscriptions,
// and anything a Python strategy sends.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "calais/config.h"
#include "calais/transport/messages.h"

namespace calais::transport {

class ZmqServer {
 public:
  // Sends a reply for the command it was created from. Safe to call later --
  // it holds the ROUTER identity, not a pointer into the loop.
  //
  // DEFERRED REPLIES ARE THE POINT. Placing an order is a network round trip;
  // answering it inline would stall every venue feed for its duration. The
  // handler keeps the sink, returns immediately, and calls it when the venue
  // answers.
  //
  // Calling a sink twice sends two replies, which would desynchronise a
  // DEALER; the server drops the second and logs it.
  class ResponseSink {
   public:
    ResponseSink() = default;
    ResponseSink(ZmqServer* server, std::string identity)
        : server_(server), identity_(std::move(identity)) {}

    void operator()(const Response& response);
    bool valid() const noexcept { return server_ != nullptr && !sent_; }

   private:
    ZmqServer* server_ = nullptr;
    std::string identity_;
    bool sent_ = false;
  };

  // Throwing is allowed; the server turns an exception into an unsuccessful
  // Response rather than letting it escape the loop.
  using CommandHandler = std::function<void(const Command&, ResponseSink)>;

  ZmqServer();
  ~ZmqServer();

  ZmqServer(const ZmqServer&) = delete;
  ZmqServer& operator=(const ZmqServer&) = delete;

  // Binds both sockets. Throws if either endpoint is unavailable -- an engine
  // that started but is unreachable is worse than one that refused to start.
  void start(const ZmqConfig& config, CommandHandler handler);
  void stop();

  // Drains whatever is queued. Returns how many commands were handled.
  std::size_t poll();

  // Publishes an event. The topic is the strategy_id, matching the Python, so
  // a subscriber filters server-side rather than receiving everything.
  void publish(const Event& event);

  bool running() const noexcept { return running_; }
  std::uint64_t commands_handled() const noexcept { return commands_handled_; }
  std::uint64_t events_published() const noexcept { return events_published_; }

 private:
  friend class ResponseSink;
  void send_response(const std::string& identity, const Response& response);

  struct Impl;
  std::unique_ptr<Impl> impl_;
  CommandHandler handler_;
  bool running_ = false;
  std::uint64_t commands_handled_ = 0;
  std::uint64_t events_published_ = 0;
};

}  // namespace calais::transport
