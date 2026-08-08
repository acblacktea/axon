// One venue's private WebSocket session: connect, authenticate, subscribe,
// consume, reconnect.
//
// Replaces the reconnect/heartbeat machinery in util/websocket_base.py plus
// the per-venue subclasses. Everything is driven by poll(), which never
// blocks, so one thread can own several venues.
//
// SESSION STATE, which is the part that matters:
//
//   kDisconnected -> kConnecting -> kAuthenticating -> kSubscribing -> kLive
//                        ^                                              |
//                        +--------------- kBackoff <--------------------+
//
// A drop at ANY point goes to kBackoff and then all the way round again. There
// is no partial recovery: re-authenticating on a socket that already failed
// once is how a session ends up half-subscribed, receiving order updates but
// not fills, which looks like the venue is broken.
//
// Backoff is exponential with jitter, matching _reconnect in the Python. The
// jitter matters more than it looks: without it, every venue session in a
// fleet that lost connectivity together reconnects in lockstep and hits the
// rate limit as one.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "calais/config.h"
#include "calais/models/portfolio.h"
#include "calais/net/ws_connection.h"
#include "calais/oms/venue_rest.h"
#include "calais/transport/hot_messages.h"

namespace calais::oms {

enum class SessionState : std::uint8_t {
  kDisconnected = 0,
  kConnecting = 1,
  kAuthenticating = 2,
  kSubscribing = 3,
  kLive = 4,
  kBackoff = 5,
  // Terminal: the credentials are wrong, or the venue said something that
  // retrying cannot fix. Distinct from kBackoff so a supervisor can stop
  // rather than loop forever against a 401.
  kFatal = 6,
};

const char* to_string(SessionState s) noexcept;

// What a session emits. Deliberately the fixed-layout hot messages rather than
// domain objects: the venue parsers produce them and the consumer copies out
// what it needs, so nothing allocates between the socket and the order book.
struct VenueSessionHandlers {
  std::function<void(const transport::OrderUpdateMsg&)> on_order;
  std::function<void(const transport::FillMsg&)> on_fill;
  std::function<void(const models::AccountSummary&)> on_account;
  // Called once each time the session reaches kLive, including after a
  // reconnect. A consumer that caches venue state should invalidate here --
  // anything that happened while disconnected was missed.
  std::function<void()> on_live;
  std::function<void(const std::string&)> on_error;

  // A reply to a request WE sent that the session itself did not claim --
  // i.e. an order-entry response. `payload` is the raw venue message so the
  // EMS can parse the venue's own result shape without the session needing to
  // know anything about orders.
  std::function<void(std::int64_t id, bool success, std::string_view payload,
                     std::string_view error)>
      on_rpc_reply;
};

class VenueSession {
 public:
  VenueSession(ExchangeConfig exchange, WebSocketConfig ws_config,
               RuntimeConfig runtime, const net::TlsContext* tls,
               VenueSessionHandlers handlers);
  virtual ~VenueSession();

  VenueSession(const VenueSession&) = delete;
  VenueSession& operator=(const VenueSession&) = delete;

  // Non-blocking. Call from the engine loop.
  void poll();

  void start();
  void stop();

  SessionState state() const noexcept { return state_; }
  bool live() const noexcept { return state_ == SessionState::kLive; }
  const std::string& exchange_name() const noexcept { return exchange_.name; }
  const std::string& last_error() const noexcept { return last_error_; }
  std::uint64_t reconnect_count() const noexcept { return reconnects_; }

  // Sends a raw venue message on the live connection. Order entry over the
  // WebSocket goes through here. Returns false if the session is not live or
  // the send buffer is full -- both of which the caller must handle rather
  // than assume away.
  bool send_raw(std::string_view payload);

 protected:
  // Per-venue hooks. Each returns false if it could not proceed, which drops
  // the session into backoff.
  virtual bool send_authentication() = 0;
  virtual bool send_subscriptions() = 0;
  // Feeds one inbound frame to the venue parser. Returns false on a protocol
  // error serious enough to warrant reconnecting.
  virtual bool handle_frame(const std::byte* data, std::size_t len,
                            std::size_t capacity) = 0;
  // Called when the venue's reply to authentication arrives. Default
  // implementation assumes any successful reply means authenticated.
  virtual void on_auth_reply(bool success, const std::string& error);
  virtual void on_subscribe_reply(bool success, const std::string& error);

  // Endpoint, chosen by env.
  virtual std::string host() const = 0;
  virtual std::string path() const = 0;

  void note_authenticated();
  void note_subscribed();
  void fail_session(std::string reason, bool fatal = false);

  // A venue may need to do work BEFORE the socket is opened -- Binance has to
  // fetch a listenKey over REST. Returning false holds the session in
  // kConnecting until a later poll; the default does nothing and connects
  // immediately.
  virtual bool prepare_connect() { return true; }
  // Per-poll work while live, e.g. renewing a credential on a timer.
  virtual void on_poll() {}

  ExchangeConfig exchange_;
  WebSocketConfig ws_config_;
  RuntimeConfig runtime_;
  VenueSessionHandlers handlers_;

 private:
  void enter_backoff();
  void begin_connect();
  double now_seconds() const;

  const net::TlsContext* tls_ = nullptr;
  net::WsConnection connection_;
  SessionState state_ = SessionState::kDisconnected;
  std::string last_error_;

  bool started_ = false;
  double backoff_until_ = 0.0;
  int backoff_attempt_ = 0;
  std::uint64_t reconnects_ = 0;
  double last_activity_ = 0.0;

  // Deadline for the CURRENT handshake stage: connect, authenticate,
  // subscribe. Each stage gets the full budget and resets it on entry.
  // Without this a venue that accepts the auth frame and never replies leaves
  // the session parked forever -- the liveness watchdog below only runs once
  // the session is live, so nothing else would ever notice.
  double handshake_deadline_ = 0.0;
  void arm_handshake_deadline();

  // Adapter so WsConnection's template poll() can call back into us.
  struct Bridge;
  std::unique_ptr<Bridge> bridge_;
};

// Builds the right subclass for a venue name. Returns nullptr for an unknown
// venue rather than throwing, so a config listing an exchange this build does
// not support fails with a clear message at startup.
// `rest` is required by Binance, whose user data stream is keyed by a
// listenKey obtained over REST and appended to the URL; the other three
// authenticate over the socket and ignore it.
std::unique_ptr<VenueSession> make_venue_session(
    const ExchangeConfig& exchange, const WebSocketConfig& ws_config,
    const RuntimeConfig& runtime, const net::TlsContext* tls, VenueRest* rest,
    VenueSessionHandlers handlers);

}  // namespace calais::oms
