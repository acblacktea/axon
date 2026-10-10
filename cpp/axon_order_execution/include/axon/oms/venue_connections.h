// Every connection the engine holds to the venues, in one place.
//
// Per configured venue: a REST client (reconciliation snapshots, Binance's
// listenKey, tickers), the private feed session (order, fill and account
// pushes), and -- on Binance and Bybit -- a second, order-entry-only session.
// Plus what they all share: the TLS context and the HTTP client.
//
// WHY ITS OWN LAYER. The connections are used by both sides of the engine:
// the OMS consumes the feed, and the EMS sends orders -- on the trade session
// where a venue has one, and on the FEED session itself on Deribit and OKX.
// Owned by either side, the other would have to reach through it. So this
// layer owns them, routes what arrives -- feed events to a VenueEventSink (the
// OMS), order-entry replies to an RpcReplyHandler (the EMS) -- and hands out
// the sessions and REST clients to whoever needs them.
//
// Single-threaded, driven by poll() from the engine loop like everything else.

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "axon/config.h"
#include "axon/models/portfolio.h"
#include "axon/net/http_client.h"
#include "axon/net/tls_stream.h"
#include "axon/oms/venue_rest.h"
#include "axon/oms/venue_session.h"
#include "axon/transport/hot_messages.h"

namespace axon::oms {

// What a venue's private feed produces, tagged with the venue it came from.
// Implemented by the OMS.
class VenueEventSink {
 public:
  virtual ~VenueEventSink() = default;
  virtual void on_order_update(const std::string& exchange,
                               const transport::OrderUpdateMsg& msg) = 0;
  virtual void on_fill(const std::string& exchange, const transport::FillMsg& msg) = 0;
  virtual void on_account(const std::string& exchange,
                          const models::AccountSummary& account) = 0;
  // The feed session reached live, first time or after a reconnect.
  virtual void on_session_live(const std::string& exchange) = 0;
};

// A reply to an order-entry request, from whichever session carried it.
// Wired to EmsService::on_rpc_reply.
using RpcReplyHandler = std::function<void(const std::string& exchange, std::int64_t id,
                                           bool success, std::string_view payload,
                                           std::string_view error)>;

class VenueConnections {
 public:
  // Sets up TLS from config.runtime. Throws if a configured extra trust
  // anchor cannot be read -- a missing CA file must stop startup, not show up
  // later as every connection failing verification.
  explicit VenueConnections(const Config& config);
  ~VenueConnections();

  VenueConnections(const VenueConnections&) = delete;
  VenueConnections& operator=(const VenueConnections&) = delete;

  // Builds and starts the connections for every configured venue. A venue
  // this build has no session for is skipped, loudly. Starting only begins
  // the connection; nothing touches the network until poll().
  void start(VenueEventSink& feed, RpcReplyHandler rpc_replies);

  // Drives every session and the HTTP client. Non-blocking.
  void poll();

  // Trade sessions first, then the feeds, then the REST clients. Safe to call
  // twice, or without start().
  void stop();

  // Lookups by venue name; nullptr when the venue has none.
  VenueRest* rest(const std::string& exchange) const;
  VenueSession* session(const std::string& exchange) const;
  VenueSession* trade_session(const std::string& exchange) const;

  // Every venue with a running feed session, in config order.
  const std::vector<std::string>& venues() const noexcept { return venues_; }
  std::map<std::string, VenueRest*> rests() const;

  net::HttpClient& http() noexcept { return *http_; }
  // "binance=live okx=backoff ..." for the heartbeat line.
  std::string state_summary() const;

 private:
  const Config& config_;
  std::unique_ptr<net::TlsContext> tls_;
  std::unique_ptr<net::HttpClient> http_;
  std::vector<std::string> venues_;
  std::map<std::string, std::unique_ptr<VenueRest>> rests_;
  std::map<std::string, std::unique_ptr<VenueSession>> sessions_;
  // Separate from sessions_ because they carry no feed and are torn down
  // first: a trade session outliving the EMS that sends on it would be a
  // use-after-free on shutdown.
  std::map<std::string, std::unique_ptr<VenueSession>> trade_sessions_;
};

}  // namespace axon::oms
