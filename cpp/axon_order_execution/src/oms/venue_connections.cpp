#include "axon/oms/venue_connections.h"

#include <fstream>
#include <sstream>
#include <stdexcept>

#include "axon/util/logging.h"

namespace axon::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.connections");
  return logger;
}

}  // namespace

VenueConnections::VenueConnections(const Config& config)
    : config_(config), tls_(std::make_unique<net::TlsContext>()) {
  if (config_.runtime.verify_tls) {
    tls_->use_default_trust_store();
    if (!config_.runtime.extra_ca_file.empty()) {
      // A corporate proxy that re-signs TLS needs its root trusted, or every
      // venue connection fails verification.
      std::ifstream in(config_.runtime.extra_ca_file);
      if (!in) {
        throw std::runtime_error("cannot read cpp.extra_ca_file " +
                                 config_.runtime.extra_ca_file);
      }
      std::stringstream pem;
      pem << in.rdbuf();
      tls_->add_trusted_certificate_pem(pem.str());
      AXON_LOG_INFO(log(), "added extra trust anchor {}", config_.runtime.extra_ca_file);
    }
  } else {
    // Loud, because a silently unverified connection to an exchange looks
    // exactly like a verified one until it is not.
    AXON_LOG_WARN(log(), "TLS VERIFICATION IS DISABLED. Never run this against a live venue.");
    tls_->set_verify_peer(false);
  }
  http_ = std::make_unique<net::HttpClient>(tls_.get());
}

VenueConnections::~VenueConnections() { stop(); }

void VenueConnections::start(VenueEventSink& feed, RpcReplyHandler rpc_replies) {
  for (const auto& [venue, exchange] : config_.exchanges) {
    // One REST client per venue, shared by the session (Binance's listenKey),
    // the reconciler (snapshots) and ticker reads, so all reuse the same
    // keep-alive connections. Never order entry: that rides the sessions.
    auto rest = make_venue_rest(exchange, http_.get());
    VenueRest* rest_ptr = rest.get();
    if (rest) {
      rests_[venue] = std::move(rest);
    }

    VenueSessionHandlers handlers;
    handlers.on_order = [&feed, venue](const transport::OrderUpdateMsg& m) {
      feed.on_order_update(venue, m);
    };
    handlers.on_fill = [&feed, venue](const transport::FillMsg& m) { feed.on_fill(venue, m); };
    handlers.on_account = [&feed, venue](const models::AccountSummary& a) {
      feed.on_account(venue, a);
    };
    handlers.on_live = [&feed, venue] {
      AXON_LOG_INFO(log(), "[{}] session live", venue);
      feed.on_session_live(venue);
    };
    handlers.on_error = [venue](const std::string& what) {
      AXON_LOG_WARN(log(), "[{}] {}", venue, what);
    };
    // Deribit and OKX send orders on this session, so their replies land here.
    handlers.on_rpc_reply = [rpc_replies, venue](std::int64_t id, bool ok,
                                                 std::string_view payload,
                                                 std::string_view error) {
      rpc_replies(venue, id, ok, payload, error);
    };

    auto session = make_venue_session(exchange, config_.websocket, config_.runtime,
                                      tls_.get(), rest_ptr, std::move(handlers));
    if (!session) {
      // Better a clear refusal at startup than a connection that never
      // authenticates for reasons nobody can see.
      AXON_LOG_ERROR(log(), "exchange '{}' is configured but not supported by this build; "
                            "skipping it", venue);
      rests_.erase(venue);
      continue;
    }
    session->start();
    sessions_[venue] = std::move(session);
    venues_.push_back(venue);

    // Binance and Bybit need a SECOND connection for order entry. There is no
    // REST order path, so on those venues this connection IS order entry:
    // while it is down, the venue cannot trade.
    VenueSessionHandlers trade_handlers;
    trade_handlers.on_rpc_reply = [rpc_replies, venue](std::int64_t id, bool ok,
                                                       std::string_view payload,
                                                       std::string_view error) {
      rpc_replies(venue, id, ok, payload, error);
    };
    trade_handlers.on_error = [venue](const std::string& what) {
      AXON_LOG_ERROR(log(), "[{}] ORDER ENTRY IS DOWN: {}", venue, what);
    };
    if (auto trade = make_trade_session(exchange, config_.websocket, config_.runtime,
                                        tls_.get(), std::move(trade_handlers))) {
      trade->start();
      trade_sessions_[venue] = std::move(trade);
    }
  }

  if (sessions_.empty()) {
    AXON_LOG_WARN(log(), "no venue sessions started");
  }
}

void VenueConnections::poll() {
  for (auto& [_, s] : sessions_) s->poll();
  for (auto& [_, s] : trade_sessions_) s->poll();
  http_->poll();
}

void VenueConnections::stop() {
  for (auto& [_, s] : trade_sessions_) s->stop();
  trade_sessions_.clear();
  for (auto& [_, s] : sessions_) s->stop();
  sessions_.clear();
  rests_.clear();
  venues_.clear();
}

VenueRest* VenueConnections::rest(const std::string& exchange) const {
  const auto it = rests_.find(exchange);
  return it == rests_.end() ? nullptr : it->second.get();
}

VenueSession* VenueConnections::session(const std::string& exchange) const {
  const auto it = sessions_.find(exchange);
  return it == sessions_.end() ? nullptr : it->second.get();
}

VenueSession* VenueConnections::trade_session(const std::string& exchange) const {
  const auto it = trade_sessions_.find(exchange);
  return it == trade_sessions_.end() ? nullptr : it->second.get();
}

std::map<std::string, VenueRest*> VenueConnections::rests() const {
  std::map<std::string, VenueRest*> out;
  for (const auto& [venue, rest] : rests_) out[venue] = rest.get();
  return out;
}

std::string VenueConnections::state_summary() const {
  std::string states;
  for (const auto& venue : venues_) {
    if (!states.empty()) states += ' ';
    states += venue;
    states += '=';
    states += to_string(sessions_.at(venue)->state());
    if (const auto t = trade_sessions_.find(venue); t != trade_sessions_.end()) {
      states += "/trade=";
      states += to_string(t->second->state());
    }
  }
  return states;
}

}  // namespace axon::oms
