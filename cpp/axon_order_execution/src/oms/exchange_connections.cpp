#include "axon/oms/exchange_connections.h"

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

ExchangeConnections::ExchangeConnections(const Config& config)
    : config_(config), tls_(std::make_unique<net::TlsContext>()) {
  if (config_.runtime.verify_tls) {
    tls_->use_default_trust_store();
    if (!config_.runtime.extra_ca_file.empty()) {
      // A corporate proxy that re-signs TLS needs its root trusted, or every
      // exchange connection fails verification.
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
    AXON_LOG_WARN(log(), "TLS VERIFICATION IS DISABLED. Never run this against a live exchange.");
    tls_->set_verify_peer(false);
  }
  http_ = std::make_unique<net::HttpClient>(tls_.get());
}

ExchangeConnections::~ExchangeConnections() { stop(); }

void ExchangeConnections::start(ExchangeEventSink& feed, RpcReplyHandler rpc_replies) {
  for (const auto& [exchange, exchange_config] : config_.exchanges) {
    // One REST client per exchange, shared by the session (Binance's listenKey),
    // the reconciler (snapshots) and ticker reads, so all reuse the same
    // keep-alive connections. Never order entry: that rides the sessions.
    auto rest = make_exchange_rest(exchange_config, http_.get());
    ExchangeRest* rest_ptr = rest.get();
    if (rest) {
      rests_[exchange] = std::move(rest);
    }

    ExchangeSessionHandlers handlers;
    handlers.on_order = [&feed, exchange](const transport::OrderUpdateMsg& m) {
      feed.on_order_update(exchange, m);
    };
    handlers.on_fill = [&feed, exchange](const transport::FillMsg& m) {
      feed.on_fill(exchange, m);
    };
    handlers.on_account = [&feed, exchange](const models::AccountSummary& a) {
      feed.on_account(exchange, a);
    };
    handlers.on_live = [&feed, exchange] {
      AXON_LOG_INFO(log(), "[{}] session live", exchange);
      feed.on_session_live(exchange);
    };
    handlers.on_error = [exchange](const std::string& what) {
      AXON_LOG_WARN(log(), "[{}] {}", exchange, what);
    };
    // Deribit and OKX send orders on this session, so their replies land here.
    handlers.on_rpc_reply = [rpc_replies, exchange](std::int64_t id, bool ok,
                                                 std::string_view payload,
                                                 std::string_view error) {
      rpc_replies(exchange, id, ok, payload, error);
    };

    auto session = make_exchange_session(exchange_config, config_.websocket, config_.runtime,
                                      tls_.get(), rest_ptr, std::move(handlers));
    if (!session) {
      // Better a clear refusal at startup than a connection that never
      // authenticates for reasons nobody can see.
      AXON_LOG_ERROR(log(), "exchange_config '{}' is configured but not supported by this build; "
                            "skipping it", exchange);
      rests_.erase(exchange);
      continue;
    }
    session->start();
    sessions_[exchange] = std::move(session);
    exchanges_.push_back(exchange);

    // Binance and Bybit need a SECOND connection for order entry. There is no
    // REST order path, so on those exchanges this connection IS order entry:
    // while it is down, the exchange cannot trade.
    ExchangeSessionHandlers trade_handlers;
    trade_handlers.on_rpc_reply = [rpc_replies, exchange](std::int64_t id, bool ok,
                                                       std::string_view payload,
                                                       std::string_view error) {
      rpc_replies(exchange, id, ok, payload, error);
    };
    trade_handlers.on_error = [exchange](const std::string& what) {
      AXON_LOG_ERROR(log(), "[{}] ORDER ENTRY IS DOWN: {}", exchange, what);
    };
    if (auto trade = make_trade_session(exchange_config, config_.websocket, config_.runtime,
                                        tls_.get(), std::move(trade_handlers))) {
      trade->start();
      trade_sessions_[exchange] = std::move(trade);
    }
  }

  if (sessions_.empty()) {
    AXON_LOG_WARN(log(), "no exchange sessions started");
  }
}

void ExchangeConnections::poll() {
  for (auto& [_, s] : sessions_) s->poll();
  for (auto& [_, s] : trade_sessions_) s->poll();
  http_->poll();
}

void ExchangeConnections::stop() {
  for (auto& [_, s] : trade_sessions_) s->stop();
  trade_sessions_.clear();
  for (auto& [_, s] : sessions_) s->stop();
  sessions_.clear();
  rests_.clear();
  exchanges_.clear();
}

ExchangeRest* ExchangeConnections::rest(const std::string& exchange) const {
  const auto it = rests_.find(exchange);
  return it == rests_.end() ? nullptr : it->second.get();
}

ExchangeSession* ExchangeConnections::session(const std::string& exchange) const {
  const auto it = sessions_.find(exchange);
  return it == sessions_.end() ? nullptr : it->second.get();
}

ExchangeSession* ExchangeConnections::trade_session(const std::string& exchange) const {
  const auto it = trade_sessions_.find(exchange);
  return it == trade_sessions_.end() ? nullptr : it->second.get();
}

std::map<std::string, ExchangeRest*> ExchangeConnections::rests() const {
  std::map<std::string, ExchangeRest*> out;
  for (const auto& [exchange, rest] : rests_) out[exchange] = rest.get();
  return out;
}

std::string ExchangeConnections::state_summary() const {
  std::string states;
  for (const auto& exchange : exchanges_) {
    if (!states.empty()) states += ' ';
    states += exchange;
    states += '=';
    states += to_string(sessions_.at(exchange)->state());
    if (const auto t = trade_sessions_.find(exchange); t != trade_sessions_.end()) {
      states += "/trade=";
      states += to_string(t->second->state());
    }
  }
  return states;
}

}  // namespace axon::oms
