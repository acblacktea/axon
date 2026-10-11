#include "axon/oms/exchange_session.h"

#include <cmath>
#include <cstring>
#include <optional>
#include <random>

#include "axon/core/clock.h"
#include "axon/util/logging.h"
#include "axon/util/metrics.h"
#include "axon/oms/detail/session_util.h"
#include "axon/oms/oms_exchange.h"

namespace axon::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.session");
  return logger;
}

using detail::monotonic_seconds;

double jitter() {
  static thread_local std::mt19937_64 rng{std::random_device{}()};
  static thread_local std::uniform_real_distribution<double> dist(0.5, 1.5);
  return dist(rng);
}

}  // namespace

const char* to_string(SessionState s) noexcept {
  switch (s) {
    case SessionState::kDisconnected: return "disconnected";
    case SessionState::kConnecting: return "connecting";
    case SessionState::kAuthenticating: return "authenticating";
    case SessionState::kSubscribing: return "subscribing";
    case SessionState::kLive: return "live";
    case SessionState::kBackoff: return "backoff";
    case SessionState::kFatal: return "fatal";
  }
  return "?";
}

// ---------------------------------------------------------------------------
// The adapter WsConnection::poll() calls into. WsConnection is a template on
// the handler so nothing is virtual on the receive path; this keeps that
// property while letting ExchangeSession stay a normal polymorphic class.
// ---------------------------------------------------------------------------
struct ExchangeSession::Bridge {
  ExchangeSession* session = nullptr;

  void on_open() {
    AXON_LOG_INFO(log(), "[{}] websocket open, authenticating",
                    session->exchange_.name);
    session->state_ = SessionState::kAuthenticating;
    session->arm_handshake_deadline();
    if (!session->send_authentication()) {
      session->fail_session("could not send authentication");
    }
  }

  void on_message(const std::byte* data, std::size_t len, net::WsOpcode) {
    session->last_activity_ = monotonic_seconds();
    // The receive buffer reserves the JSON parser's padding, so the parser can
    // read straight out of it. `len + padding` is the capacity it may touch.
    if (!session->handle_frame(data, len, len + net::kJsonParserPadding)) {
      session->fail_session("exchange protocol error");
    }
  }

  void on_close(const net::WsCloseInfo& info) {
    AXON_LOG_WARN(log(), "[{}] exchange closed the connection: code={}",
                    session->exchange_.name,
                    static_cast<int>(info.code));
    session->enter_backoff();
  }

  void on_error(const char* what) {
    AXON_LOG_WARN(log(), "[{}] connection error: {}", session->exchange_.name,
                    what);
    session->last_error_ = what;
    session->enter_backoff();
  }
};

// ---------------------------------------------------------------------------
ExchangeSession::ExchangeSession(ExchangeConfig exchange_config, WebSocketConfig ws_config,
                           RuntimeConfig runtime, const net::TlsContext* tls,
                           ExchangeSessionHandlers handlers)
    : exchange_(std::move(exchange_config)),
      ws_config_(ws_config),
      runtime_(runtime),
      handlers_(std::move(handlers)),
      tls_(tls),
      bridge_(std::make_unique<Bridge>()) {
  bridge_->session = this;
}

ExchangeSession::~ExchangeSession() = default;

double ExchangeSession::now_seconds() const { return monotonic_seconds(); }

void ExchangeSession::start() {
  started_ = true;
  backoff_attempt_ = 0;
  begin_connect();
}

void ExchangeSession::stop() {
  started_ = false;
  connection_.abort();
  state_ = SessionState::kDisconnected;
  util::get_metrics().set_ws_connected(exchange_.name, false);
}

void ExchangeSession::begin_connect() {
  // Some exchanges need a credential fetched before the URL is even known.
  if (!prepare_connect()) {
    state_ = SessionState::kConnecting;
    arm_handshake_deadline();
    return;
  }

  net::WsConnectionConfig cfg;
  cfg.host = host();
  cfg.port = 443;
  cfg.target = path();
  cfg.use_tls = true;
  cfg.rx_buffer_bytes = runtime_.rx_buffer_bytes;
  cfg.tx_buffer_bytes = runtime_.tx_buffer_bytes;
  cfg.max_message_bytes = runtime_.max_message_bytes;

  AXON_LOG_INFO(log(), "[{}] connecting to wss://{}{}", exchange_.name, cfg.host,
                  cfg.target);
  state_ = SessionState::kConnecting;
  last_activity_ = now_seconds();
  arm_handshake_deadline();
  connection_.connect(cfg, tls_);
}

void ExchangeSession::enter_backoff() {
  if (!started_) {
    state_ = SessionState::kDisconnected;
    return;
  }
  connection_.abort();
  util::get_metrics().set_ws_connected(exchange_.name, false);
  util::get_metrics().inc_ws_reconnect(exchange_.name);
  ++reconnects_;

  // Exponential with jitter. The jitter is not cosmetic: without it every
  // session that dropped together comes back in lockstep and trips the rate
  // limit as one.
  const double base = static_cast<double>(ws_config_.reconnect_delay_seconds);
  const double cap = static_cast<double>(ws_config_.max_reconnect_delay_seconds);
  const double delay =
      std::min(cap, base * std::pow(2.0, backoff_attempt_)) * jitter();
  ++backoff_attempt_;

  backoff_until_ = now_seconds() + delay;
  state_ = SessionState::kBackoff;
  AXON_LOG_WARN(log(), "[{}] reconnecting in {:.1f}s (attempt {})",
                  exchange_.name, delay, backoff_attempt_);
}

void ExchangeSession::arm_handshake_deadline() {
  handshake_deadline_ =
      now_seconds() + static_cast<double>(ws_config_.request_timeout_seconds);
}

void ExchangeSession::fail_session(std::string reason, bool fatal) {
  last_error_ = std::move(reason);
  AXON_LOG_ERROR(log(), "[{}] session failed: {}", exchange_.name, last_error_);
  if (handlers_.on_error) {
    handlers_.on_error(last_error_);
  }
  if (fatal) {
    // Wrong credentials do not get better by retrying. Looping against a 401
    // just burns the exchange's rate limit and hides the real problem.
    connection_.abort();
    state_ = SessionState::kFatal;
    return;
  }
  enter_backoff();
}

void ExchangeSession::note_authenticated() {
  AXON_LOG_INFO(log(), "[{}] authenticated, subscribing", exchange_.name);
  state_ = SessionState::kSubscribing;
  arm_handshake_deadline();
  if (!send_subscriptions()) {
    fail_session("could not send subscriptions");
  }
}

void ExchangeSession::note_subscribed() {
  if (state_ == SessionState::kLive) {
    return;
  }
  AXON_LOG_INFO(log(), "[{}] live", exchange_.name);
  state_ = SessionState::kLive;
  backoff_attempt_ = 0;
  util::get_metrics().set_ws_connected(exchange_.name, true);
  if (handlers_.on_live) {
    // Anything that happened while we were away was missed; a consumer caching
    // exchange state must invalidate here rather than trust what it has.
    handlers_.on_live();
  }
}

void ExchangeSession::on_auth_reply(bool success, const std::string& error) {
  if (!success) {
    // Treated as fatal: an authentication rejection is a credential problem,
    // and retrying it forever is worse than stopping.
    fail_session("authentication rejected: " + error, /*fatal=*/true);
    return;
  }
  note_authenticated();
}

void ExchangeSession::on_subscribe_reply(bool success, const std::string& error) {
  if (!success) {
    fail_session("subscription rejected: " + error);
    return;
  }
  note_subscribed();
}

bool ExchangeSession::send_raw(std::string_view payload) {
  if (!connection_.open()) {
    return false;
  }
  return connection_.send_text(payload);
}

void ExchangeSession::poll() {
  if (!started_ || state_ == SessionState::kFatal) {
    return;
  }

  if (state_ == SessionState::kBackoff) {
    if (now_seconds() >= backoff_until_) {
      begin_connect();
    }
    return;
  }

  on_poll();

  // Still waiting on whatever prepare_connect() needs -- retry it rather than
  // polling a connection that was never opened.
  if (state_ == SessionState::kConnecting && !connection_.open() &&
      connection_.state() == net::WsConnectionState::kIdle) {
    begin_connect();
    return;
  }

  if (!connection_.poll(*bridge_)) {
    // poll() returning false means the connection is finished one way or
    // another. If we were not already heading for backoff, go there now.
    if (state_ != SessionState::kBackoff && state_ != SessionState::kFatal) {
      enter_backoff();
    }
    return;
  }

  // A handshake stage that never completes. The Python retries the individual
  // auth/subscribe RPC up to max_request_retries times; this drops the whole
  // connection and reconnects instead, which recovers from the case a retry
  // cannot -- a socket the exchange has silently stopped serving. The backoff it
  // enters is the same exponential-with-jitter, so the exchange sees the same
  // request rate either way.
  if (state_ == SessionState::kConnecting ||
      state_ == SessionState::kAuthenticating ||
      state_ == SessionState::kSubscribing) {
    if (now_seconds() > handshake_deadline_) {
      AXON_LOG_WARN(log(), "[{}] {} did not complete within {}s",
                      exchange_.name, to_string(state_),
                      ws_config_.request_timeout_seconds);
      util::get_metrics().inc_ws_heartbeat_miss(exchange_.name);
      connection_.abort();
      enter_backoff();
      return;
    }
  }

  // Liveness. A socket that is open but silent for several heartbeat periods
  // is a half-open connection: the peer is gone and TCP has not noticed. This
  // is the case keepalive alone is too slow to catch.
  if (state_ == SessionState::kLive) {
    const double silence = now_seconds() - last_activity_;
    if (silence > ws_config_.heartbeat_interval_seconds * 3.0) {
      util::get_metrics().inc_ws_heartbeat_miss(exchange_.name);
      AXON_LOG_WARN(log(), "[{}] silent for {:.0f}s, reconnecting",
                      exchange_.name, silence);
      enter_backoff();
    }
  }
}
// ---------------------------------------------------------------------------
// Each exchange's sessions live in exchanges/<exchange>/<exchange>_oms.cpp; these look the
// exchange up and ask it.
// ---------------------------------------------------------------------------
std::unique_ptr<ExchangeSession> make_trade_session(
    const ExchangeConfig& exchange_config, const WebSocketConfig& ws_config,
    const RuntimeConfig& runtime, const net::TlsContext* tls,
    ExchangeSessionHandlers handlers) {
  const OmsExchange* exchange = oms_exchange_for(exchange_config.name);
  if (exchange == nullptr || exchange->make_trade_session == nullptr) {
    // OKX and Deribit send orders on the feed session; nothing to build.
    return nullptr;
  }
  return exchange->make_trade_session(exchange_config, ws_config, runtime, tls,
                                      std::move(handlers));
}

std::unique_ptr<ExchangeSession> make_exchange_session(
    const ExchangeConfig& exchange_config, const WebSocketConfig& ws_config,
    const RuntimeConfig& runtime, const net::TlsContext* tls, ExchangeRest* rest,
    ExchangeSessionHandlers handlers) {
  const OmsExchange* exchange = oms_exchange_for(exchange_config.name);
  if (exchange == nullptr) {
    return nullptr;
  }
  return exchange->make_session(exchange_config, ws_config, runtime, tls, rest,
                                std::move(handlers));
}

}  // namespace axon::oms
