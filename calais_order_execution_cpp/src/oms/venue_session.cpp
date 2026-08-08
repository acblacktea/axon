#include "calais/oms/venue_session.h"

#include <cmath>
#include <cstring>
#include <random>

#include "calais/core/clock.h"
#include "calais/util/logging.h"
#include "calais/util/metrics.h"
#include "calais/venue/binance/binance_parser.h"
#include "calais/venue/bybit/bybit_builder.h"
#include "calais/venue/bybit/bybit_parser.h"
#include "calais/venue/deribit/deribit_builder.h"
#include "calais/venue/deribit/deribit_parser.h"
#include "calais/venue/okx/okx_builder.h"
#include "calais/venue/okx/okx_parser.h"

namespace calais::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.session");
  return logger;
}

double monotonic_seconds() {
  return static_cast<double>(core::monotonic_ns()) / 1e9;
}

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
// property while letting VenueSession stay a normal polymorphic class.
// ---------------------------------------------------------------------------
struct VenueSession::Bridge {
  VenueSession* session = nullptr;

  void on_open() {
    CALAIS_LOG_INFO(log(), "[{}] websocket open, authenticating",
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
      session->fail_session("venue protocol error");
    }
  }

  void on_close(const net::WsCloseInfo& info) {
    CALAIS_LOG_WARN(log(), "[{}] venue closed the connection: code={}",
                    session->exchange_.name,
                    static_cast<int>(info.code));
    session->enter_backoff();
  }

  void on_error(const char* what) {
    CALAIS_LOG_WARN(log(), "[{}] connection error: {}", session->exchange_.name,
                    what);
    session->last_error_ = what;
    session->enter_backoff();
  }
};

// ---------------------------------------------------------------------------
VenueSession::VenueSession(ExchangeConfig exchange, WebSocketConfig ws_config,
                           RuntimeConfig runtime, const net::TlsContext* tls,
                           VenueSessionHandlers handlers)
    : exchange_(std::move(exchange)),
      ws_config_(ws_config),
      runtime_(runtime),
      handlers_(std::move(handlers)),
      tls_(tls),
      bridge_(std::make_unique<Bridge>()) {
  bridge_->session = this;
}

VenueSession::~VenueSession() = default;

double VenueSession::now_seconds() const { return monotonic_seconds(); }

void VenueSession::start() {
  started_ = true;
  backoff_attempt_ = 0;
  begin_connect();
}

void VenueSession::stop() {
  started_ = false;
  connection_.abort();
  state_ = SessionState::kDisconnected;
  util::get_metrics().set_ws_connected(exchange_.name, false);
}

void VenueSession::begin_connect() {
  // Some venues need a credential fetched before the URL is even known.
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

  CALAIS_LOG_INFO(log(), "[{}] connecting to wss://{}{}", exchange_.name, cfg.host,
                  cfg.target);
  state_ = SessionState::kConnecting;
  last_activity_ = now_seconds();
  arm_handshake_deadline();
  connection_.connect(cfg, tls_);
}

void VenueSession::enter_backoff() {
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
  CALAIS_LOG_WARN(log(), "[{}] reconnecting in {:.1f}s (attempt {})",
                  exchange_.name, delay, backoff_attempt_);
}

void VenueSession::arm_handshake_deadline() {
  handshake_deadline_ =
      now_seconds() + static_cast<double>(ws_config_.request_timeout_seconds);
}

void VenueSession::fail_session(std::string reason, bool fatal) {
  last_error_ = std::move(reason);
  CALAIS_LOG_ERROR(log(), "[{}] session failed: {}", exchange_.name, last_error_);
  if (handlers_.on_error) {
    handlers_.on_error(last_error_);
  }
  if (fatal) {
    // Wrong credentials do not get better by retrying. Looping against a 401
    // just burns the venue's rate limit and hides the real problem.
    connection_.abort();
    state_ = SessionState::kFatal;
    return;
  }
  enter_backoff();
}

void VenueSession::note_authenticated() {
  CALAIS_LOG_INFO(log(), "[{}] authenticated, subscribing", exchange_.name);
  state_ = SessionState::kSubscribing;
  arm_handshake_deadline();
  if (!send_subscriptions()) {
    fail_session("could not send subscriptions");
  }
}

void VenueSession::note_subscribed() {
  if (state_ == SessionState::kLive) {
    return;
  }
  CALAIS_LOG_INFO(log(), "[{}] live", exchange_.name);
  state_ = SessionState::kLive;
  backoff_attempt_ = 0;
  util::get_metrics().set_ws_connected(exchange_.name, true);
  if (handlers_.on_live) {
    // Anything that happened while we were away was missed; a consumer caching
    // venue state must invalidate here rather than trust what it has.
    handlers_.on_live();
  }
}

void VenueSession::on_auth_reply(bool success, const std::string& error) {
  if (!success) {
    // Treated as fatal: an authentication rejection is a credential problem,
    // and retrying it forever is worse than stopping.
    fail_session("authentication rejected: " + error, /*fatal=*/true);
    return;
  }
  note_authenticated();
}

void VenueSession::on_subscribe_reply(bool success, const std::string& error) {
  if (!success) {
    fail_session("subscription rejected: " + error);
    return;
  }
  note_subscribed();
}

bool VenueSession::send_raw(std::string_view payload) {
  if (!connection_.open()) {
    return false;
  }
  return connection_.send_text(payload);
}

void VenueSession::poll() {
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
  // cannot -- a socket the venue has silently stopped serving. The backoff it
  // enters is the same exponential-with-jitter, so the venue sees the same
  // request rate either way.
  if (state_ == SessionState::kConnecting ||
      state_ == SessionState::kAuthenticating ||
      state_ == SessionState::kSubscribing) {
    if (now_seconds() > handshake_deadline_) {
      CALAIS_LOG_WARN(log(), "[{}] {} did not complete within {}s",
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
      CALAIS_LOG_WARN(log(), "[{}] silent for {:.0f}s, reconnecting",
                      exchange_.name, silence);
      enter_backoff();
    }
  }
}

// ===========================================================================
// Deribit
// ===========================================================================
namespace {

class DeribitSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::deribit::kTestnetHost
                                              : venue::deribit::kProductionHost);
  }
  std::string path() const override { return std::string(venue::deribit::kPath); }

  bool send_authentication() override {
    char buf[venue::deribit::kMaxRequestBytes];
    const std::size_t n = builder_.authenticate(buf, sizeof(buf), exchange_.api_key,
                                                exchange_.api_secret, auth_id_);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool send_subscriptions() override {
    char buf[venue::deribit::kMaxRequestBytes];
    std::int64_t id = 0;

    // Heartbeat first: Deribit will not send test_requests until asked, and
    // without them a dead connection is only noticed by the silence timer.
    std::size_t n = builder_.set_heartbeat(
        buf, sizeof(buf), ws_config_.heartbeat_interval_seconds, id);
    if (n == 0 || !send_raw(std::string_view(buf, n))) {
      return false;
    }

    std::vector<std::string> channel_storage{
        std::string(venue::deribit::kAllOrdersChannel),
        std::string(venue::deribit::kAllTradesChannel)};
    for (const auto& currency : currencies_) {
      channel_storage.push_back("user.portfolio." + currency);
    }
    std::vector<std::string_view> channels;
    channels.reserve(channel_storage.size());
    for (const auto& c : channel_storage) {
      channels.emplace_back(c);
    }

    n = builder_.subscribe(buf, sizeof(buf), channels.data(), channels.size(),
                           subscribe_id_);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    Sink sink{this};
    const auto result = parser_.parse(reinterpret_cast<const char*>(data), len,
                                      capacity, sink);

    switch (result.kind) {
      case venue::VenueMessageKind::kHeartbeatTestRequest: {
        // Deribit drops the connection if this goes unanswered, so it is
        // answered here rather than queued behind anything else.
        char buf[venue::deribit::kMaxRequestBytes];
        const std::size_t n =
            venue::deribit::DeribitBuilder::test_response(buf, sizeof(buf));
        send_raw(std::string_view(buf, n));
        break;
      }
      case venue::VenueMessageKind::kRpcResult:
        if (result.rpc_id == auth_id_) {
          on_auth_reply(true, {});
        } else if (result.rpc_id == subscribe_id_) {
          on_subscribe_reply(true, {});
        } else if (handlers_.on_rpc_reply) {
          // Not ours: an order-entry reply. Hand over the RAW frame -- the EMS
          // knows Deribit's result shape, the session does not and should not.
          handlers_.on_rpc_reply(result.rpc_id, true,
                                 std::string_view(reinterpret_cast<const char*>(data), len),
                                 {});
        }
        break;
      case venue::VenueMessageKind::kRpcError:
        if (result.rpc_id == auth_id_) {
          on_auth_reply(false, std::string(result.rpc_error_text));
        } else if (result.rpc_id == subscribe_id_) {
          on_subscribe_reply(false, std::string(result.rpc_error_text));
        } else if (handlers_.on_rpc_reply) {
          handlers_.on_rpc_reply(result.rpc_id, false,
                                 std::string_view(reinterpret_cast<const char*>(data), len),
                                 result.rpc_error_text);
        }
        break;
      case venue::VenueMessageKind::kParseError:
        // A single malformed frame is not worth dropping a session over.
        CALAIS_LOG_WARN(log(), "[deribit] parse error: {}",
                        result.error != nullptr ? result.error : "?");
        break;
      default:
        break;
    }

    if (result.unknown_order_state) {
      CALAIS_LOG_WARN(log(), "[deribit] unrecognised order_state; mapped to open");
    }
    return true;
  }

 private:
  struct Sink {
    DeribitSession* self;
    void on_order(const transport::OrderUpdateMsg& m) {
      if (self->handlers_.on_order) {
        self->handlers_.on_order(m);
      }
    }
    void on_fill(const transport::FillMsg& m) {
      if (self->handlers_.on_fill) {
        self->handlers_.on_fill(m);
      }
    }
    void on_account(const models::AccountSummary& a) {
      if (self->handlers_.on_account) {
        self->handlers_.on_account(a);
      }
    }
  };

  venue::deribit::DeribitParser parser_;
  venue::deribit::DeribitBuilder builder_;
  std::int64_t auth_id_ = -1;
  std::int64_t subscribe_id_ = -1;
  std::vector<std::string> currencies_{"BTC"};
};

// ===========================================================================
// Bybit
// ===========================================================================
class BybitSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::bybit::kTestnetHost
                                              : venue::bybit::kProductionHost);
  }
  std::string path() const override { return std::string(venue::bybit::kPath); }

  bool send_authentication() override {
    char buf[venue::bybit::kMaxRequestBytes];
    // Expiry a few seconds out. Bybit rejects a signature whose expiry has
    // passed, and clock skew is the usual cause of a login that works locally
    // and fails in production.
    const std::int64_t expires = core::wall_clock_ns() / 1'000'000LL + 10'000LL;
    const std::size_t n =
        builder_.ws_auth(buf, sizeof(buf), exchange_.api_key,
                         exchange_.api_secret, expires, auth_id_);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool send_subscriptions() override {
    const std::string_view topics[] = {venue::bybit::kOrderTopic,
                                       venue::bybit::kExecutionTopic,
                                       venue::bybit::kWalletTopic};
    char buf[venue::bybit::kMaxRequestBytes];
    std::int64_t id = 0;
    const std::size_t n = builder_.ws_subscribe(buf, sizeof(buf), topics, 3, id);
    subscribe_id_ = id;
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    Sink sink{this};
    const auto result = parser_.parse(reinterpret_cast<const char*>(data), len,
                                      capacity, sink);
    if (result.kind == venue::VenueMessageKind::kSessionEvent) {
      // Bybit acknowledges auth and subscribe with the same shape; the req_id
      // tells them apart.
      if (result.rpc_id == auth_id_) {
        on_auth_reply(true, {});
      } else if (result.rpc_id == subscribe_id_) {
        on_subscribe_reply(true, {});
      }
    } else if (result.kind == venue::VenueMessageKind::kRpcError) {
      const std::string text(result.rpc_error_text);
      if (result.rpc_id == auth_id_) {
        on_auth_reply(false, text);
      } else {
        on_subscribe_reply(false, text);
      }
    }
    return true;
  }

 private:
  struct Sink {
    BybitSession* self;
    void on_order(const transport::OrderUpdateMsg& m) {
      if (self->handlers_.on_order) self->handlers_.on_order(m);
    }
    void on_fill(const transport::FillMsg& m) {
      if (self->handlers_.on_fill) self->handlers_.on_fill(m);
    }
    void on_account(const models::AccountSummary& a) {
      if (self->handlers_.on_account) self->handlers_.on_account(a);
    }
  };

  venue::bybit::BybitParser parser_;
  venue::bybit::BybitBuilder builder_;
  std::int64_t auth_id_ = -1;
  std::int64_t subscribe_id_ = -1;
};

// ===========================================================================
// OKX
// ===========================================================================
class OkxSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::okx::kTestnetHost
                                              : venue::okx::kProductionHost);
  }
  std::string path() const override { return "/ws/v5/private"; }

  bool send_authentication() override {
    char buf[venue::okx::kMaxRequestBytes];
    // SECONDS, not milliseconds. OKX rejects a millisecond timestamp here even
    // though its REST signing uses them, with an error that says nothing.
    const std::int64_t seconds = core::wall_clock_ns() / 1'000'000'000LL;
    const std::size_t n = venue::okx::OkxBuilder::ws_login(
        buf, sizeof(buf), exchange_.api_key, exchange_.passphrase,
        exchange_.api_secret, seconds);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool send_subscriptions() override {
    const std::string_view channels[] = {venue::okx::kOrdersChannel,
                                         venue::okx::kFillsChannel,
                                         venue::okx::kAccountChannel};
    char buf[venue::okx::kMaxRequestBytes];
    const std::size_t n =
        venue::okx::OkxBuilder::ws_subscribe(buf, sizeof(buf), channels, 3);
    return n > 0 && send_raw(std::string_view(buf, n));
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    const std::string_view frame(reinterpret_cast<const char*>(data), len);
    // OKX's liveness exchange is plain text, not JSON.
    if (frame == "ping") {
      send_raw("pong");
      return true;
    }
    if (frame == "pong") {
      return true;
    }

    Sink sink{this};
    const auto result = parser_.parse(reinterpret_cast<const char*>(data), len,
                                      capacity, sink);
    if (result.kind == venue::VenueMessageKind::kSessionEvent) {
      if (result.channel == "login") {
        on_auth_reply(true, {});
      } else if (result.channel == "subscribe") {
        // OKX acknowledges each channel separately; the first is enough to
        // call the session live.
        on_subscribe_reply(true, {});
      }
    } else if (result.kind == venue::VenueMessageKind::kRpcError) {
      const std::string text(result.rpc_error_text);
      fail_session("okx error: " + text, /*fatal=*/text.find("Login") != std::string::npos);
    }
    return true;
  }

 private:
  struct Sink {
    OkxSession* self;
    void on_order(const transport::OrderUpdateMsg& m) {
      if (self->handlers_.on_order) self->handlers_.on_order(m);
    }
    void on_fill(const transport::FillMsg& m) {
      if (self->handlers_.on_fill) self->handlers_.on_fill(m);
    }
    void on_account(const models::AccountSummary& a) {
      if (self->handlers_.on_account) self->handlers_.on_account(a);
    }
  };

  venue::okx::OkxParser parser_;
};

// ===========================================================================
// Binance USDT-M
//
// The odd one: no WebSocket authentication at all. The stream is identified by
// a listenKey fetched over REST and appended to the URL, and that key EXPIRES
// after 60 minutes unless renewed. Renewal is a REST PUT every 30 minutes; if
// it lapses the venue sends listenKeyExpired and stops delivering, which the
// parser reports as kSessionExpired.
// ===========================================================================
class BinanceSession final : public VenueSession {
 public:
  using VenueSession::VenueSession;

  void set_rest(VenueRest* rest) { rest_ = rest; }

 protected:
  std::string host() const override {
    return std::string(exchange_.is_testnet() ? venue::binance::kTestnetWsHost
                                              : venue::binance::kProductionWsHost);
  }
  // The key is part of the path, so this is only valid once we have one.
  std::string path() const override { return "/ws/" + listen_key_; }

  bool prepare_connect() override {
    if (!listen_key_.empty()) {
      return true;
    }
    if (rest_ == nullptr) {
      fail_session("binance needs a REST client for its listenKey", /*fatal=*/true);
      return false;
    }
    if (listen_key_pending_) {
      return false;  // already asked; wait for the reply
    }
    listen_key_pending_ = true;
    rest_->create_listen_key([this](std::string key, const std::string& error) {
      listen_key_pending_ = false;
      if (!error.empty() || key.empty()) {
        // A bad API key fails here rather than at a WebSocket auth step,
        // because there is no WebSocket auth step.
        fail_session("could not obtain a listenKey: " + error);
        return;
      }
      listen_key_ = std::move(key);
      last_keepalive_ = monotonic_seconds();
      CALAIS_LOG_INFO(log(), "[binance] listenKey obtained ({}...)",
                      listen_key_.substr(0, 8));
    });
    return false;
  }

  void on_poll() override {
    if (listen_key_.empty() || rest_ == nullptr) {
      return;
    }
    // Renew at 30 minutes against a 60-minute expiry. Halving the interval is
    // deliberate: one missed renewal must not cost the stream.
    constexpr double kRenewInterval = 30.0 * 60.0;
    if (monotonic_seconds() - last_keepalive_ < kRenewInterval) {
      return;
    }
    last_keepalive_ = monotonic_seconds();
    rest_->keepalive_listen_key(listen_key_, [](const std::string&,
                                                const std::string& error) {
      if (!error.empty()) {
        CALAIS_LOG_WARN(log(), "[binance] listenKey keepalive failed: {}", error);
      }
    });
  }

  bool send_authentication() override {
    // Nothing to authenticate: possessing the listenKey IS the authentication.
    note_authenticated();
    return true;
  }

  bool send_subscriptions() override {
    // Nothing to subscribe to either -- ORDER_TRADE_UPDATE and ACCOUNT_UPDATE
    // arrive unbidden on a user data stream.
    note_subscribed();
    return true;
  }

  bool handle_frame(const std::byte* data, std::size_t len,
                    std::size_t capacity) override {
    Sink sink{this};
    const auto result = parser_.parse(reinterpret_cast<const char*>(data), len,
                                      capacity, sink);
    if (result.kind == venue::VenueMessageKind::kSessionExpired) {
      CALAIS_LOG_WARN(log(), "[binance] listenKey expired; re-establishing");
      listen_key_.clear();  // force a fresh key on the next connect
      fail_session("listenKey expired");
      return false;
    }
    return true;
  }

 private:
  struct Sink {
    BinanceSession* self;
    void on_order(const transport::OrderUpdateMsg& m) {
      if (self->handlers_.on_order) self->handlers_.on_order(m);
    }
    void on_fill(const transport::FillMsg& m) {
      if (self->handlers_.on_fill) self->handlers_.on_fill(m);
    }
    void on_account(const models::AccountSummary& a) {
      if (self->handlers_.on_account) self->handlers_.on_account(a);
    }
  };

  venue::binance::BinanceParser parser_;
  VenueRest* rest_ = nullptr;
  std::string listen_key_;
  bool listen_key_pending_ = false;
  double last_keepalive_ = 0.0;
};

}  // namespace

// ---------------------------------------------------------------------------
std::unique_ptr<VenueSession> make_venue_session(
    const ExchangeConfig& exchange, const WebSocketConfig& ws_config,
    const RuntimeConfig& runtime, const net::TlsContext* tls, VenueRest* rest,
    VenueSessionHandlers handlers) {
  if (exchange.name == "deribit") {
    return std::make_unique<DeribitSession>(exchange, ws_config, runtime, tls,
                                            std::move(handlers));
  }
  if (exchange.name == "bybit") {
    return std::make_unique<BybitSession>(exchange, ws_config, runtime, tls,
                                          std::move(handlers));
  }
  if (exchange.name == "okx") {
    return std::make_unique<OkxSession>(exchange, ws_config, runtime, tls,
                                        std::move(handlers));
  }
  if (exchange.name == "binance") {
    auto session = std::make_unique<BinanceSession>(exchange, ws_config, runtime,
                                                    tls, std::move(handlers));
    session->set_rest(rest);
    return session;
  }
  return nullptr;
}

}  // namespace calais::oms
