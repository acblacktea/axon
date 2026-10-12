#include "axon/market_data/websocket_client.h"

#include <algorithm>
#include <openssl/err.h>

#include "axon/market_data/metrics.h"

namespace axon::market_data {

// ---------------------------------------------------------------------------
// Ctor / Dtor
// ---------------------------------------------------------------------------

WebSocketClient::WebSocketClient(net::io_context& ioc,
                                 Config cfg,
                                 MessageCallback on_message,
                                 StateCallback on_state_change,
                                 std::shared_ptr<spdlog::logger> logger)
    : ioc_(ioc)
    , ssl_ctx_(ssl::context::tlsv12_client)
    , cfg_(std::move(cfg))
    , on_message_(std::move(on_message))
    , on_state_change_(std::move(on_state_change))
    , logger_(logger ? std::move(logger) : spdlog::default_logger())
{
    ssl_ctx_.set_default_verify_paths();
    ssl_ctx_.set_verify_mode(ssl::verify_none);
}

WebSocketClient::~WebSocketClient() { stop(); }

// ---------------------------------------------------------------------------
// Start / Stop
// ---------------------------------------------------------------------------

void WebSocketClient::start() {
    if (running_) return;
    running_ = true;
    ++gen_;
    // Publish the gauge as 0 before the first attempt. A series that only
    // appears on the first successful connect is absent while the feed is
    // down, and "absent" reads as "not configured" rather than "down".
    get_metrics().set_ws_connected(cfg_.tag, false);
    logger_->info("[{}] starting WebSocket client -> {}:{}{}",
                  cfg_.tag, cfg_.host, cfg_.port, cfg_.path);
    net::co_spawn(ioc_, run(shared_from_this(), gen_), net::detached);
}

void WebSocketClient::stop() {
    if (!running_) return;
    running_ = false;
    if (connected_) {
        connected_ = false;
        record_state(false);
    }
    write_queue_.clear();
    writing_ = false;
    if (ws_) {
        // Closing the socket aborts whatever is pending on it -- resolve
        // aside, every stage of connect, the read, a write. Those operations
        // still complete afterwards, with operation_aborted, and the
        // coroutines awaiting them hold their own reference to the stream, so
        // dropping ours here frees nothing they are using.
        beast::get_lowest_layer(*ws_).close();
        ws_.reset();
    }
    logger_->info("[{}] WebSocket client stopped", cfg_.tag);
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::send(std::string msg) {
    co_await write(std::move(msg));
}

net::awaitable<void> WebSocketClient::write(Outgoing msg) {
    if (!connected_ || !ws_) co_return;

    write_queue_.push_back(std::move(msg));
    if (writing_) co_return; // the in-flight drain will pick it up

    auto self = shared_from_this();
    auto ws   = ws_;
    writing_  = true;
    try {
        while (!write_queue_.empty() && connected_ && ws_ == ws) {
            auto front = std::move(write_queue_.front());
            write_queue_.pop_front();
            if (front)
                co_await ws->async_write(net::buffer(*front), net::use_awaitable);
            else
                co_await ws->async_ping({}, net::use_awaitable);
        }
    } catch (...) {
        // Only while this is still the live stream: after a reconnect the
        // flag belongs to the new connection's drain.
        if (ws_ == ws) writing_ = false;
        throw;
    }
    if (ws_ == ws) writing_ = false;
}

// ---------------------------------------------------------------------------
// Main coroutine: connect, read until the link ends, back off, repeat
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::run(std::shared_ptr<WebSocketClient> self,
                                          std::uint64_t gen) {
    // Consecutive sessions that failed, or did not last; drives the backoff.
    int failures = 0;

    while (live(gen)) {
        try {
            auto ws = co_await connect(gen);
            // The ping loop ends by itself once this stream is no longer the
            // live one.
            net::co_spawn(ioc_, ping_loop(self, ws, gen), net::detached);
            co_await read_loop(std::move(ws), gen);
        } catch (const boost::system::system_error& e) {
            if (live(gen)) {
                if (!connected_) get_metrics().inc_ws_connect_failure(cfg_.tag);
                // Filter out expected disconnection errors
                if (e.code() != net::error::operation_aborted &&
                    e.code() != websocket::error::closed) {
                    logger_->error("[{}] connection error: {}", cfg_.tag, e.what());
                }
            }
        } catch (const std::exception& e) {
            if (live(gen)) {
                if (!connected_) get_metrics().inc_ws_connect_failure(cfg_.tag);
                logger_->error("[{}] error: {}", cfg_.tag, e.what());
            }
        }

        // Stopped (or stopped and restarted): stop() has already torn the
        // connection down, and nothing here belongs to this coroutine any more.
        if (!live(gen)) co_return;

        // A session that stayed up at least as long as the shortest backoff
        // is not part of a run of failures; the next drop starts over. Read
        // before notify_state() clears session_start_.
        const bool healthy =
            session_start_ > 0.0 &&
            steady_seconds() - session_start_ >= cfg_.reconnect_base_delay;
        failures = healthy ? 1 : failures + 1;

        connected_ = false;
        write_queue_.clear();
        writing_ = false;
        ws_.reset();
        notify_state(false);
        if (!live(gen)) co_return; // the state callback may have stopped us

        get_metrics().inc_ws_reconnect(cfg_.tag);
        const double delay = backoff_delay(failures);
        logger_->info("[{}] reconnecting in {:.1f}s (attempt {})",
                      cfg_.tag, delay, failures);

        net::steady_timer timer(ioc_);
        timer.expires_after(std::chrono::milliseconds(
            static_cast<int64_t>(delay * 1000)));
        co_await timer.async_wait(net::use_awaitable);
    }
}

double WebSocketClient::backoff_delay(int failures) {
    const double capped = std::min(
        cfg_.reconnect_base_delay * (1 << std::min(failures - 1, 5)),
        cfg_.reconnect_max_delay);
    // Added on top rather than spread around the delay, so that a retry never
    // comes sooner than the schedule says.
    std::uniform_real_distribution<double> jitter(0.0, 0.2);
    return capped * (1.0 + jitter(rng_));
}

// ---------------------------------------------------------------------------
// Connect (TCP -> SSL -> WebSocket handshake)
// ---------------------------------------------------------------------------

net::awaitable<WebSocketClient::Stream> WebSocketClient::connect(std::uint64_t gen) {
    // Covers DNS, TCP, TLS and the WebSocket upgrade as one number. Splitting
    // them would be nicer but they fail as a unit here, and the useful
    // question in production is "is the venue slow to let us in", not which
    // of four stages owns the milliseconds.
    const double connect_start = steady_seconds();
    auto executor = co_await net::this_coro::executor;

    // A stop() during a stage that holds no socket (the resolve) cannot abort
    // it; it is noticed here instead.
    auto check_live = [&] {
        if (!live(gen))
            throw boost::system::system_error(net::error::operation_aborted);
    };

    // DNS resolve
    tcp::resolver resolver(executor);
    auto results = co_await resolver.async_resolve(
        cfg_.host, cfg_.port, net::use_awaitable);
    check_live();

    // Create new WebSocket stream. This coroutine works on its own reference:
    // stop() drops ws_ while the operations below may still be pending.
    auto ws = std::make_shared<WsStream>(executor, ssl_ctx_);
    ws_ = ws;

    // TCP connect
    auto& tcp_layer = beast::get_lowest_layer(*ws);
    tcp_layer.expires_after(cfg_.connect_timeout);
    co_await tcp_layer.async_connect(results, net::use_awaitable);
    check_live();

    // SNI hostname
    if (!SSL_set_tlsext_host_name(
            ws->next_layer().native_handle(), cfg_.host.c_str())) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
                              net::error::get_ssl_category()));
    }

    // SSL handshake
    tcp_layer.expires_after(cfg_.connect_timeout);
    co_await ws->next_layer().async_handshake(
        ssl::stream_base::client, net::use_awaitable);
    check_live();

    // WebSocket handshake
    tcp_layer.expires_never();
    ws->set_option(websocket::stream_base::timeout::suggested(
        beast::role_type::client));
    ws->set_option(websocket::stream_base::decorator(
        [](websocket::request_type& req) {
            req.set(http::field::user_agent, "axon_market_data/0.1");
        }));
    // Control frames are proof of life too: a pong answers our control ping.
    // Only ever invoked from inside an async_read on this stream, which
    // read_loop() runs while holding the client alive.
    ws->control_callback([this](websocket::frame_type, beast::string_view) {
        last_rx_ = Clock::now();
    });

    std::string host_header = cfg_.host + ":" + cfg_.port;
    co_await ws->async_handshake(host_header, cfg_.path, net::use_awaitable);
    check_live();

    connected_ = true;
    last_rx_   = Clock::now();
    get_metrics().observe_ws_connect(cfg_.tag, steady_seconds() - connect_start);
    notify_state(true);
    logger_->info("[{}] connected to {}", cfg_.tag, cfg_.host);
    co_return ws;
}

// ---------------------------------------------------------------------------
// Read loop
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::read_loop(Stream ws, std::uint64_t gen) {
    beast::flat_buffer buf;
    while (live(gen) && connected_ && ws_ == ws) {
        buf.clear();
        co_await ws->async_read(buf, net::use_awaitable);
        if (!live(gen)) co_return;
        last_rx_ = Clock::now();

        auto data = static_cast<const char*>(buf.data().data());
        auto size = beast::buffer_bytes(buf.data());

        // Everything downstream of the socket -- parse, book update, serialize
        // and ZMQ send -- happens inside this call, on this thread. Timing it
        // here is the only place that sees the whole cost of one frame.
        //
        // The adapters drop and count a frame they cannot read; this catch is
        // the backstop. One frame must never cost the connection.
        const double t0 = steady_seconds();
        try {
            on_message_(std::string_view(data, size));
        } catch (const std::exception& e) {
            logger_->error("[{}] message handler threw, frame dropped: {}",
                           cfg_.tag, e.what());
        }
        get_metrics().observe_handler(cfg_.tag, steady_seconds() - t0);
    }
}

// ---------------------------------------------------------------------------
// Ping loop (heartbeat + dead-peer detection)
// ---------------------------------------------------------------------------

// Sends a heartbeat every ping_interval, and drops the connection when one
// goes unanswered -- nothing at all received since it was sent -- for
// pong_timeout. Without this a half-open link (the venue gone, no RST) is
// never noticed: the read just waits forever while every topic on it is
// silent. A dead peer is therefore caught within ping_interval + pong_timeout
// of its last frame.
net::awaitable<void> WebSocketClient::ping_loop(std::shared_ptr<WebSocketClient> self,
                                                Stream ws, std::uint64_t gen) {
    auto alive = [&] { return live(gen) && connected_ && ws_ == ws; };

    net::steady_timer                timer(ioc_);
    Clock::time_point                next_ping = Clock::now() + cfg_.ping_interval;
    std::optional<Clock::time_point> unanswered; // when the oldest unanswered ping went out

    while (alive()) {
        auto wake = next_ping;
        if (unanswered) wake = std::min(wake, *unanswered + cfg_.pong_timeout);
        timer.expires_at(wake);
        co_await timer.async_wait(net::use_awaitable);

        if (!alive()) co_return;
        const auto now = Clock::now();

        if (unanswered && last_rx_ >= *unanswered) unanswered.reset();
        if (unanswered && now >= *unanswered + cfg_.pong_timeout) {
            logger_->warn("[{}] nothing received for {}s after a heartbeat, "
                          "dropping the connection",
                          cfg_.tag, cfg_.pong_timeout.count());
            // The pending read fails, and run() reconnects as for any drop.
            beast::get_lowest_layer(*ws).close();
            co_return;
        }

        if (now >= next_ping) {
            if (!unanswered) unanswered = now;
            // Not awaited: a write stalled behind a full socket buffer must
            // not stop the deadline above from being checked.
            Outgoing hb;
            if (!cfg_.ping_text.empty()) hb = cfg_.ping_text;
            net::co_spawn(ioc_, write(std::move(hb)), net::detached);
            next_ping = now + cfg_.ping_interval;
        }
    }
}

// ---------------------------------------------------------------------------
// State notification
// ---------------------------------------------------------------------------

void WebSocketClient::notify_state(bool connected) {
    record_state(connected);
    if (on_state_change_) on_state_change_(connected);
}

void WebSocketClient::record_state(bool connected) {
    auto& metrics = get_metrics();
    metrics.set_ws_connected(cfg_.tag, connected);

    if (connected) {
        session_start_ = steady_seconds();
    } else if (session_start_ > 0.0) {
        // Guarded by session_start_ rather than by the call site: the teardown
        // paths call this more than once for a single drop, and an unguarded
        // version would record a pile of zero-length sessions.
        metrics.observe_ws_session(cfg_.tag, steady_seconds() - session_start_);
        session_start_ = 0.0;
    }
}

} // namespace axon::market_data
