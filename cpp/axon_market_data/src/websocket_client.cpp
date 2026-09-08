#include "axon_market_data/websocket_client.hpp"

#include <algorithm>
#include <openssl/err.h>

namespace axon_market_data {

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
    logger_->info("[{}] starting WebSocket client -> {}:{}{}",
                  cfg_.tag, cfg_.host, cfg_.port, cfg_.path);
    net::co_spawn(ioc_, run(), net::detached);
}

void WebSocketClient::stop() {
    if (!running_) return;
    running_ = false;
    connected_ = false;
    write_queue_.clear();
    writing_ = false;
    if (ws_) {
        beast::get_lowest_layer(*ws_).close();
        ws_.reset();
    }
    logger_->info("[{}] WebSocket client stopped", cfg_.tag);
}

// ---------------------------------------------------------------------------
// Send
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::send(std::string msg) {
    if (!connected_ || !ws_) co_return;

    write_queue_.push_back(std::move(msg));
    if (writing_) co_return; // the in-flight drain will pick it up

    writing_ = true;
    try {
        while (!write_queue_.empty() && connected_ && ws_) {
            auto front = std::move(write_queue_.front());
            write_queue_.pop_front();
            co_await ws_->async_write(net::buffer(front), net::use_awaitable);
        }
    } catch (...) {
        writing_ = false;
        throw;
    }
    writing_ = false;
}

// ---------------------------------------------------------------------------
// Main coroutine
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::run() {
    try {
        co_await connect();
        // Run read loop and ping loop concurrently
        // ping_loop will exit when connection drops
        net::co_spawn(ioc_, ping_loop(), net::detached);
        co_await read_loop();
    } catch (const boost::system::system_error& e) {
        // Filter out expected disconnection errors
        if (e.code() != net::error::operation_aborted &&
            e.code() != websocket::error::closed) {
            logger_->error("[{}] connection error: {}", cfg_.tag, e.what());
        }
    } catch (const std::exception& e) {
        logger_->error("[{}] error: {}", cfg_.tag, e.what());
    }

    connected_ = false;
    write_queue_.clear();
    writing_ = false;
    notify_state(false);

    if (running_) {
        co_await reconnect();
    }
}

// ---------------------------------------------------------------------------
// Connect (TCP -> SSL -> WebSocket handshake)
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::connect() {
    auto executor = co_await net::this_coro::executor;

    // DNS resolve
    tcp::resolver resolver(executor);
    auto results = co_await resolver.async_resolve(
        cfg_.host, cfg_.port, net::use_awaitable);

    // Create new WebSocket stream
    ws_ = std::make_unique<WsStream>(executor, ssl_ctx_);

    // TCP connect
    auto& tcp_layer = beast::get_lowest_layer(*ws_);
    tcp_layer.expires_after(cfg_.connect_timeout);
    co_await tcp_layer.async_connect(results, net::use_awaitable);

    // SNI hostname
    if (!SSL_set_tlsext_host_name(
            ws_->next_layer().native_handle(), cfg_.host.c_str())) {
        throw beast::system_error(
            beast::error_code(static_cast<int>(::ERR_get_error()),
                              net::error::get_ssl_category()));
    }

    // SSL handshake
    tcp_layer.expires_after(cfg_.connect_timeout);
    co_await ws_->next_layer().async_handshake(
        ssl::stream_base::client, net::use_awaitable);

    // WebSocket handshake
    tcp_layer.expires_never();
    ws_->set_option(websocket::stream_base::timeout::suggested(
        beast::role_type::client));
    ws_->set_option(websocket::stream_base::decorator(
        [](websocket::request_type& req) {
            req.set(http::field::user_agent, "axon_market_data/0.1");
        }));

    std::string host_header = cfg_.host + ":" + cfg_.port;
    co_await ws_->async_handshake(host_header, cfg_.path, net::use_awaitable);

    connected_ = true;
    notify_state(true);
    logger_->info("[{}] connected to {}", cfg_.tag, cfg_.host);
}

// ---------------------------------------------------------------------------
// Read loop
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::read_loop() {
    while (running_ && connected_) {
        read_buf_.clear();
        co_await ws_->async_read(read_buf_, net::use_awaitable);

        auto data = static_cast<const char*>(read_buf_.data().data());
        auto size = beast::buffer_bytes(read_buf_.data());
        on_message_(std::string_view(data, size));
    }
}

// ---------------------------------------------------------------------------
// Ping loop (heartbeat)
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::ping_loop() {
    net::steady_timer timer(ioc_);

    while (running_ && connected_) {
        timer.expires_after(cfg_.ping_interval);
        co_await timer.async_wait(net::use_awaitable);

        if (!running_ || !connected_) co_return;

        try {
            if (!cfg_.ping_text.empty()) {
                co_await send(cfg_.ping_text);
            } else if (!writing_) {
                // async_ping is a write operation too — skip if one is in flight
                co_await ws_->async_ping({}, net::use_awaitable);
            }
        } catch (...) {
            logger_->warn("[{}] ping failed, connection likely lost", cfg_.tag);
            co_return;
        }
    }
}

// ---------------------------------------------------------------------------
// Reconnect with exponential backoff
// ---------------------------------------------------------------------------

net::awaitable<void> WebSocketClient::reconnect() {
    int attempt = 0;

    while (running_) {
        ++attempt;
        double delay = std::min(
            cfg_.reconnect_base_delay * (1 << std::min(attempt - 1, 5)),
            cfg_.reconnect_max_delay);

        logger_->info("[{}] reconnecting in {:.0f}s (attempt {})",
                      cfg_.tag, delay, attempt);

        net::steady_timer timer(ioc_);
        timer.expires_after(std::chrono::milliseconds(
            static_cast<int64_t>(delay * 1000)));
        co_await timer.async_wait(net::use_awaitable);

        if (!running_) co_return;

        try {
            ws_.reset();
            co_await connect();

            // Restart ping loop
            net::co_spawn(ioc_, ping_loop(), net::detached);

            // Notify so adapter can re-subscribe
            // (on_state_change(true) already called in connect())

            // Resume read loop — if it throws, we'll reconnect again
            co_await read_loop();

        } catch (const std::exception& e) {
            logger_->warn("[{}] reconnect attempt {} failed: {}",
                          cfg_.tag, attempt, e.what());
            connected_ = false;
            write_queue_.clear();
            writing_ = false;
            notify_state(false);
            continue;
        }

        // read_loop exited normally — reconnect again
        connected_ = false;
        write_queue_.clear();
        writing_ = false;
        notify_state(false);
    }
}

// ---------------------------------------------------------------------------
// State notification
// ---------------------------------------------------------------------------

void WebSocketClient::notify_state(bool connected) {
    if (on_state_change_) on_state_change_(connected);
}

} // namespace axon_market_data
