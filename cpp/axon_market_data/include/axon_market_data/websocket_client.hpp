#pragma once

#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <spdlog/spdlog.h>

namespace axon_market_data {

namespace net       = boost::asio;
namespace beast     = boost::beast;
namespace websocket = beast::websocket;
namespace ssl       = net::ssl;
namespace http      = beast::http;
using tcp           = net::ip::tcp;

using WsStream = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

class WebSocketClient : public std::enable_shared_from_this<WebSocketClient> {
public:
    using MessageCallback = std::function<void(std::string_view)>;
    using StateCallback   = std::function<void(bool connected)>;

    struct Config {
        std::string host;
        std::string port              = "443";
        std::string path              = "/ws";
        std::string tag               = "ws";  // for logging
        std::chrono::seconds connect_timeout{10};
        std::chrono::seconds ping_interval{20};
        // If non-empty, send this text frame as heartbeat instead of a
        // WebSocket ping control frame (OKX: "ping", Bybit: {"op":"ping"}).
        std::string ping_text;
        std::chrono::seconds pong_timeout{10};
        double reconnect_base_delay   = 5.0;   // seconds
        double reconnect_max_delay    = 60.0;   // seconds
    };

    WebSocketClient(net::io_context& ioc,
                    Config cfg,
                    MessageCallback on_message,
                    StateCallback on_state_change = nullptr,
                    std::shared_ptr<spdlog::logger> logger = nullptr);
    ~WebSocketClient();

    WebSocketClient(const WebSocketClient&)            = delete;
    WebSocketClient& operator=(const WebSocketClient&) = delete;

    void start();
    void stop();

    // Send a text message. Must be called from the io_context thread.
    // Writes are serialized through an internal queue — Beast allows only one
    // outstanding write (or ping) per stream.
    net::awaitable<void> send(std::string msg);

    bool is_connected() const { return connected_; }

    // Access the raw read buffer (for adapters that need it)
    const beast::flat_buffer& read_buffer() const { return read_buf_; }

private:
    net::awaitable<void> run();
    net::awaitable<void> connect();
    net::awaitable<void> read_loop();
    net::awaitable<void> ping_loop();
    net::awaitable<void> reconnect();

    void notify_state(bool connected);

    net::io_context&                ioc_;
    ssl::context                    ssl_ctx_;
    Config                          cfg_;
    MessageCallback                 on_message_;
    StateCallback                   on_state_change_;
    std::shared_ptr<spdlog::logger> logger_;

    std::unique_ptr<WsStream>       ws_;
    beast::flat_buffer              read_buf_;
    std::deque<std::string>         write_queue_;
    bool                            writing_   = false;
    bool                            running_   = false;
    bool                            connected_ = false;

    // steady_seconds() at the moment this connection came up, 0 while down.
    // Closing it out in notify_state() is what makes the session-duration
    // histogram immune to the several redundant notify_state(false) calls on
    // the teardown paths.
    double                          session_start_ = 0.0;
};

} // namespace axon_market_data
