#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
#include <random>
#include <string>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <spdlog/spdlog.h>

namespace axon::market_data {

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
        // How long a heartbeat may go unanswered before the link is declared
        // dead and torn down. Any frame received after the heartbeat counts as
        // the answer: a venue that is streaming data is plainly alive, and the
        // text heartbeats are answered in each venue's own format.
        std::chrono::seconds pong_timeout{10};
        // Reconnect backoff doubles from the base up to the max, plus up to 20%
        // random jitter on top so that connections which dropped together do
        // not retry in lockstep. It restarts from the base after a session
        // that stayed up for at least the base delay.
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

    // Must be called on a client owned by a shared_ptr. The connection's
    // coroutines hold a reference to the client until they finish, so a
    // client is never destroyed under an operation still in flight -- which
    // also means the owner has to call stop() rather than just drop it.
    void start();
    void stop();

    // Send a text message. Must be called from the io_context thread.
    // Writes are serialized through an internal queue — Beast allows only one
    // outstanding write (or ping) per stream.
    net::awaitable<void> send(std::string msg);

    bool is_connected() const { return connected_; }

private:
    using Clock  = std::chrono::steady_clock;
    using Stream = std::shared_ptr<WsStream>;
    // A queued outgoing frame: text, or nullopt for a WebSocket control ping.
    // Pings go through the same queue because Beast allows only one write
    // operation -- and a ping is one -- in flight at a time.
    using Outgoing = std::optional<std::string>;

    // Every coroutine takes the client by shared_ptr (`self`) and the stream
    // it works on by shared_ptr, so neither stop() nor a reconnect can free
    // either while an operation on it is pending. `gen` is the start() that
    // spawned it: a coroutine left over from before a stop() must not touch
    // the state of a later start().
    net::awaitable<void> run(std::shared_ptr<WebSocketClient> self, std::uint64_t gen);
    net::awaitable<Stream> connect(std::uint64_t gen);
    net::awaitable<void> read_loop(Stream ws, std::uint64_t gen);
    net::awaitable<void> ping_loop(std::shared_ptr<WebSocketClient> self,
                                   Stream ws, std::uint64_t gen);
    net::awaitable<void> write(Outgoing msg);

    bool live(std::uint64_t gen) const { return running_ && gen == gen_; }
    double backoff_delay(int failures);

    // Metrics only; notify_state() also tells the owner.
    void record_state(bool connected);
    void notify_state(bool connected);

    net::io_context&                ioc_;
    ssl::context                    ssl_ctx_;
    Config                          cfg_;
    MessageCallback                 on_message_;
    StateCallback                   on_state_change_;
    std::shared_ptr<spdlog::logger> logger_;

    Stream                          ws_;
    std::deque<Outgoing>            write_queue_;
    bool                            writing_   = false;
    bool                            running_   = false;
    bool                            connected_ = false;
    std::uint64_t                   gen_       = 0;

    // When the current connection last received anything, data or control
    // frame. The heartbeat check compares it against when the ping went out.
    Clock::time_point               last_rx_{};

    std::mt19937                    rng_{std::random_device{}()};

    // steady_seconds() at the moment this connection came up, 0 while down.
    // Closing it out in notify_state() is what makes the session-duration
    // histogram immune to the several redundant notify_state(false) calls on
    // the teardown paths.
    double                          session_start_ = 0.0;
};

} // namespace axon::market_data
