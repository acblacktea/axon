// An in-process WebSocket server over real TLS on loopback, for exercising the
// client's connection lifecycle: connect, receive, send, heartbeat, the server
// closing or dropping the link, and reconnection.
//
// It runs on its own thread and io_context. The test thread scripts it through
// the methods below, which post onto that thread; what it observed is read
// back under a mutex.

#pragma once

#include <atomic>
#include <deque>
#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <openssl/evp.h>
#include <openssl/x509.h>

namespace mds_test {

namespace ws_server_detail {
namespace net       = boost::asio;
namespace beast     = boost::beast;
namespace websocket = beast::websocket;
namespace ssl       = net::ssl;
using tcp           = net::ip::tcp;

// A throwaway self-signed P-256 certificate, generated per server so no key
// material lives in the repository.
inline void install_self_signed(ssl::context& ctx) {
    EVP_PKEY* key = EVP_EC_gen("P-256");
    X509*     crt = X509_new();
    X509_set_version(crt, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(crt), 1);
    X509_gmtime_adj(X509_getm_notBefore(crt), 0);
    X509_gmtime_adj(X509_getm_notAfter(crt), 3600);
    X509_set_pubkey(crt, key);
    X509_NAME* name = X509_get_subject_name(crt);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               reinterpret_cast<const unsigned char*>("localhost"), -1, -1, 0);
    X509_set_issuer_name(crt, name);
    X509_sign(crt, key, EVP_sha256());
    SSL_CTX_use_certificate(ctx.native_handle(), crt);
    SSL_CTX_use_PrivateKey(ctx.native_handle(), key);
    X509_free(crt);
    EVP_PKEY_free(key);
}
} // namespace ws_server_detail

class WsTestServer {
    using Ws = ws_server_detail::websocket::stream<
        ws_server_detail::beast::ssl_stream<ws_server_detail::beast::tcp_stream>>;

    struct Conn {
        explicit Conn(ws_server_detail::tcp::socket s, ws_server_detail::ssl::context& ctx)
            : ws(std::move(s), ctx) {}
        Ws                      ws;
        std::deque<std::string> outq;
        bool                    writing = false;
    };

public:
    WsTestServer() : ssl_(ws_server_detail::ssl::context::tls_server), acceptor_(ioc_) {
        namespace net = ws_server_detail::net;
        ws_server_detail::install_self_signed(ssl_);
        ws_server_detail::tcp::endpoint ep(net::ip::make_address("127.0.0.1"), 0);
        acceptor_.open(ep.protocol());
        acceptor_.set_option(net::socket_base::reuse_address(true));
        acceptor_.bind(ep);
        acceptor_.listen();
        port_ = acceptor_.local_endpoint().port();
        net::co_spawn(ioc_, accept_loop(), net::detached);
        thread_ = std::thread([this] { ioc_.run(); });
    }

    ~WsTestServer() { shutdown(); }

    unsigned short port() const { return port_; }

    // TCP connections accepted, and WebSocket handshakes completed.
    int accepted() const { return accepted_; }
    int handshakes() const { return handshakes_; }
    int open_connections() const { return open_; }

    std::vector<std::string> received() const {
        std::lock_guard lock(mu_);
        return received_;
    }

    // Sends a text frame to every open connection.
    void send(std::string text) {
        on_server([this, text = std::move(text)] {
            for (auto& c : conns_) enqueue(c, text);
        });
    }

    // A clean WebSocket close (code 1000) from the server side.
    void close_all() {
        on_server([this] {
            for (auto& c : conns_)
                ws_server_detail::net::co_spawn(
                    ioc_,
                    [c]() -> ws_server_detail::net::awaitable<void> {
                        try {
                            co_await c->ws.async_close(ws_server_detail::websocket::close_code::normal,
                                                       ws_server_detail::net::use_awaitable);
                        } catch (...) {
                        }
                    },
                    ws_server_detail::net::detached);
        });
    }

    // The TCP connection vanishes without a close handshake, as when a venue
    // load balancer resets it.
    void drop_all() {
        on_server([this] {
            for (auto& c : conns_) {
                boost::system::error_code ec;
                ws_server_detail::beast::get_lowest_layer(c->ws).socket().close(ec);
            }
        });
    }

    // Further connection attempts are refused.
    void stop_accepting() {
        on_server([this] {
            boost::system::error_code ec;
            acceptor_.close(ec);
        });
    }

    void shutdown() {
        if (!thread_.joinable()) return;
        stop_accepting();
        drop_all();
        ws_server_detail::net::post(ioc_, [this] { ioc_.stop(); });
        thread_.join();
    }

private:
    template <class F>
    void on_server(F&& f) {
        ws_server_detail::net::post(ioc_, std::forward<F>(f));
    }

    ws_server_detail::net::awaitable<void> accept_loop() {
        namespace net = ws_server_detail::net;
        for (;;) {
            ws_server_detail::tcp::socket sock(ioc_);
            try {
                co_await acceptor_.async_accept(sock, net::use_awaitable);
            } catch (...) {
                co_return;
            }
            ++accepted_;
            auto conn = std::make_shared<Conn>(std::move(sock), ssl_);
            net::co_spawn(ioc_, session(conn), net::detached);
        }
    }

    ws_server_detail::net::awaitable<void> session(std::shared_ptr<Conn> c) {
        namespace net = ws_server_detail::net;
        try {
            co_await c->ws.next_layer().async_handshake(ws_server_detail::ssl::stream_base::server,
                                                        net::use_awaitable);
            co_await c->ws.async_accept(net::use_awaitable);
        } catch (...) {
            co_return;
        }
        ++handshakes_;
        ++open_;
        conns_.push_back(c);
        try {
            for (;;) {
                ws_server_detail::beast::flat_buffer buf;
                co_await c->ws.async_read(buf, net::use_awaitable);
                std::lock_guard lock(mu_);
                received_.push_back(ws_server_detail::beast::buffers_to_string(buf.data()));
            }
        } catch (...) {
        }
        conns_.remove(c);
        --open_;
    }

    void enqueue(const std::shared_ptr<Conn>& c, std::string text) {
        namespace net = ws_server_detail::net;
        c->outq.push_back(std::move(text));
        if (c->writing) return;
        c->writing = true;
        net::co_spawn(
            ioc_,
            [c]() -> net::awaitable<void> {
                try {
                    while (!c->outq.empty()) {
                        auto msg = std::move(c->outq.front());
                        c->outq.pop_front();
                        co_await c->ws.async_write(net::buffer(msg), net::use_awaitable);
                    }
                } catch (...) {
                }
                c->writing = false;
            },
            net::detached);
    }

    ws_server_detail::net::io_context    ioc_;
    ws_server_detail::ssl::context       ssl_;
    ws_server_detail::tcp::acceptor      acceptor_;
    unsigned short                       port_ = 0;
    std::thread                          thread_;
    std::list<std::shared_ptr<Conn>>     conns_;  // server thread only
    std::atomic<int>                     accepted_{0};
    std::atomic<int>                     handshakes_{0};
    std::atomic<int>                     open_{0};
    mutable std::mutex                   mu_;
    std::vector<std::string>             received_;
};

}  // namespace mds_test
