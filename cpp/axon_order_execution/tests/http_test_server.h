// A loopback HTTP/1.1 server for HttpClient tests: answers one connection's
// requests with a fixed body, over TLS or plain TCP. Same shape as
// WsTestServer, minus the WebSocket.

#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstddef>
#include <string>
#include <thread>
#include <vector>

#include "axon/core/clock.h"
#include "axon/net/tcp_socket.h"
#include "axon/net/tls_stream.h"

class HttpTestServer {
 public:
  HttpTestServer(std::string body, bool use_tls) : body_(std::move(body)), use_tls_(use_tls) {
    if (use_tls_) {
      cert_ = axon::net::make_self_signed_certificate("localhost");
      axon::net::use_certificate(ctx_, cert_.certificate_pem, cert_.private_key_pem);
      ctx_.set_verify_peer(false);
    }
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    const int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(listen_fd_, 4);
    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);
    thread_ = std::thread([this] { run(); });
  }

  ~HttpTestServer() {
    stop_.store(true, std::memory_order_release);
    ::shutdown(listen_fd_, SHUT_RDWR);
    ::close(listen_fd_);
    thread_.join();
  }

  std::uint16_t port() const noexcept { return port_; }
  const std::string& certificate_pem() const noexcept { return cert_.certificate_pem; }

 private:
  void run() {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      return;
    }
    axon::net::TcpSocket sock = axon::net::TcpSocket::adopt(fd);
    axon::net::TlsStream tls;
    if (use_tls_) {
      tls.start_server(ctx_);
    }
    const std::string response = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                                 "Content-Length: " +
                                 std::to_string(body_.size()) + "\r\n\r\n" + body_;
    std::vector<std::byte> in(16384);
    std::vector<std::byte> out(16384);
    std::string plain_rx;
    std::string plain_tx;

    while (!stop_.load(std::memory_order_acquire)) {
      const auto r = sock.read(in.data(), in.size());
      if (r.status == axon::net::IoStatus::kOk) {
        if (use_tls_) {
          std::size_t fed = 0;
          while (fed < r.bytes) {
            const auto f = tls.feed_encrypted(in.data() + fed, r.bytes - fed);
            if (f.status != axon::net::IoStatus::kOk) break;
            fed += f.bytes;
          }
        } else {
          plain_rx.append(reinterpret_cast<const char*>(in.data()), r.bytes);
        }
      } else if (r.status != axon::net::IoStatus::kWouldBlock) {
        return;
      }
      if (use_tls_) {
        if (!tls.established()) tls.handshake();
        while (tls.established()) {
          const auto p = tls.read_plaintext(in.data(), in.size());
          if (p.status != axon::net::IoStatus::kOk) break;
          plain_rx.append(reinterpret_cast<const char*>(in.data()), p.bytes);
        }
      }
      // One response per request head.
      for (auto end = plain_rx.find("\r\n\r\n"); end != std::string::npos;
           end = plain_rx.find("\r\n\r\n")) {
        plain_rx.erase(0, end + 4);
        plain_tx += response;
      }
      if (use_tls_) {
        while (!plain_tx.empty()) {
          const auto w = tls.write_plaintext(plain_tx.data(), plain_tx.size());
          if (w.status != axon::net::IoStatus::kOk) break;
          plain_tx.erase(0, w.bytes);
        }
        for (;;) {
          const auto t = tls.take_encrypted(out.data(), out.size());
          if (t.status != axon::net::IoStatus::kOk) break;
          if (!write_all(sock, out.data(), t.bytes)) return;
        }
      } else if (!plain_tx.empty()) {
        if (!write_all(sock, plain_tx.data(), plain_tx.size())) return;
        plain_tx.clear();
      }
      axon::core::cpu_pause();
    }
  }

  bool write_all(axon::net::TcpSocket& sock, const void* data, std::size_t n) {
    std::size_t sent = 0;
    while (sent < n && !stop_.load(std::memory_order_acquire)) {
      const auto w = sock.write(static_cast<const char*>(data) + sent, n - sent);
      if (w.status == axon::net::IoStatus::kWouldBlock) continue;
      if (w.status != axon::net::IoStatus::kOk) return false;
      sent += w.bytes;
    }
    return true;
  }

  std::string body_;
  bool use_tls_;
  axon::net::TlsContext ctx_;
  axon::net::SelfSignedCertificate cert_;
  int listen_fd_ = -1;
  std::uint16_t port_ = 0;
  std::atomic<bool> stop_{false};
  std::thread thread_;
};
