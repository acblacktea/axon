// A real WebSocket server, in-process, for end-to-end tests.
//
// Real TCP on loopback, real TLS with a certificate generated at runtime, real
// RFC 6455 framing. Nothing is stubbed, because a mock of the transport would
// only prove the mock and the client agree -- and every bug worth catching
// here lives in the places they would agree to skip: the handshake, the record
// boundaries, partial writes, close semantics.
//
// The certificate is generated fresh per test run rather than committed, so
// there is no private key in the repository and nothing to expire.
//
// Deliberately uses the SAME TlsStream pump as the client, in server mode.
// That means a bug symmetric between the two could hide -- but the alternative
// (a second TLS implementation) is worse, and the interop that matters is
// checked separately by the fact that OpenSSL is on both ends doing a real
// handshake with real certificate verification.

#pragma once

#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "axon/core/platform.h"
#include "axon/net/tcp_socket.h"
#include "axon/net/tls_stream.h"
#include "axon/net/ws_frame.h"
#include "axon/net/ws_handshake.h"

namespace axon::test {

class WsTestServer {
 public:
  struct Options {
    bool use_tls = true;
    // Send a ping as soon as the connection opens, so the client's automatic
    // pong can be observed.
    bool ping_on_open = false;
    // Refuse the upgrade, to exercise the client's failure path.
    bool reject_handshake = false;
    // Return a Sec-WebSocket-Accept that does not match the key.
    bool wrong_accept = false;
    // Offer an extension the client never asked for.
    bool offer_extension = false;
    // Send the first frame in the SAME segment as the handshake response, which
    // is legal and is how a client that over-consumes loses its first message.
    std::string greeting;
    // Common name for the generated certificate.
    std::string common_name = "localhost";
  };

  WsTestServer() : WsTestServer(Options{}) {}

  explicit WsTestServer(Options options) : options_(std::move(options)) {
    if (options_.use_tls) {
      cert_ = net::make_self_signed_certificate(options_.common_name);
      net::use_certificate(ctx_, cert_.certificate_pem, cert_.private_key_pem);
      // A server does not verify its client here.
      ctx_.set_verify_peer(false);
    }
    listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    const int one = 1;
    ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;  // ephemeral
    ::bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    ::listen(listen_fd_, 4);

    socklen_t len = sizeof(addr);
    ::getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&addr), &len);
    port_ = ntohs(addr.sin_port);

    thread_ = std::thread([this] { run(); });
  }

  ~WsTestServer() { stop(); }

  void stop() {
    stop_.store(true, std::memory_order_release);
    if (listen_fd_ >= 0) {
      ::shutdown(listen_fd_, SHUT_RDWR);
      ::close(listen_fd_);
      listen_fd_ = -1;
    }
    if (thread_.joinable()) {
      thread_.join();
    }
  }

  std::uint16_t port() const noexcept { return port_; }
  const std::string& certificate_pem() const noexcept {
    return cert_.certificate_pem;
  }

  std::uint64_t messages_echoed() const noexcept {
    return echoed_.load(std::memory_order_relaxed);
  }
  std::uint64_t pongs_received() const noexcept {
    return pongs_.load(std::memory_order_relaxed);
  }
  bool saw_close() const noexcept { return saw_close_.load(std::memory_order_relaxed); }

 private:
  void run() {
    const int fd = ::accept(listen_fd_, nullptr, nullptr);
    if (fd < 0) {
      return;
    }
    net::TcpSocket sock = net::TcpSocket::adopt(fd);

    net::TlsStream tls;
    if (options_.use_tls) {
      tls.start_server(ctx_);
    }

    std::vector<std::byte> cipher_in(16384);
    std::vector<std::byte> cipher_out(16384);
    std::string plain_rx;
    std::string plain_tx;
    bool handshake_done = false;

    auto pump = [&]() -> bool {
      // socket -> tls
      for (;;) {
        const auto r = sock.read(cipher_in.data(), cipher_in.size());
        if (r.status == net::IoStatus::kWouldBlock) {
          break;
        }
        if (r.status != net::IoStatus::kOk) {
          return false;
        }
        if (options_.use_tls) {
          std::size_t fed = 0;
          while (fed < r.bytes) {
            const auto f = tls.feed_encrypted(cipher_in.data() + fed, r.bytes - fed);
            if (f.status != net::IoStatus::kOk) {
              break;
            }
            fed += f.bytes;
          }
        } else {
          plain_rx.append(reinterpret_cast<const char*>(cipher_in.data()), r.bytes);
        }
      }

      if (options_.use_tls) {
        if (!tls.established()) {
          tls.handshake();
        }
        if (tls.established()) {
          for (;;) {
            const auto p = tls.read_plaintext(cipher_in.data(), cipher_in.size());
            if (p.status != net::IoStatus::kOk) {
              break;
            }
            plain_rx.append(reinterpret_cast<const char*>(cipher_in.data()), p.bytes);
          }
        }
        // plaintext -> tls
        while (!plain_tx.empty()) {
          const auto w = tls.write_plaintext(plain_tx.data(), plain_tx.size());
          if (w.status != net::IoStatus::kOk) {
            break;
          }
          plain_tx.erase(0, w.bytes);
        }
        // tls -> socket
        for (;;) {
          const auto t = tls.take_encrypted(cipher_out.data(), cipher_out.size());
          if (t.status != net::IoStatus::kOk) {
            break;
          }
          std::size_t sent = 0;
          while (sent < t.bytes) {
            const auto w = sock.write(cipher_out.data() + sent, t.bytes - sent);
            if (w.status == net::IoStatus::kWouldBlock) {
              continue;
            }
            if (w.status != net::IoStatus::kOk) {
              return false;
            }
            sent += w.bytes;
          }
        }
      } else {
        while (!plain_tx.empty()) {
          const auto w = sock.write(plain_tx.data(), plain_tx.size());
          if (w.status == net::IoStatus::kWouldBlock) {
            break;
          }
          if (w.status != net::IoStatus::kOk) {
            return false;
          }
          plain_tx.erase(0, w.bytes);
        }
      }
      return true;
    };

    while (!stop_.load(std::memory_order_acquire)) {
      if (!pump()) {
        break;
      }

      if (!handshake_done) {
        const std::size_t end = plain_rx.find("\r\n\r\n");
        if (end == std::string::npos) {
          core::cpu_pause();
          continue;
        }
        const std::string request = plain_rx.substr(0, end + 4);
        plain_rx.erase(0, end + 4);

        if (options_.reject_handshake) {
          plain_tx += "HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n";
          pump();
          break;
        }

        // Echo back the Sec-WebSocket-Accept the client's key demands.
        std::string key;
        const std::size_t kpos = request.find("Sec-WebSocket-Key: ");
        if (kpos != std::string::npos) {
          const std::size_t kend = request.find("\r\n", kpos);
          key = request.substr(kpos + 19, kend - kpos - 19);
        }
        const std::string accept =
            options_.wrong_accept ? "AAAAAAAAAAAAAAAAAAAAAAAAAAA="
                                  : net::ws_compute_accept(key);

        plain_tx += "HTTP/1.1 101 Switching Protocols\r\n";
        plain_tx += "Upgrade: websocket\r\n";
        plain_tx += "Connection: Upgrade\r\n";
        if (options_.offer_extension) {
          plain_tx += "Sec-WebSocket-Extensions: permessage-deflate\r\n";
        }
        plain_tx += "Sec-WebSocket-Accept: " + accept + "\r\n\r\n";

        // Optionally append a frame to the SAME buffer, so it lands in the
        // same TLS record and the same TCP segment as the response.
        if (!options_.greeting.empty()) {
          plain_tx += encode_server_frame(net::WsOpcode::kText,
                                          options_.greeting);
        }
        if (options_.ping_on_open) {
          plain_tx += encode_server_frame(net::WsOpcode::kPing, "hb");
        }
        handshake_done = true;
        continue;
      }

      // Frame loop: echo text/binary, count pongs, mirror close.
      for (;;) {
        const auto decoded = net::ws_decode_frame(
            reinterpret_cast<const std::byte*>(plain_rx.data()), plain_rx.size());
        if (decoded.status != net::WsDecodeStatus::kOk) {
          break;
        }

        std::string payload(reinterpret_cast<const char*>(decoded.payload),
                            static_cast<std::size_t>(decoded.header.payload_length));
        if (decoded.header.masked) {
          net::ws_mask(reinterpret_cast<std::byte*>(payload.data()),
                       payload.size(), decoded.header.mask_key);
        }

        switch (decoded.header.opcode) {
          case net::WsOpcode::kText:
          case net::WsOpcode::kBinary:
            plain_tx += encode_server_frame(decoded.header.opcode, payload);
            echoed_.fetch_add(1, std::memory_order_relaxed);
            break;
          case net::WsOpcode::kPong:
            pongs_.fetch_add(1, std::memory_order_relaxed);
            break;
          case net::WsOpcode::kPing:
            plain_tx += encode_server_frame(net::WsOpcode::kPong, payload);
            break;
          case net::WsOpcode::kClose:
            saw_close_.store(true, std::memory_order_relaxed);
            plain_tx += encode_server_frame(net::WsOpcode::kClose, payload);
            break;
          default:
            break;
        }
        plain_rx.erase(0, decoded.total_size);
      }
      core::cpu_pause();
    }

    // Give anything queued a chance to leave before the socket closes.
    for (int i = 0; i < 1000 && !plain_tx.empty(); ++i) {
      if (!pump()) {
        break;
      }
    }
  }

  // Server-to-client frames are UNMASKED, per the RFC.
  static std::string encode_server_frame(net::WsOpcode opcode,
                                         std::string_view payload) {
    std::vector<std::byte> buf(payload.size() + net::kMaxFrameHeaderSize);
    const std::size_t n = net::ws_encode_header(
        buf.data(), buf.size(), opcode, /*fin=*/true, payload.size(),
        /*masked=*/false, 0);
    std::memcpy(buf.data() + n, payload.data(), payload.size());
    return std::string(reinterpret_cast<const char*>(buf.data()),
                       n + payload.size());
  }

  Options options_;
  net::TlsContext ctx_;
  net::SelfSignedCertificate cert_;
  int listen_fd_ = -1;
  std::uint16_t port_ = 0;
  std::thread thread_;
  std::atomic<bool> stop_{false};
  std::atomic<std::uint64_t> echoed_{0};
  std::atomic<std::uint64_t> pongs_{0};
  std::atomic<bool> saw_close_{false};
};

}  // namespace axon::test
