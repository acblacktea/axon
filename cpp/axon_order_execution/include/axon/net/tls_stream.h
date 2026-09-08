// TLS over OpenSSL memory BIOs.
//
// The usual way to use OpenSSL is SSL_set_fd, which hands OpenSSL the socket
// and lets it do its own read() and write(). That is simpler and wrong here,
// for one reason: it puts the socket calls inside OpenSSL, where they cannot
// be batched, timed, or driven from a busy-poll loop, and it forces a copy
// into OpenSSL's internal buffers on every read.
//
// So this uses MEMORY BIOs and a pump model. The caller owns the socket and
// the buffers, and moves bytes explicitly:
//
//     socket.read(buf)          -> feed_encrypted(buf)   -- ciphertext IN
//     read_plaintext(out)                                -- plaintext OUT
//     write_plaintext(msg)                               -- plaintext IN
//     take_encrypted(buf)       -> socket.write(buf)     -- ciphertext OUT
//
// Every step is non-blocking and returns how far it got, so the whole thing
// drops into a poll loop with no threads and no callbacks.
//
// COST, because it is a large share of a 10us budget: an AES-GCM record with
// AES-NI runs around 0.5-2us each way on a small message. That is 10-40% of
// the target and it is not optional -- every venue is wss:// only. What IS
// controllable is the number of records: one SSL_write per logical message,
// never per field.
//
// Certificate verification is ON by default and hostname checking with it.
// Turning either off is a deliberate act with a named method, because a
// silently unverified TLS connection to an exchange is indistinguishable from
// a verified one right up until it is not.

#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "axon/net/tcp_socket.h"

// Forward declarations so OpenSSL headers do not leak into every consumer.
extern "C" {
struct ssl_st;
struct ssl_ctx_st;
struct bio_st;
struct x509_st;
}

namespace axon::net {

class TlsError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// A client TLS configuration. Build one per process, share it across
// connections -- it holds the trust store, and re-reading that per connection
// is both slow and pointless.
class TlsContext {
 public:
  TlsContext();
  ~TlsContext();

  TlsContext(const TlsContext&) = delete;
  TlsContext& operator=(const TlsContext&) = delete;
  TlsContext(TlsContext&&) noexcept;
  TlsContext& operator=(TlsContext&&) noexcept;

  // Loads the platform's default trust store. Required before connecting to a
  // real venue; throws if no trust store can be found, rather than silently
  // continuing with an empty one.
  void use_default_trust_store();

  // Trusts a single PEM certificate in addition to whatever else is trusted.
  // Used by the tests, which stand up a real TLS server with a generated
  // self-signed certificate and then verify against it -- so verification
  // stays ON in the tests rather than being switched off to make them pass.
  void add_trusted_certificate_pem(std::string_view pem);

  // Disables peer verification. There is no good reason to call this against
  // an exchange; it exists so a diagnostic tool can be pointed at a proxy.
  void set_verify_peer(bool enabled) noexcept;
  bool verify_peer() const noexcept { return verify_peer_; }

  ssl_ctx_st* native() const noexcept { return ctx_; }

 private:
  ssl_ctx_st* ctx_ = nullptr;
  bool verify_peer_ = true;
};

enum class TlsState : std::uint8_t {
  kIdle = 0,
  kHandshaking = 1,
  kEstablished = 2,
  // close_notify sent or received; the socket may still need draining.
  kClosing = 3,
  kClosed = 4,
  kError = 5,
};

class TlsStream {
 public:
  TlsStream() noexcept = default;
  ~TlsStream();

  TlsStream(const TlsStream&) = delete;
  TlsStream& operator=(const TlsStream&) = delete;
  TlsStream(TlsStream&&) noexcept;
  TlsStream& operator=(TlsStream&&) noexcept;

  // Begins a client handshake. `hostname` is used for SNI and, when the
  // context verifies peers, for hostname validation -- a certificate that is
  // valid but for the wrong host is rejected.
  void start_client(const TlsContext& ctx, std::string_view hostname);

  // Also usable as a server, which is what makes an in-process end-to-end test
  // possible without a subprocess.
  void start_server(const TlsContext& ctx);

  // Drives the handshake as far as the currently available bytes allow.
  // kWouldBlock means "feed me more ciphertext and/or send what take_encrypted
  // gives you, then call again".
  IoResult handshake() noexcept;

  TlsState state() const noexcept { return state_; }
  bool established() const noexcept { return state_ == TlsState::kEstablished; }

  // ---- the pump ---------------------------------------------------------

  // Ciphertext from the socket into TLS. Returns how much was consumed.
  IoResult feed_encrypted(const void* data, std::size_t len) noexcept;

  // Ciphertext out of TLS, to be written to the socket. kWouldBlock means
  // there is nothing pending.
  IoResult take_encrypted(void* out, std::size_t cap) noexcept;

  bool has_encrypted_pending() const noexcept;

  // Application data.
  IoResult read_plaintext(void* out, std::size_t cap) noexcept;
  IoResult write_plaintext(const void* data, std::size_t len) noexcept;

  // Sends close_notify. The caller must still flush take_encrypted().
  void shutdown() noexcept;

  void reset() noexcept;

  // Negotiated protocol version and cipher, for the startup log. Worth
  // recording: a venue silently downgrading is something you want to see.
  std::string description() const;

  // Human-readable reason the stream failed, or empty.
  const std::string& last_error() const noexcept { return last_error_; }

 private:
  IoResult map_ssl_result(int rc) noexcept;

  ssl_st* ssl_ = nullptr;
  bio_st* rbio_ = nullptr;  // ciphertext IN  (we write, OpenSSL reads)
  bio_st* wbio_ = nullptr;  // ciphertext OUT (OpenSSL writes, we read)
  TlsState state_ = TlsState::kIdle;
  std::string last_error_;
};

// Generates a self-signed certificate and key, both PEM.
//
// Test scaffolding, deliberately kept in the shipping library rather than in
// the test tree: standing up a real TLS server is the only way to prove the
// pump above actually interoperates, and a diagnostic build wants the same
// ability. Never use this for anything a venue will see.
struct SelfSignedCertificate {
  std::string certificate_pem;
  std::string private_key_pem;
};

SelfSignedCertificate make_self_signed_certificate(
    std::string_view common_name = "localhost", int valid_days = 1);

// Loads a certificate and key into a context so it can act as a server.
void use_certificate(TlsContext& ctx, std::string_view certificate_pem,
                     std::string_view private_key_pem);

}  // namespace axon::net
