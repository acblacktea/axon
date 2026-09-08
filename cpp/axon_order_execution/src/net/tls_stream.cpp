#include "axon/net/tls_stream.h"

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>

#include <cstring>
#include <utility>

namespace axon::net {
namespace {

std::string drain_openssl_errors() {
  std::string out;
  unsigned long code = 0;
  char buf[256];
  while ((code = ERR_get_error()) != 0) {
    ERR_error_string_n(code, buf, sizeof(buf));
    if (!out.empty()) {
      out += "; ";
    }
    out += buf;
  }
  return out;
}

IoResult ok(std::size_t n) noexcept { return {IoStatus::kOk, n, 0}; }
IoResult would_block() noexcept { return {IoStatus::kWouldBlock, 0, 0}; }
IoResult closed() noexcept { return {IoStatus::kClosed, 0, 0}; }
IoResult failed() noexcept { return {IoStatus::kError, 0, 0}; }

}  // namespace

// ===========================================================================
// TlsContext
// ===========================================================================
TlsContext::TlsContext() {
  ctx_ = SSL_CTX_new(TLS_method());
  if (ctx_ == nullptr) {
    throw TlsError("SSL_CTX_new failed: " + drain_openssl_errors());
  }

  // TLS 1.2 is the floor. Every venue supports 1.2 and most support 1.3;
  // anything older has known breaks and is not worth the compatibility.
  SSL_CTX_set_min_proto_version(ctx_, TLS1_2_VERSION);

  // Let OpenSSL retry a write with a moved buffer. Without this, a partial
  // SSL_write followed by a retry from a different address is an error --
  // which is exactly what happens when the caller owns the buffers.
  SSL_CTX_set_mode(ctx_, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER |
                             SSL_MODE_ENABLE_PARTIAL_WRITE);

  SSL_CTX_set_verify(ctx_, SSL_VERIFY_PEER, nullptr);
}

TlsContext::~TlsContext() {
  if (ctx_ != nullptr) {
    SSL_CTX_free(ctx_);
    ctx_ = nullptr;
  }
}

TlsContext::TlsContext(TlsContext&& o) noexcept
    : ctx_(std::exchange(o.ctx_, nullptr)), verify_peer_(o.verify_peer_) {}

TlsContext& TlsContext::operator=(TlsContext&& o) noexcept {
  if (this != &o) {
    if (ctx_ != nullptr) {
      SSL_CTX_free(ctx_);
    }
    ctx_ = std::exchange(o.ctx_, nullptr);
    verify_peer_ = o.verify_peer_;
  }
  return *this;
}

void TlsContext::use_default_trust_store() {
  if (SSL_CTX_set_default_verify_paths(ctx_) != 1) {
    // Failing loudly matters: continuing with an empty trust store would make
    // every certificate invalid, which looks like a venue problem.
    throw TlsError("could not load the default trust store: " +
                   drain_openssl_errors());
  }
}

void TlsContext::add_trusted_certificate_pem(std::string_view pem) {
  BIO* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
  if (bio == nullptr) {
    throw TlsError("BIO_new_mem_buf failed");
  }
  X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
  BIO_free(bio);
  if (cert == nullptr) {
    throw TlsError("could not parse the PEM certificate: " +
                   drain_openssl_errors());
  }

  X509_STORE* store = SSL_CTX_get_cert_store(ctx_);
  const int rc = X509_STORE_add_cert(store, cert);
  X509_free(cert);
  if (rc != 1) {
    throw TlsError("X509_STORE_add_cert failed: " + drain_openssl_errors());
  }
}

void TlsContext::set_verify_peer(bool enabled) noexcept {
  verify_peer_ = enabled;
  SSL_CTX_set_verify(ctx_, enabled ? SSL_VERIFY_PEER : SSL_VERIFY_NONE, nullptr);
}

void use_certificate(TlsContext& ctx, std::string_view certificate_pem,
                     std::string_view private_key_pem) {
  BIO* cert_bio = BIO_new_mem_buf(certificate_pem.data(),
                                  static_cast<int>(certificate_pem.size()));
  X509* cert = PEM_read_bio_X509(cert_bio, nullptr, nullptr, nullptr);
  BIO_free(cert_bio);
  if (cert == nullptr) {
    throw TlsError("could not parse the certificate: " + drain_openssl_errors());
  }
  const int rc1 = SSL_CTX_use_certificate(ctx.native(), cert);
  X509_free(cert);
  if (rc1 != 1) {
    throw TlsError("SSL_CTX_use_certificate failed: " + drain_openssl_errors());
  }

  BIO* key_bio = BIO_new_mem_buf(private_key_pem.data(),
                                 static_cast<int>(private_key_pem.size()));
  EVP_PKEY* key = PEM_read_bio_PrivateKey(key_bio, nullptr, nullptr, nullptr);
  BIO_free(key_bio);
  if (key == nullptr) {
    throw TlsError("could not parse the private key: " + drain_openssl_errors());
  }
  const int rc2 = SSL_CTX_use_PrivateKey(ctx.native(), key);
  EVP_PKEY_free(key);
  if (rc2 != 1) {
    throw TlsError("SSL_CTX_use_PrivateKey failed: " + drain_openssl_errors());
  }
  if (SSL_CTX_check_private_key(ctx.native()) != 1) {
    throw TlsError("certificate and private key do not match");
  }
}

// ===========================================================================
// Self-signed certificate generation (test/diagnostic scaffolding)
// ===========================================================================
SelfSignedCertificate make_self_signed_certificate(std::string_view common_name,
                                                   int valid_days) {
  EVP_PKEY* key = EVP_RSA_gen(2048);
  if (key == nullptr) {
    throw TlsError("EVP_RSA_gen failed: " + drain_openssl_errors());
  }

  X509* cert = X509_new();
  if (cert == nullptr) {
    EVP_PKEY_free(key);
    throw TlsError("X509_new failed");
  }

  ASN1_INTEGER_set(X509_get_serialNumber(cert), 1);
  X509_gmtime_adj(X509_getm_notBefore(cert), 0);
  X509_gmtime_adj(X509_getm_notAfter(cert),
                  static_cast<long>(valid_days) * 24 * 60 * 60);
  X509_set_pubkey(cert, key);
  X509_set_version(cert, 2);  // v3

  X509_NAME* name = X509_get_subject_name(cert);
  const std::string cn(common_name);
  X509_NAME_add_entry_by_txt(
      name, "CN", MBSTRING_ASC,
      reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0);
  X509_set_issuer_name(cert, name);

  // A subjectAltName is REQUIRED for hostname verification: modern OpenSSL
  // ignores the CN entirely when checking a hostname, so a certificate without
  // a SAN fails verification no matter what its CN says.
  const std::string san = "DNS:" + cn + ",IP:127.0.0.1";
  X509_EXTENSION* ext = X509V3_EXT_conf_nid(
      nullptr, nullptr, NID_subject_alt_name, san.c_str());
  if (ext != nullptr) {
    X509_add_ext(cert, ext, -1);
    X509_EXTENSION_free(ext);
  }

  if (X509_sign(cert, key, EVP_sha256()) == 0) {
    X509_free(cert);
    EVP_PKEY_free(key);
    throw TlsError("X509_sign failed: " + drain_openssl_errors());
  }

  SelfSignedCertificate out;

  BIO* cert_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_X509(cert_bio, cert);
  char* cert_data = nullptr;
  const long cert_len = BIO_get_mem_data(cert_bio, &cert_data);
  out.certificate_pem.assign(cert_data, static_cast<std::size_t>(cert_len));
  BIO_free(cert_bio);

  BIO* key_bio = BIO_new(BIO_s_mem());
  PEM_write_bio_PrivateKey(key_bio, key, nullptr, nullptr, 0, nullptr, nullptr);
  char* key_data = nullptr;
  const long key_len = BIO_get_mem_data(key_bio, &key_data);
  out.private_key_pem.assign(key_data, static_cast<std::size_t>(key_len));
  BIO_free(key_bio);

  X509_free(cert);
  EVP_PKEY_free(key);
  return out;
}

// ===========================================================================
// TlsStream
// ===========================================================================
TlsStream::~TlsStream() { reset(); }

TlsStream::TlsStream(TlsStream&& o) noexcept
    : ssl_(std::exchange(o.ssl_, nullptr)),
      rbio_(std::exchange(o.rbio_, nullptr)),
      wbio_(std::exchange(o.wbio_, nullptr)),
      state_(std::exchange(o.state_, TlsState::kIdle)),
      last_error_(std::move(o.last_error_)) {}

TlsStream& TlsStream::operator=(TlsStream&& o) noexcept {
  if (this != &o) {
    reset();
    ssl_ = std::exchange(o.ssl_, nullptr);
    rbio_ = std::exchange(o.rbio_, nullptr);
    wbio_ = std::exchange(o.wbio_, nullptr);
    state_ = std::exchange(o.state_, TlsState::kIdle);
    last_error_ = std::move(o.last_error_);
  }
  return *this;
}

void TlsStream::reset() noexcept {
  if (ssl_ != nullptr) {
    // SSL_free releases the BIOs it owns, so they must not be freed again.
    SSL_free(ssl_);
    ssl_ = nullptr;
    rbio_ = nullptr;
    wbio_ = nullptr;
  }
  state_ = TlsState::kIdle;
  last_error_.clear();
}

namespace {

void attach_memory_bios(ssl_st* ssl, bio_st*& rbio, bio_st*& wbio) {
  rbio = BIO_new(BIO_s_mem());
  wbio = BIO_new(BIO_s_mem());
  if (rbio == nullptr || wbio == nullptr) {
    throw TlsError("BIO_new failed");
  }
  // Non-blocking semantics: an empty read BIO must report "retry", not EOF.
  BIO_set_mem_eof_return(rbio, -1);
  BIO_set_mem_eof_return(wbio, -1);
  SSL_set_bio(ssl, rbio, wbio);  // takes ownership of both
}

}  // namespace

void TlsStream::start_client(const TlsContext& ctx, std::string_view hostname) {
  reset();

  ssl_ = SSL_new(ctx.native());
  if (ssl_ == nullptr) {
    throw TlsError("SSL_new failed: " + drain_openssl_errors());
  }
  attach_memory_bios(ssl_, rbio_, wbio_);

  const std::string host(hostname);

  // SNI. Without it a venue behind shared infrastructure returns the wrong
  // certificate, or none.
  SSL_set_tlsext_host_name(ssl_, host.c_str());

  if (ctx.verify_peer()) {
    // Hostname verification is SEPARATE from chain verification in OpenSSL.
    // Skipping this leaves a connection that accepts any valid certificate for
    // any host -- which passes every test and defeats the point.
    SSL_set1_host(ssl_, host.c_str());
    SSL_set_hostflags(ssl_, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
  }

  SSL_set_connect_state(ssl_);
  state_ = TlsState::kHandshaking;
}

void TlsStream::start_server(const TlsContext& ctx) {
  reset();
  ssl_ = SSL_new(ctx.native());
  if (ssl_ == nullptr) {
    throw TlsError("SSL_new failed: " + drain_openssl_errors());
  }
  attach_memory_bios(ssl_, rbio_, wbio_);
  SSL_set_accept_state(ssl_);
  state_ = TlsState::kHandshaking;
}

IoResult TlsStream::map_ssl_result(int rc) noexcept {
  const int err = SSL_get_error(ssl_, rc);
  switch (err) {
    case SSL_ERROR_NONE:
      return ok(0);
    case SSL_ERROR_WANT_READ:
    case SSL_ERROR_WANT_WRITE:
      return would_block();
    case SSL_ERROR_ZERO_RETURN:
      state_ = TlsState::kClosing;
      return closed();
    case SSL_ERROR_SYSCALL:
    case SSL_ERROR_SSL:
    default: {
      const std::string detail = drain_openssl_errors();
      last_error_ = detail.empty() ? "TLS error" : detail;
      state_ = TlsState::kError;
      return failed();
    }
  }
}

IoResult TlsStream::handshake() noexcept {
  if (ssl_ == nullptr) {
    return failed();
  }
  if (state_ == TlsState::kEstablished) {
    return ok(0);
  }
  if (state_ == TlsState::kError) {
    return failed();
  }

  ERR_clear_error();
  const int rc = SSL_do_handshake(ssl_);
  if (rc == 1) {
    state_ = TlsState::kEstablished;
    return ok(0);
  }
  return map_ssl_result(rc);
}

IoResult TlsStream::feed_encrypted(const void* data, std::size_t len) noexcept {
  if (rbio_ == nullptr || len == 0) {
    return would_block();
  }
  const int n = BIO_write(rbio_, data, static_cast<int>(len));
  if (n <= 0) {
    return BIO_should_retry(rbio_) ? would_block() : failed();
  }
  return ok(static_cast<std::size_t>(n));
}

IoResult TlsStream::take_encrypted(void* out, std::size_t cap) noexcept {
  if (wbio_ == nullptr || cap == 0) {
    return would_block();
  }
  const int n = BIO_read(wbio_, out, static_cast<int>(cap));
  if (n <= 0) {
    return would_block();
  }
  return ok(static_cast<std::size_t>(n));
}

bool TlsStream::has_encrypted_pending() const noexcept {
  return wbio_ != nullptr && BIO_ctrl_pending(wbio_) > 0;
}

IoResult TlsStream::read_plaintext(void* out, std::size_t cap) noexcept {
  if (ssl_ == nullptr || cap == 0) {
    return would_block();
  }
  ERR_clear_error();
  const int n = SSL_read(ssl_, out, static_cast<int>(cap));
  if (n > 0) {
    return ok(static_cast<std::size_t>(n));
  }
  return map_ssl_result(n);
}

IoResult TlsStream::write_plaintext(const void* data, std::size_t len) noexcept {
  if (ssl_ == nullptr) {
    return failed();
  }
  if (len == 0) {
    return ok(0);
  }
  ERR_clear_error();
  const int n = SSL_write(ssl_, data, static_cast<int>(len));
  if (n > 0) {
    return ok(static_cast<std::size_t>(n));
  }
  return map_ssl_result(n);
}

void TlsStream::shutdown() noexcept {
  if (ssl_ != nullptr && state_ == TlsState::kEstablished) {
    SSL_shutdown(ssl_);
    state_ = TlsState::kClosing;
  }
}

std::string TlsStream::description() const {
  if (ssl_ == nullptr) {
    return "not started";
  }
  const char* version = SSL_get_version(ssl_);
  const char* cipher = SSL_get_cipher_name(ssl_);
  std::string out;
  out += version != nullptr ? version : "?";
  out += " / ";
  out += cipher != nullptr ? cipher : "?";
  return out;
}

}  // namespace axon::net
