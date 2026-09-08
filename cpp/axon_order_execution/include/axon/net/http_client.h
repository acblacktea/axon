// Non-blocking HTTP/1.1 client over the same TLS stream the WebSocket uses.
//
// Replaces util/http.py. Needed for three things the WebSocket cannot do:
// Binance's listenKey, order entry on the three perpetual venues, and every
// reconciliation snapshot.
//
// ASYNC BY SUBMISSION, not by coroutine. submit() queues a request and returns
// an id; poll() drives the sockets and invokes the completion callback when a
// response is whole. That keeps the engine's single busy-poll loop as the only
// scheduler in the process -- a blocking REST call inside it would stall every
// venue feed for the duration of an internet round trip.
//
// CONNECTIONS ARE KEPT ALIVE, one per host. This is worth more than it looks:
// a fresh TLS handshake costs two extra round trips, so a per-request
// connection would triple the latency of every order on the perp venues.
// HTTP/1.1 without pipelining means one request in flight per connection, so
// the pool holds several per host.
//
// NOT a general-purpose HTTP client. No redirects, no chunked request bodies,
// no compression, no cookies. It speaks exactly the subset four exchange REST
// APIs use, and anything else is a deliberate omission rather than a gap.

#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "axon/net/tls_stream.h"

namespace axon::net {

struct HttpHeader {
  std::string name;
  std::string value;
};

struct HttpRequest {
  std::string method = "GET";
  // Includes the query string. Already signed where the venue signs the query.
  std::string path = "/";
  std::string host;
  std::uint16_t port = 443;
  bool use_tls = true;
  std::vector<HttpHeader> headers;
  std::string body;
  // Gives up after this long. A venue that stops responding mid-request must
  // not hold a slot forever.
  double timeout_seconds = 10.0;
};

struct HttpResponse {
  // 0 means the request never completed; `error` says why.
  int status = 0;
  std::vector<HttpHeader> headers;
  std::string body;
  std::string error;
  // Wall time from submission to completion, for the EMS latency histogram.
  double elapsed_seconds = 0.0;

  bool ok() const noexcept { return status >= 200 && status < 300; }
};

class HttpClient {
 public:
  using Callback = std::function<void(const HttpResponse&)>;

  // `tls` must outlive the client. `max_connections_per_host` bounds how many
  // requests can be in flight to one venue; beyond that they queue.
  explicit HttpClient(const TlsContext* tls,
                      std::size_t max_connections_per_host = 4);
  ~HttpClient();

  HttpClient(const HttpClient&) = delete;
  HttpClient& operator=(const HttpClient&) = delete;

  // Queues a request. The callback runs from poll(), on the calling thread.
  // Returns a monotonic id, useful for logs.
  std::uint64_t submit(HttpRequest request, Callback callback);

  // Drives every connection. Returns how many responses completed.
  std::size_t poll();

  std::size_t pending() const noexcept;
  std::uint64_t requests_sent() const noexcept { return requests_sent_; }
  std::uint64_t requests_failed() const noexcept { return requests_failed_; }

 private:
  struct Impl;
  std::unique_ptr<Impl> impl_;
  std::uint64_t next_id_ = 1;
  std::uint64_t requests_sent_ = 0;
  std::uint64_t requests_failed_ = 0;
};

// Parses a response head. Exposed so the parser can be exercised without a
// socket. Returns the number of bytes the head occupied, or 0 if it is not yet
// complete.
//
// `content_length` is -1 when the response is chunked, which the four venues
// do use for larger snapshots.
std::size_t parse_http_head(std::string_view data, int& status,
                            std::vector<HttpHeader>& headers,
                            long long& content_length, bool& chunked,
                            bool& keep_alive);

// Decodes a chunked body in place. Returns false if the encoding is malformed,
// true with `complete` set once the terminating zero-length chunk arrives.
bool decode_chunked(std::string_view input, std::string& out, bool& complete,
                    std::size_t& consumed);

}  // namespace axon::net
