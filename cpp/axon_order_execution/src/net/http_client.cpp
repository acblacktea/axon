#include "axon/net/http_client.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <deque>
#include <list>
#include <map>

#include "axon/core/clock.h"
#include "axon/net/byte_buffer.h"
#include "axon/net/tcp_socket.h"

namespace axon::net {
namespace {

double now_seconds() {
  return static_cast<double>(core::monotonic_ns()) / 1e9;
}

bool iequals(std::string_view a, std::string_view b) {
  if (a.size() != b.size()) {
    return false;
  }
  for (std::size_t i = 0; i < a.size(); ++i) {
    if (std::tolower(static_cast<unsigned char>(a[i])) !=
        std::tolower(static_cast<unsigned char>(b[i]))) {
      return false;
    }
  }
  return true;
}

std::string_view trim(std::string_view s) {
  std::size_t b = 0;
  while (b < s.size() && (s[b] == ' ' || s[b] == '\t')) {
    ++b;
  }
  std::size_t e = s.size();
  while (e > b && (s[e - 1] == ' ' || s[e - 1] == '\t' || s[e - 1] == '\r')) {
    --e;
  }
  return s.substr(b, e - b);
}

}  // namespace

// ---------------------------------------------------------------------------
std::size_t parse_http_head(std::string_view data, int& status,
                            std::vector<HttpHeader>& headers,
                            long long& content_length, bool& chunked,
                            bool& keep_alive) {
  const std::size_t end = data.find("\r\n\r\n");
  if (end == std::string_view::npos) {
    return 0;
  }

  status = 0;
  headers.clear();
  content_length = -1;
  chunked = false;
  // HTTP/1.1 keeps the connection alive unless told otherwise.
  keep_alive = true;

  const std::string_view head = data.substr(0, end);
  std::size_t line_start = 0;

  // Status line: HTTP/1.x SSS reason
  const std::size_t first_eol = head.find("\r\n");
  const std::string_view status_line =
      first_eol == std::string_view::npos ? head : head.substr(0, first_eol);
  if (status_line.size() < 12 || !status_line.starts_with("HTTP/1.")) {
    return end + 4;  // consumed, but status stays 0 so the caller fails it
  }
  for (std::size_t i = 9; i < 12; ++i) {
    const char c = status_line[i];
    if (c < '0' || c > '9') {
      return end + 4;
    }
    status = status * 10 + (c - '0');
  }
  line_start = (first_eol == std::string_view::npos) ? head.size() : first_eol + 2;

  while (line_start < head.size()) {
    std::size_t eol = head.find("\r\n", line_start);
    if (eol == std::string_view::npos) {
      eol = head.size();
    }
    const std::string_view line = head.substr(line_start, eol - line_start);
    line_start = eol + 2;

    const std::size_t colon = line.find(':');
    if (colon == std::string_view::npos) {
      continue;
    }
    HttpHeader header;
    header.name = std::string(trim(line.substr(0, colon)));
    header.value = std::string(trim(line.substr(colon + 1)));

    if (iequals(header.name, "Content-Length")) {
      content_length = std::strtoll(header.value.c_str(), nullptr, 10);
    } else if (iequals(header.name, "Transfer-Encoding")) {
      // Venues use chunked for larger snapshots, so this is not theoretical.
      chunked = header.value.find("chunked") != std::string::npos;
    } else if (iequals(header.name, "Connection")) {
      keep_alive = !iequals(header.value, "close");
    }
    headers.push_back(std::move(header));
  }
  return end + 4;
}

bool decode_chunked(std::string_view input, std::string& out, bool& complete,
                    std::size_t& consumed) {
  out.clear();
  complete = false;
  consumed = 0;

  std::size_t pos = 0;
  for (;;) {
    const std::size_t eol = input.find("\r\n", pos);
    if (eol == std::string_view::npos) {
      return true;  // need more bytes
    }
    // The size line may carry chunk extensions after a ';'.
    std::string_view size_text = input.substr(pos, eol - pos);
    if (const std::size_t semi = size_text.find(';');
        semi != std::string_view::npos) {
      size_text = size_text.substr(0, semi);
    }
    size_text = trim(size_text);
    if (size_text.empty()) {
      return false;
    }

    char* parse_end = nullptr;
    const std::string size_str(size_text);
    const unsigned long long size = std::strtoull(size_str.c_str(), &parse_end, 16);
    if (parse_end == size_str.c_str()) {
      return false;  // not hex
    }

    const std::size_t body_start = eol + 2;
    if (size == 0) {
      // Terminating chunk, followed by optional trailers and a blank line.
      const std::size_t trailer_end = input.find("\r\n", body_start);
      if (trailer_end == std::string_view::npos) {
        return true;  // need more
      }
      complete = true;
      consumed = trailer_end + 2;
      return true;
    }
    if (body_start + size + 2 > input.size()) {
      return true;  // need more
    }
    out.append(input.substr(body_start, size));
    pos = body_start + size + 2;  // skip the chunk's trailing CRLF
  }
}

// ---------------------------------------------------------------------------
namespace {

struct PendingRequest {
  std::uint64_t id = 0;
  HttpRequest request;
  HttpClient::Callback callback;
  double submitted_at = 0.0;
};

// One connection to one host, with whatever request it is currently serving.
struct Connection {
  std::string host;
  std::uint16_t port = 0;
  bool use_tls = true;

  TcpSocket socket;
  TlsStream tls;
  ByteBuffer rx{1u << 16};
  ByteBuffer cipher_rx{1u << 16};
  ByteBuffer cipher_tx{1u << 16};
  std::string tx;  // plaintext pending write

  enum class Phase { kConnecting, kTlsHandshake, kIdle, kSending, kReceiving };
  Phase phase = Phase::kConnecting;

  std::unique_ptr<PendingRequest> current;
  std::string head_buffer;
  bool head_done = false;
  int status = 0;
  std::vector<HttpHeader> headers;
  long long content_length = -1;
  bool chunked = false;
  bool keep_alive = true;
  std::string body;
  double deadline = 0.0;
  // Bytes that arrived after the current response ended. With keep-alive and
  // no pipelining this should always be empty, but a server that sends
  // anything extra must not have it silently prepended to the NEXT response --
  // that is how two responses get spliced together.
  std::string carry_over;
  std::uint64_t requests_served = 0;
  // Retired after this many, so a long-lived connection cannot accumulate
  // whatever state a venue's load balancer decides to attach to it.
  static constexpr std::uint64_t kMaxRequestsPerConnection = 1000;

  // Clears everything that belongs to ONE response, leaving the socket and TLS
  // session intact. Getting this wrong is how a stale Content-Length or a
  // half-parsed head leaks into the next request, so it clears every field
  // rather than the ones that look like they matter.
  void reset_for_next_request() {
    current.reset();
    head_buffer.clear();
    head_done = false;
    status = 0;
    headers.clear();
    content_length = -1;
    chunked = false;
    keep_alive = true;
    body.clear();
    phase = Phase::kIdle;
  }

  bool reusable() const {
    return keep_alive && requests_served < kMaxRequestsPerConnection;
  }
};

}  // namespace

struct HttpClient::Impl {
  const TlsContext* tls = nullptr;
  std::size_t max_per_host = 4;
  std::map<std::string, std::deque<std::unique_ptr<PendingRequest>>> queues;
  std::list<std::unique_ptr<Connection>> connections;

  static std::string key(const HttpRequest& r) {
    return r.host + ":" + std::to_string(r.port);
  }

  std::size_t connections_for(const std::string& k) const {
    std::size_t n = 0;
    for (const auto& c : connections) {
      if (c->host + ":" + std::to_string(c->port) == k) {
        ++n;
      }
    }
    return n;
  }

  // An established connection to `k` with nothing in flight, if there is one.
  // Reusing it skips a TCP handshake AND a TLS handshake -- two extra round
  // trips, which on a colocated link is most of the latency of an order.
  Connection* idle_connection(const std::string& k) {
    for (auto& c : connections) {
      if (c->phase == Connection::Phase::kIdle && c->current == nullptr &&
          c->host + ":" + std::to_string(c->port) == k && c->reusable()) {
        return c.get();
      }
    }
    return nullptr;
  }
};

HttpClient::HttpClient(const TlsContext* tls, std::size_t max_connections_per_host)
    : impl_(std::make_unique<Impl>()) {
  impl_->tls = tls;
  impl_->max_per_host = std::max<std::size_t>(1, max_connections_per_host);
}

HttpClient::~HttpClient() = default;

std::uint64_t HttpClient::submit(HttpRequest request, Callback callback) {
  auto pending = std::make_unique<PendingRequest>();
  pending->id = next_id_++;
  pending->submitted_at = now_seconds();
  pending->request = std::move(request);
  pending->callback = std::move(callback);

  const std::uint64_t id = pending->id;
  impl_->queues[Impl::key(pending->request)].push_back(std::move(pending));
  return id;
}

std::size_t HttpClient::pending() const noexcept {
  std::size_t n = 0;
  for (const auto& [_, q] : impl_->queues) {
    n += q.size();
  }
  for (const auto& c : impl_->connections) {
    if (c->current) {
      ++n;
    }
  }
  return n;
}

namespace {

void finish(HttpClient::Callback& callback, HttpResponse response) {
  if (callback) {
    callback(response);
  }
}

std::string build_request_bytes(const HttpRequest& r) {
  std::string out;
  out.reserve(256 + r.body.size());
  out += r.method;
  out += ' ';
  out += r.path;
  out += " HTTP/1.1\r\nHost: ";
  out += r.host;
  if (!((r.use_tls && r.port == 443) || (!r.use_tls && r.port == 80))) {
    out += ':';
    out += std::to_string(r.port);
  }
  out += "\r\n";
  for (const auto& h : r.headers) {
    out += h.name;
    out += ": ";
    out += h.value;
    out += "\r\n";
  }
  // Explicit: a venue that defaults to close would cost a TLS handshake per
  // request, which is most of the latency of an order.
  out += "Connection: keep-alive\r\n";
  if (!r.body.empty()) {
    out += "Content-Length: ";
    out += std::to_string(r.body.size());
    out += "\r\n";
  }
  out += "\r\n";
  out += r.body;
  return out;
}

}  // namespace

std::size_t HttpClient::poll() {
  std::size_t completed = 0;

  // Start connections for queued work.
  for (auto& [key, queue] : impl_->queues) {
    // Idle connections first: a reused one skips both handshakes.
    while (!queue.empty()) {
      Connection* idle = impl_->idle_connection(key);
      if (idle == nullptr) {
        break;
      }
      auto pending = std::move(queue.front());
      queue.pop_front();
      idle->deadline = now_seconds() + pending->request.timeout_seconds;
      idle->tx = build_request_bytes(pending->request);
      idle->body = std::move(idle->carry_over);
      idle->carry_over.clear();
      idle->current = std::move(pending);
      idle->phase = Connection::Phase::kSending;
      ++requests_sent_;
    }

    while (!queue.empty() &&
           impl_->connections_for(key) < impl_->max_per_host) {
      auto pending = std::move(queue.front());
      queue.pop_front();

      auto conn = std::make_unique<Connection>();
      conn->host = pending->request.host;
      conn->port = pending->request.port;
      conn->use_tls = pending->request.use_tls;
      conn->deadline = now_seconds() + pending->request.timeout_seconds;

      const IoResult connected =
          conn->socket.connect_to_host(conn->host, conn->port);
      if (connected.status == IoStatus::kError) {
        HttpResponse response;
        response.error = "connect failed: " + std::string(std::strerror(connected.error));
        response.elapsed_seconds = now_seconds() - pending->submitted_at;
        finish(pending->callback, response);
        ++requests_failed_;
        ++completed;
        continue;
      }
      conn->current = std::move(pending);
      conn->phase = Connection::Phase::kConnecting;
      impl_->connections.push_back(std::move(conn));
      ++requests_sent_;
    }
  }

  // Drive each connection.
  for (auto it = impl_->connections.begin(); it != impl_->connections.end();) {
    Connection& c = **it;
    bool drop = false;

    auto fail = [&](std::string reason) {
      if (c.current) {
        HttpResponse response;
        response.error = std::move(reason);
        response.elapsed_seconds = now_seconds() - c.current->submitted_at;
        finish(c.current->callback, response);
        ++requests_failed_;
        ++completed;
        c.current.reset();
      }
      drop = true;
    };

    if (c.current && now_seconds() > c.deadline) {
      fail("request timed out");
    }

    if (!drop && c.phase == Connection::Phase::kConnecting) {
      const IoResult status = c.socket.connect_status();
      if (status.status == IoStatus::kError) {
        fail("connect failed: " + std::string(std::strerror(status.error)));
      } else if (status.status == IoStatus::kOk) {
        if (c.use_tls) {
          c.tls.start_client(*impl_->tls, c.host);
          c.phase = Connection::Phase::kTlsHandshake;
        } else {
          c.tx = build_request_bytes(c.current->request);
          c.phase = Connection::Phase::kSending;
        }
      }
    }

    // --- byte movement ---------------------------------------------------
    auto pump = [&]() -> bool {
      if (!c.use_tls) {
        while (!c.tx.empty()) {
          const IoResult w = c.socket.write(c.tx.data(), c.tx.size());
          if (w.status == IoStatus::kWouldBlock) {
            break;
          }
          if (w.status != IoStatus::kOk) {
            return false;
          }
          c.tx.erase(0, w.bytes);
        }
        for (;;) {
          if (!c.rx.ensure_writable(4096)) {
            return false;
          }
          const IoResult r = c.socket.read(c.rx.writable(), c.rx.writable_size());
          if (r.status == IoStatus::kWouldBlock) {
            break;
          }
          if (r.status == IoStatus::kClosed) {
            // A close with a complete body is a legitimate end of response.
            return c.head_done;
          }
          if (r.status != IoStatus::kOk) {
            return false;
          }
          c.rx.commit(r.bytes);
        }
        return true;
      }

      // TLS: socket -> cipher_rx -> tls -> rx, and tx -> tls -> cipher_tx ->
      // socket. Same pump as WsConnection, kept separate because the lifetime
      // and error handling differ.
      while (!c.tx.empty()) {
        const IoResult w = c.tls.write_plaintext(c.tx.data(), c.tx.size());
        if (w.status != IoStatus::kOk) {
          break;
        }
        c.tx.erase(0, w.bytes);
      }
      for (;;) {
        if (!c.cipher_tx.ensure_writable(4096)) {
          break;
        }
        const IoResult t =
            c.tls.take_encrypted(c.cipher_tx.writable(), c.cipher_tx.writable_size());
        if (t.status != IoStatus::kOk) {
          break;
        }
        c.cipher_tx.commit(t.bytes);
      }
      while (!c.cipher_tx.empty()) {
        const IoResult w =
            c.socket.write(c.cipher_tx.readable(), c.cipher_tx.readable_size());
        if (w.status == IoStatus::kWouldBlock) {
          break;
        }
        if (w.status != IoStatus::kOk) {
          return false;
        }
        c.cipher_tx.consume(w.bytes);
      }

      bool peer_closed = false;
      for (;;) {
        if (!c.cipher_rx.ensure_writable(4096)) {
          break;
        }
        const IoResult r =
            c.socket.read(c.cipher_rx.writable(), c.cipher_rx.writable_size());
        if (r.status == IoStatus::kWouldBlock) {
          break;
        }
        if (r.status == IoStatus::kClosed) {
          peer_closed = true;
          break;
        }
        if (r.status != IoStatus::kOk) {
          return false;
        }
        c.cipher_rx.commit(r.bytes);
      }
      while (!c.cipher_rx.empty()) {
        const IoResult f =
            c.tls.feed_encrypted(c.cipher_rx.readable(), c.cipher_rx.readable_size());
        if (f.status != IoStatus::kOk) {
          break;
        }
        c.cipher_rx.consume(f.bytes);
      }
      if (c.tls.established()) {
        for (;;) {
          if (!c.rx.ensure_writable(4096)) {
            return false;
          }
          const IoResult p = c.tls.read_plaintext(c.rx.writable(), c.rx.writable_size());
          if (p.status == IoStatus::kWouldBlock) {
            break;
          }
          if (p.status == IoStatus::kClosed) {
            peer_closed = true;
            break;
          }
          if (p.status != IoStatus::kOk) {
            return false;
          }
          c.rx.commit(p.bytes);
        }
      }
      if (peer_closed && c.rx.empty() && !c.head_done) {
        return false;
      }
      return true;
    };

    if (!drop && !pump()) {
      fail("connection lost");
    }

    if (!drop && c.phase == Connection::Phase::kTlsHandshake) {
      const IoResult hs = c.tls.handshake();
      if (hs.status == IoStatus::kError) {
        fail("tls handshake failed: " + c.tls.last_error());
      } else if (c.tls.established()) {
        c.tx = build_request_bytes(c.current->request);
        c.phase = Connection::Phase::kSending;
        pump();
      }
    }

    if (!drop && c.phase == Connection::Phase::kSending && c.tx.empty()) {
      c.phase = Connection::Phase::kReceiving;
    }

    // --- response assembly ------------------------------------------------
    if (!drop && c.phase == Connection::Phase::kReceiving && c.rx.readable_size() > 0) {
      c.head_buffer.append(reinterpret_cast<const char*>(c.rx.readable()),
                           c.rx.readable_size());
      c.rx.consume(c.rx.readable_size());

      if (!c.head_done) {
        const std::size_t consumed =
            parse_http_head(c.head_buffer, c.status, c.headers, c.content_length,
                            c.chunked, c.keep_alive);
        if (consumed > 0) {
          c.head_done = true;
          c.body = c.head_buffer.substr(consumed);
          c.head_buffer.clear();
          if (c.status == 0) {
            fail("malformed HTTP response");
          }
        }
      } else {
        c.body += c.head_buffer;
        c.head_buffer.clear();
      }
    }

    if (!drop && c.head_done) {
      bool complete = false;
      std::string decoded;

      if (c.chunked) {
        std::size_t consumed = 0;
        if (!decode_chunked(c.body, decoded, complete, consumed)) {
          fail("malformed chunked encoding");
        }
      } else if (c.content_length >= 0) {
        complete = c.body.size() >= static_cast<std::size_t>(c.content_length);
        if (complete) {
          decoded = c.body.substr(0, static_cast<std::size_t>(c.content_length));
        }
      } else {
        // No length and not chunked: the body ends when the connection does.
        // Rare against these venues, but a 204 with no body lands here.
        complete = c.status == 204 || c.status == 304;
        decoded = c.body;
      }

      if (!drop && complete) {
        HttpResponse response;
        response.status = c.status;
        response.headers = c.headers;
        response.body = std::move(decoded);
        response.elapsed_seconds = now_seconds() - c.current->submitted_at;

        // Anything past this response belongs to the next one. Carry it rather
        // than dropping it, and rather than leaving it where it would be
        // spliced onto the next response's head.
        if (!c.chunked && c.content_length >= 0 &&
            c.body.size() > static_cast<std::size_t>(c.content_length)) {
          c.carry_over = c.body.substr(static_cast<std::size_t>(c.content_length));
        }

        const bool reuse = c.keep_alive;
        ++c.requests_served;

        // Reset BEFORE the callback: it may submit another request, and that
        // request must not land on a connection still holding this response's
        // state.
        auto callback = std::move(c.current->callback);
        c.reset_for_next_request();
        if (!reuse) {
          drop = true;
        }

        finish(callback, response);
        ++completed;
      }
    }

    if (drop) {
      it = impl_->connections.erase(it);
    } else {
      ++it;
    }
  }

  return completed;
}

}  // namespace axon::net
