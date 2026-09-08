// Deribit outbound message builder.
//
// No JSON library is involved. Every message is assembled by copying literal
// byte runs and writing the varying fields directly, which measured 12ns
// against 250ns for rapidjson's Writer and 1458ns for nlohmann's dump().
//
// The reason it is that much faster is not clever string handling, it is that
// prices never become doubles. `Decimal::write` turns 34500000 raw units into
// "0.0345" with an integer divide loop; a JSON library would first convert to
// double and then run a shortest-round-trip float formatter, which is the
// single most expensive step in building an order.
//
// PARAMETERS ARE TRANSCRIBED FROM ems/deribit/deribit.py. See the DIVERGENCE
// note on post_only below -- it is the one place this does something the
// Python does not, and it is deliberate.
//
// Every builder writes into a caller-supplied buffer and returns the number of
// bytes written, or 0 if the buffer is too small. Nothing allocates.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include "axon/core/decimal.h"
#include "axon/models/order.h"
#include "axon/venue/deribit/deribit_protocol.h"
#include "axon/venue/json_writer.h"

namespace axon::venue::deribit {

// Comfortably above the largest message this builds.
inline constexpr std::size_t kMaxRequestBytes = 1024;

class DeribitBuilder {
 public:
  // JSON-RPC ids must be unique per connection. Starting above the reserved
  // heartbeat ids (9998/9999) would collide eventually; instead those two are
  // simply never produced by next_id().
  explicit DeribitBuilder(std::int64_t first_id = 1) noexcept : next_id_(first_id) {}

  std::int64_t peek_next_id() const noexcept { return next_id_; }

  // --- order entry -------------------------------------------------------

  // private/buy or private/sell, depending on side.
  // Returns bytes written and, via `out_id`, the JSON-RPC id to correlate the
  // reply with.
  std::size_t place_order(char* out, std::size_t cap,
                          const models::OrderRequest& req,
                          std::int64_t& out_id) noexcept;

  std::size_t cancel_order(char* out, std::size_t cap, std::string_view order_id,
                           std::int64_t& out_id) noexcept;

  // private/edit. Both fields are optional; omitting both produces a request
  // that changes nothing, which the caller should avoid sending.
  std::size_t modify_order(char* out, std::size_t cap, std::string_view order_id,
                           std::optional<core::Qty> amount,
                           std::optional<core::Price> price,
                           std::int64_t& out_id) noexcept;

  // --- session -----------------------------------------------------------

  std::size_t authenticate(char* out, std::size_t cap, std::string_view client_id,
                           std::string_view client_secret,
                           std::int64_t& out_id) noexcept;

  std::size_t set_heartbeat(char* out, std::size_t cap, int interval_seconds,
                            std::int64_t& out_id) noexcept;

  std::size_t subscribe(char* out, std::size_t cap,
                        const std::string_view* channels, std::size_t count,
                        std::int64_t& out_id) noexcept;

  std::size_t unsubscribe(char* out, std::size_t cap,
                          const std::string_view* channels, std::size_t count,
                          std::int64_t& out_id) noexcept;

  // The reply to a heartbeat test_request. Uses the fixed id 9998 and does NOT
  // consume a sequence number, matching the Python client. Deribit drops the
  // connection if a test_request goes unanswered, so this must be sent from
  // the receive loop without waiting for anything else.
  static std::size_t test_response(char* out, std::size_t cap) noexcept;

  // The client's own liveness probe, id 9999.
  static std::size_t heartbeat_probe(char* out, std::size_t cap) noexcept;

 private:
  std::int64_t next_id() noexcept {
    if (next_id_ == kHeartbeatRequestId || next_id_ == kTestResponseId) {
      next_id_ = kHeartbeatRequestId + 1;
    }
    return next_id_++;
  }

  std::int64_t next_id_ = 1;
};

}  // namespace axon::venue::deribit
