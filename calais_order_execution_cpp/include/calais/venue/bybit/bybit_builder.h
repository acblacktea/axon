// Bybit v5: outbound message builder.
//
// Two transports, and they sign differently:
//
//   WEBSOCKET (auth + subscribe). Signature is
//       HMAC_SHA256(secret, "GET/realtime" + expires)  -> lowercase hex
//   where `expires` is a millisecond epoch a little in the future. Ported from
//   _authenticate in oms/bybit/bybit_ws.py, which uses now + 10s.
//
//   REST (order entry). Signature is
//       HMAC_SHA256(secret, timestamp + api_key + recv_window + body) -> hex
//   sent in the X-BAPI-SIGN header. The body must be signed EXACTLY as sent,
//   so the builder returns the body and the caller signs those same bytes.
//
// LATENCY NOTE: Bybit has a WebSocket trade endpoint (/v5/trade) that would
// remove the HTTP round trip from order entry. Not implemented, for the same
// reason as Binance's -- the Python does not use it, so there is no proven
// mapping to port. It is the biggest remaining win on this venue.
//
// `category` is on every request. Sending the wrong one, or omitting it, is
// how a perp order ends up routed at a spot symbol.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "calais/core/decimal.h"
#include "calais/models/order.h"
#include "calais/venue/bybit/bybit_parser.h"
#include "calais/venue/json_writer.h"

namespace calais::venue::bybit {

inline constexpr std::string_view kProductionRestHost = "api.bybit.com";
inline constexpr std::string_view kTestnetRestHost = "api-testnet.bybit.com";

inline constexpr std::string_view kCreateOrderPath = "/v5/order/create";
inline constexpr std::string_view kCancelOrderPath = "/v5/order/cancel";
inline constexpr std::string_view kAmendOrderPath = "/v5/order/amend";

inline constexpr std::string_view kOrderTopic = "order";
inline constexpr std::string_view kExecutionTopic = "execution";
inline constexpr std::string_view kWalletTopic = "wallet";

inline constexpr std::size_t kMaxRequestBytes = 1024;

class BybitBuilder {
 public:
  explicit BybitBuilder(std::int64_t first_req_id = 1) noexcept
      : next_id_(first_req_id) {}

  // --- WebSocket ---------------------------------------------------------

  // {"req_id":"N","op":"auth","args":[key, expires, signature]}
  //
  // `expires_ms` should be a few seconds in the future; Bybit rejects a
  // signature whose expiry has passed, and clock skew is the usual cause of a
  // login that works locally and fails in production.
  std::size_t ws_auth(char* out, std::size_t cap, std::string_view api_key,
                      std::string_view api_secret, std::int64_t expires_ms,
                      std::int64_t& out_id);

  std::size_t ws_subscribe(char* out, std::size_t cap,
                           const std::string_view* topics, std::size_t count,
                           std::int64_t& out_id) noexcept;

  std::size_t ws_unsubscribe(char* out, std::size_t cap,
                             const std::string_view* topics, std::size_t count,
                             std::int64_t& out_id) noexcept;

  static std::size_t ws_ping(char* out, std::size_t cap) noexcept;

  // The exact string Bybit signs for WebSocket auth. Exposed so a test can
  // check the signature without reimplementing the recipe.
  static std::string ws_signature(std::string_view api_secret,
                                  std::int64_t expires_ms);

  // --- REST bodies -------------------------------------------------------

  static std::size_t place_order_body(char* out, std::size_t cap,
                                      const models::OrderRequest& req) noexcept;

  static std::size_t cancel_order_body(char* out, std::size_t cap,
                                       std::string_view symbol,
                                       std::string_view order_id) noexcept;

  static std::size_t amend_order_body(char* out, std::size_t cap,
                                      std::string_view symbol,
                                      std::string_view order_id,
                                      std::optional<core::Qty> amount,
                                      std::optional<core::Price> price) noexcept;

  // X-BAPI-SIGN for a REST request: HMAC over
  // timestamp + api_key + recv_window + body.
  static std::string rest_signature(std::string_view api_secret,
                                    std::int64_t timestamp_ms,
                                    std::string_view api_key, int recv_window_ms,
                                    std::string_view body);

 private:
  std::int64_t next_id() noexcept { return next_id_++; }
  std::int64_t next_id_ = 1;
};

}  // namespace calais::venue::bybit
