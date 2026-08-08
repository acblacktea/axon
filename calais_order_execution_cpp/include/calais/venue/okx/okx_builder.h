// OKX v5: outbound message builder.
//
// Signing, ported from _authenticate in oms/okx/okx_ws.py and the REST client:
//
//   WEBSOCKET login: base64(HMAC_SHA256(secret, timestamp + "GET" +
//                                       "/users/self/verify"))
//   where timestamp is a SECOND-resolution epoch as a string.
//
//   REST:            base64(HMAC_SHA256(secret, timestamp + method +
//                                       requestPath + body))
//   where timestamp is an ISO-8601 string with milliseconds and a Z suffix.
//
// Two things that catch people out, both preserved here:
//   * OKX signatures are BASE64, not hex. Bybit and Binance use hex.
//   * OKX also requires a PASSPHRASE header/field alongside the key. It is not
//     derivable from anything; it is set when the API key is created.
//
// LATENCY NOTE: OKX supports order entry over the WebSocket (op:"order"),
// which would remove the HTTP round trip. Not implemented, for the same reason
// as the other two -- the Python uses REST, so there is no proven mapping to
// port.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "calais/core/decimal.h"
#include "calais/models/order.h"
#include "calais/venue/json_writer.h"

namespace calais::venue::okx {

inline constexpr std::string_view kProductionRestHost = "www.okx.com";

inline constexpr std::string_view kPlaceOrderPath = "/api/v5/trade/order";
inline constexpr std::string_view kCancelOrderPath = "/api/v5/trade/cancel-order";
inline constexpr std::string_view kAmendOrderPath = "/api/v5/trade/amend-order";

inline constexpr std::string_view kOrdersChannel = "orders";
inline constexpr std::string_view kFillsChannel = "fills";
inline constexpr std::string_view kAccountChannel = "account";

// Margin mode used by the Python EMS. Cross is what the strategies assume.
inline constexpr std::string_view kTradeMode = "cross";

// OKX instrument type for perpetual swaps, used when subscribing.
inline constexpr std::string_view kInstType = "SWAP";

inline constexpr std::size_t kMaxRequestBytes = 1024;

class OkxBuilder {
 public:
  // --- WebSocket ---------------------------------------------------------

  // {"op":"login","args":[{apiKey, passphrase, timestamp, sign}]}
  //
  // `timestamp_seconds` is a SECOND-resolution epoch. OKX rejects a
  // millisecond value here even though its REST signing uses milliseconds --
  // a mismatch that produces a login failure with no useful error.
  static std::size_t ws_login(char* out, std::size_t cap,
                              std::string_view api_key,
                              std::string_view passphrase,
                              std::string_view api_secret,
                              std::int64_t timestamp_seconds);

  // {"op":"subscribe","args":[{"channel":...,"instType":"SWAP"}, ...]}
  static std::size_t ws_subscribe(char* out, std::size_t cap,
                                  const std::string_view* channels,
                                  std::size_t count,
                                  std::string_view inst_type = kInstType) noexcept;

  static std::size_t ws_unsubscribe(char* out, std::size_t cap,
                                    const std::string_view* channels,
                                    std::size_t count,
                                    std::string_view inst_type = kInstType) noexcept;

  // OKX's heartbeat is the literal text "ping", not JSON.
  static constexpr std::string_view ws_ping() noexcept { return "ping"; }

  static std::string ws_signature(std::string_view api_secret,
                                  std::int64_t timestamp_seconds);

  // --- REST bodies -------------------------------------------------------

  static std::size_t place_order_body(char* out, std::size_t cap,
                                      const models::OrderRequest& req) noexcept;

  static std::size_t cancel_order_body(char* out, std::size_t cap,
                                       std::string_view inst_id,
                                       std::string_view order_id) noexcept;

  static std::size_t amend_order_body(char* out, std::size_t cap,
                                      std::string_view inst_id,
                                      std::string_view order_id,
                                      std::optional<core::Qty> amount,
                                      std::optional<core::Price> price) noexcept;

  // OK-ACCESS-SIGN. `timestamp_iso` must be the same string sent in
  // OK-ACCESS-TIMESTAMP, e.g. "2026-08-08T12:34:56.789Z".
  static std::string rest_signature(std::string_view api_secret,
                                    std::string_view timestamp_iso,
                                    std::string_view http_method,
                                    std::string_view request_path,
                                    std::string_view body);
};

}  // namespace calais::venue::okx
