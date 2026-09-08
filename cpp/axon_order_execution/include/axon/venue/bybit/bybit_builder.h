// Bybit v5: outbound message builder.
//
// SIGNING, for both connections' logins:
//       HMAC_SHA256(secret, "GET/realtime" + expires)  -> lowercase hex
// where `expires` is a millisecond epoch a little in the future. Ported from
// _authenticate in oms/bybit/bybit_ws.py, which uses now + 10s.
//
// Individual order requests are NOT signed -- the connection is authenticated
// once at login. (The reconciler's REST reads sign themselves, in venue_rest.)
//
// ORDER ENTRY OVER THE WEBSOCKET is implemented below (`ws_place_order` and
// friends). It removes the HTTP round trip -- 1-2 RTTs, most of a colocated
// budget.
//
// IT NEEDS ITS OWN CONNECTION. Bybit's trade endpoint is /v5/trade, a
// different path from the /v5/private stream, so a second socket with its own
// `op:"auth"` login is required. That is what oms::make_trade_session builds.
//
// The envelope carries the same X-BAPI-* values a REST request puts in HTTP
// headers, but inside a "header" object and WITHOUT a signature -- the
// connection was authenticated once at login, so individual requests are not
// signed. This is the one structural difference from REST worth remembering.
//
// UNVERIFIED, AND NOW LOAD-BEARING. The envelope and `op` names came from
// Bybit's documentation, not from the running Python (which used REST). The
// REST order path has been removed, so there is no proven alternative beside
// this one: validate it against testnet before trading it.
//
// `category` is on every request. Sending the wrong one, or omitting it, is
// how a perp order ends up routed at a spot symbol.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "axon/core/decimal.h"
#include "axon/models/order.h"
#include "axon/venue/bybit/bybit_parser.h"
#include "axon/venue/json_writer.h"

namespace axon::venue::bybit {

inline constexpr std::string_view kProductionRestHost = "api.bybit.com";
inline constexpr std::string_view kTestnetRestHost = "api-testnet.bybit.com";


// The trade endpoint. A DIFFERENT path from the /v5/private stream, on the
// same host, and it needs its own authenticated connection.
inline constexpr std::string_view kTradePath = "/v5/trade";

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

  // --- WebSocket order entry (the /v5/trade endpoint) --------------------
  //
  // {"reqId":"<id>",
  //  "header":{"X-BAPI-TIMESTAMP":"<ms>","X-BAPI-RECV-WINDOW":"<ms>"},
  //  "op":"order.create","args":[<the REST body, verbatim>]}
  //
  // `args` is exactly what place_order_body produces, so these delegate to it.
  // No signature: the connection is authenticated at login, not per request.
  std::size_t ws_place_order(char* out, std::size_t cap,
                             const models::OrderRequest& req,
                             std::int64_t timestamp_ms, std::int64_t& out_id,
                             int recv_window_ms = 5000) noexcept;

  std::size_t ws_cancel_order(char* out, std::size_t cap, std::string_view symbol,
                              std::string_view order_id,
                              std::int64_t timestamp_ms, std::int64_t& out_id,
                              int recv_window_ms = 5000) noexcept;

  std::size_t ws_amend_order(char* out, std::size_t cap, std::string_view symbol,
                             std::string_view order_id,
                             std::optional<core::Qty> amount,
                             std::optional<core::Price> price,
                             std::int64_t timestamp_ms, std::int64_t& out_id,
                             int recv_window_ms = 5000) noexcept;

  std::int64_t peek_next_id() const noexcept { return next_id_; }

  // The exact string Bybit signs for WebSocket auth. Exposed so a test can
  // check the signature without reimplementing the recipe.
  static std::string ws_signature(std::string_view api_secret,
                                  std::int64_t expires_ms);

  // --- order arguments ---------------------------------------------------
  //
  // The JSON object describing one order. It is what `args` carries in the
  // WebSocket requests above, and it is kept as a separate builder so those
  // three do not each restate the field mapping.

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

 private:
  std::int64_t next_id() noexcept { return next_id_++; }
  std::int64_t next_id_ = 1;
};

}  // namespace axon::venue::bybit
