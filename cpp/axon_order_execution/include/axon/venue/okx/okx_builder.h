// OKX v5: outbound message builder.
//
// Signing, ported from _authenticate in oms/okx/okx_ws.py:
//
//   WEBSOCKET login: base64(HMAC_SHA256(secret, timestamp + "GET" +
//                                       "/users/self/verify"))
//   where timestamp is a SECOND-resolution epoch as a string.
//
// Individual order requests are not signed; the connection is authenticated
// once at login. (The reconciler's REST reads sign themselves, in venue_rest.)
//
// Two things that catch people out, both preserved here:
//   * OKX signatures are BASE64, not hex. Bybit and Binance use hex.
//   * OKX also requires a PASSPHRASE header/field alongside the key. It is not
//     derivable from anything; it is set when the API key is created.
//
// ORDER ENTRY OVER THE WEBSOCKET is implemented below (`ws_place_order` and
// friends), and it removes the HTTP round trip -- 1-2 RTTs, most of a
// colocated budget.
//
// OKX is the easy one of the three: `op:"order"` rides the SAME private
// connection that already carries orders, fills and account, so there is no
// second socket and no second login. Its `args` are byte-identical to the REST
// body, which is why the builders below delegate to the REST body builders
// rather than restating the field mapping -- one mapping, one place to fix.
//
// UNVERIFIED, AND NOW LOAD-BEARING. The `op` names and the envelope shape came
// from OKX's documentation, not from the running Python (which used REST).
// Everything else in this file was transcribed from code that has been
// reconciling against the live venue for a long time.
//
// The REST order path has been removed, so there is no proven alternative
// beside this one any more: if the mapping is wrong, orders do not go out.
// Validate it against OKX's demo trading before trading it.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "axon/core/decimal.h"
#include "axon/models/order.h"
#include "axon/venue/json_writer.h"

namespace axon::venue::okx {

inline constexpr std::string_view kProductionRestHost = "www.okx.com";


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

  // --- WebSocket order entry ---------------------------------------------
  //
  // {"id":"<id>","op":"order","args":[<the REST body, verbatim>]}
  //
  // `id` is a STRING on OKX and is capped at 32 characters; a counter cannot
  // reach that. It correlates the reply, which arrives on the same connection
  // as everything else.
  //
  // These are non-static because they consume an id. The REST body builders
  // they delegate to stay static.
  std::size_t ws_place_order(char* out, std::size_t cap,
                             const models::OrderRequest& req,
                             std::int64_t& out_id) noexcept;

  std::size_t ws_cancel_order(char* out, std::size_t cap,
                              std::string_view inst_id,
                              std::string_view order_id,
                              std::int64_t& out_id) noexcept;

  std::size_t ws_amend_order(char* out, std::size_t cap, std::string_view inst_id,
                             std::string_view order_id,
                             std::optional<core::Qty> amount,
                             std::optional<core::Price> price,
                             std::int64_t& out_id) noexcept;

  std::int64_t peek_next_id() const noexcept { return next_id_; }

  // OKX's heartbeat is the literal text "ping", not JSON.
  static constexpr std::string_view ws_ping() noexcept { return "ping"; }

  static std::string ws_signature(std::string_view api_secret,
                                  std::int64_t timestamp_seconds);

  // --- order arguments ---------------------------------------------------
  //
  // The JSON object describing one order. It is what `args` carries in the
  // WebSocket requests above, and it is kept as a separate builder so those
  // three do not each restate the field mapping.

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

 private:
  std::int64_t next_id() noexcept { return next_id_++; }
  std::int64_t next_id_ = 1;
};

}  // namespace axon::venue::okx
