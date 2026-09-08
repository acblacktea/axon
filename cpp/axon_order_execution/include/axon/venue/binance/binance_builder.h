// Binance USDT-M perpetual: outbound request builder.
//
// Binance is structurally unlike the other three: its WebSocket user data
// stream is READ ONLY, so order entry cannot ride it. Orders go out on a
// SECOND connection, to the ws-fapi endpoint, built below.
//
// ORDER ENTRY OVER THE WEBSOCKET is implemented below (`ws_place_order` and
// friends), on the separate ws-fapi endpoint. It avoids the HTTP round trip
// per order -- 1-2 RTTs, which on a colocated link is most of the end-to-end
// budget.
//
// Binance is the awkward one of the three:
//
//   * IT NEEDS ITS OWN CONNECTION. ws-fapi is a different host from the user
//     data stream, so a second socket is required.
//   * THERE IS NO SESSION LOGIN. Every request carries `apiKey` and its own
//     `signature`, exactly like REST. Nothing is authenticated once.
//   * THE SIGNATURE PAYLOAD IS THE PARAMETERS SORTED BY NAME, joined as
//     key=value&... -- unlike REST, where the order is whatever the query
//     string happens to be. Get the order wrong and the venue answers -1022
//     with nothing useful in it.
//
// That last point is why the parameters below go through ONE emitter that
// drives both the signing string and the JSON body. Writing them twice is how
// you end up signing one thing and sending another, which is the single
// hardest failure here to diagnose.
//
// UNVERIFIED, AND NOW LOAD-BEARING. The method names, envelope and signing
// recipe came from Binance's documentation, not from the running Python (which
// used REST). The REST order path has been removed, so there is no proven
// alternative beside this one: validate it against testnet before trading it.
//
// SIGNING: HMAC-SHA256, hex. The signature covers the bytes as sent, so the
// parameter order below is part of the contract -- re-ordering them changes
// the digest.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "axon/core/decimal.h"
#include "axon/models/order.h"
#include "axon/venue/json_writer.h"

namespace axon::venue::binance {

inline constexpr std::string_view kProductionRestHost = "fapi.binance.com";
inline constexpr std::string_view kTestnetRestHost = "testnet.binancefuture.com";

// The WebSocket order-entry endpoint. A DIFFERENT HOST from the user data
// stream, and it carries no subscriptions -- only request/response.
inline constexpr std::string_view kWsApiProductionHost = "ws-fapi.binance.com";
inline constexpr std::string_view kWsApiTestnetHost = "testnet.binancefuture.com";
inline constexpr std::string_view kWsApiPath = "/ws-fapi/v1";


inline constexpr std::size_t kMaxRequestBytes = 1024;

class BinanceBuilder {
 public:
  // --- WebSocket order entry (the ws-fapi endpoint) ----------------------
  //
  // {"id":"<id>","method":"order.place","params":{...,"signature":"<hex>"}}
  //
  // Every value is emitted as a JSON STRING so the bytes signed and the bytes
  // sent are literally the same text. Binance accepts numeric parameters as
  // strings, and the alternative -- bare numbers in JSON, plain text in the
  // signature -- means two representations of one value that must agree.
  //
  // Returns 0 if the request does not fit OR if `label` contains a character
  // that JSON would have to escape. A label like `a"b` cannot be both signed
  // raw and sent escaped, so it is REJECTED rather than silently signed as one
  // thing and sent as another. Callers should keep client order ids
  // alphanumeric, which every real one is.
  std::size_t ws_place_order(char* out, std::size_t cap,
                             const models::OrderRequest& req,
                             std::string_view api_key,
                             std::string_view api_secret,
                             std::int64_t timestamp_ms, std::int64_t& out_id,
                             int recv_window_ms = 5000);

  std::size_t ws_cancel_order(char* out, std::size_t cap, std::string_view symbol,
                              std::string_view order_id,
                              std::string_view api_key,
                              std::string_view api_secret,
                              std::int64_t timestamp_ms, std::int64_t& out_id,
                              int recv_window_ms = 5000);

  std::size_t ws_modify_order(char* out, std::size_t cap, std::string_view symbol,
                              std::string_view order_id, models::OrderSide side,
                              core::Qty amount, core::Price price,
                              std::string_view api_key,
                              std::string_view api_secret,
                              std::int64_t timestamp_ms, std::int64_t& out_id,
                              int recv_window_ms = 5000);

  std::int64_t peek_next_id() const noexcept { return next_id_; }

 private:
  // ws-fapi correlates a reply by `id`. Unlike REST, where nothing needs
  // correlating because the response comes back on the same HTTP exchange.
  std::int64_t next_id() noexcept { return next_id_++; }
  std::int64_t next_id_ = 1;
};

}  // namespace axon::venue::binance
