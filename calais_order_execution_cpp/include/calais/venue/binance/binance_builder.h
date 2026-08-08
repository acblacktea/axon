// Binance USDT-M perpetual: outbound request builder.
//
// Binance is structurally unlike the other three: its WebSocket user data
// stream is READ ONLY. Orders go over REST as a signed query string, which is
// what ems/binance/binance.py does and what this builds.
//
// LATENCY NOTE, and it matters at a 10us target: Binance also offers a
// WebSocket order-entry API (`order.place` on a separate ws-fapi endpoint)
// that would avoid an HTTP round trip per order -- worth 1-2 RTTs, which on a
// colocated link is most of the end-to-end budget. It is NOT implemented here
// because the Python does not use it, so there is no proven field mapping to
// port and no way to verify one without a live account. This is the single
// biggest remaining latency win on Binance and should be the first thing done
// after the socket layer lands.
//
// SIGNING: HMAC-SHA256 over the exact query string, appended as
// `&signature=<hex>`. The signature covers the bytes as sent, so the parameter
// order below is part of the contract -- re-ordering them changes the digest.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "calais/core/decimal.h"
#include "calais/models/order.h"
#include "calais/venue/json_writer.h"

namespace calais::venue::binance {

inline constexpr std::string_view kProductionRestHost = "fapi.binance.com";
inline constexpr std::string_view kTestnetRestHost = "testnet.binancefuture.com";

inline constexpr std::string_view kOrderPath = "/fapi/v1/order";
inline constexpr std::string_view kListenKeyPath = "/fapi/v1/listenKey";

inline constexpr std::size_t kMaxRequestBytes = 1024;

class BinanceBuilder {
 public:
  // Builds the UNSIGNED query string for a new order, matching place_order in
  // the Python EMS:
  //   symbol, side, type, quantity, [price, timeInForce], [newClientOrderId]
  // Returns bytes written, or 0 if the buffer is too small.
  static std::size_t place_order_query(char* out, std::size_t cap,
                                       const models::OrderRequest& req,
                                       std::int64_t timestamp_ms,
                                       int recv_window_ms = 5000) noexcept;

  static std::size_t cancel_order_query(char* out, std::size_t cap,
                                        std::string_view symbol,
                                        std::string_view order_id,
                                        std::int64_t timestamp_ms,
                                        int recv_window_ms = 5000) noexcept;

  // PUT /fapi/v1/order. Binance requires side and quantity and price on an
  // amend, so the caller must supply the values the existing order had --
  // matching modify_order in the Python, which reads the order first.
  static std::size_t modify_order_query(char* out, std::size_t cap,
                                        std::string_view symbol,
                                        std::string_view order_id,
                                        models::OrderSide side,
                                        core::Qty amount, core::Price price,
                                        std::int64_t timestamp_ms,
                                        int recv_window_ms = 5000) noexcept;

  // Appends "&signature=<hex>" to a query already in `buf`.
  //
  // `len` is the current length and `cap` the buffer size. Returns the new
  // length, or 0 if it does not fit. Keeping this separate from the builders
  // is deliberate: the signature must cover exactly the bytes that go on the
  // wire, so nothing may be inserted between building and signing.
  static std::size_t sign_query(char* buf, std::size_t len, std::size_t cap,
                                std::string_view api_secret);
};

}  // namespace calais::venue::binance
