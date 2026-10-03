// What a venue does differently on the order path.
//
// The C++ counterpart of ems/base.py plus the four ems/<venue>/ modules: this
// header is the interface, and each venue supplies one instance of it from its
// own translation unit. ems_service.cpp holds no venue knowledge at all.
//
// The table is NOT here to make the venues look alike. Their differences are
// load-bearing and documented throughout this codebase -- Deribit cancels by
// id alone, OKX accepts a request and rejects the order separately, Binance
// signs every message. The point is that each difference is stated ONCE, in a
// named field, instead of being rediscovered as `exchange == "..."` at every
// call site. It used to be spelled out in sixteen places, two of which were
// hand-kept copies of each other.

#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string_view>

#include "axon/config.h"
#include "axon/core/decimal.h"
#include "axon/ems/ems_service.h"
#include "axon/models/order.h"
#include "axon/venue/binance/binance_builder.h"
#include "axon/venue/bybit/bybit_builder.h"
#include "axon/venue/deribit/deribit_builder.h"
#include "axon/venue/okx/okx_builder.h"

namespace axon::ems {

// One stack buffer serves every venue, sized for the largest request any of
// them can produce. Checked here so a venue whose limit grows past the others
// fails to compile rather than truncating an order.
inline constexpr std::size_t kMaxRequestBytes = venue::bybit::kMaxRequestBytes;
static_assert(kMaxRequestBytes >= venue::deribit::kMaxRequestBytes);
static_assert(kMaxRequestBytes >= venue::okx::kMaxRequestBytes);
static_assert(kMaxRequestBytes >= venue::binance::kMaxRequestBytes);

// Order-entry request ids start far above anything a session handshake
// consumes.
//
// This is not cosmetic. A session claims its own auth and subscribe replies by
// id before passing anything on, so an order that happened to draw the same id
// as the auth request would be read as a second authentication acknowledgement
// -- which on Deribit re-runs the subscription. Both counters used to start at
// 1, so the FIRST order on a connection could do exactly that.
inline constexpr std::int64_t kEmsFirstRequestId = 1'000'000;

// What a builder needs beyond the request itself. Passed to every venue so the
// signatures are uniform; a venue ignores what it does not use.
struct BuildContext {
  // Credentials. Null unless configured -- only Binance needs them per
  // request, because it signs each one rather than logging the session in.
  const ExchangeConfig* config;
  std::int64_t now_ms;
};

struct VenueOps {
  const char* name;

  // Binance and Bybit each need a second, order-entry-only connection. Deribit
  // and OKX send orders on the private stream they already have.
  bool own_trade_connection;

  // Deribit cancels and amends by order id alone. The others need the symbol,
  // which only the order store knows -- hence EmsService::set_order_lookup(),
  // and hence a cancel that fails locally when the order is not cached.
  bool needs_symbol;

  // Each returns the number of bytes written, or 0 if the request could not be
  // built. 0 is a rejection, not a retry: there is no second encoding.
  std::size_t (*build_place)(char* buf, std::size_t cap,
                             const models::OrderRequest& request,
                             const BuildContext& ctx, std::int64_t& id);
  std::size_t (*build_cancel)(char* buf, std::size_t cap,
                              std::string_view symbol, std::string_view order_id,
                              const BuildContext& ctx, std::int64_t& id);
  std::size_t (*build_modify)(char* buf, std::size_t cap,
                              const models::Order& existing,
                              std::optional<core::Qty> amount,
                              std::optional<core::Price> price,
                              const BuildContext& ctx, std::int64_t& id);

  // Interprets a reply the venue itself called successful. The venues disagree
  // about what that means, which is the whole reason this is per-venue: see
  // okx_ems.cpp.
  OrderResult (*interpret_reply)(std::string_view payload,
                                 const models::OrderRequest& request);
};

// The venue this exchange name maps to, or nullptr if this build has no
// order-entry implementation for it. The ONLY place a venue is chosen by name.
const VenueOps* ops_for(std::string_view exchange);

}  // namespace axon::ems
