// Binary hot-path messages.
//
// This is the data plane: what actually crosses the shared-memory ring between
// a strategy and the engine when an order is placed, cancelled, or filled.
// Everything here is fixed-layout POD, so producing a message is a few stores
// and consuming one is a pointer cast. No parse, no allocation, no length
// negotiation.
//
// Compared with the JSON control plane on the same message (~500 bytes):
//   build   ~2-5us   ->  ~40ns
//   parse   ~3-10us  ->  ~0ns (cast)
//
// The cost is rigidity, and it is a real cost:
//
//   * Strings are fixed-width arrays. A value that does not fit is REJECTED,
//     not truncated -- a silently truncated instrument name would route an
//     order to the wrong contract. The widths below have generous headroom;
//     revisit them if a venue introduces longer symbols.
//
//   * Both sides must be compiled from the same headers. There is no version
//     negotiation beyond the header's `version` field, which exists so a
//     mismatch fails loudly at startup instead of misparsing.
//
//   * Adding a field is a breaking change. That is why the control plane still
//     exists: anything not on the critical path stays in JSON, where evolution
//     is free.
//
// Only place/cancel/modify/order-update/fill belong here. If you are tempted
// to add get_positions, put it in the JSON control plane instead.

#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string_view>
#include <type_traits>

#include "calais/core/clock.h"
#include "calais/core/decimal.h"
#include "calais/core/timestamp.h"
#include "calais/models/enums.h"
#include "calais/models/fill.h"
#include "calais/models/order.h"

namespace calais::transport {

// ---------------------------------------------------------------------------
// Fixed-width string
// ---------------------------------------------------------------------------
template <std::size_t N>
struct FixedString {
  static constexpr std::size_t kCapacity = N;

  char data[N];

  // Zero-fills the tail so the representation is canonical: two FixedStrings
  // holding the same text always compare equal byte-for-byte, and no
  // uninitialised stack bytes are published into shared memory.
  bool assign(std::string_view s) noexcept {
    if (s.size() > N) {
      return false;
    }
    std::memcpy(data, s.data(), s.size());
    std::memset(data + s.size(), 0, N - s.size());
    return true;
  }

  void clear() noexcept { std::memset(data, 0, N); }

  std::string_view view() const noexcept {
    const void* nul = std::memchr(data, '\0', N);
    const std::size_t len =
        nul == nullptr ? N
                       : static_cast<std::size_t>(static_cast<const char*>(nul) - data);
    return std::string_view(data, len);
  }

  bool empty() const noexcept { return data[0] == '\0'; }
};

// Widths. Sized against the longest real values plus headroom, and kept to
// multiples of 8 so the following int64 fields stay naturally aligned.
inline constexpr std::size_t kInstrumentCap = 40;  // "BTC-27JUN25-100000-C" = 20
inline constexpr std::size_t kOrderIdCap = 40;
inline constexpr std::size_t kInternalIdCap = 32;  // uuid4().hex is exactly 32
inline constexpr std::size_t kTradeIdCap = 32;
inline constexpr std::size_t kStrategyIdCap = 24;
inline constexpr std::size_t kLabelCap = 32;
inline constexpr std::size_t kExchangeCap = 16;
inline constexpr std::size_t kCurrencyCap = 8;

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------
enum class HotMsgType : std::uint16_t {
  kNone = 0,
  kPlaceOrder = 1,
  kCancelOrder = 2,
  kModifyOrder = 3,
  kOrderUpdate = 4,
  kFill = 5,
  kHeartbeat = 6,
};

inline constexpr std::uint16_t kHotMsgVersion = 1;

// Stamp slots carried inside every message. Each hop fills the next one, so a
// message arrives at the reporting thread carrying its own latency breakdown
// and no correlation by id is ever needed.
//
// Costs 32 bytes per message and one register read per hop. That is the
// cheapest observability in the system and it should never be compiled out --
// a latency problem that only shows up under production load cannot be
// reproduced anywhere else.
inline constexpr std::size_t kHotStampSlots = 4;

enum HotStamp : std::size_t {
  kStampOrigin = 0,   // producer created the message
  kStampEnqueued = 1, // producer committed it to the ring
  kStampDequeued = 2, // consumer picked it up
  kStampHandled = 3,  // consumer finished acting on it
};

struct HotHeader {
  std::uint16_t type;     // HotMsgType
  std::uint16_t version;  // kHotMsgVersion
  std::uint32_t payload_bytes;
  std::uint64_t seq;
  core::Ticks stamps[kHotStampSlots];

  void init(HotMsgType t, std::uint32_t bytes, std::uint64_t sequence) noexcept {
    type = static_cast<std::uint16_t>(t);
    version = kHotMsgVersion;
    payload_bytes = bytes;
    seq = sequence;
    stamps[0] = core::now_ticks();
    stamps[1] = 0;
    stamps[2] = 0;
    stamps[3] = 0;
  }

  void stamp(HotStamp slot) noexcept {
    stamps[static_cast<std::size_t>(slot)] = core::now_ticks();
  }

  HotMsgType kind() const noexcept { return static_cast<HotMsgType>(type); }
};

static_assert(sizeof(HotHeader) == 48, "HotHeader layout is part of the ABI");

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

struct PlaceOrderMsg {
  HotHeader hdr;

  FixedString<kExchangeCap> exchange;
  FixedString<kInstrumentCap> instrument;
  FixedString<kInternalIdCap> internal_order_id;
  FixedString<kStrategyIdCap> strategy_id;
  FixedString<kLabelCap> label;

  std::int64_t price_raw;   // core::Price raw units; valid iff has_price
  std::int64_t amount_raw;  // core::Qty raw units

  std::uint8_t side;         // models::OrderSide
  std::uint8_t order_type;   // models::OrderType
  std::uint8_t post_only;
  std::uint8_t reject_post_only;
  std::uint8_t has_price;
  std::uint8_t reserved[3];
};

struct CancelOrderMsg {
  HotHeader hdr;

  FixedString<kExchangeCap> exchange;
  FixedString<kInstrumentCap> instrument;
  FixedString<kOrderIdCap> order_id;
  FixedString<kInternalIdCap> internal_order_id;
  FixedString<kStrategyIdCap> strategy_id;
};

struct ModifyOrderMsg {
  HotHeader hdr;

  FixedString<kExchangeCap> exchange;
  FixedString<kInstrumentCap> instrument;
  FixedString<kOrderIdCap> order_id;
  FixedString<kInternalIdCap> internal_order_id;
  FixedString<kStrategyIdCap> strategy_id;

  std::int64_t new_price_raw;
  std::int64_t new_amount_raw;

  std::uint8_t has_new_price;
  std::uint8_t has_new_amount;
  std::uint8_t reserved[6];
};

struct OrderUpdateMsg {
  HotHeader hdr;

  FixedString<kExchangeCap> exchange;
  FixedString<kInstrumentCap> instrument;
  FixedString<kOrderIdCap> order_id;
  FixedString<kInternalIdCap> internal_order_id;
  FixedString<kStrategyIdCap> strategy_id;

  std::int64_t price_raw;
  std::int64_t amount_raw;
  std::int64_t filled_amount_raw;
  std::int64_t average_price_raw;
  // Exchange-reported event time, nanoseconds since epoch. Distinct from the
  // hdr stamps, which are local counter reads: comparing the two is how you
  // separate exchange-side latency from your own.
  std::int64_t exchange_ts_ns;

  std::uint8_t side;
  std::uint8_t order_type;
  std::uint8_t status;  // models::OrderStatus
  std::uint8_t liquidity;
  std::uint8_t has_price;
  std::uint8_t has_average_price;
  std::uint8_t reserved[2];
};

struct FillMsg {
  HotHeader hdr;

  FixedString<kExchangeCap> exchange;
  FixedString<kInstrumentCap> instrument;
  FixedString<kTradeIdCap> trade_id;
  FixedString<kOrderIdCap> order_id;
  FixedString<kStrategyIdCap> strategy_id;
  FixedString<kCurrencyCap> fee_currency;

  std::int64_t price_raw;
  std::int64_t amount_raw;
  std::int64_t fee_raw;
  std::int64_t exchange_ts_ns;

  std::uint8_t side;
  std::uint8_t liquidity;
  std::uint8_t reserved[6];
};

struct HeartbeatMsg {
  HotHeader hdr;
  std::uint64_t counter;
};

// Every hot message must be memcpy-able and layout-compatible across the two
// processes sharing the ring.
#define CALAIS_ASSERT_HOT_POD(T)                                          \
  static_assert(std::is_trivially_copyable_v<T>, #T " must be memcpy-able"); \
  static_assert(std::is_standard_layout_v<T>, #T " must be standard layout"); \
  static_assert(offsetof(T, hdr) == 0, #T " must start with its header")

CALAIS_ASSERT_HOT_POD(PlaceOrderMsg);
CALAIS_ASSERT_HOT_POD(CancelOrderMsg);
CALAIS_ASSERT_HOT_POD(ModifyOrderMsg);
CALAIS_ASSERT_HOT_POD(OrderUpdateMsg);
CALAIS_ASSERT_HOT_POD(FillMsg);
CALAIS_ASSERT_HOT_POD(HeartbeatMsg);

#undef CALAIS_ASSERT_HOT_POD

// Size the shared-memory ring's slots with this.
inline constexpr std::size_t kMaxHotMessageBytes = std::max({
    sizeof(PlaceOrderMsg),
    sizeof(CancelOrderMsg),
    sizeof(ModifyOrderMsg),
    sizeof(OrderUpdateMsg),
    sizeof(FillMsg),
    sizeof(HeartbeatMsg),
});

// ---------------------------------------------------------------------------
// Conversion to/from the domain models.
//
// Encoding is hot-path safe (fixed work, no allocation) and returns false
// rather than truncating when a string does not fit. Decoding allocates
// std::strings and is therefore NOT hot-path safe -- it exists for the
// reconciler, for persistence, and for tests.
// ---------------------------------------------------------------------------

inline bool encode_place_order(const models::OrderRequest& req,
                               std::string_view exchange, std::uint64_t seq,
                               PlaceOrderMsg& out) noexcept {
  out.hdr.init(HotMsgType::kPlaceOrder, sizeof(PlaceOrderMsg), seq);

  if (!out.exchange.assign(exchange) || !out.instrument.assign(req.instrument) ||
      !out.internal_order_id.assign(req.internal_order_id) ||
      !out.strategy_id.assign(req.strategy_id.value_or(std::string())) ||
      !out.label.assign(req.label.value_or(std::string()))) {
    return false;
  }

  out.amount_raw = req.amount.raw();
  out.has_price = req.price.has_value() ? 1 : 0;
  out.price_raw = req.price.has_value() ? req.price->raw() : 0;
  out.side = static_cast<std::uint8_t>(req.side);
  out.order_type = static_cast<std::uint8_t>(req.order_type);
  out.post_only = req.post_only ? 1 : 0;
  out.reject_post_only = req.reject_post_only ? 1 : 0;
  std::memset(out.reserved, 0, sizeof(out.reserved));
  return true;
}

inline models::OrderRequest decode_place_order(const PlaceOrderMsg& msg) {
  models::OrderRequest req;
  req.instrument = std::string(msg.instrument.view());
  req.side = static_cast<models::OrderSide>(msg.side);
  req.amount = core::Qty::from_raw(msg.amount_raw);
  req.order_type = static_cast<models::OrderType>(msg.order_type);
  if (msg.has_price != 0) {
    req.price = core::Price::from_raw(msg.price_raw);
  }
  req.post_only = msg.post_only != 0;
  req.reject_post_only = msg.reject_post_only != 0;
  req.internal_order_id = std::string(msg.internal_order_id.view());
  if (!msg.strategy_id.empty()) {
    req.strategy_id = std::string(msg.strategy_id.view());
  }
  if (!msg.label.empty()) {
    req.label = std::string(msg.label.view());
  }
  return req;
}

inline bool encode_order_update(const models::Order& order, std::uint64_t seq,
                                OrderUpdateMsg& out) noexcept {
  out.hdr.init(HotMsgType::kOrderUpdate, sizeof(OrderUpdateMsg), seq);

  if (!out.exchange.assign(order.exchange) ||
      !out.instrument.assign(order.instrument) ||
      !out.order_id.assign(order.order_id) ||
      !out.internal_order_id.assign(
          order.internal_order_id.value_or(std::string())) ||
      !out.strategy_id.assign(order.strategy_id.value_or(std::string()))) {
    return false;
  }

  out.amount_raw = order.amount.raw();
  out.filled_amount_raw = order.filled_amount.raw();
  out.has_price = order.price.has_value() ? 1 : 0;
  out.price_raw = order.price.has_value() ? order.price->raw() : 0;
  out.has_average_price = order.average_price.has_value() ? 1 : 0;
  out.average_price_raw =
      order.average_price.has_value() ? order.average_price->raw() : 0;
  out.exchange_ts_ns = order.updated_at.ns();
  out.side = static_cast<std::uint8_t>(order.side);
  out.order_type = static_cast<std::uint8_t>(order.order_type);
  out.status = static_cast<std::uint8_t>(order.status);
  out.liquidity = static_cast<std::uint8_t>(order.liquidity);
  std::memset(out.reserved, 0, sizeof(out.reserved));
  return true;
}

inline models::Order decode_order_update(const OrderUpdateMsg& msg) {
  models::Order o;
  o.order_id = std::string(msg.order_id.view());
  o.exchange = std::string(msg.exchange.view());
  o.instrument = std::string(msg.instrument.view());
  o.side = static_cast<models::OrderSide>(msg.side);
  o.order_type = static_cast<models::OrderType>(msg.order_type);
  o.amount = core::Qty::from_raw(msg.amount_raw);
  o.status = static_cast<models::OrderStatus>(msg.status);
  if (!msg.internal_order_id.empty()) {
    o.internal_order_id = std::string(msg.internal_order_id.view());
  }
  if (msg.has_price != 0) {
    o.price = core::Price::from_raw(msg.price_raw);
  }
  o.filled_amount = core::Qty::from_raw(msg.filled_amount_raw);
  if (msg.has_average_price != 0) {
    o.average_price = core::Price::from_raw(msg.average_price_raw);
  }
  o.liquidity = static_cast<models::Liquidity>(msg.liquidity);
  if (!msg.strategy_id.empty()) {
    o.strategy_id = std::string(msg.strategy_id.view());
  }
  o.updated_at = core::Timestamp::from_ns(msg.exchange_ts_ns);
  o.created_at = o.updated_at;
  return o;
}

inline bool encode_fill(const models::Fill& fill, std::uint64_t seq,
                        FillMsg& out) noexcept {
  out.hdr.init(HotMsgType::kFill, sizeof(FillMsg), seq);

  if (!out.exchange.assign(fill.exchange) ||
      !out.instrument.assign(fill.instrument) ||
      !out.trade_id.assign(fill.trade_id) || !out.order_id.assign(fill.order_id) ||
      !out.strategy_id.assign(fill.strategy_id.value_or(std::string())) ||
      !out.fee_currency.assign(fill.fee_currency)) {
    return false;
  }

  out.price_raw = fill.price.raw();
  out.amount_raw = fill.amount.raw();
  out.fee_raw = fill.fee.raw();
  out.exchange_ts_ns = fill.timestamp.ns();
  out.side = static_cast<std::uint8_t>(fill.side);
  out.liquidity = static_cast<std::uint8_t>(fill.liquidity);
  std::memset(out.reserved, 0, sizeof(out.reserved));
  return true;
}

inline models::Fill decode_fill(const FillMsg& msg) {
  models::Fill f;
  f.trade_id = std::string(msg.trade_id.view());
  f.order_id = std::string(msg.order_id.view());
  f.exchange = std::string(msg.exchange.view());
  f.instrument = std::string(msg.instrument.view());
  f.side = static_cast<models::OrderSide>(msg.side);
  f.amount = core::Qty::from_raw(msg.amount_raw);
  f.price = core::Price::from_raw(msg.price_raw);
  f.fee = core::Price::from_raw(msg.fee_raw);
  f.fee_currency = std::string(msg.fee_currency.view());
  f.liquidity = static_cast<models::Liquidity>(msg.liquidity);
  f.timestamp = core::Timestamp::from_ns(msg.exchange_ts_ns);
  if (!msg.strategy_id.empty()) {
    f.strategy_id = std::string(msg.strategy_id.view());
  }
  return f;
}

inline bool encode_cancel_order(std::string_view exchange,
                                std::string_view instrument,
                                std::string_view order_id,
                                std::string_view internal_order_id,
                                std::string_view strategy_id, std::uint64_t seq,
                                CancelOrderMsg& out) noexcept {
  out.hdr.init(HotMsgType::kCancelOrder, sizeof(CancelOrderMsg), seq);
  return out.exchange.assign(exchange) && out.instrument.assign(instrument) &&
         out.order_id.assign(order_id) &&
         out.internal_order_id.assign(internal_order_id) &&
         out.strategy_id.assign(strategy_id);
}

}  // namespace calais::transport
