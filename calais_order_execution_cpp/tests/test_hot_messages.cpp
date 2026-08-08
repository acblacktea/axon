#include "calais/transport/hot_messages.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>

#include "calais/core/shm_ring.h"

using namespace calais::transport;
using calais::core::Price;
using calais::core::Qty;
using calais::core::ShmRing;
using calais::core::Timestamp;
using calais::models::Liquidity;
using calais::models::OrderSide;
using calais::models::OrderStatus;
using calais::models::OrderType;

namespace {

calais::models::OrderRequest sample_request() {
  calais::models::OrderRequest r;
  r.instrument = "BTC-27JUN25-100000-C";
  r.side = OrderSide::kBuy;
  r.amount = *Qty::from_string("2.5");
  r.order_type = OrderType::kLimit;
  r.price = *Price::from_string("0.0345");
  r.label = "chase-maker";
  r.post_only = true;
  r.reject_post_only = false;
  r.internal_order_id = "0123456789abcdef0123456789abcdef";
  r.strategy_id = "alpha";
  return r;
}

}  // namespace

TEST(FixedString, AssignAndView) {
  FixedString<16> s{};
  ASSERT_TRUE(s.assign("hello"));
  EXPECT_EQ(s.view(), "hello");
  EXPECT_FALSE(s.empty());

  s.clear();
  EXPECT_TRUE(s.empty());
  EXPECT_EQ(s.view(), "");
}

TEST(FixedString, ExactFitHasNoNulTerminator) {
  FixedString<5> s{};
  ASSERT_TRUE(s.assign("12345"));
  EXPECT_EQ(s.view(), "12345");
  EXPECT_EQ(s.view().size(), 5u);
}

TEST(FixedString, RejectsOverlongRatherThanTruncating) {
  // A truncated instrument name would route an order to the wrong contract.
  FixedString<4> s{};
  s.assign("ok");
  EXPECT_FALSE(s.assign("far too long"));
  EXPECT_EQ(s.view(), "ok") << "a rejected assign must not corrupt the value";
}

TEST(FixedString, ZeroFillsTailSoRepresentationIsCanonical) {
  FixedString<16> a{};
  FixedString<16> b{};
  std::memset(a.data, 0xAB, sizeof(a.data));
  std::memset(b.data, 0x00, sizeof(b.data));
  ASSERT_TRUE(a.assign("same"));
  ASSERT_TRUE(b.assign("same"));
  EXPECT_EQ(std::memcmp(a.data, b.data, sizeof(a.data)), 0)
      << "identical text must produce identical bytes";
}

TEST(HotHeader, LayoutIsStable) {
  EXPECT_EQ(sizeof(HotHeader), 48u);
  EXPECT_EQ(offsetof(HotHeader, type), 0u);
  EXPECT_EQ(offsetof(HotHeader, version), 2u);
  EXPECT_EQ(offsetof(HotHeader, payload_bytes), 4u);
  EXPECT_EQ(offsetof(HotHeader, seq), 8u);
  EXPECT_EQ(offsetof(HotHeader, stamps), 16u);
}

TEST(HotHeader, InitStampsOriginAndClearsTheRest) {
  HotHeader h{};
  h.init(HotMsgType::kPlaceOrder, 128, 42);

  EXPECT_EQ(h.kind(), HotMsgType::kPlaceOrder);
  EXPECT_EQ(h.version, kHotMsgVersion);
  EXPECT_EQ(h.payload_bytes, 128u);
  EXPECT_EQ(h.seq, 42u);
  EXPECT_GT(h.stamps[kStampOrigin], 0u);
  EXPECT_EQ(h.stamps[kStampEnqueued], 0u);
  EXPECT_EQ(h.stamps[kStampDequeued], 0u);
  EXPECT_EQ(h.stamps[kStampHandled], 0u);
}

TEST(HotHeader, StampsFillProgressively) {
  HotHeader h{};
  h.init(HotMsgType::kFill, 64, 1);
  h.stamp(kStampEnqueued);
  h.stamp(kStampDequeued);
  h.stamp(kStampHandled);

  EXPECT_GE(h.stamps[kStampEnqueued], h.stamps[kStampOrigin]);
  EXPECT_GE(h.stamps[kStampDequeued], h.stamps[kStampEnqueued]);
  EXPECT_GE(h.stamps[kStampHandled], h.stamps[kStampDequeued]);
}

TEST(HotMessages, MaxSizeCoversEveryMessage) {
  EXPECT_GE(kMaxHotMessageBytes, sizeof(PlaceOrderMsg));
  EXPECT_GE(kMaxHotMessageBytes, sizeof(CancelOrderMsg));
  EXPECT_GE(kMaxHotMessageBytes, sizeof(ModifyOrderMsg));
  EXPECT_GE(kMaxHotMessageBytes, sizeof(OrderUpdateMsg));
  EXPECT_GE(kMaxHotMessageBytes, sizeof(FillMsg));
  EXPECT_GE(kMaxHotMessageBytes, sizeof(HeartbeatMsg));

  // Sanity bound: if a message ever exceeds a few hundred bytes, something
  // that belongs on the control plane has crept onto the hot path.
  EXPECT_LT(kMaxHotMessageBytes, 512u);
}

TEST(HotMessages, PlaceOrderRoundTrip) {
  const auto req = sample_request();

  PlaceOrderMsg msg{};
  ASSERT_TRUE(encode_place_order(req, "deribit", 7, msg));

  EXPECT_EQ(msg.hdr.kind(), HotMsgType::kPlaceOrder);
  EXPECT_EQ(msg.hdr.seq, 7u);
  EXPECT_EQ(msg.hdr.payload_bytes, sizeof(PlaceOrderMsg));
  EXPECT_EQ(msg.exchange.view(), "deribit");

  const auto back = decode_place_order(msg);
  EXPECT_EQ(back.instrument, req.instrument);
  EXPECT_EQ(back.side, req.side);
  EXPECT_EQ(back.amount, req.amount);
  EXPECT_EQ(back.order_type, req.order_type);
  ASSERT_TRUE(back.price.has_value());
  EXPECT_EQ(*back.price, *req.price);
  EXPECT_EQ(back.post_only, req.post_only);
  EXPECT_EQ(back.reject_post_only, req.reject_post_only);
  EXPECT_EQ(back.internal_order_id, req.internal_order_id);
  EXPECT_EQ(back.strategy_id, req.strategy_id);
  EXPECT_EQ(back.label, req.label);
}

TEST(HotMessages, PlaceOrderWithoutPrice) {
  auto req = sample_request();
  req.order_type = OrderType::kMarket;
  req.price.reset();
  req.label.reset();
  req.strategy_id.reset();

  PlaceOrderMsg msg{};
  ASSERT_TRUE(encode_place_order(req, "okx", 1, msg));
  EXPECT_EQ(msg.has_price, 0);

  const auto back = decode_place_order(msg);
  EXPECT_FALSE(back.price.has_value());
  EXPECT_FALSE(back.label.has_value());
  EXPECT_FALSE(back.strategy_id.has_value());
  EXPECT_EQ(back.order_type, OrderType::kMarket);
}

TEST(HotMessages, EncodeRejectsOverlongStrings) {
  auto req = sample_request();
  req.instrument = std::string(kInstrumentCap + 1, 'X');

  PlaceOrderMsg msg{};
  EXPECT_FALSE(encode_place_order(req, "deribit", 1, msg));
}

TEST(HotMessages, OrderUpdateRoundTrip) {
  calais::models::Order o;
  o.order_id = "DERIBIT-12345";
  o.exchange = "deribit";
  o.instrument = "BTC-27JUN25-100000-C";
  o.side = OrderSide::kSell;
  o.order_type = OrderType::kLimit;
  o.amount = *Qty::from_string("10");
  o.status = OrderStatus::kPartiallyFilled;
  o.internal_order_id = "0123456789abcdef0123456789abcdef";
  o.price = *Price::from_string("0.0512");
  o.filled_amount = *Qty::from_string("3.25");
  o.average_price = *Price::from_string("0.0509");
  o.liquidity = Liquidity::kTaker;
  o.strategy_id = "beta";
  o.updated_at = *Timestamp::from_iso8601("2026-08-07T12:34:56.123456");

  OrderUpdateMsg msg{};
  ASSERT_TRUE(encode_order_update(o, 3, msg));

  const auto back = decode_order_update(msg);
  EXPECT_EQ(back.order_id, o.order_id);
  EXPECT_EQ(back.exchange, o.exchange);
  EXPECT_EQ(back.instrument, o.instrument);
  EXPECT_EQ(back.side, o.side);
  EXPECT_EQ(back.order_type, o.order_type);
  EXPECT_EQ(back.amount, o.amount);
  EXPECT_EQ(back.status, o.status);
  EXPECT_EQ(back.internal_order_id, o.internal_order_id);
  EXPECT_EQ(back.price, o.price);
  EXPECT_EQ(back.filled_amount, o.filled_amount);
  EXPECT_EQ(back.average_price, o.average_price);
  EXPECT_EQ(back.liquidity, o.liquidity);
  EXPECT_EQ(back.strategy_id, o.strategy_id);
  EXPECT_EQ(back.updated_at, o.updated_at);
}

TEST(HotMessages, OrderUpdateKeepsFullNanosecondTimestamp) {
  // Unlike the JSON path, the binary path must not lose sub-microsecond
  // resolution -- that is the whole reason it exists.
  calais::models::Order o;
  o.order_id = "x";
  o.exchange = "e";
  o.instrument = "i";
  o.amount = Qty::from_integer(1);
  o.updated_at = Timestamp::from_ns(1'786'106'096'123'456'789LL);

  OrderUpdateMsg msg{};
  ASSERT_TRUE(encode_order_update(o, 1, msg));
  EXPECT_EQ(decode_order_update(msg).updated_at.ns(), 1'786'106'096'123'456'789LL);
}

TEST(HotMessages, FillRoundTrip) {
  calais::models::Fill f;
  f.trade_id = "TRADE-9876";
  f.order_id = "DERIBIT-12345";
  f.exchange = "deribit";
  f.instrument = "BTC-27JUN25-100000-C";
  f.side = OrderSide::kBuy;
  f.amount = *Qty::from_string("1.5");
  f.price = *Price::from_string("0.0345");
  f.fee = *Price::from_string("0.0000123");
  f.fee_currency = "BTC";
  f.liquidity = Liquidity::kMaker;
  f.timestamp = Timestamp::from_millis(1'786'106'096'123LL);
  f.strategy_id = "alpha";

  FillMsg msg{};
  ASSERT_TRUE(encode_fill(f, 11, msg));

  const auto back = decode_fill(msg);
  EXPECT_EQ(back.trade_id, f.trade_id);
  EXPECT_EQ(back.order_id, f.order_id);
  EXPECT_EQ(back.exchange, f.exchange);
  EXPECT_EQ(back.instrument, f.instrument);
  EXPECT_EQ(back.side, f.side);
  EXPECT_EQ(back.amount, f.amount);
  EXPECT_EQ(back.price, f.price);
  EXPECT_EQ(back.fee, f.fee);
  EXPECT_EQ(back.fee_currency, f.fee_currency);
  EXPECT_EQ(back.liquidity, f.liquidity);
  EXPECT_EQ(back.timestamp, f.timestamp);
  EXPECT_EQ(back.strategy_id, f.strategy_id);
}

TEST(HotMessages, CancelOrderEncode) {
  CancelOrderMsg msg{};
  ASSERT_TRUE(encode_cancel_order("bybit", "BTCUSDT", "ORD-1", "abc", "gamma", 5,
                                  msg));
  EXPECT_EQ(msg.hdr.kind(), HotMsgType::kCancelOrder);
  EXPECT_EQ(msg.hdr.seq, 5u);
  EXPECT_EQ(msg.exchange.view(), "bybit");
  EXPECT_EQ(msg.instrument.view(), "BTCUSDT");
  EXPECT_EQ(msg.order_id.view(), "ORD-1");
  EXPECT_EQ(msg.internal_order_id.view(), "abc");
  EXPECT_EQ(msg.strategy_id.view(), "gamma");
}

TEST(HotMessages, InternalIdCapacityMatchesUuid4Hex) {
  // uuid4().hex is exactly 32 characters; the field must hold it without
  // needing a NUL, which is why view() falls back to the full capacity.
  EXPECT_EQ(kInternalIdCap, calais::models::kInternalIdLength);

  auto req = sample_request();
  req.internal_order_id = calais::models::generate_internal_id();
  ASSERT_EQ(req.internal_order_id.size(), 32u);

  PlaceOrderMsg msg{};
  ASSERT_TRUE(encode_place_order(req, "deribit", 1, msg));
  EXPECT_EQ(decode_place_order(msg).internal_order_id, req.internal_order_id);
}

// ---------------------------------------------------------------------------
// The integration that matters: a hot message travelling through the shared
// memory ring, byte for byte, carrying its own latency stamps.
// ---------------------------------------------------------------------------
TEST(HotMessages, TravelThroughShmRingIntact) {
  const std::string path =
      "/tmp/calais_hot_msgs_" + std::to_string(::getpid());
  ShmRing ring = ShmRing::create(path, kMaxHotMessageBytes, 64);
  ring.set_unlink_on_destroy(true);

  const auto req = sample_request();

  PlaceOrderMsg sent{};
  ASSERT_TRUE(encode_place_order(req, "deribit", 1, sent));
  sent.hdr.stamp(kStampEnqueued);
  ASSERT_TRUE(ring.try_push(&sent, sizeof(sent)));

  std::uint32_t len = 0;
  const void* raw = ring.try_acquire_read(len);
  ASSERT_NE(raw, nullptr);
  ASSERT_EQ(len, sizeof(PlaceOrderMsg));

  // The payoff of the fixed layout: consuming a message is a cast, not a parse.
  const auto* received = static_cast<const PlaceOrderMsg*>(raw);
  EXPECT_EQ(received->hdr.kind(), HotMsgType::kPlaceOrder);
  EXPECT_EQ(received->instrument.view(), req.instrument);
  EXPECT_EQ(received->amount_raw, req.amount.raw());
  EXPECT_EQ(received->price_raw, req.price->raw());
  EXPECT_GE(received->hdr.stamps[kStampEnqueued],
            received->hdr.stamps[kStampOrigin]);

  EXPECT_EQ(std::memcmp(&sent, raw, sizeof(PlaceOrderMsg)), 0)
      << "message must cross the ring byte-identical";

  ring.commit_read();
  EXPECT_TRUE(ring.empty());
}

TEST(HotMessages, PayloadAlignmentAllowsDirectCast) {
  // The 16-byte slot header exists so the payload lands 16-byte aligned and a
  // reinterpret_cast to a POD with 8-byte members is not an unaligned access.
  const std::string path =
      "/tmp/calais_hot_align_" + std::to_string(::getpid());
  ShmRing ring = ShmRing::create(path, kMaxHotMessageBytes, 8);
  ring.set_unlink_on_destroy(true);

  FillMsg msg{};
  msg.hdr.init(HotMsgType::kFill, sizeof(FillMsg), 1);
  ASSERT_TRUE(ring.try_push(&msg, sizeof(msg)));

  std::uint32_t len = 0;
  const void* raw = ring.try_acquire_read(len);
  ASSERT_NE(raw, nullptr);
  EXPECT_EQ(reinterpret_cast<std::uintptr_t>(raw) % 16u, 0u);
  ring.commit_read();
}
