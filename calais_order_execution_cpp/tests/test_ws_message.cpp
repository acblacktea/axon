#include "calais/net/ws_message.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

using namespace calais::net;

namespace {

WsFrameHeader hdr(WsOpcode op, bool fin, std::size_t len) {
  WsFrameHeader h;
  h.opcode = op;
  h.fin = fin;
  h.payload_length = len;
  return h;
}

const std::byte* as_bytes(std::string_view s) {
  return reinterpret_cast<const std::byte*>(s.data());
}

std::string payload_of(const WsMessageEvent& e) {
  return std::string(reinterpret_cast<const char*>(e.payload), e.payload_size);
}

}  // namespace

TEST(WsAssembler, SingleTextFrameIsZeroCopy) {
  WsMessageAssembler a;
  const std::string_view msg = R"({"method":"subscription"})";

  const auto e = a.feed(hdr(WsOpcode::kText, true, msg.size()), as_bytes(msg));

  ASSERT_EQ(e.type, WsEvent::kMessage);
  EXPECT_EQ(e.opcode, WsOpcode::kText);
  EXPECT_EQ(payload_of(e), msg);
  // The payload must point into the caller's buffer, not a copy. This is the
  // entire reason feed() reports a pointer instead of returning a string.
  EXPECT_EQ(e.payload, as_bytes(msg));
  EXPECT_EQ(a.buffered_bytes(), 0u);
  EXPECT_EQ(a.fragmented_messages(), 0u);
}

TEST(WsAssembler, SingleBinaryFrame) {
  WsMessageAssembler a;
  const auto e = a.feed(hdr(WsOpcode::kBinary, true, 3), as_bytes("abc"));
  ASSERT_EQ(e.type, WsEvent::kMessage);
  EXPECT_EQ(e.opcode, WsOpcode::kBinary);
  EXPECT_EQ(payload_of(e), "abc");
}

TEST(WsAssembler, EmptyMessage) {
  WsMessageAssembler a;
  const auto e = a.feed(hdr(WsOpcode::kText, true, 0), as_bytes(""));
  ASSERT_EQ(e.type, WsEvent::kMessage);
  EXPECT_EQ(e.payload_size, 0u);
}

TEST(WsAssembler, ReassemblesFragments) {
  WsMessageAssembler a;

  auto e = a.feed(hdr(WsOpcode::kText, false, 5), as_bytes("Hello"));
  EXPECT_EQ(e.type, WsEvent::kNone);
  EXPECT_TRUE(a.in_fragmented_message());
  EXPECT_EQ(a.buffered_bytes(), 5u);

  e = a.feed(hdr(WsOpcode::kContinuation, false, 2), as_bytes(", "));
  EXPECT_EQ(e.type, WsEvent::kNone);

  e = a.feed(hdr(WsOpcode::kContinuation, true, 5), as_bytes("World"));
  ASSERT_EQ(e.type, WsEvent::kMessage);
  EXPECT_EQ(payload_of(e), "Hello, World");
  EXPECT_EQ(e.opcode, WsOpcode::kText) << "opcode comes from the FIRST frame";
  EXPECT_FALSE(a.in_fragmented_message());
  EXPECT_EQ(a.fragmented_messages(), 1u);
}

TEST(WsAssembler, EmptyContinuationFrames) {
  WsMessageAssembler a;
  a.feed(hdr(WsOpcode::kBinary, false, 2), as_bytes("ab"));
  a.feed(hdr(WsOpcode::kContinuation, false, 0), as_bytes(""));
  const auto e = a.feed(hdr(WsOpcode::kContinuation, true, 2), as_bytes("cd"));
  ASSERT_EQ(e.type, WsEvent::kMessage);
  EXPECT_EQ(payload_of(e), "abcd");
}

// ---------------------------------------------------------------------------
// Control frames interleaved with fragments -- the case naive clients break on
// ---------------------------------------------------------------------------

TEST(WsAssembler, PingBetweenFragmentsDoesNotDisturbTheMessage) {
  WsMessageAssembler a;

  auto e = a.feed(hdr(WsOpcode::kText, false, 5), as_bytes("Hello"));
  EXPECT_EQ(e.type, WsEvent::kNone);

  // A ping arrives mid-message. It must be reported immediately so the caller
  // can pong -- an exchange will drop the connection if we stall on it.
  e = a.feed(hdr(WsOpcode::kPing, true, 4), as_bytes("ping"));
  ASSERT_EQ(e.type, WsEvent::kPing);
  EXPECT_EQ(payload_of(e), "ping");
  EXPECT_TRUE(a.in_fragmented_message()) << "the message must still be open";
  EXPECT_EQ(a.buffered_bytes(), 5u) << "the ping must not touch the buffer";

  e = a.feed(hdr(WsOpcode::kContinuation, true, 5), as_bytes("World"));
  ASSERT_EQ(e.type, WsEvent::kMessage);
  EXPECT_EQ(payload_of(e), "HelloWorld");
}

TEST(WsAssembler, PongAndCloseAlsoPassThrough) {
  WsMessageAssembler a;
  a.feed(hdr(WsOpcode::kBinary, false, 1), as_bytes("x"));

  auto e = a.feed(hdr(WsOpcode::kPong, true, 2), as_bytes("hi"));
  EXPECT_EQ(e.type, WsEvent::kPong);
  EXPECT_TRUE(a.in_fragmented_message());

  const auto close = std::vector<std::byte>{std::byte{0x03}, std::byte{0xE8}};
  e = a.feed(hdr(WsOpcode::kClose, true, 2), close.data());
  ASSERT_EQ(e.type, WsEvent::kClose);
  EXPECT_EQ(e.close.code, WsCloseCode::kNormal);
}

TEST(WsAssembler, EmptyPingIsFine) {
  WsMessageAssembler a;
  const auto e = a.feed(hdr(WsOpcode::kPing, true, 0), as_bytes(""));
  EXPECT_EQ(e.type, WsEvent::kPing);
  EXPECT_EQ(e.payload_size, 0u);
}

TEST(WsAssembler, MalformedCloseIsAProtocolError) {
  WsMessageAssembler a;
  const auto bad = std::vector<std::byte>{std::byte{0x03}, std::byte{0xED}};  // 1005
  const auto e = a.feed(hdr(WsOpcode::kClose, true, 2), bad.data());
  EXPECT_EQ(e.type, WsEvent::kProtocolError);
  EXPECT_NE(e.error, nullptr);
}

// ---------------------------------------------------------------------------
// Protocol errors
// ---------------------------------------------------------------------------

TEST(WsAssembler, ContinuationWithoutAStartedMessage) {
  WsMessageAssembler a;
  const auto e = a.feed(hdr(WsOpcode::kContinuation, true, 3), as_bytes("abc"));
  EXPECT_EQ(e.type, WsEvent::kProtocolError);
  EXPECT_NE(e.error, nullptr);
}

TEST(WsAssembler, NewDataFrameWhileFragmentsAreOpen) {
  WsMessageAssembler a;
  a.feed(hdr(WsOpcode::kText, false, 3), as_bytes("abc"));

  const auto e = a.feed(hdr(WsOpcode::kText, true, 3), as_bytes("def"));
  EXPECT_EQ(e.type, WsEvent::kProtocolError);
  // State must be cleared, so a caller that logs buffered_bytes() while
  // handling the failure does not see a phantom half-message.
  EXPECT_FALSE(a.in_fragmented_message());
  EXPECT_EQ(a.buffered_bytes(), 0u);
}

TEST(WsAssembler, EnforcesMaxMessageSize) {
  // A peer that never sets FIN would otherwise grow the buffer without bound.
  WsMessageAssembler a(16);

  auto e = a.feed(hdr(WsOpcode::kBinary, false, 10), as_bytes("0123456789"));
  EXPECT_EQ(e.type, WsEvent::kNone);

  e = a.feed(hdr(WsOpcode::kContinuation, false, 10), as_bytes("0123456789"));
  EXPECT_EQ(e.type, WsEvent::kProtocolError);
  EXPECT_FALSE(a.in_fragmented_message());
}

TEST(WsAssembler, RejectsOversizedFirstFragment) {
  WsMessageAssembler a(8);
  const std::string big(100, 'x');
  const auto e = a.feed(hdr(WsOpcode::kBinary, false, big.size()),
                        as_bytes(big));
  EXPECT_EQ(e.type, WsEvent::kProtocolError);
}

TEST(WsAssembler, UnfragmentedMessageIsNotSubjectToTheReassemblyLimit) {
  // The limit exists to bound REASSEMBLY. A single frame is already in the
  // caller's buffer, so there is nothing to bound.
  WsMessageAssembler a(8);
  const std::string big(100, 'x');
  const auto e = a.feed(hdr(WsOpcode::kBinary, true, big.size()), as_bytes(big));
  EXPECT_EQ(e.type, WsEvent::kMessage);
  EXPECT_EQ(e.payload_size, 100u);
}

// ---------------------------------------------------------------------------
// UTF-8 enforcement on text messages
// ---------------------------------------------------------------------------

TEST(WsAssembler, RejectsInvalidUtf8InASingleTextFrame) {
  WsMessageAssembler a;
  const auto bad = std::vector<std::byte>{std::byte{0xFF}, std::byte{0xFE}};
  const auto e = a.feed(hdr(WsOpcode::kText, true, 2), bad.data());
  EXPECT_EQ(e.type, WsEvent::kProtocolError);
}

TEST(WsAssembler, RejectsInvalidUtf8SplitAcrossFragments) {
  // The sequence is only invalid once assembled: each fragment on its own is
  // an incomplete-but-plausible prefix. Validating per-fragment would miss it.
  WsMessageAssembler a;
  const auto first = std::vector<std::byte>{std::byte{0xE2}};
  const auto second = std::vector<std::byte>{std::byte{0x28}, std::byte{0xA1}};

  auto e = a.feed(hdr(WsOpcode::kText, false, 1), first.data());
  EXPECT_EQ(e.type, WsEvent::kNone);

  e = a.feed(hdr(WsOpcode::kContinuation, true, 2), second.data());
  EXPECT_EQ(e.type, WsEvent::kProtocolError);
}

TEST(WsAssembler, AcceptsMultiByteUtf8SplitAcrossFragments) {
  // The mirror case: a valid 3-byte sequence split down the middle must
  // assemble and pass.
  WsMessageAssembler a;
  const auto first = std::vector<std::byte>{std::byte{0xE2}, std::byte{0x82}};
  const auto second = std::vector<std::byte>{std::byte{0xAC}};

  auto e = a.feed(hdr(WsOpcode::kText, false, 2), first.data());
  EXPECT_EQ(e.type, WsEvent::kNone);

  e = a.feed(hdr(WsOpcode::kContinuation, true, 1), second.data());
  ASSERT_EQ(e.type, WsEvent::kMessage);
  EXPECT_EQ(e.payload_size, 3u);
}

TEST(WsAssembler, BinaryFramesAreNotUtf8Checked) {
  WsMessageAssembler a;
  const auto bad = std::vector<std::byte>{std::byte{0xFF}, std::byte{0xFE}};
  const auto e = a.feed(hdr(WsOpcode::kBinary, true, 2), bad.data());
  EXPECT_EQ(e.type, WsEvent::kMessage);
}

// ---------------------------------------------------------------------------

TEST(WsAssembler, ResetClearsPartialState) {
  WsMessageAssembler a;
  a.feed(hdr(WsOpcode::kText, false, 3), as_bytes("abc"));
  ASSERT_TRUE(a.in_fragmented_message());

  a.reset();
  EXPECT_FALSE(a.in_fragmented_message());
  EXPECT_EQ(a.buffered_bytes(), 0u);

  const auto e = a.feed(hdr(WsOpcode::kText, true, 2), as_bytes("hi"));
  EXPECT_EQ(e.type, WsEvent::kMessage);
}

TEST(WsAssembler, SurvivesManyMessagesWithoutGrowing) {
  WsMessageAssembler a;
  a.reserve(4096);

  for (int i = 0; i < 10000; ++i) {
    const auto e = a.feed(hdr(WsOpcode::kText, true, 5), as_bytes("hello"));
    ASSERT_EQ(e.type, WsEvent::kMessage);
  }
  EXPECT_EQ(a.fragmented_messages(), 0u);
  EXPECT_EQ(a.buffered_bytes(), 0u);
}
