#include "axon/net/ws_frame.h"

#include <gtest/gtest.h>

#include <cstring>
#include <random>
#include <string>
#include <vector>

using namespace axon::net;

namespace {

std::vector<std::byte> bytes(std::initializer_list<int> vals) {
  std::vector<std::byte> v;
  v.reserve(vals.size());
  for (int x : vals) {
    v.push_back(static_cast<std::byte>(x));
  }
  return v;
}

std::vector<std::byte> from_string(std::string_view s) {
  std::vector<std::byte> v(s.size());
  if (!s.empty()) {
    std::memcpy(v.data(), s.data(), s.size());
  }
  return v;
}

std::string to_string(const std::byte* p, std::size_t n) {
  return std::string(reinterpret_cast<const char*>(p), n);
}

}  // namespace

// ---------------------------------------------------------------------------
// Decoding: the three payload length encodings
// ---------------------------------------------------------------------------

TEST(WsFrame, DecodesShortUnmaskedTextFrame) {
  // FIN + text, length 5, unmasked, "Hello". RFC 6455 section 5.7 example.
  const auto f = bytes({0x81, 0x05, 'H', 'e', 'l', 'l', 'o'});
  const auto r = ws_decode_frame(f.data(), f.size());

  ASSERT_EQ(r.status, WsDecodeStatus::kOk) << (r.error ? r.error : "");
  EXPECT_TRUE(r.header.fin);
  EXPECT_FALSE(r.header.rsv1);
  EXPECT_EQ(r.header.opcode, WsOpcode::kText);
  EXPECT_FALSE(r.header.masked);
  EXPECT_EQ(r.header.payload_length, 5u);
  EXPECT_EQ(r.header.header_size, 2u);
  EXPECT_EQ(r.total_size, 7u);
  EXPECT_EQ(to_string(r.payload, 5), "Hello");
}

TEST(WsFrame, DecodesMaskedClientFrameFromTheRfc) {
  // RFC 6455 section 5.7: a masked "Hello" from a client.
  const auto f = bytes({0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d,
                        0x51, 0x58});
  const auto r = ws_decode_frame(f.data(), f.size());

  ASSERT_EQ(r.status, WsDecodeStatus::kOk);
  EXPECT_TRUE(r.header.masked);
  EXPECT_EQ(r.header.mask_key, 0x37fa213du);
  EXPECT_EQ(r.header.payload_length, 5u);
  EXPECT_EQ(r.header.header_size, 6u);

  std::vector<std::byte> payload(r.payload, r.payload + 5);
  ws_mask(payload.data(), payload.size(), r.header.mask_key);
  EXPECT_EQ(to_string(payload.data(), payload.size()), "Hello");
}

TEST(WsFrame, DecodesExtended16BitLength) {
  std::vector<std::byte> f = bytes({0x82, 0x7E, 0x01, 0x00});  // 256 bytes
  f.resize(4 + 256, std::byte{0xAB});

  const auto r = ws_decode_frame(f.data(), f.size());
  ASSERT_EQ(r.status, WsDecodeStatus::kOk);
  EXPECT_EQ(r.header.opcode, WsOpcode::kBinary);
  EXPECT_EQ(r.header.payload_length, 256u);
  EXPECT_EQ(r.header.header_size, 4u);
  EXPECT_EQ(r.total_size, 260u);
}

TEST(WsFrame, DecodesExtended64BitLength) {
  std::vector<std::byte> f =
      bytes({0x82, 0x7F, 0, 0, 0, 0, 0, 0x01, 0x00, 0x00});  // 65536
  f.resize(10 + 65536, std::byte{0xCD});

  const auto r = ws_decode_frame(f.data(), f.size());
  ASSERT_EQ(r.status, WsDecodeStatus::kOk);
  EXPECT_EQ(r.header.payload_length, 65536u);
  EXPECT_EQ(r.header.header_size, 10u);
}

TEST(WsFrame, DecodesEmptyPayload) {
  const auto f = bytes({0x81, 0x00});
  const auto r = ws_decode_frame(f.data(), f.size());
  ASSERT_EQ(r.status, WsDecodeStatus::kOk);
  EXPECT_EQ(r.header.payload_length, 0u);
  EXPECT_EQ(r.total_size, 2u);
}

// ---------------------------------------------------------------------------
// Incremental arrival -- the normal case on a stream socket
// ---------------------------------------------------------------------------

TEST(WsFrame, ReportsIncompleteForEveryPrefix) {
  const auto f = bytes({0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d,
                        0x51, 0x58});
  for (std::size_t n = 0; n < f.size(); ++n) {
    const auto r = ws_decode_frame(f.data(), n);
    EXPECT_EQ(r.status, WsDecodeStatus::kIncomplete)
        << "prefix length " << n << " should not parse";
  }
  EXPECT_EQ(ws_decode_frame(f.data(), f.size()).status, WsDecodeStatus::kOk);
}

TEST(WsFrame, IncompleteForTruncatedExtendedLengths) {
  const auto f16 = bytes({0x82, 0x7E, 0x01});
  EXPECT_EQ(ws_decode_frame(f16.data(), f16.size()).status,
            WsDecodeStatus::kIncomplete);

  const auto f64 = bytes({0x82, 0x7F, 0, 0, 0, 0});
  EXPECT_EQ(ws_decode_frame(f64.data(), f64.size()).status,
            WsDecodeStatus::kIncomplete);
}

TEST(WsFrame, ExtraBytesAfterAFrameAreLeftAlone) {
  // Two frames in one read is normal; the decoder must report only the first.
  auto f = bytes({0x81, 0x02, 'h', 'i'});
  const auto second = bytes({0x81, 0x01, 'x'});
  f.insert(f.end(), second.begin(), second.end());

  const auto r = ws_decode_frame(f.data(), f.size());
  ASSERT_EQ(r.status, WsDecodeStatus::kOk);
  EXPECT_EQ(r.total_size, 4u);

  const auto r2 = ws_decode_frame(f.data() + r.total_size, f.size() - r.total_size);
  ASSERT_EQ(r2.status, WsDecodeStatus::kOk);
  EXPECT_EQ(to_string(r2.payload, 1), "x");
}

// ---------------------------------------------------------------------------
// Protocol errors
// ---------------------------------------------------------------------------

TEST(WsFrame, RejectsReservedBits) {
  // We negotiate no extensions, so RSV1 (permessage-deflate) is an error.
  for (int rsv : {0x40, 0x20, 0x10}) {
    const auto f = bytes({0x81 | rsv, 0x00});
    const auto r = ws_decode_frame(f.data(), f.size());
    EXPECT_EQ(r.status, WsDecodeStatus::kProtocolError) << "rsv bit " << rsv;
    EXPECT_NE(r.error, nullptr);
  }
}

TEST(WsFrame, RejectsReservedOpcodes) {
  for (int op : {0x3, 0x4, 0x5, 0x6, 0x7, 0xB, 0xC, 0xD, 0xE, 0xF}) {
    const auto f = bytes({0x80 | op, 0x00});
    const auto r = ws_decode_frame(f.data(), f.size());
    EXPECT_EQ(r.status, WsDecodeStatus::kProtocolError)
        << "opcode 0x" << std::hex << op;
  }
}

TEST(WsFrame, AcceptsAllValidOpcodes) {
  for (int op : {0x0, 0x1, 0x2, 0x8, 0x9, 0xA}) {
    const auto f = bytes({0x80 | op, 0x00});
    const auto r = ws_decode_frame(f.data(), f.size());
    EXPECT_EQ(r.status, WsDecodeStatus::kOk) << "opcode 0x" << std::hex << op;
  }
}

TEST(WsFrame, RejectsOversizedControlFrame) {
  // Control payloads are capped at 125 so they can always be answered without
  // buffering.
  std::vector<std::byte> f = bytes({0x89, 0x7E, 0x00, 0x80});  // ping, 128 bytes
  f.resize(4 + 128, std::byte{0});
  const auto r = ws_decode_frame(f.data(), f.size());
  EXPECT_EQ(r.status, WsDecodeStatus::kProtocolError);
}

TEST(WsFrame, AcceptsControlFrameAtExactlyTheLimit) {
  std::vector<std::byte> f = bytes({0x89, 125});
  f.resize(2 + 125, std::byte{0x41});
  const auto r = ws_decode_frame(f.data(), f.size());
  EXPECT_EQ(r.status, WsDecodeStatus::kOk);
  EXPECT_EQ(r.header.payload_length, 125u);
}

TEST(WsFrame, RejectsFragmentedControlFrame) {
  const auto f = bytes({0x09, 0x00});  // ping with FIN clear
  const auto r = ws_decode_frame(f.data(), f.size());
  EXPECT_EQ(r.status, WsDecodeStatus::kProtocolError);
}

TEST(WsFrame, Rejects64BitLengthWithReservedHighBit) {
  const auto f = bytes({0x82, 0x7F, 0x80, 0, 0, 0, 0, 0, 0, 0});
  const auto r = ws_decode_frame(f.data(), f.size());
  EXPECT_EQ(r.status, WsDecodeStatus::kProtocolError);
}

TEST(WsFrame, AcceptsNonMinimalLengthEncoding) {
  // Deliberate leniency: the RFC says a SENDER must use the minimal encoding,
  // but dropping a live exchange connection over three wasted bytes is a far
  // worse failure than tolerating them. Our encoder is still strict.
  const auto f = bytes({0x81, 0x7E, 0x00, 0x05, 'H', 'e', 'l', 'l', 'o'});
  const auto r = ws_decode_frame(f.data(), f.size());
  ASSERT_EQ(r.status, WsDecodeStatus::kOk);
  EXPECT_EQ(r.header.payload_length, 5u);
  EXPECT_EQ(to_string(r.payload, 5), "Hello");
}

TEST(WsFrame, DecodeNeverReadsPastTheBuffer) {
  // Run every prefix of a large frame through the decoder under whatever
  // sanitizer is active. A one-byte overread here is exactly the bug that
  // turns a malformed frame into a crash in production.
  std::vector<std::byte> f = bytes({0x82, 0x7F, 0, 0, 0, 0, 0, 0, 0x10, 0x00});
  f.resize(10 + 4096, std::byte{0x5A});

  for (std::size_t n = 0; n <= f.size(); ++n) {
    std::vector<std::byte> exact(f.begin(), f.begin() + static_cast<long>(n));
    const auto r = ws_decode_frame(exact.data(), exact.size());
    EXPECT_NE(r.status, WsDecodeStatus::kProtocolError) << "at " << n;
  }
}

// ---------------------------------------------------------------------------
// Masking
// ---------------------------------------------------------------------------

TEST(WsMask, IsItsOwnInverse) {
  std::mt19937_64 rng(42);
  for (std::size_t len = 0; len < 300; ++len) {
    std::vector<std::byte> original(len);
    for (auto& b : original) {
      b = static_cast<std::byte>(rng() & 0xFF);
    }
    const auto key = static_cast<std::uint32_t>(rng());

    std::vector<std::byte> work = original;
    ws_mask(work.data(), work.size(), key);
    if (len > 0 && key != 0) {
      // Not a strict requirement, but a key that leaves the data untouched
      // would mean the mask is a no-op and the test proves nothing.
      EXPECT_NE(work, original) << "len " << len;
    }
    ws_mask(work.data(), work.size(), key);
    EXPECT_EQ(work, original) << "len " << len;
  }
}

TEST(WsMask, MatchesTheRfcVector) {
  auto payload = from_string("Hello");
  ws_mask(payload.data(), payload.size(), 0x37fa213du);
  const auto expected = bytes({0x7f, 0x9f, 0x4d, 0x51, 0x58});
  EXPECT_EQ(payload, expected);
}

TEST(WsMask, PiecewiseWithOffsetMatchesWholeBuffer) {
  // The 8-byte fast path only engages once the key phase aligns, so masking a
  // buffer in chunks must give the same result as masking it in one go --
  // otherwise a payload split across two socket reads would be corrupted.
  std::mt19937_64 rng(7);
  std::vector<std::byte> original(257);
  for (auto& b : original) {
    b = static_cast<std::byte>(rng() & 0xFF);
  }
  const std::uint32_t key = 0xDEADBEEF;

  std::vector<std::byte> whole = original;
  ws_mask(whole.data(), whole.size(), key, 0);

  for (std::size_t split = 0; split <= original.size(); ++split) {
    std::vector<std::byte> piecewise = original;
    ws_mask(piecewise.data(), split, key, 0);
    ws_mask(piecewise.data() + split, piecewise.size() - split, key, split);
    EXPECT_EQ(piecewise, whole) << "split at " << split;
  }
}

TEST(WsMask, HandlesUnalignedBuffers) {
  // The fast path uses unaligned 64-bit accesses; make sure every start offset
  // within a word behaves.
  std::vector<std::byte> backing(512, std::byte{0x11});
  for (std::size_t shift = 0; shift < 8; ++shift) {
    std::vector<std::byte> original(backing.begin() + static_cast<long>(shift),
                                    backing.end());
    std::vector<std::byte> work = original;
    ws_mask(work.data(), work.size(), 0x01020304);
    ws_mask(work.data(), work.size(), 0x01020304);
    EXPECT_EQ(work, original) << "shift " << shift;
  }
}

TEST(WsMask, ZeroLengthIsSafe) {
  std::byte dummy{0};
  ws_mask(&dummy, 0, 0x12345678);
  EXPECT_EQ(dummy, std::byte{0});
}

// ---------------------------------------------------------------------------
// Encoding
// ---------------------------------------------------------------------------

TEST(WsFrame, HeaderSizeMatchesEncodedSize) {
  struct Case {
    std::uint64_t len;
    std::size_t unmasked;
    std::size_t masked;
  };
  const Case cases[] = {
      {0, 2, 6},      {125, 2, 6},        {126, 4, 8},
      {65535, 4, 8},  {65536, 10, 14},    {1u << 20, 10, 14},
  };

  std::byte buf[kMaxFrameHeaderSize];
  for (const auto& c : cases) {
    EXPECT_EQ(ws_header_size(c.len, false), c.unmasked) << c.len;
    EXPECT_EQ(ws_header_size(c.len, true), c.masked) << c.len;
    EXPECT_EQ(ws_encode_header(buf, sizeof(buf), WsOpcode::kBinary, true, c.len,
                               false, 0),
              c.unmasked);
    EXPECT_EQ(ws_encode_header(buf, sizeof(buf), WsOpcode::kBinary, true, c.len,
                               true, 0x11223344),
              c.masked);
  }
}

TEST(WsFrame, EncoderUsesMinimalLengthEncoding) {
  std::byte buf[kMaxFrameHeaderSize];

  ws_encode_header(buf, sizeof(buf), WsOpcode::kText, true, 125, false, 0);
  EXPECT_EQ(static_cast<int>(buf[1]) & 0x7F, 125);

  ws_encode_header(buf, sizeof(buf), WsOpcode::kText, true, 126, false, 0);
  EXPECT_EQ(static_cast<int>(buf[1]) & 0x7F, 126);

  ws_encode_header(buf, sizeof(buf), WsOpcode::kText, true, 65535, false, 0);
  EXPECT_EQ(static_cast<int>(buf[1]) & 0x7F, 126);

  ws_encode_header(buf, sizeof(buf), WsOpcode::kText, true, 65536, false, 0);
  EXPECT_EQ(static_cast<int>(buf[1]) & 0x7F, 127);
}

TEST(WsFrame, EncodeHeaderRefusesUndersizedBuffer) {
  std::byte buf[3];
  EXPECT_EQ(ws_encode_header(buf, sizeof(buf), WsOpcode::kText, true, 1000,
                             false, 0),
            0u);
}

TEST(WsFrame, EncodeThenDecodeRoundTrips) {
  std::mt19937_64 rng(99);
  const std::size_t lengths[] = {0, 1, 5, 125, 126, 127, 200, 65535, 65536, 70000};

  for (std::size_t len : lengths) {
    std::vector<std::byte> payload(len);
    for (auto& b : payload) {
      b = static_cast<std::byte>(rng() & 0xFF);
    }

    std::vector<std::byte> out(len + kMaxFrameHeaderSize);
    const std::uint32_t key = static_cast<std::uint32_t>(rng()) | 1u;
    const std::size_t n = ws_encode_frame(out.data(), out.size(),
                                          WsOpcode::kBinary, true,
                                          payload.data(), len, key);
    ASSERT_GT(n, 0u) << "len " << len;
    EXPECT_EQ(n, ws_header_size(len, true) + len);

    const auto r = ws_decode_frame(out.data(), n);
    ASSERT_EQ(r.status, WsDecodeStatus::kOk) << "len " << len;
    EXPECT_TRUE(r.header.masked) << "client frames must always be masked";
    EXPECT_EQ(r.header.mask_key, key);
    EXPECT_EQ(r.header.payload_length, len);

    std::vector<std::byte> got(r.payload, r.payload + len);
    ws_mask(got.data(), got.size(), r.header.mask_key);
    EXPECT_EQ(got, payload) << "len " << len;
  }
}

TEST(WsFrame, EncodeFrameRefusesUndersizedBuffer) {
  std::byte buf[8];
  const char payload[] = "0123456789";
  EXPECT_EQ(ws_encode_frame(buf, sizeof(buf), WsOpcode::kText, true, payload, 10,
                            0x11223344),
            0u);
}

TEST(WsFrame, RandomMaskKeysVary) {
  // A constant key would still round-trip, so the round-trip tests cannot
  // catch a broken generator.
  std::vector<std::uint32_t> keys;
  for (int i = 0; i < 64; ++i) {
    keys.push_back(ws_random_mask_key());
  }
  std::sort(keys.begin(), keys.end());
  keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
  EXPECT_GT(keys.size(), 60u) << "mask key generator is not varying";
}

// ---------------------------------------------------------------------------
// Close frames
// ---------------------------------------------------------------------------

TEST(WsClose, BuildAndParseRoundTrip) {
  std::byte buf[kMaxControlPayload];
  const std::size_t n = ws_build_close_payload(buf, sizeof(buf),
                                               WsCloseCode::kGoingAway, "bye");
  ASSERT_EQ(n, 5u);

  const auto info = ws_parse_close_payload(buf, n);
  ASSERT_TRUE(info.valid);
  EXPECT_EQ(info.code, WsCloseCode::kGoingAway);
  EXPECT_EQ(info.reason, "bye");
}

TEST(WsClose, EmptyPayloadMeansNoStatus) {
  const auto info = ws_parse_close_payload(nullptr, 0);
  EXPECT_TRUE(info.valid);
  EXPECT_EQ(info.code, WsCloseCode::kNoStatusReceived);
  EXPECT_TRUE(info.reason.empty());
}

TEST(WsClose, RejectsSingleBytePayload) {
  const auto b = bytes({0x03});
  EXPECT_FALSE(ws_parse_close_payload(b.data(), b.size()).valid);
}

TEST(WsClose, RejectsReservedAndUndefinedCodes) {
  // 1005/1006/1015 are local-only signals; below 1000 and 1016-2999 are
  // undefined. A peer sending one is misbehaving.
  for (int code : {0, 999, 1004, 1005, 1006, 1016, 2000, 2999}) {
    const auto b = bytes({(code >> 8) & 0xFF, code & 0xFF});
    EXPECT_FALSE(ws_parse_close_payload(b.data(), b.size()).valid)
        << "code " << code;
  }
}

TEST(WsClose, AcceptsDefinedAndApplicationCodes) {
  for (int code : {1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 3000,
                   4000, 4999}) {
    const auto b = bytes({(code >> 8) & 0xFF, code & 0xFF});
    const auto info = ws_parse_close_payload(b.data(), b.size());
    EXPECT_TRUE(info.valid) << "code " << code;
    EXPECT_EQ(static_cast<int>(info.code), code);
  }
}

TEST(WsClose, RejectsInvalidUtf8Reason) {
  const auto b = bytes({0x03, 0xE8, 0xFF, 0xFE});
  EXPECT_FALSE(ws_parse_close_payload(b.data(), b.size()).valid);
}

TEST(WsClose, BuildRefusesOversizedReason) {
  std::byte buf[kMaxControlPayload];
  const std::string too_long(124, 'x');
  EXPECT_EQ(ws_build_close_payload(buf, sizeof(buf), WsCloseCode::kNormal,
                                   too_long),
            0u);
  const std::string just_fits(123, 'x');
  EXPECT_EQ(ws_build_close_payload(buf, sizeof(buf), WsCloseCode::kNormal,
                                   just_fits),
            125u);
}

// ---------------------------------------------------------------------------
// UTF-8 validation
// ---------------------------------------------------------------------------

TEST(WsUtf8, AcceptsValidSequences) {
  const char* valid[] = {
      "",
      "hello",
      "\xC3\xA9",                  // e-acute, 2 bytes
      "\xE2\x82\xAC",              // euro sign, 3 bytes
      "\xF0\x9F\x98\x80",          // emoji, 4 bytes
      "\xEF\xBB\xBF",              // BOM
      "\xED\x9F\xBF",              // U+D7FF, just below the surrogate range
      "\xEE\x80\x80",              // U+E000, just above it
      "\xF4\x8F\xBF\xBF",          // U+10FFFF, the maximum
      "mixed \xC3\xA9 and \xE2\x82\xAC",
  };
  for (const char* s : valid) {
    EXPECT_TRUE(ws_is_valid_utf8(reinterpret_cast<const std::byte*>(s),
                                 std::strlen(s)))
        << "should accept: " << s;
  }
}

TEST(WsUtf8, RejectsOverlongEncodings) {
  // These decode to valid code points but use more bytes than necessary --
  // historically a way to smuggle '/' or NUL past a naive validator.
  const auto overlong_nul = bytes({0xC0, 0x80});
  EXPECT_FALSE(ws_is_valid_utf8(overlong_nul.data(), overlong_nul.size()));

  const auto overlong_slash = bytes({0xC0, 0xAF});
  EXPECT_FALSE(ws_is_valid_utf8(overlong_slash.data(), overlong_slash.size()));

  const auto overlong_3 = bytes({0xE0, 0x80, 0x80});
  EXPECT_FALSE(ws_is_valid_utf8(overlong_3.data(), overlong_3.size()));

  const auto overlong_4 = bytes({0xF0, 0x80, 0x80, 0x80});
  EXPECT_FALSE(ws_is_valid_utf8(overlong_4.data(), overlong_4.size()));
}

TEST(WsUtf8, RejectsSurrogates) {
  // U+D800 .. U+DFFF are reserved for UTF-16 and are not valid UTF-8.
  const auto low = bytes({0xED, 0xA0, 0x80});   // U+D800
  EXPECT_FALSE(ws_is_valid_utf8(low.data(), low.size()));
  const auto high = bytes({0xED, 0xBF, 0xBF});  // U+DFFF
  EXPECT_FALSE(ws_is_valid_utf8(high.data(), high.size()));
}

TEST(WsUtf8, RejectsOutOfRange) {
  const auto beyond = bytes({0xF4, 0x90, 0x80, 0x80});  // U+110000
  EXPECT_FALSE(ws_is_valid_utf8(beyond.data(), beyond.size()));
  const auto five_byte = bytes({0xF8, 0x88, 0x80, 0x80, 0x80});
  EXPECT_FALSE(ws_is_valid_utf8(five_byte.data(), five_byte.size()));
}

TEST(WsUtf8, RejectsTruncatedAndStrayContinuations) {
  const auto truncated = bytes({0xE2, 0x82});
  EXPECT_FALSE(ws_is_valid_utf8(truncated.data(), truncated.size()));

  const auto stray = bytes({0x80});
  EXPECT_FALSE(ws_is_valid_utf8(stray.data(), stray.size()));

  const auto bad_continuation = bytes({0xC3, 0x28});
  EXPECT_FALSE(ws_is_valid_utf8(bad_continuation.data(),
                                bad_continuation.size()));
}

TEST(WsUtf8, AsciiFastPathNeverSkipsValidation) {
  // The validator tests eight bytes at a time and only falls back to per-byte
  // decoding when a high bit appears. An invalid sequence must be caught no
  // matter where it lands relative to that 8-byte stride -- in particular when
  // it straddles a word boundary, which is the case a naive fast path misses.
  const auto invalid_sequences = {
      bytes({0xC0, 0x80}),              // overlong NUL
      bytes({0xED, 0xA0, 0x80}),        // surrogate
      bytes({0xF4, 0x90, 0x80, 0x80}),  // beyond U+10FFFF
      bytes({0x80}),                    // stray continuation
      bytes({0xE2, 0x82}),              // truncated
  };

  for (const auto& bad : invalid_sequences) {
    for (std::size_t prefix = 0; prefix < 24; ++prefix) {
      std::vector<std::byte> buf(prefix, std::byte{'a'});
      buf.insert(buf.end(), bad.begin(), bad.end());
      EXPECT_FALSE(ws_is_valid_utf8(buf.data(), buf.size()))
          << "invalid sequence at ASCII offset " << prefix;

      // And with ASCII trailing it too, so the bad bytes sit inside a word
      // rather than at the end of the buffer.
      buf.insert(buf.end(), 16, std::byte{'z'});
      EXPECT_FALSE(ws_is_valid_utf8(buf.data(), buf.size()))
          << "invalid sequence embedded at offset " << prefix;
    }
  }
}

TEST(WsUtf8, AsciiFastPathAcceptsValidMultiByteAtEveryOffset) {
  // The mirror case: a legitimate multi-byte sequence must survive the fast
  // path wherever it falls, including straddling the 8-byte stride.
  const auto euro = bytes({0xE2, 0x82, 0xAC});
  const auto emoji = bytes({0xF0, 0x9F, 0x98, 0x80});

  for (const auto& seq : {euro, emoji}) {
    for (std::size_t prefix = 0; prefix < 24; ++prefix) {
      std::vector<std::byte> buf(prefix, std::byte{'a'});
      buf.insert(buf.end(), seq.begin(), seq.end());
      buf.insert(buf.end(), 16, std::byte{'z'});
      EXPECT_TRUE(ws_is_valid_utf8(buf.data(), buf.size()))
          << "valid sequence at offset " << prefix;
    }
  }
}

TEST(WsUtf8, LongAsciiPayloadIsAccepted) {
  // The path that actually runs in production: a few hundred bytes of JSON.
  const std::string json(1024, 'x');
  EXPECT_TRUE(ws_is_valid_utf8(reinterpret_cast<const std::byte*>(json.data()),
                               json.size()));

  // One bad byte anywhere in it must still be caught.
  for (std::size_t pos = 0; pos < json.size(); pos += 7) {
    std::string corrupted = json;
    corrupted[pos] = static_cast<char>(0xFF);
    EXPECT_FALSE(ws_is_valid_utf8(
        reinterpret_cast<const std::byte*>(corrupted.data()), corrupted.size()))
        << "corruption at " << pos;
  }
}

TEST(WsUtf8, ValidatorNeverReadsPastTheBuffer) {
  // Every prefix of a multi-byte sequence must be rejected as truncated, not
  // read off the end looking for continuation bytes.
  const auto full = bytes({'a', 0xF0, 0x9F, 0x98, 0x80, 'b'});
  for (std::size_t n = 0; n <= full.size(); ++n) {
    std::vector<std::byte> exact(full.begin(), full.begin() + static_cast<long>(n));
    const bool ok = ws_is_valid_utf8(exact.data(), exact.size());
    const bool should_be_ok = (n == 0 || n == 1 || n == 5 || n == 6);
    EXPECT_EQ(ok, should_be_ok) << "prefix length " << n;
  }
}
