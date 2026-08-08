#include "calais/net/byte_buffer.h"

#include <gtest/gtest.h>

#include <cstring>
#include <string>
#include <vector>

using calais::net::ByteBuffer;

namespace {

void write_str(ByteBuffer& b, std::string_view s) {
  ASSERT_GE(b.writable_size(), s.size());
  std::memcpy(b.writable(), s.data(), s.size());
  b.commit(s.size());
}

std::string read_str(const ByteBuffer& b, std::size_t n) {
  return std::string(reinterpret_cast<const char*>(b.readable()), n);
}

}  // namespace

TEST(ByteBuffer, StartsEmpty) {
  ByteBuffer b(1024);
  EXPECT_TRUE(b.valid());
  EXPECT_EQ(b.capacity(), 1024u);
  EXPECT_TRUE(b.empty());
  EXPECT_EQ(b.readable_size(), 0u);
  EXPECT_EQ(b.writable_size(), 1024u);
  EXPECT_EQ(b.compactions(), 0u);
}

TEST(ByteBuffer, DefaultConstructedIsInvalid) {
  ByteBuffer b;
  EXPECT_FALSE(b.valid());
  EXPECT_EQ(b.capacity(), 0u);
  EXPECT_EQ(b.writable_size(), 0u);
}

TEST(ByteBuffer, WriteThenRead) {
  ByteBuffer b(64);
  write_str(b, "hello");

  EXPECT_EQ(b.readable_size(), 5u);
  EXPECT_EQ(b.writable_size(), 59u);
  EXPECT_EQ(read_str(b, 5), "hello");

  b.consume(5);
  EXPECT_TRUE(b.empty());
}

TEST(ByteBuffer, PartialConsumeAdvancesTheReadCursor) {
  ByteBuffer b(64);
  write_str(b, "hello world");

  b.consume(6);
  EXPECT_EQ(b.readable_size(), 5u);
  EXPECT_EQ(read_str(b, 5), "world");
}

TEST(ByteBuffer, DrainingFullyResetsBothCursors) {
  // The common case on an idle connection: after the last message is consumed
  // the whole capacity must be available again without a memmove.
  ByteBuffer b(64);
  write_str(b, "abcdefgh");
  b.consume(8);

  EXPECT_TRUE(b.empty());
  EXPECT_EQ(b.writable_size(), 64u) << "cursors should have reset to zero";
  EXPECT_EQ(b.compactions(), 0u) << "draining must not require a copy";
}

TEST(ByteBuffer, CompactReclaimsConsumedSpace) {
  ByteBuffer b(16);
  write_str(b, "0123456789");
  b.consume(7);

  EXPECT_EQ(b.readable_size(), 3u);
  EXPECT_EQ(b.writable_size(), 6u);

  const std::size_t writable = b.compact();
  EXPECT_EQ(writable, 13u);
  EXPECT_EQ(b.readable_size(), 3u);
  EXPECT_EQ(read_str(b, 3), "789") << "unread bytes must survive compaction";
  EXPECT_EQ(b.compactions(), 1u);
}

TEST(ByteBuffer, CompactIsANoOpWhenNothingWasConsumed) {
  ByteBuffer b(32);
  write_str(b, "abc");
  b.compact();
  EXPECT_EQ(b.compactions(), 0u) << "nothing to move, so nothing to count";
  EXPECT_EQ(read_str(b, 3), "abc");
}

TEST(ByteBuffer, EnsureWritableCompactsOnlyWhenNeeded) {
  ByteBuffer b(16);
  write_str(b, "0123456789");
  b.consume(8);
  EXPECT_EQ(b.writable_size(), 6u);

  EXPECT_TRUE(b.ensure_writable(4));
  EXPECT_EQ(b.compactions(), 0u) << "6 bytes already free, no copy needed";

  EXPECT_TRUE(b.ensure_writable(10));
  EXPECT_EQ(b.compactions(), 1u);
  EXPECT_EQ(b.writable_size(), 14u);
  EXPECT_EQ(read_str(b, 2), "89");
}

TEST(ByteBuffer, EnsureWritableFailsWhenTheBufferIsTrulyTooSmall) {
  // This must be distinguishable from "needs a compaction": a message larger
  // than the whole buffer can never be received, and the connection has to
  // fail rather than silently truncate.
  ByteBuffer b(16);
  write_str(b, "0123456789");

  EXPECT_FALSE(b.ensure_writable(100));
  EXPECT_EQ(b.readable_size(), 10u) << "failure must not destroy buffered data";
  EXPECT_EQ(read_str(b, 10), "0123456789");
}

TEST(ByteBuffer, StreamingWithRepeatedCompaction) {
  // Simulates a socket loop: fill, parse whole messages, leave a partial one,
  // compact, repeat. Nothing may be lost or duplicated across the boundary.
  ByteBuffer b(64);
  std::string produced;
  std::string consumed;

  int counter = 0;
  for (int round = 0; round < 500; ++round) {
    while (b.writable_size() >= 7) {
      char msg[8];
      std::snprintf(msg, sizeof(msg), "m%05d", counter++);
      std::memcpy(b.writable(), msg, 6);
      b.commit(6);
      produced.append(msg, 6);
    }

    // Consume all but a deliberate partial tail.
    while (b.readable_size() >= 6 + 3) {
      consumed.append(reinterpret_cast<const char*>(b.readable()), 6);
      b.consume(6);
    }
    b.compact();
  }

  while (b.readable_size() >= 6) {
    consumed.append(reinterpret_cast<const char*>(b.readable()), 6);
    b.consume(6);
  }

  EXPECT_EQ(consumed, produced.substr(0, consumed.size()));
  EXPECT_GT(b.compactions(), 0u);
}

TEST(ByteBuffer, ReadableIsStableAcrossWritesWithoutCompaction) {
  // The parser holds a pointer into the buffer while more data is appended.
  // That pointer must stay valid until compact() is called -- this is exactly
  // what makes zero-copy parsing safe.
  ByteBuffer b(64);
  write_str(b, "first");
  const std::byte* p = b.readable();

  write_str(b, "second");
  EXPECT_EQ(b.readable(), p);
  EXPECT_EQ(read_str(b, 11), "firstsecond");
}

TEST(ByteBuffer, IsCacheLineAligned) {
  // The receive buffer is a DMA target; sharing its first line with anything
  // else would cost a coherency miss on every read.
  for (std::size_t cap : {64u, 1024u, 65536u}) {
    ByteBuffer b(cap);
    EXPECT_EQ(reinterpret_cast<std::uintptr_t>(b.writable()) %
                  calais::core::kCacheLineSize,
              0u)
        << "capacity " << cap;
  }
}

TEST(ByteBuffer, Clear) {
  ByteBuffer b(32);
  write_str(b, "abc");
  b.clear();
  EXPECT_TRUE(b.empty());
  EXPECT_EQ(b.writable_size(), 32u);
}

TEST(ByteBuffer, MoveTransfersOwnership) {
  ByteBuffer a(32);
  write_str(a, "hello");

  ByteBuffer b = std::move(a);
  EXPECT_TRUE(b.valid());
  EXPECT_FALSE(a.valid());  // NOLINT(bugprone-use-after-move) -- the point
  EXPECT_EQ(b.readable_size(), 5u);
  EXPECT_EQ(read_str(b, 5), "hello");
}

TEST(ByteBuffer, MoveAssignmentReleasesTheOldBuffer) {
  ByteBuffer a(32);
  write_str(a, "aaa");
  ByteBuffer b(64);
  write_str(b, "bbbb");

  b = std::move(a);
  EXPECT_EQ(b.capacity(), 32u);
  EXPECT_EQ(b.readable_size(), 3u);
  EXPECT_EQ(read_str(b, 3), "aaa");
}

TEST(ByteBuffer, FillToCapacity) {
  ByteBuffer b(16);
  const std::string full(16, 'x');
  write_str(b, full);

  EXPECT_EQ(b.writable_size(), 0u);
  EXPECT_EQ(b.readable_size(), 16u);
  EXPECT_FALSE(b.ensure_writable(1));

  b.consume(16);
  EXPECT_EQ(b.writable_size(), 16u);
}
