#include "axon/core/spsc_ring.h"

#include <gtest/gtest.h>

#include <atomic>
#include <cstdint>
#include <thread>
#include <vector>

using axon::core::kCacheLineSize;
using axon::core::SpscRing;

namespace {

struct Payload {
  std::uint64_t seq;
  std::uint64_t checksum;
  char tag[16];
};

}  // namespace

TEST(SpscRing, StartsEmpty) {
  SpscRing<int, 8> ring;
  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(ring.size(), 0u);
  EXPECT_EQ(ring.capacity(), 8u);

  int out = 0;
  EXPECT_FALSE(ring.try_pop(out));
}

TEST(SpscRing, PushPopFifo) {
  SpscRing<int, 8> ring;
  for (int i = 0; i < 8; ++i) {
    ASSERT_TRUE(ring.try_push(i));
  }
  EXPECT_EQ(ring.size(), 8u);

  for (int i = 0; i < 8; ++i) {
    int out = -1;
    ASSERT_TRUE(ring.try_pop(out));
    EXPECT_EQ(out, i);
  }
  EXPECT_TRUE(ring.empty());
}

TEST(SpscRing, RejectsPushWhenFull) {
  SpscRing<int, 4> ring;
  for (int i = 0; i < 4; ++i) {
    ASSERT_TRUE(ring.try_push(i));
  }
  EXPECT_FALSE(ring.try_push(99));
  EXPECT_EQ(ring.size(), 4u);

  // Making room lets exactly one more in.
  int out = 0;
  ASSERT_TRUE(ring.try_pop(out));
  EXPECT_TRUE(ring.try_push(99));
  EXPECT_FALSE(ring.try_push(100));
}

TEST(SpscRing, WrapsAroundManyTimes) {
  SpscRing<int, 4> ring;
  for (int round = 0; round < 1000; ++round) {
    for (int i = 0; i < 3; ++i) {
      ASSERT_TRUE(ring.try_push(round * 10 + i));
    }
    for (int i = 0; i < 3; ++i) {
      int out = -1;
      ASSERT_TRUE(ring.try_pop(out));
      ASSERT_EQ(out, round * 10 + i);
    }
  }
}

TEST(SpscRing, ZeroCopyApi) {
  SpscRing<Payload, 8> ring;

  Payload* slot = ring.try_acquire_write();
  ASSERT_NE(slot, nullptr);
  slot->seq = 7;
  slot->checksum = 42;
  std::memcpy(slot->tag, "hello", 6);
  ring.commit_write();

  EXPECT_EQ(ring.size(), 1u);

  const Payload* read = ring.try_acquire_read();
  ASSERT_NE(read, nullptr);
  EXPECT_EQ(read->seq, 7u);
  EXPECT_EQ(read->checksum, 42u);
  EXPECT_STREQ(read->tag, "hello");
  ring.commit_read();

  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(ring.try_acquire_read(), nullptr);
}

TEST(SpscRing, AcquireWriteReturnsNullWhenFull) {
  SpscRing<int, 2> ring;
  ASSERT_NE(ring.try_acquire_write(), nullptr);
  ring.commit_write();
  ASSERT_NE(ring.try_acquire_write(), nullptr);
  ring.commit_write();
  EXPECT_EQ(ring.try_acquire_write(), nullptr);
}

TEST(SpscRing, CursorsAreOnSeparateCacheLines) {
  // False sharing between the two cursors would cost ~100ns per operation and
  // is invisible in a single-threaded test, so assert the layout directly.
  SpscRing<std::uint64_t, 16> ring;
  const auto base = reinterpret_cast<std::uintptr_t>(&ring);
  EXPECT_EQ(base % kCacheLineSize, 0u) << "ring must be cache-line aligned";
  EXPECT_GE(sizeof(ring), 3 * kCacheLineSize)
      << "cursors and storage should not share a line";
}

// ---------------------------------------------------------------------------
// The test that actually matters: a real producer and consumer on two threads,
// verifying not just that every item arrives but that none is duplicated,
// reordered, or corrupted.
// ---------------------------------------------------------------------------
TEST(SpscRing, ConcurrentProducerConsumerPreservesEveryItemInOrder) {
  constexpr std::uint64_t kItems = 2'000'000;
  SpscRing<Payload, 1024> ring;

  std::atomic<bool> producer_done{false};
  std::atomic<std::uint64_t> push_spins{0};

  std::thread producer([&] {
    for (std::uint64_t i = 0; i < kItems; ++i) {
      Payload p{};
      p.seq = i;
      p.checksum = i * 2654435761ULL;
      while (!ring.try_push(p)) {
        push_spins.fetch_add(1, std::memory_order_relaxed);
        axon::core::cpu_pause();
      }
    }
    producer_done.store(true, std::memory_order_release);
  });

  std::uint64_t received = 0;
  std::uint64_t corrupt = 0;
  std::uint64_t out_of_order = 0;

  std::thread consumer([&] {
    Payload p{};
    while (true) {
      if (ring.try_pop(p)) {
        if (p.seq != received) {
          ++out_of_order;
        }
        if (p.checksum != p.seq * 2654435761ULL) {
          ++corrupt;
        }
        ++received;
        if (received == kItems) {
          break;
        }
      } else if (producer_done.load(std::memory_order_acquire) && ring.empty()) {
        break;
      } else {
        axon::core::cpu_pause();
      }
    }
  });

  producer.join();
  consumer.join();

  EXPECT_EQ(received, kItems);
  EXPECT_EQ(out_of_order, 0u);
  EXPECT_EQ(corrupt, 0u);
  EXPECT_TRUE(ring.empty());
}

TEST(SpscRing, ConcurrentZeroCopyPathIsAlsoSafe) {
  constexpr std::uint64_t kItems = 500'000;
  SpscRing<Payload, 256> ring;
  std::atomic<bool> done{false};

  std::thread producer([&] {
    for (std::uint64_t i = 0; i < kItems; ++i) {
      Payload* slot = nullptr;
      while ((slot = ring.try_acquire_write()) == nullptr) {
        axon::core::cpu_pause();
      }
      slot->seq = i;
      slot->checksum = ~i;
      ring.commit_write();
    }
    done.store(true, std::memory_order_release);
  });

  std::uint64_t received = 0;
  std::uint64_t errors = 0;
  std::thread consumer([&] {
    while (received < kItems) {
      const Payload* p = ring.try_acquire_read();
      if (p == nullptr) {
        if (done.load(std::memory_order_acquire) && ring.empty()) {
          break;
        }
        axon::core::cpu_pause();
        continue;
      }
      if (p->seq != received || p->checksum != ~received) {
        ++errors;
      }
      ring.commit_read();
      ++received;
    }
  });

  producer.join();
  consumer.join();

  EXPECT_EQ(received, kItems);
  EXPECT_EQ(errors, 0u);
}
