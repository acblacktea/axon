#include "axon/core/object_pool.h"

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <vector>

using axon::core::ObjectPool;

namespace {

struct Thing {
  int a = 0;
  double b = 0.0;
  std::string s;
};

}  // namespace

TEST(ObjectPool, StartsFull) {
  ObjectPool<Thing> pool(16);
  EXPECT_EQ(pool.capacity(), 16u);
  EXPECT_EQ(pool.available(), 16u);
  EXPECT_EQ(pool.in_use(), 0u);
  EXPECT_EQ(pool.high_water(), 0u);
  EXPECT_EQ(pool.exhaustion_count(), 0u);
}

TEST(ObjectPool, AcquireAndRelease) {
  ObjectPool<Thing> pool(4);

  Thing* a = pool.acquire();
  ASSERT_NE(a, nullptr);
  EXPECT_EQ(pool.in_use(), 1u);
  EXPECT_EQ(pool.available(), 3u);

  pool.release(a);
  EXPECT_EQ(pool.in_use(), 0u);
  EXPECT_EQ(pool.available(), 4u);
}

TEST(ObjectPool, HandsOutDistinctObjects) {
  ObjectPool<Thing> pool(64);
  std::set<Thing*> seen;
  for (int i = 0; i < 64; ++i) {
    Thing* p = pool.acquire();
    ASSERT_NE(p, nullptr);
    EXPECT_TRUE(seen.insert(p).second) << "pool handed out the same object twice";
  }
  EXPECT_EQ(seen.size(), 64u);
}

TEST(ObjectPool, ExhaustsRatherThanGrows) {
  // Growing would defer a malloc to the worst possible moment. Returning
  // nullptr keeps the failure visible and bounded.
  ObjectPool<Thing> pool(2);
  ASSERT_NE(pool.acquire(), nullptr);
  ASSERT_NE(pool.acquire(), nullptr);

  EXPECT_EQ(pool.acquire(), nullptr);
  EXPECT_EQ(pool.exhaustion_count(), 1u);
  EXPECT_EQ(pool.acquire(), nullptr);
  EXPECT_EQ(pool.exhaustion_count(), 2u);
  EXPECT_EQ(pool.capacity(), 2u);
}

TEST(ObjectPool, AcquireResetsStaleState) {
  // The bug this prevents: a recycled order still carrying the previous
  // strategy's id or a non-zero filled_amount.
  ObjectPool<Thing> pool(1);

  Thing* first = pool.acquire();
  ASSERT_NE(first, nullptr);
  first->a = 12345;
  first->b = 6.25;
  first->s = "stale-strategy-id";
  pool.release(first);

  Thing* second = pool.acquire();
  ASSERT_EQ(second, first) << "expected the same slot back";
  EXPECT_EQ(second->a, 0);
  EXPECT_DOUBLE_EQ(second->b, 0.0);
  EXPECT_TRUE(second->s.empty());
}

TEST(ObjectPool, AcquireDirtySkipsTheReset) {
  ObjectPool<Thing> pool(1);
  Thing* first = pool.acquire();
  first->a = 999;
  pool.release(first);

  Thing* second = pool.acquire_dirty();
  ASSERT_EQ(second, first);
  EXPECT_EQ(second->a, 999) << "acquire_dirty must not pay for a reset";
}

TEST(ObjectPool, TracksHighWaterMark) {
  ObjectPool<Thing> pool(10);
  std::vector<Thing*> held;
  for (int i = 0; i < 7; ++i) {
    held.push_back(pool.acquire());
  }
  EXPECT_EQ(pool.high_water(), 7u);

  for (Thing* p : held) {
    pool.release(p);
  }
  EXPECT_EQ(pool.in_use(), 0u);
  EXPECT_EQ(pool.high_water(), 7u) << "high water must not decay";

  Thing* one = pool.acquire();
  EXPECT_EQ(pool.high_water(), 7u);
  pool.release(one);
}

TEST(ObjectPool, ReleaseOfNullIsHarmless) {
  ObjectPool<Thing> pool(4);
  pool.release(nullptr);
  EXPECT_EQ(pool.available(), 4u);
}

TEST(ObjectPool, ReleaseOfForeignPointerIsIgnored) {
  // Corrupting the free list would be far worse than dropping the release.
  ObjectPool<Thing> pool(4);
  Thing outsider;
  pool.release(&outsider);
  EXPECT_EQ(pool.available(), 4u);

  for (int i = 0; i < 4; ++i) {
    EXPECT_NE(pool.acquire(), nullptr);
  }
  EXPECT_EQ(pool.acquire(), nullptr);
}

TEST(ObjectPool, ChurnStaysConsistent) {
  ObjectPool<Thing> pool(32);
  std::vector<Thing*> held;

  for (int round = 0; round < 10000; ++round) {
    while (held.size() < 32) {
      Thing* p = pool.acquire();
      if (p == nullptr) {
        break;
      }
      p->a = round;
      held.push_back(p);
    }
    ASSERT_EQ(pool.in_use(), held.size());

    const std::size_t to_free = held.size() / 2;
    for (std::size_t i = 0; i < to_free; ++i) {
      pool.release(held.back());
      held.pop_back();
    }
    ASSERT_EQ(pool.in_use(), held.size());
  }

  for (Thing* p : held) {
    pool.release(p);
  }
  EXPECT_EQ(pool.available(), 32u);
  EXPECT_EQ(pool.exhaustion_count(), 0u);
}

TEST(ObjectPool, ZeroCapacityIsUsableAndAlwaysEmpty) {
  ObjectPool<Thing> pool(0);
  EXPECT_EQ(pool.capacity(), 0u);
  EXPECT_EQ(pool.acquire(), nullptr);
  EXPECT_EQ(pool.exhaustion_count(), 1u);
}
