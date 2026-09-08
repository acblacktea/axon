#include "axon/core/shm_ring.h"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

#include "axon/core/platform.h"

using axon::core::ShmRing;
using axon::core::ShmRingError;

namespace {

struct Msg {
  std::uint64_t seq;
  std::uint64_t checksum;
  char tag[24];
};

std::string temp_path(const char* name) {
  // Deliberately not /dev/shm: this must run on macOS too. In production on
  // Linux the ring belongs on /dev/shm so the kernel never tries to write
  // ring pages back to a disk.
  return std::string("/tmp/axon_shm_test_") + name + "_" +
         std::to_string(::getpid());
}

class ShmRingTest : public ::testing::Test {
 protected:
  void TearDown() override {
    for (const auto& p : paths_) {
      ::unlink(p.c_str());
    }
  }
  std::string track(std::string p) {
    paths_.push_back(p);
    return p;
  }
  std::vector<std::string> paths_;
};

}  // namespace

TEST_F(ShmRingTest, CreateAndInspect) {
  const auto path = track(temp_path("create"));
  ShmRing ring = ShmRing::create(path, sizeof(Msg), 64);

  EXPECT_TRUE(ring.valid());
  EXPECT_EQ(ring.slot_count(), 64u);
  EXPECT_EQ(ring.max_payload(), sizeof(Msg));
  EXPECT_TRUE(ring.empty());
  EXPECT_EQ(ring.push_failures(), 0u);
  EXPECT_EQ(ring.path(), path);
}

TEST_F(ShmRingTest, RejectsBadGeometry) {
  const auto path = track(temp_path("geometry"));
  EXPECT_THROW(ShmRing::create(path, sizeof(Msg), 63), ShmRingError);
  EXPECT_THROW(ShmRing::create(path, sizeof(Msg), 0), ShmRingError);
  EXPECT_THROW(ShmRing::create(path, sizeof(Msg), 1), ShmRingError);
  EXPECT_THROW(ShmRing::create(path, 0, 64), ShmRingError);
}

TEST_F(ShmRingTest, OpenRejectsMissingAndCorruptFiles) {
  EXPECT_THROW(ShmRing::open("/tmp/axon_definitely_missing_ring"), ShmRingError);

  const auto path = track(temp_path("corrupt"));
  {
    std::FILE* f = std::fopen(path.c_str(), "wb");
    ASSERT_NE(f, nullptr);
    const char junk[512] = {};
    std::fwrite(junk, 1, sizeof(junk), f);
    std::fclose(f);
  }
  EXPECT_THROW(ShmRing::open(path), ShmRingError);
}

TEST_F(ShmRingTest, SingleProcessRoundTrip) {
  const auto path = track(temp_path("roundtrip"));
  ShmRing ring = ShmRing::create(path, sizeof(Msg), 8);

  Msg sent{};
  sent.seq = 99;
  sent.checksum = 12345;
  std::snprintf(sent.tag, sizeof(sent.tag), "hello");
  ASSERT_TRUE(ring.try_push(&sent, sizeof(sent)));
  EXPECT_EQ(ring.size(), 1u);

  Msg got{};
  std::uint32_t len = 0;
  ASSERT_TRUE(ring.try_pop(&got, sizeof(got), len));
  EXPECT_EQ(len, sizeof(Msg));
  EXPECT_EQ(got.seq, 99u);
  EXPECT_EQ(got.checksum, 12345u);
  EXPECT_STREQ(got.tag, "hello");
  EXPECT_TRUE(ring.empty());
}

TEST_F(ShmRingTest, RejectsOversizedPayload) {
  const auto path = track(temp_path("oversize"));
  ShmRing ring = ShmRing::create(path, 16, 8);

  char big[64] = {};
  EXPECT_FALSE(ring.try_push(big, sizeof(big)));
  EXPECT_EQ(ring.push_failures(), 1u);
  EXPECT_TRUE(ring.empty());
}

TEST_F(ShmRingTest, FullRingRefusesAndCounts) {
  const auto path = track(temp_path("full"));
  ShmRing ring = ShmRing::create(path, sizeof(std::uint64_t), 4);

  for (std::uint64_t i = 0; i < 4; ++i) {
    ASSERT_TRUE(ring.try_push(&i, sizeof(i)));
  }
  std::uint64_t extra = 99;
  EXPECT_FALSE(ring.try_push(&extra, sizeof(extra)));
  EXPECT_EQ(ring.push_failures(), 1u);
  EXPECT_EQ(ring.size(), 4u);
}

TEST_F(ShmRingTest, UndersizedReadBufferLeavesMessageInPlace) {
  // Losing an order because the consumer's buffer was a byte too small would
  // be catastrophic and silent. It must stall instead.
  const auto path = track(temp_path("undersized"));
  ShmRing ring = ShmRing::create(path, sizeof(Msg), 8);

  Msg sent{};
  sent.seq = 5;
  ASSERT_TRUE(ring.try_push(&sent, sizeof(sent)));

  char small[4];
  std::uint32_t len = 0;
  EXPECT_FALSE(ring.try_pop(small, sizeof(small), len));
  EXPECT_EQ(len, sizeof(Msg));
  EXPECT_EQ(ring.size(), 1u) << "message must not be consumed";

  Msg got{};
  EXPECT_TRUE(ring.try_pop(&got, sizeof(got), len));
  EXPECT_EQ(got.seq, 5u);
}

TEST_F(ShmRingTest, VariableLengthMessages) {
  const auto path = track(temp_path("varlen"));
  ShmRing ring = ShmRing::create(path, 128, 16);

  for (std::uint32_t n = 1; n <= 16; ++n) {
    std::vector<char> payload(n, static_cast<char>('a' + (n % 26)));
    ASSERT_TRUE(ring.try_push(payload.data(), n));
  }
  for (std::uint32_t n = 1; n <= 16; ++n) {
    char buf[128] = {};
    std::uint32_t len = 0;
    ASSERT_TRUE(ring.try_pop(buf, sizeof(buf), len));
    EXPECT_EQ(len, n);
    for (std::uint32_t i = 0; i < n; ++i) {
      ASSERT_EQ(buf[i], static_cast<char>('a' + (n % 26)));
    }
  }
}

TEST_F(ShmRingTest, MoveTransfersOwnership) {
  const auto path = track(temp_path("move"));
  ShmRing a = ShmRing::create(path, sizeof(std::uint64_t), 8);
  std::uint64_t v = 7;
  ASSERT_TRUE(a.try_push(&v, sizeof(v)));

  ShmRing b = std::move(a);
  EXPECT_TRUE(b.valid());
  EXPECT_FALSE(a.valid());  // NOLINT(bugprone-use-after-move) -- that is the point
  EXPECT_EQ(b.size(), 1u);

  std::uint64_t got = 0;
  std::uint32_t len = 0;
  EXPECT_TRUE(b.try_pop(&got, sizeof(got), len));
  EXPECT_EQ(got, 7u);
}

TEST_F(ShmRingTest, TwoHandlesInOneProcessShareState) {
  const auto path = track(temp_path("twohandles"));
  ShmRing writer = ShmRing::create(path, sizeof(std::uint64_t), 8);
  ShmRing reader = ShmRing::open(path);

  EXPECT_EQ(reader.slot_count(), 8u);
  EXPECT_EQ(reader.max_payload(), sizeof(std::uint64_t));

  std::uint64_t v = 4242;
  ASSERT_TRUE(writer.try_push(&v, sizeof(v)));

  std::uint64_t got = 0;
  std::uint32_t len = 0;
  ASSERT_TRUE(reader.try_pop(&got, sizeof(got), len));
  EXPECT_EQ(got, 4242u);
}

TEST_F(ShmRingTest, CreateTruncatesAStaleRing) {
  // A crashed run must not leave cursors that a restart would inherit.
  const auto path = track(temp_path("stale"));
  {
    ShmRing old = ShmRing::create(path, sizeof(std::uint64_t), 8);
    std::uint64_t v = 1;
    ASSERT_TRUE(old.try_push(&v, sizeof(v)));
    ASSERT_EQ(old.size(), 1u);
  }
  ShmRing fresh = ShmRing::create(path, sizeof(std::uint64_t), 8);
  EXPECT_TRUE(fresh.empty());
  EXPECT_EQ(fresh.push_failures(), 0u);
}

TEST_F(ShmRingTest, UnlinkOnDestroyRemovesBackingFile) {
  const auto path = temp_path("unlink");
  {
    ShmRing ring = ShmRing::create(path, sizeof(std::uint64_t), 8);
    ring.set_unlink_on_destroy(true);
    EXPECT_EQ(::access(path.c_str(), F_OK), 0);
  }
  EXPECT_NE(::access(path.c_str(), F_OK), 0);
}

TEST_F(ShmRingTest, TwoThreadsPreserveEveryMessageInOrder) {
  const auto path = track(temp_path("threads"));
  constexpr std::uint64_t kItems = 500'000;

  ShmRing producer_ring = ShmRing::create(path, sizeof(Msg), 1024);
  ShmRing consumer_ring = ShmRing::open(path);

  std::atomic<bool> done{false};

  std::thread producer([&] {
    for (std::uint64_t i = 0; i < kItems; ++i) {
      Msg m{};
      m.seq = i;
      m.checksum = i * 11400714819323198485ULL;
      while (!producer_ring.try_push(&m, sizeof(m))) {
        axon::core::cpu_pause();
      }
    }
    done.store(true, std::memory_order_release);
  });

  std::uint64_t received = 0;
  std::uint64_t errors = 0;
  std::thread consumer([&] {
    Msg m{};
    std::uint32_t len = 0;
    while (received < kItems) {
      if (consumer_ring.try_pop(&m, sizeof(m), len)) {
        if (m.seq != received || m.checksum != received * 11400714819323198485ULL) {
          ++errors;
        }
        ++received;
      } else if (done.load(std::memory_order_acquire) && consumer_ring.empty()) {
        break;
      } else {
        axon::core::cpu_pause();
      }
    }
  });

  producer.join();
  consumer.join();

  EXPECT_EQ(received, kItems);
  EXPECT_EQ(errors, 0u);
}

// ---------------------------------------------------------------------------
// The real thing: two separate PROCESSES sharing the ring. This is what the
// component exists for, and it is the only way to prove the atomics really are
// address-free and that nothing depends on a shared address space.
// ---------------------------------------------------------------------------
TEST_F(ShmRingTest, TwoProcessesExchangeMessages) {
  const auto path = track(temp_path("procs"));
  constexpr std::uint64_t kItems = 200'000;

  ShmRing ring = ShmRing::create(path, sizeof(Msg), 1024);

  const pid_t pid = ::fork();
  ASSERT_GE(pid, 0) << "fork failed";

  if (pid == 0) {
    // Child: producer. _exit rather than exit so no gtest teardown runs here.
    int status = 0;
    try {
      ShmRing child_ring = ShmRing::open(path);
      for (std::uint64_t i = 0; i < kItems; ++i) {
        Msg m{};
        m.seq = i;
        m.checksum = i * 11400714819323198485ULL;
        int spins = 0;
        while (!child_ring.try_push(&m, sizeof(m))) {
          axon::core::cpu_pause();
          if (++spins > 100'000'000) {
            status = 2;  // consumer wedged
            break;
          }
        }
        if (status != 0) {
          break;
        }
      }
    } catch (...) {
      status = 1;
    }
    ::_exit(status);
  }

  std::uint64_t received = 0;
  std::uint64_t errors = 0;
  Msg m{};
  std::uint32_t len = 0;
  std::uint64_t idle_spins = 0;

  while (received < kItems) {
    if (ring.try_pop(&m, sizeof(m), len)) {
      idle_spins = 0;
      if (len != sizeof(Msg) || m.seq != received ||
          m.checksum != received * 11400714819323198485ULL) {
        ++errors;
      }
      ++received;
    } else {
      axon::core::cpu_pause();
      if (++idle_spins > 500'000'000) {
        break;  // producer died; fail below rather than hang the suite
      }
    }
  }

  int status = 0;
  ASSERT_EQ(::waitpid(pid, &status, 0), pid);
  EXPECT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0) << "child producer reported failure";

  EXPECT_EQ(received, kItems);
  EXPECT_EQ(errors, 0u);
}
