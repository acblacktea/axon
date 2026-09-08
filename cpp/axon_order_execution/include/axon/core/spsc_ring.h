// Single-producer / single-consumer lock-free ring buffer, in-process.
//
// This is the only sanctioned way for a hot-path thread to hand work to
// another thread in this codebase. A mutex costs 2-10us whenever it actually
// contends (futex sleep + wake), which is more than the entire internal
// latency budget; this costs ~30-80ns and never sleeps.
//
// SPSC ONLY. Exactly one thread may push and exactly one (different) thread
// may pop. There is no runtime check for this -- violating it corrupts the
// buffer silently. If you need many producers, give each producer its own ring
// and have the consumer poll them round-robin; that stays lock-free and
// preserves per-producer ordering, which an MPMC queue would not.
//
// Two design details that matter for latency:
//
//   1. Producer and consumer cursors live on separate cache lines. Sharing one
//      line makes every push invalidate the consumer's copy and vice versa --
//      the classic false-sharing trap that costs ~100ns per operation.
//
//   2. Each side caches the other's cursor. The producer only re-reads the
//      consumer's cursor when its cached copy says the ring is full. In the
//      common (non-full) case the producer never touches the consumer's cache
//      line at all, so the two threads run without any coherency traffic
//      beyond the data itself.
//
// Cursors are free-running sequence numbers and are never wrapped; only the
// index derived from them is masked. Unsigned wraparound at 2^64 is well
// defined and the subtraction-based comparisons stay correct across it.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

#include "axon/core/platform.h"

namespace axon::core {

template <typename T, std::size_t Capacity>
class SpscRing {
  static_assert(Capacity >= 2, "Capacity must be at least 2");
  static_assert((Capacity & (Capacity - 1)) == 0,
                "Capacity must be a power of two so index masking is a single AND");
  static_assert(std::is_trivially_copyable_v<T>,
                "hot-path payloads must be trivially copyable; a type with a "
                "destructor or heap member does not belong in a lock-free ring");

 public:
  using value_type = T;

  static constexpr std::size_t kCapacity = Capacity;
  static constexpr std::size_t kMask = Capacity - 1;

  SpscRing() noexcept = default;
  SpscRing(const SpscRing&) = delete;
  SpscRing& operator=(const SpscRing&) = delete;

  // ---- producer side ----------------------------------------------------

  bool try_push(const T& value) noexcept {
    T* slot = try_acquire_write();
    if (AXON_UNLIKELY(slot == nullptr)) {
      return false;
    }
    *slot = value;
    commit_write();
    return true;
  }

  // Zero-copy push: write directly into the ring slot, then commit.
  //
  //   if (auto* s = ring.try_acquire_write()) {
  //     s->field = ...;            // construct in place, no temporary
  //     ring.commit_write();
  //   }
  //
  // The slot stays valid until commit_write(). Do not call try_acquire_write()
  // twice without committing in between.
  T* try_acquire_write() noexcept {
    const std::uint64_t tail = tail_.load(std::memory_order_relaxed);

    if (AXON_UNLIKELY(tail - cached_head_ >= Capacity)) {
      // Cached view says full -- take the cache miss and check for real.
      cached_head_ = head_.load(std::memory_order_acquire);
      if (tail - cached_head_ >= Capacity) {
        return nullptr;
      }
    }
    return &slots_[tail & kMask];
  }

  void commit_write() noexcept {
    // Release: everything written into the slot must be visible to the
    // consumer before it can observe the advanced cursor.
    tail_.store(tail_.load(std::memory_order_relaxed) + 1,
                std::memory_order_release);
  }

  // ---- consumer side ----------------------------------------------------

  bool try_pop(T& out) noexcept {
    const T* slot = try_acquire_read();
    if (AXON_UNLIKELY(slot == nullptr)) {
      return false;
    }
    out = *slot;
    commit_read();
    return true;
  }

  const T* try_acquire_read() noexcept {
    const std::uint64_t head = head_.load(std::memory_order_relaxed);

    if (AXON_UNLIKELY(head == cached_tail_)) {
      cached_tail_ = tail_.load(std::memory_order_acquire);
      if (head == cached_tail_) {
        return nullptr;
      }
    }
    return &slots_[head & kMask];
  }

  void commit_read() noexcept {
    head_.store(head_.load(std::memory_order_relaxed) + 1,
                std::memory_order_release);
  }

  // ---- observation ------------------------------------------------------
  // Safe to call from either side, but the result is a snapshot that may be
  // stale the instant it returns. For metrics and tests, not control flow.

  std::size_t size() const noexcept {
    const std::uint64_t tail = tail_.load(std::memory_order_acquire);
    const std::uint64_t head = head_.load(std::memory_order_acquire);
    return static_cast<std::size_t>(tail - head);
  }

  bool empty() const noexcept { return size() == 0; }

  static constexpr std::size_t capacity() noexcept { return Capacity; }

 private:
  // Producer-owned line: the tail cursor plus the producer's cached view of
  // the consumer's cursor.
  alignas(kCacheLineSize) std::atomic<std::uint64_t> tail_{0};
  std::uint64_t cached_head_{0};

  // Consumer-owned line.
  alignas(kCacheLineSize) std::atomic<std::uint64_t> head_{0};
  std::uint64_t cached_tail_{0};

  // Storage on its own line so the first slot never shares with a cursor.
  alignas(kCacheLineSize) T slots_[Capacity]{};
};

}  // namespace axon::core
