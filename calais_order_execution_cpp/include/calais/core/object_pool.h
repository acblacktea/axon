// Fixed-capacity object pool.
//
// The hot path must never call malloc. Not because malloc is slow on average
// -- glibc's fast bins are ~30ns -- but because its tail is unbounded: an
// arena lock, an mmap to grow the heap, or a page fault on first touch turns a
// 30ns allocation into tens of microseconds, and it will happen during the
// burst of activity right after a big print, which is exactly when you cannot
// afford it.
//
// Everything here is allocated once at startup and reused forever. Exhausting
// the pool returns nullptr rather than growing: a pool that grows under load
// has merely deferred the malloc to the worst possible moment. Size it for the
// worst case and alert on high-water mark.

#pragma once

#include <cstddef>
#include <cstdint>
#include <type_traits>
#include <vector>

#include "calais/core/platform.h"

namespace calais::core {

template <typename T>
class ObjectPool {
  static_assert(std::is_default_constructible_v<T>,
                "pooled objects are constructed up front at startup");

 public:
  static constexpr std::uint32_t kInvalidIndex = UINT32_MAX;

  explicit ObjectPool(std::uint32_t capacity)
      : storage_(capacity), next_(capacity), capacity_(capacity) {
    // Free list is an index chain in a side array rather than an intrusive
    // pointer inside T, so T needs no pool-specific fields and the chain
    // pointers stay off the object's cache lines.
    for (std::uint32_t i = 0; i + 1 < capacity; ++i) {
      next_[i] = i + 1;
    }
    if (capacity > 0) {
      next_[capacity - 1] = kInvalidIndex;
      free_head_ = 0;
    }
    available_ = capacity;
  }

  ObjectPool(const ObjectPool&) = delete;
  ObjectPool& operator=(const ObjectPool&) = delete;

  // Returns a value-initialised object, or nullptr if exhausted.
  //
  // The reset costs a memset of sizeof(T). It is deliberate: reusing a pooled
  // order without clearing it is how a stale strategy_id or filled_amount
  // leaks into a fresh order, and that bug is invisible in testing and
  // expensive in production.
  T* acquire() noexcept {
    T* p = acquire_dirty();
    if (p != nullptr) {
      *p = T{};
    }
    return p;
  }

  // Skips the reset. Only for callers that provably overwrite every field.
  T* acquire_dirty() noexcept {
    if (CALAIS_UNLIKELY(free_head_ == kInvalidIndex)) {
      ++exhaustion_count_;
      return nullptr;
    }
    const std::uint32_t idx = free_head_;
    free_head_ = next_[idx];
    --available_;
    const std::uint32_t in_use = capacity_ - available_;
    if (in_use > high_water_) {
      high_water_ = in_use;
    }
    return &storage_[idx];
  }

  // Releasing a pointer that did not come from this pool, or releasing twice,
  // is undefined. In debug builds the index range is checked.
  void release(T* p) noexcept {
    if (p == nullptr) {
      return;
    }
    const std::ptrdiff_t offset = p - storage_.data();
    if (offset < 0 || static_cast<std::size_t>(offset) >= storage_.size()) {
      return;  // not ours; ignore rather than corrupt the chain
    }
    const std::uint32_t idx = static_cast<std::uint32_t>(offset);
    next_[idx] = free_head_;
    free_head_ = idx;
    ++available_;
  }

  std::uint32_t capacity() const noexcept { return capacity_; }
  std::uint32_t available() const noexcept { return available_; }
  std::uint32_t in_use() const noexcept { return capacity_ - available_; }

  // Peak simultaneous usage since construction. Export this as a gauge; if it
  // approaches capacity, the pool is undersized.
  std::uint32_t high_water() const noexcept { return high_water_; }

  // Number of times acquire() returned nullptr. Must stay at zero.
  std::uint64_t exhaustion_count() const noexcept { return exhaustion_count_; }

 private:
  std::vector<T> storage_;
  std::vector<std::uint32_t> next_;
  std::uint32_t capacity_ = 0;
  std::uint32_t free_head_ = kInvalidIndex;
  std::uint32_t available_ = 0;
  std::uint32_t high_water_ = 0;
  std::uint64_t exhaustion_count_ = 0;
};

}  // namespace calais::core
