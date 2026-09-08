// Cross-process SPSC ring over shared memory.
//
// This replaces the ZMQ hop between a strategy process and the engine on the
// hot path. Rough one-way costs measured on the same box:
//
//   ZMQ tcp://127.0.0.1   ~15-30us      (kernel TCP stack, two syscalls)
//   ZMQ ipc://            ~7-15us       (unix socket, two syscalls)
//   this                  ~0.1-0.3us    (two stores and a cache line transfer)
//
// The saving is not the copy -- it is the syscalls and the scheduler wakeups.
// Which is also the catch: the consumer must BUSY-POLL. If you block on a
// condition variable to avoid burning a core you have handed the 2-10us futex
// wakeup straight back and would have been better off with ZMQ ipc.
//
// The ZMQ JSON protocol from the Python implementation is still the CONTROL
// plane -- subscriptions, queries, anything not latency-critical, and anything
// that has to stay compatible with an existing Python strategy. This ring
// carries only the fixed-layout hot messages in transport/hot_messages.h.
//
// Constraints, none of which are checked at runtime:
//   - exactly one producer process, exactly one consumer process
//   - both processes must be the same architecture and ABI (they share raw
//     struct layouts, not a serialised format)
//   - if the consumer dies mid-slot the ring is not self-healing; the
//     supervisor must recreate it
//
// Backed by a plain file + MAP_SHARED rather than shm_open, because the path
// semantics are identical on Linux and macOS that way. Point it at /dev/shm on
// Linux (tmpfs, no writeback) -- a path on a real filesystem would let the
// kernel try to flush ring pages to disk.

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>

#include "axon/core/platform.h"

namespace axon::core {

class ShmRingError : public std::runtime_error {
 public:
  using std::runtime_error::runtime_error;
};

// Layout of the header mapped at offset 0. Field order is part of the on-disk
// ABI -- bump kShmRingVersion if you change it.
struct ShmRingHeader {
  alignas(kCacheLineSize) std::uint32_t magic;
  std::uint32_t version;
  std::uint32_t slot_stride;   // bytes per slot, incl. the 16-byte slot header
  std::uint32_t slot_count;    // power of two
  std::uint32_t max_payload;
  std::uint32_t reserved;
  std::uint64_t data_offset;
  std::uint64_t total_bytes;

  // Producer cursor. Its own cache line: the consumer reads it constantly and
  // must not invalidate anything else when the producer advances it.
  alignas(kCacheLineSize) std::atomic<std::uint64_t> tail;

  // Consumer cursor.
  alignas(kCacheLineSize) std::atomic<std::uint64_t> head;

  // Incremented by the producer when a push finds the ring full. A non-zero
  // value here in production means the consumer is not keeping up and orders
  // or fills were refused -- alert on it.
  alignas(kCacheLineSize) std::atomic<std::uint64_t> push_failures;

  alignas(kCacheLineSize) std::uint64_t trailing_pad;
};

static_assert(std::atomic<std::uint64_t>::is_always_lock_free,
              "shared-memory cursors must be lock-free; a mutex-backed atomic "
              "would live in the wrong process's address space");

inline constexpr std::uint32_t kShmRingMagic = 0x43414C53;  // 'CALS'
inline constexpr std::uint32_t kShmRingVersion = 1;

// Per-slot header, kept at 16 bytes so every payload starts 16-byte aligned
// and can be reinterpret_cast to a POD message without an unaligned access.
inline constexpr std::uint32_t kShmSlotHeaderSize = 16;

class ShmRing {
 public:
  ShmRing() noexcept = default;
  ~ShmRing();

  ShmRing(const ShmRing&) = delete;
  ShmRing& operator=(const ShmRing&) = delete;
  ShmRing(ShmRing&& other) noexcept;
  ShmRing& operator=(ShmRing&& other) noexcept;

  // Creates (or re-creates) the backing file and initialises the header.
  // `slot_count` must be a power of two. Throws ShmRingError on failure.
  static ShmRing create(const std::string& path, std::uint32_t max_payload,
                        std::uint32_t slot_count);

  // Attaches to a ring created by another process. Throws if the file is
  // missing, truncated, or has a mismatched magic/version.
  static ShmRing open(const std::string& path);

  bool valid() const noexcept { return header_ != nullptr; }

  // ---- producer ---------------------------------------------------------

  bool try_push(const void* data, std::uint32_t len) noexcept;

  // Zero-copy: returns a pointer to the payload area of the next free slot, or
  // nullptr if full. Write up to max_payload() bytes, then commit_write(len).
  void* try_acquire_write() noexcept;
  void commit_write(std::uint32_t len) noexcept;

  // ---- consumer ---------------------------------------------------------

  // Copies the next message into `out`. Returns false if empty or if the
  // message does not fit in `cap` (in which case the message is left in place
  // -- an undersized buffer must not silently drop an order).
  bool try_pop(void* out, std::uint32_t cap, std::uint32_t& out_len) noexcept;

  // Zero-copy: returns a pointer into the ring, valid until commit_read().
  const void* try_acquire_read(std::uint32_t& out_len) noexcept;
  void commit_read() noexcept;

  // ---- observation ------------------------------------------------------

  std::uint32_t max_payload() const noexcept;
  std::uint32_t slot_count() const noexcept;
  std::uint64_t push_failures() const noexcept;
  std::size_t size() const noexcept;
  bool empty() const noexcept { return size() == 0; }
  const std::string& path() const noexcept { return path_; }

  // If set, the destructor unlinks the backing file. The creating process
  // normally owns this; attaching processes must not.
  void set_unlink_on_destroy(bool v) noexcept { unlink_on_destroy_ = v; }

 private:
  void close() noexcept;
  std::byte* slot_at(std::uint64_t seq) const noexcept;

  ShmRingHeader* header_ = nullptr;
  std::byte* data_ = nullptr;
  void* mapping_ = nullptr;
  std::size_t mapping_size_ = 0;
  int fd_ = -1;
  std::string path_;
  bool unlink_on_destroy_ = false;

  // Local (per-process) caches of the peer cursor, exactly as in SpscRing.
  std::uint64_t cached_head_ = 0;
  std::uint64_t cached_tail_ = 0;

  std::uint64_t mask_ = 0;
  std::uint32_t stride_ = 0;
};

}  // namespace axon::core
