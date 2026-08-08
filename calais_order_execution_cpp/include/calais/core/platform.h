// Platform abstraction for the hot path.
//
// Everything here must be zero-cost in release builds. Anything that cannot be
// (thread pinning, page locking) returns a status the caller can log, rather
// than throwing -- these are startup-time concerns and a Mac dev box is
// expected to fail some of them.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace calais::core {

// ---------------------------------------------------------------------------
// Cache line size
//
// Apple Silicon uses 128-byte cache lines; x86_64 uses 64. We over-align rather
// than under-align: wasting 64 bytes per ring cursor is free, false sharing on
// a hot cursor is not.
// ---------------------------------------------------------------------------
#if defined(__aarch64__) || defined(__ARM_ARCH_ISA_A64)
inline constexpr std::size_t kCacheLineSize = 128;
#else
inline constexpr std::size_t kCacheLineSize = 64;
#endif

// ---------------------------------------------------------------------------
// Branch hints / inlining
// ---------------------------------------------------------------------------
#if defined(__GNUC__) || defined(__clang__)
#define CALAIS_LIKELY(x)       __builtin_expect(!!(x), 1)
#define CALAIS_UNLIKELY(x)     __builtin_expect(!!(x), 0)
#define CALAIS_ALWAYS_INLINE   inline __attribute__((always_inline))
#define CALAIS_NOINLINE        __attribute__((noinline))
#define CALAIS_HOT             __attribute__((hot))
#else
#define CALAIS_LIKELY(x)       (x)
#define CALAIS_UNLIKELY(x)     (x)
#define CALAIS_ALWAYS_INLINE   inline
#define CALAIS_NOINLINE
#define CALAIS_HOT
#endif

// ---------------------------------------------------------------------------
// Spin-wait hint
//
// Used in busy-poll loops. On x86 this is PAUSE (drops the hyperthread's
// pipeline priority and avoids the memory-order-violation penalty on loop
// exit). On ARM64 this is YIELD.
//
// Never call this in a loop that lacks a bound -- a busy-poll thread should
// spin on a hot cursor, not on a syscall.
// ---------------------------------------------------------------------------
CALAIS_ALWAYS_INLINE void cpu_pause() noexcept {
#if defined(__x86_64__) || defined(__i386__)
  __builtin_ia32_pause();
#elif defined(__aarch64__)
  __asm__ __volatile__("yield" ::: "memory");
#else
  // Fall back to a compiler barrier so the spin loop is not hoisted.
  __asm__ __volatile__("" ::: "memory");
#endif
}

// ---------------------------------------------------------------------------
// Startup-time tuning. All of these are best-effort and Linux-only in
// practice; on macOS they return false with a reason so startup can log
// "running unpinned, latency numbers are indicative only".
// ---------------------------------------------------------------------------

struct TuningResult {
  bool ok = false;
  std::string detail;

  explicit operator bool() const noexcept { return ok; }
};

// Pin the calling thread to a single core. Linux only.
//
// macOS deliberately provides no true affinity API (thread_policy_set with
// THREAD_AFFINITY_POLICY is an advisory hint the scheduler may ignore, and is
// a no-op on Apple Silicon), so this reports failure there rather than
// pretending to have worked.
TuningResult pin_current_thread_to_core(int core_id) noexcept;

// Lock all current and future pages into RAM, so the hot path can never take a
// page fault. Requires RLIMIT_MEMLOCK headroom.
TuningResult lock_all_memory() noexcept;

// Number of cores usable by this process.
int online_core_count() noexcept;

}  // namespace calais::core
