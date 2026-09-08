// What do C++20 coroutines actually cost?
//
// The question is not "are coroutines fast" but "where in this system can they
// go". The answer turns on one property: a coroutine frame is HEAP ALLOCATED
// unless the compiler can elide it, and HALO (Heap Allocation eLision
// Optimisation) only fires when the coroutine is inlined into its caller and
// its lifetime is provably bounded. That is fragile -- it silently stops
// working when you move the call behind a virtual, store the handle, or cross
// a translation unit.
//
// An unbounded malloc inside the receive path is exactly what a 10us budget
// cannot absorb. So this measures four things:
//
//   1. a plain function chain              -- the baseline
//   2. a coroutine that never suspends     -- does HALO fire?
//   3. the same with a pooled frame        -- allocation made bounded
//   4. suspend/resume round trips          -- the cost when it DOES yield
//
// Read (2) against (1) to see whether the allocation was elided, and (3)
// against (2) to see what a pool buys when it was not.

#include <coroutine>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <vector>

#include "axon/core/clock.h"
#include "axon/core/histogram.h"

using namespace axon;
using core::Histogram;
using core::now_ticks;
using core::Ticks;

namespace {

template <typename T>
void keep(T&& value) {
  __asm__ __volatile__("" : : "r,m"(value) : "memory");
}

// ---------------------------------------------------------------------------
// A frame pool. Coroutine frames for a given coroutine are a fixed size, so a
// free list of fixed blocks is all that is needed -- and it turns an unbounded
// malloc into a pointer swap.
// ---------------------------------------------------------------------------
class FramePool {
 public:
  static constexpr std::size_t kBlockSize = 512;
  static constexpr std::size_t kBlocks = 4096;

  FramePool() : storage_(kBlockSize * kBlocks) {
    free_list_.reserve(kBlocks);
    for (std::size_t i = kBlocks; i-- > 0;) {
      free_list_.push_back(storage_.data() + i * kBlockSize);
    }
  }

  void* allocate(std::size_t n) {
    if (n > kBlockSize || free_list_.empty()) {
      ++overflows_;
      return std::malloc(n);  // must never happen in production
    }
    void* p = free_list_.back();
    free_list_.pop_back();
    ++allocations_;
    const std::size_t in_use = kBlocks - free_list_.size();
    if (in_use > peak_) {
      peak_ = in_use;
    }
    return p;
  }

  void deallocate(void* p) noexcept {
    auto* c = static_cast<char*>(p);
    if (c < storage_.data() || c >= storage_.data() + storage_.size()) {
      std::free(p);
      return;
    }
    free_list_.push_back(c);
  }

  std::size_t overflows() const noexcept { return overflows_; }
  // Total calls to operator new. Zero means the compiler elided every frame
  // allocation (HALO) and the pool was never reached -- which is itself the
  // most important thing this benchmark can tell us.
  std::size_t allocations() const noexcept { return allocations_; }
  std::size_t peak_used() const noexcept { return peak_; }

 private:
  std::vector<char> storage_;
  std::vector<char*> free_list_;
  std::size_t overflows_ = 0;
  std::size_t allocations_ = 0;
  std::size_t peak_ = 0;
};

FramePool& pool() {
  static FramePool p;
  return p;
}

// ---------------------------------------------------------------------------
// Minimal eager task: runs to the first suspend point on creation.
// ---------------------------------------------------------------------------
struct Task {
  struct promise_type {
    Task get_return_object() noexcept {
      return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  std::coroutine_handle<promise_type> handle;

  explicit Task(std::coroutine_handle<promise_type> h) noexcept : handle(h) {}
  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;
  ~Task() {
    if (handle) {
      handle.destroy();
    }
  }
};

// Same thing, but the frame comes from the pool.
struct PooledTask {
  struct promise_type {
    static void* operator new(std::size_t n) { return pool().allocate(n); }
    static void operator delete(void* p, std::size_t) noexcept {
      pool().deallocate(p);
    }

    PooledTask get_return_object() noexcept {
      return PooledTask{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  std::coroutine_handle<promise_type> handle;

  explicit PooledTask(std::coroutine_handle<promise_type> h) noexcept
      : handle(h) {}
  PooledTask(const PooledTask&) = delete;
  PooledTask& operator=(const PooledTask&) = delete;
  ~PooledTask() {
    if (handle) {
      handle.destroy();
    }
  }
};

// A lazy task that suspends on demand, for measuring resume cost.
struct LazyTask {
  struct promise_type {
    LazyTask get_return_object() noexcept {
      return LazyTask{std::coroutine_handle<promise_type>::from_promise(*this)};
    }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    void return_void() noexcept {}
    void unhandled_exception() noexcept { std::terminate(); }
  };

  std::coroutine_handle<promise_type> handle;

  explicit LazyTask(std::coroutine_handle<promise_type> h) noexcept
      : handle(h) {}
  LazyTask(const LazyTask&) = delete;
  ~LazyTask() {
    if (handle) {
      handle.destroy();
    }
  }
};

// ---------------------------------------------------------------------------
// The work. Deliberately trivial and identical in all variants, so what is
// measured is the machinery and not the payload.
// ---------------------------------------------------------------------------
std::uint64_t g_sink = 0;

void step_a(std::uint64_t v) { g_sink += v * 3; }
void step_b(std::uint64_t v) { g_sink ^= v; }
void step_c(std::uint64_t v) { g_sink += v >> 1; }

void plain_chain(std::uint64_t v) {
  step_a(v);
  step_b(v);
  step_c(v);
}

Task coro_chain(std::uint64_t v) {
  step_a(v);
  step_b(v);
  step_c(v);
  co_return;
}

PooledTask pooled_chain(std::uint64_t v) {
  step_a(v);
  step_b(v);
  step_c(v);
  co_return;
}

LazyTask suspending_chain(std::uint64_t v, int suspends) {
  for (int i = 0; i < suspends; ++i) {
    step_a(v);
    co_await std::suspend_always{};
  }
}

// ---------------------------------------------------------------------------
void report(const char* name, const Histogram& h, const char* note = "") {
  std::printf("%-46s %8.2f  %s\n", name, static_cast<double>(h.p50()) / 1000.0,
              note);
}

template <typename F>
Histogram measure_amortised(int batches, int batch, F&& body) {
  Histogram h(60'000'000'000ULL, 3);
  const double ns_per_tick = core::clock_info().ns_per_tick;
  for (int i = 0; i < batch; ++i) {
    body();
  }
  for (int b = 0; b < batches; ++b) {
    const Ticks t0 = now_ticks();
    for (int i = 0; i < batch; ++i) {
      body();
    }
    const Ticks t1 = now_ticks();
    h.record(static_cast<std::uint64_t>(static_cast<double>(t1 - t0) *
                                        ns_per_tick * 1000.0 /
                                        static_cast<double>(batch)));
  }
  return h;
}

}  // namespace

int main() {
  const auto& info = core::clock_info();
  std::printf("clock: %s, granularity %.2fns\n", info.source, info.resolution_ns);
  std::printf("all figures nanoseconds per operation, amortised over batches\n\n");

  std::printf("%-46s %8s\n", "", "ns");
  std::printf("%s\n", std::string(74, '-').c_str());

  std::uint64_t v = 1;

  report("plain function chain (baseline)",
         measure_amortised(2000, 1000, [&] {
           v = v * 6364136223846793005ULL + 1;
           plain_chain(v);
           keep(g_sink);
         }));

  report("coroutine, default frame allocation",
         measure_amortised(2000, 1000, [&] {
           v = v * 6364136223846793005ULL + 1;
           Task t = coro_chain(v);
           keep(g_sink);
         }),
         "HALO fires => same as baseline; if not, this is a malloc");

  report("coroutine, pooled frame", measure_amortised(2000, 1000, [&] {
           v = v * 6364136223846793005ULL + 1;
           PooledTask t = pooled_chain(v);
           keep(g_sink);
         }),
         "allocation made bounded");

  // Force the frame to escape so HALO cannot fire, which is what happens the
  // moment a coroutine is stored, queued, or returned across an interface.
  report("coroutine, frame forced to escape",
         measure_amortised(2000, 1000, [&] {
           v = v * 6364136223846793005ULL + 1;
           auto* t = new Task(coro_chain(v));
           keep(t->handle.address());
           delete t;
         }),
         "the realistic case for a scheduler");

  std::printf("\n%-46s %8s\n", "suspend / resume round trip", "ns");
  std::printf("%s\n", std::string(74, '-').c_str());

  for (int suspends : {1, 4, 16}) {
    char label[64];
    std::snprintf(label, sizeof(label), "%d suspend(s), create+resume+destroy",
                  suspends);
    Histogram h = measure_amortised(1000, 200, [&] {
      v = v * 6364136223846793005ULL + 1;
      LazyTask t = suspending_chain(v, suspends);
      for (int i = 0; i <= suspends; ++i) {
        if (!t.handle.done()) {
          t.handle.resume();
        }
      }
      keep(g_sink);
    });
    report(label, h);
  }

  // Isolate the resume itself: build once, resume many times.
  {
    Histogram h(60'000'000'000ULL, 3);
    const double ns_per_tick = core::clock_info().ns_per_tick;
    constexpr int kBatch = 1000;
    for (int b = 0; b < 2000; ++b) {
      LazyTask t = suspending_chain(v, kBatch + 1);
      const Ticks t0 = now_ticks();
      for (int i = 0; i < kBatch; ++i) {
        t.handle.resume();
      }
      const Ticks t1 = now_ticks();
      h.record(static_cast<std::uint64_t>(static_cast<double>(t1 - t0) *
                                          ns_per_tick * 1000.0 / kBatch));
    }
    report("bare resume() (frame already hot)", h,
           "this is the floor for a suspension");
  }

  std::printf(
      "\npool: %zu allocations, peak %zu frames in use, %zu overflows\n",
      pool().allocations(), pool().peak_used(), pool().overflows());
  if (pool().allocations() == 0) {
    std::printf(
        "  => operator new was NEVER called: the compiler elided every frame\n"
        "     (HALO). The pool bought nothing here because there was nothing\n"
        "     to allocate. Do not conclude that pooling is unnecessary -- see\n"
        "     the 'forced to escape' row for what happens when HALO cannot\n"
        "     fire, which is the normal case behind an interface boundary.\n");
  }
  std::printf("sink: %llu\n", static_cast<unsigned long long>(g_sink));
  return 0;
}
