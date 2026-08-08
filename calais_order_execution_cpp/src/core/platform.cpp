#include "calais/core/platform.h"

#include <cerrno>
#include <cstring>
#include <thread>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#endif

namespace calais::core {

TuningResult pin_current_thread_to_core([[maybe_unused]] int core_id) noexcept {
#if defined(__linux__)
  if (core_id < 0 || core_id >= online_core_count()) {
    return {false, "core_id out of range"};
  }
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(static_cast<unsigned>(core_id), &set);
  const int rc = ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set);
  if (rc != 0) {
    return {false, std::string("pthread_setaffinity_np: ") + std::strerror(rc)};
  }
  return {true, "pinned"};
#else
  return {false,
          "thread pinning unavailable on this platform (macOS has no real "
          "affinity API); deploy on Linux for latency-critical runs"};
#endif
}

TuningResult lock_all_memory() noexcept {
#if defined(__linux__)
  if (::mlockall(MCL_CURRENT | MCL_FUTURE) != 0) {
    return {false, std::string("mlockall: ") + std::strerror(errno)};
  }
  return {true, "locked"};
#else
  return {false, "mlockall not supported on this platform"};
#endif
}

int online_core_count() noexcept {
  const unsigned n = std::thread::hardware_concurrency();
  return n == 0 ? 1 : static_cast<int>(n);
}

}  // namespace calais::core
