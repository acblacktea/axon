#include "axon/models/ids.h"

#include <random>

#include "axon/core/clock.h"

namespace axon::models {
namespace {

// xoshiro256++ -- fast, good statistical quality, tiny state. Seeded once per
// thread from std::random_device plus the cycle counter, so two threads
// starting in the same microsecond still diverge.
struct Rng {
  std::uint64_t s[4];

  Rng() noexcept {
    std::random_device rd;
    for (auto& v : s) {
      v = (static_cast<std::uint64_t>(rd()) << 32) ^ static_cast<std::uint64_t>(rd());
    }
    s[0] ^= core::now_ticks();
    s[1] ^= static_cast<std::uint64_t>(core::wall_clock_ns());
    if ((s[0] | s[1] | s[2] | s[3]) == 0) {
      s[0] = 0x9E3779B97F4A7C15ULL;  // state must not be all zero
    }
  }

  static std::uint64_t rotl(std::uint64_t x, int k) noexcept {
    return (x << k) | (x >> (64 - k));
  }

  std::uint64_t next() noexcept {
    const std::uint64_t result = rotl(s[0] + s[3], 23) + s[0];
    const std::uint64_t t = s[1] << 17;
    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = rotl(s[3], 45);
    return result;
  }
};

thread_local Rng t_rng;

constexpr char kHex[] = "0123456789abcdef";

void write_hex64(char* out, std::uint64_t v) noexcept {
  for (int i = 15; i >= 0; --i) {
    out[i] = kHex[v & 0xF];
    v >>= 4;
  }
}

}  // namespace

void generate_internal_id(char* out) noexcept {
  write_hex64(out, t_rng.next());
  write_hex64(out + 16, t_rng.next());
}

std::string generate_internal_id() {
  std::string s(kInternalIdLength, '\0');
  generate_internal_id(s.data());
  return s;
}

}  // namespace axon::models
