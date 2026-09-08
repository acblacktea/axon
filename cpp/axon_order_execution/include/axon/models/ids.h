// Internal order id generation.
//
// Mirrors the Python side's `uuid.uuid4().hex`: 32 lowercase hex characters,
// no dashes. Kept identical because the id is written into Postgres and
// published to Python strategies, so both implementations must produce ids
// that are indistinguishable.
//
// NOT cryptographically random. This is a collision-avoidance id, not a
// secret: it is never used for authentication and an attacker who could guess
// one gains nothing. Seeding from std::random_device once per thread and then
// running a fast PRNG costs ~15ns per id, against ~300ns for reseeding from
// the OS each time. That difference matters when placing a burst of orders.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

namespace axon::models {

// 32 hex chars, no NUL.
inline constexpr std::size_t kInternalIdLength = 32;

// Writes exactly kInternalIdLength bytes into `out`. Does not NUL-terminate.
void generate_internal_id(char* out) noexcept;

std::string generate_internal_id();

}  // namespace axon::models
