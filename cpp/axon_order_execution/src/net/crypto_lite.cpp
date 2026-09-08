#include "axon/net/crypto_lite.h"

#include <cstring>

namespace axon::net {
namespace {

constexpr std::uint32_t rotl(std::uint32_t v, int n) noexcept {
  return (v << n) | (v >> (32 - n));
}

constexpr char kB64[] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

int b64_value(char c) noexcept {
  if (c >= 'A' && c <= 'Z') {
    return c - 'A';
  }
  if (c >= 'a' && c <= 'z') {
    return c - 'a' + 26;
  }
  if (c >= '0' && c <= '9') {
    return c - '0' + 52;
  }
  if (c == '+') {
    return 62;
  }
  if (c == '/') {
    return 63;
  }
  return -1;
}

}  // namespace

// ---------------------------------------------------------------------------
// SHA-1, straight from RFC 3174.
// ---------------------------------------------------------------------------
Sha1Digest sha1(const void* data, std::size_t len) noexcept {
  std::uint32_t h[5] = {0x67452301u, 0xEFCDAB89u, 0x98BADCFEu, 0x10325476u,
                        0xC3D2E1F0u};

  const auto* bytes = static_cast<const std::uint8_t*>(data);
  const std::uint64_t bit_len = static_cast<std::uint64_t>(len) * 8u;

  // Process 64-byte blocks, synthesising the padded tail rather than copying
  // the whole message into a padded buffer.
  std::uint8_t block[64];
  std::size_t pos = 0;

  auto process = [&h](const std::uint8_t* b) noexcept {
    std::uint32_t w[80];
    for (int i = 0; i < 16; ++i) {
      w[i] = (static_cast<std::uint32_t>(b[i * 4]) << 24) |
             (static_cast<std::uint32_t>(b[i * 4 + 1]) << 16) |
             (static_cast<std::uint32_t>(b[i * 4 + 2]) << 8) |
             static_cast<std::uint32_t>(b[i * 4 + 3]);
    }
    for (int i = 16; i < 80; ++i) {
      w[i] = rotl(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }

    std::uint32_t a = h[0];
    std::uint32_t bb = h[1];
    std::uint32_t c = h[2];
    std::uint32_t d = h[3];
    std::uint32_t e = h[4];

    for (int i = 0; i < 80; ++i) {
      std::uint32_t f = 0;
      std::uint32_t k = 0;
      if (i < 20) {
        f = (bb & c) | ((~bb) & d);
        k = 0x5A827999u;
      } else if (i < 40) {
        f = bb ^ c ^ d;
        k = 0x6ED9EBA1u;
      } else if (i < 60) {
        f = (bb & c) | (bb & d) | (c & d);
        k = 0x8F1BBCDCu;
      } else {
        f = bb ^ c ^ d;
        k = 0xCA62C1D6u;
      }
      const std::uint32_t temp = rotl(a, 5) + f + e + k + w[i];
      e = d;
      d = c;
      c = rotl(bb, 30);
      bb = a;
      a = temp;
    }

    h[0] += a;
    h[1] += bb;
    h[2] += c;
    h[3] += d;
    h[4] += e;
  };

  while (pos + 64 <= len) {
    process(bytes + pos);
    pos += 64;
  }

  const std::size_t tail = len - pos;
  std::memset(block, 0, sizeof(block));
  if (tail > 0) {
    std::memcpy(block, bytes + pos, tail);
  }
  block[tail] = 0x80;

  if (tail + 1 > 56) {
    // No room for the length in this block; flush it and use a second.
    process(block);
    std::memset(block, 0, sizeof(block));
  }
  for (int i = 0; i < 8; ++i) {
    block[56 + i] = static_cast<std::uint8_t>((bit_len >> (56 - 8 * i)) & 0xFF);
  }
  process(block);

  Sha1Digest out{};
  for (int i = 0; i < 5; ++i) {
    out[static_cast<std::size_t>(i * 4)] =
        static_cast<std::byte>((h[i] >> 24) & 0xFF);
    out[static_cast<std::size_t>(i * 4 + 1)] =
        static_cast<std::byte>((h[i] >> 16) & 0xFF);
    out[static_cast<std::size_t>(i * 4 + 2)] =
        static_cast<std::byte>((h[i] >> 8) & 0xFF);
    out[static_cast<std::size_t>(i * 4 + 3)] =
        static_cast<std::byte>(h[i] & 0xFF);
  }
  return out;
}

// ---------------------------------------------------------------------------
std::string base64_encode(const void* data, std::size_t len) {
  const auto* p = static_cast<const std::uint8_t*>(data);
  std::string out;
  out.reserve(((len + 2) / 3) * 4);

  std::size_t i = 0;
  for (; i + 3 <= len; i += 3) {
    const std::uint32_t v = (static_cast<std::uint32_t>(p[i]) << 16) |
                            (static_cast<std::uint32_t>(p[i + 1]) << 8) |
                            static_cast<std::uint32_t>(p[i + 2]);
    out.push_back(kB64[(v >> 18) & 0x3F]);
    out.push_back(kB64[(v >> 12) & 0x3F]);
    out.push_back(kB64[(v >> 6) & 0x3F]);
    out.push_back(kB64[v & 0x3F]);
  }

  const std::size_t rem = len - i;
  if (rem == 1) {
    const std::uint32_t v = static_cast<std::uint32_t>(p[i]) << 16;
    out.push_back(kB64[(v >> 18) & 0x3F]);
    out.push_back(kB64[(v >> 12) & 0x3F]);
    out.push_back('=');
    out.push_back('=');
  } else if (rem == 2) {
    const std::uint32_t v = (static_cast<std::uint32_t>(p[i]) << 16) |
                            (static_cast<std::uint32_t>(p[i + 1]) << 8);
    out.push_back(kB64[(v >> 18) & 0x3F]);
    out.push_back(kB64[(v >> 12) & 0x3F]);
    out.push_back(kB64[(v >> 6) & 0x3F]);
    out.push_back('=');
  }
  return out;
}

bool base64_decode(std::string_view in, std::string& out) {
  if (in.size() % 4 != 0) {
    return false;
  }
  out.clear();
  out.reserve(in.size() / 4 * 3);

  for (std::size_t i = 0; i < in.size(); i += 4) {
    int vals[4];
    int pad = 0;
    for (std::size_t j = 0; j < 4; ++j) {
      const char c = in[i + j];
      if (c == '=') {
        // Padding is only legal in the final quad, and only in the last two
        // positions.
        if (i + 4 != in.size() || j < 2) {
          return false;
        }
        vals[j] = 0;
        ++pad;
      } else {
        if (pad != 0) {
          return false;  // data after padding
        }
        vals[j] = b64_value(c);
        if (vals[j] < 0) {
          return false;
        }
      }
    }

    const std::uint32_t v = (static_cast<std::uint32_t>(vals[0]) << 18) |
                            (static_cast<std::uint32_t>(vals[1]) << 12) |
                            (static_cast<std::uint32_t>(vals[2]) << 6) |
                            static_cast<std::uint32_t>(vals[3]);
    out.push_back(static_cast<char>((v >> 16) & 0xFF));
    if (pad < 2) {
      out.push_back(static_cast<char>((v >> 8) & 0xFF));
    }
    if (pad < 1) {
      out.push_back(static_cast<char>(v & 0xFF));
    }
  }
  return true;
}

}  // namespace axon::net

// ---------------------------------------------------------------------------
// SHA-256 (FIPS 180-4) and HMAC-SHA256 (RFC 2104).
// ---------------------------------------------------------------------------
namespace axon::net {
namespace {

constexpr std::uint32_t kSha256K[64] = {
    0x428a2f98u, 0x71374491u, 0xb5c0fbcfu, 0xe9b5dba5u, 0x3956c25bu, 0x59f111f1u,
    0x923f82a4u, 0xab1c5ed5u, 0xd807aa98u, 0x12835b01u, 0x243185beu, 0x550c7dc3u,
    0x72be5d74u, 0x80deb1feu, 0x9bdc06a7u, 0xc19bf174u, 0xe49b69c1u, 0xefbe4786u,
    0x0fc19dc6u, 0x240ca1ccu, 0x2de92c6fu, 0x4a7484aau, 0x5cb0a9dcu, 0x76f988dau,
    0x983e5152u, 0xa831c66du, 0xb00327c8u, 0xbf597fc7u, 0xc6e00bf3u, 0xd5a79147u,
    0x06ca6351u, 0x14292967u, 0x27b70a85u, 0x2e1b2138u, 0x4d2c6dfcu, 0x53380d13u,
    0x650a7354u, 0x766a0abbu, 0x81c2c92eu, 0x92722c85u, 0xa2bfe8a1u, 0xa81a664bu,
    0xc24b8b70u, 0xc76c51a3u, 0xd192e819u, 0xd6990624u, 0xf40e3585u, 0x106aa070u,
    0x19a4c116u, 0x1e376c08u, 0x2748774cu, 0x34b0bcb5u, 0x391c0cb3u, 0x4ed8aa4au,
    0x5b9cca4fu, 0x682e6ff3u, 0x748f82eeu, 0x78a5636fu, 0x84c87814u, 0x8cc70208u,
    0x90befffau, 0xa4506cebu, 0xbef9a3f7u, 0xc67178f2u};

constexpr std::uint32_t rotr(std::uint32_t v, int n) noexcept {
  return (v >> n) | (v << (32 - n));
}

void sha256_block(std::uint32_t h[8], const std::uint8_t* b) noexcept {
  std::uint32_t w[64];
  for (int i = 0; i < 16; ++i) {
    w[i] = (static_cast<std::uint32_t>(b[i * 4]) << 24) |
           (static_cast<std::uint32_t>(b[i * 4 + 1]) << 16) |
           (static_cast<std::uint32_t>(b[i * 4 + 2]) << 8) |
           static_cast<std::uint32_t>(b[i * 4 + 3]);
  }
  for (int i = 16; i < 64; ++i) {
    const std::uint32_t s0 =
        rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3);
    const std::uint32_t s1 =
        rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10);
    w[i] = w[i - 16] + s0 + w[i - 7] + s1;
  }

  std::uint32_t a = h[0];
  std::uint32_t bb = h[1];
  std::uint32_t c = h[2];
  std::uint32_t d = h[3];
  std::uint32_t e = h[4];
  std::uint32_t f = h[5];
  std::uint32_t g = h[6];
  std::uint32_t hh = h[7];

  for (int i = 0; i < 64; ++i) {
    const std::uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
    const std::uint32_t ch = (e & f) ^ ((~e) & g);
    const std::uint32_t temp1 = hh + S1 + ch + kSha256K[i] + w[i];
    const std::uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
    const std::uint32_t maj = (a & bb) ^ (a & c) ^ (bb & c);
    const std::uint32_t temp2 = S0 + maj;

    hh = g;
    g = f;
    f = e;
    e = d + temp1;
    d = c;
    c = bb;
    bb = a;
    a = temp1 + temp2;
  }

  h[0] += a;
  h[1] += bb;
  h[2] += c;
  h[3] += d;
  h[4] += e;
  h[5] += f;
  h[6] += g;
  h[7] += hh;
}

}  // namespace

Sha256Digest sha256(const void* data, std::size_t len) noexcept {
  std::uint32_t h[8] = {0x6a09e667u, 0xbb67ae85u, 0x3c6ef372u, 0xa54ff53au,
                        0x510e527fu, 0x9b05688cu, 0x1f83d9abu, 0x5be0cd19u};

  const auto* bytes = static_cast<const std::uint8_t*>(data);
  const std::uint64_t bit_len = static_cast<std::uint64_t>(len) * 8u;

  std::size_t pos = 0;
  while (pos + 64 <= len) {
    sha256_block(h, bytes + pos);
    pos += 64;
  }

  std::uint8_t block[64];
  const std::size_t tail = len - pos;
  std::memset(block, 0, sizeof(block));
  if (tail > 0) {
    std::memcpy(block, bytes + pos, tail);
  }
  block[tail] = 0x80;
  if (tail + 1 > 56) {
    sha256_block(h, block);
    std::memset(block, 0, sizeof(block));
  }
  for (int i = 0; i < 8; ++i) {
    block[56 + i] = static_cast<std::uint8_t>((bit_len >> (56 - 8 * i)) & 0xFF);
  }
  sha256_block(h, block);

  Sha256Digest out{};
  for (int i = 0; i < 8; ++i) {
    const auto base = static_cast<std::size_t>(i * 4);
    out[base] = static_cast<std::byte>((h[i] >> 24) & 0xFF);
    out[base + 1] = static_cast<std::byte>((h[i] >> 16) & 0xFF);
    out[base + 2] = static_cast<std::byte>((h[i] >> 8) & 0xFF);
    out[base + 3] = static_cast<std::byte>(h[i] & 0xFF);
  }
  return out;
}

Sha256Digest hmac_sha256(std::string_view key, std::string_view message) noexcept {
  constexpr std::size_t kBlock = 64;
  std::uint8_t k[kBlock];
  std::memset(k, 0, sizeof(k));

  // A key longer than the block is hashed first; a shorter one is zero-padded.
  if (key.size() > kBlock) {
    const Sha256Digest hashed = sha256(key);
    std::memcpy(k, hashed.data(), hashed.size());
  } else if (!key.empty()) {
    std::memcpy(k, key.data(), key.size());
  }

  std::uint8_t ipad[kBlock];
  std::uint8_t opad[kBlock];
  for (std::size_t i = 0; i < kBlock; ++i) {
    ipad[i] = static_cast<std::uint8_t>(k[i] ^ 0x36);
    opad[i] = static_cast<std::uint8_t>(k[i] ^ 0x5c);
  }

  std::string inner;
  inner.reserve(kBlock + message.size());
  inner.append(reinterpret_cast<const char*>(ipad), kBlock);
  inner.append(message);
  const Sha256Digest inner_digest = sha256(inner);

  std::string outer;
  outer.reserve(kBlock + inner_digest.size());
  outer.append(reinterpret_cast<const char*>(opad), kBlock);
  outer.append(reinterpret_cast<const char*>(inner_digest.data()),
               inner_digest.size());
  return sha256(outer);
}

std::string to_hex(const void* data, std::size_t len) {
  static constexpr char kHexDigits[] = "0123456789abcdef";
  const auto* p = static_cast<const std::uint8_t*>(data);
  std::string out;
  out.reserve(len * 2);
  for (std::size_t i = 0; i < len; ++i) {
    out.push_back(kHexDigits[p[i] >> 4]);
    out.push_back(kHexDigits[p[i] & 0xF]);
  }
  return out;
}

}  // namespace axon::net
