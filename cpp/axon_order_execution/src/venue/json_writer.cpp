#include "axon/venue/json_writer.h"

namespace axon::venue {
namespace detail {

void Writer::json_string(std::string_view s) noexcept {
  raw("\"");
  for (char c : s) {
    if (!ok) {
      return;
    }
    switch (c) {
      case '"':
        raw("\\\"");
        break;
      case '\\':
        raw("\\\\");
        break;
      case '\n':
        raw("\\n");
        break;
      case '\r':
        raw("\\r");
        break;
      case '\t':
        raw("\\t");
        break;
      default:
        if (static_cast<unsigned char>(c) < 0x20) {
          // Control characters must be escaped as \u00XX.
          char buf[7] = {'\\', 'u', '0', '0', 0, 0, 0};
          static const char* kHex = "0123456789abcdef";
          buf[4] = kHex[(static_cast<unsigned char>(c) >> 4) & 0xF];
          buf[5] = kHex[static_cast<unsigned char>(c) & 0xF];
          raw(std::string_view(buf, 6));
        } else {
          raw(std::string_view(&c, 1));
        }
        break;
    }
  }
  raw("\"");
}

void Writer::integer(std::int64_t v) noexcept {
  if (!ok) {
    return;
  }
  char scratch[24];
  std::size_t len = 0;
  bool negative = false;
  std::uint64_t magnitude;
  if (v < 0) {
    negative = true;
    magnitude = static_cast<std::uint64_t>(-(v + 1)) + 1U;
  } else {
    magnitude = static_cast<std::uint64_t>(v);
  }
  if (magnitude == 0) {
    scratch[len++] = '0';
  } else {
    while (magnitude > 0) {
      scratch[len++] = static_cast<char>('0' + (magnitude % 10));
      magnitude /= 10;
    }
  }
  const std::size_t total = len + (negative ? 1 : 0);
  if (static_cast<std::size_t>(end - p) < total) {
    ok = false;
    return;
  }
  if (negative) {
    *p++ = '-';
  }
  while (len > 0) {
    *p++ = scratch[--len];
  }
}

}  // namespace detail
}  // namespace axon::venue
