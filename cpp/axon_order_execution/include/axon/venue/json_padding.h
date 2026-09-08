// The one simdjson fact the network layer needs to know.
//
// simdjson On-Demand reads up to SIMDJSON_PADDING bytes past the end of a
// document, so a receive buffer that a venue parser will read from directly
// must carry that much slack beyond its usable capacity. Feeding it an
// unpadded buffer is a heap overread -- the kind a sanitizer catches and
// production does not.
//
// This constant is duplicated here rather than including <simdjson.h> from
// net/, deliberately. The network layer has no business depending on a JSON
// parser, and dragging a 400k-line header into every connection translation
// unit to learn one number would be a poor trade. The static assertion in
// venue/json_view.h keeps the two honest.

#pragma once

#include <cstddef>

namespace axon::net {

inline constexpr std::size_t kJsonParserPadding = 64;

}  // namespace axon::net
