// What a venue parser reports, independent of which venue produced it.
//
// The four venues speak four unrelated protocols -- Deribit is JSON-RPC 2.0,
// Binance is an event-tagged user data stream, Bybit is topic/data, OKX is
// arg/data -- but the OMS above them needs one shape. This is that shape.
//
// It is deliberately a plain result struct rather than a class hierarchy: the
// parsers are templates on the handler so everything inlines, and a virtual
// dispatch here would put an indirect call on the receive path for no benefit.

#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

namespace calais::venue {

enum class VenueMessageKind : std::uint8_t {
  // Not something this build knows about. Ignore it -- venues add message
  // types without warning, and an unknown one must not be fatal.
  kUnknown = 0,
  // Malformed, or structurally not what the channel promised. The connection
  // is usually still fine; count it and move on.
  kParseError = 1,
  // Liveness traffic that needs no reply.
  kHeartbeat = 2,
  // Liveness traffic that MUST be answered immediately, or the venue drops
  // the connection. Deribit's test_request is the case that matters.
  kHeartbeatTestRequest = 3,
  kOrderUpdate = 4,
  kTradeUpdate = 5,
  kPortfolioUpdate = 6,
  // A reply to something we sent.
  kRpcResult = 7,
  kRpcError = 8,
  // A login/subscribe acknowledgement. Informational.
  kSessionEvent = 9,
  // The session is no longer usable and must be re-established --
  // Binance's listenKeyExpired. Distinct from an error because the correct
  // response is "reconnect", not "log and continue".
  kSessionExpired = 10,
};

struct VenueParseResult {
  VenueMessageKind kind = VenueMessageKind::kUnknown;

  // Correlation id for kRpcResult / kRpcError, where the venue provides one.
  std::int64_t rpc_id = 0;

  // Points into the caller's buffer and is valid only until that buffer is
  // reused -- which is exactly as long as the parse result itself is useful.
  std::string_view rpc_error_text;
  std::string_view channel;

  // How many order/fill/account objects were handed to the handler. A single
  // frame commonly carries several on every venue except Deribit's order
  // channel.
  std::size_t emitted = 0;

  // The venue sent an order status this build does not recognise. The update
  // was still emitted, mapped to "open", because losing it entirely would be
  // worse -- but this must be counted and alerted on, not ignored. Each
  // venue's protocol header documents its own mapping.
  bool unknown_order_state = false;

  // Static string; null unless kind is kParseError.
  const char* error = nullptr;
};

}  // namespace calais::venue
