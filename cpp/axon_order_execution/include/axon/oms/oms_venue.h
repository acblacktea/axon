// What a venue supplies on the OMS side, as one row per venue. The OMS
// counterpart of ems/venue_ops.h.
//
// Each venue lives in exchanges/<venue>/<venue>_oms.cpp -- its private feed session,
// its order-entry session where it has one, and its REST snapshots -- and
// exposes exactly one of these. Nothing outside that file knows the venue's
// classes, and nothing else asks which venue it is talking to: the one place
// a venue is chosen by name is oms_venue_for().

#pragma once

#include <memory>
#include <string_view>

#include "axon/config.h"
#include "axon/net/http_client.h"
#include "axon/net/tls_stream.h"
#include "axon/oms/venue_rest.h"
#include "axon/oms/venue_session.h"

namespace axon::oms {

struct OmsVenue {
  const char* name;

  // The private feed: order, fill and account pushes. `rest` is for venues
  // whose stream is keyed over REST (Binance's listenKey); the rest ignore it.
  std::unique_ptr<VenueSession> (*make_session)(const ExchangeConfig& exchange,
                                                const WebSocketConfig& ws_config,
                                                const RuntimeConfig& runtime,
                                                const net::TlsContext* tls, VenueRest* rest,
                                                VenueSessionHandlers handlers);

  // The order-entry-only connection, or null when orders ride the feed
  // session (Deribit, OKX).
  std::unique_ptr<VenueSession> (*make_trade_session)(const ExchangeConfig& exchange,
                                                      const WebSocketConfig& ws_config,
                                                      const RuntimeConfig& runtime,
                                                      const net::TlsContext* tls,
                                                      VenueSessionHandlers handlers);

  // Reconciliation snapshots, positions, tickers -- never order entry.
  std::unique_ptr<VenueRest> (*make_rest)(const ExchangeConfig& exchange,
                                          net::HttpClient* http);
};

// The venue this exchange name maps to, or nullptr if this build has none.
const OmsVenue* oms_venue_for(std::string_view exchange);

}  // namespace axon::oms
