// What an exchange supplies on the OMS side, as one row per exchange. The OMS
// counterpart of ems/exchange_ops.h.
//
// Each exchange lives in exchanges/<exchange>/<exchange>_oms.cpp -- its private feed session,
// its order-entry session where it has one, and its REST snapshots -- and
// exposes exactly one of these. Nothing outside that file knows the exchange's
// classes, and nothing else asks which exchange it is talking to: the one place
// an exchange is chosen by name is oms_exchange_for().

#pragma once

#include <memory>
#include <string_view>

#include "axon/config.h"
#include "axon/net/http_client.h"
#include "axon/net/tls_stream.h"
#include "axon/oms/exchange_rest.h"
#include "axon/oms/exchange_session.h"

namespace axon::oms {

struct OmsExchange {
  const char* name;

  // The private feed: order, fill and account pushes. `rest` is for exchanges
  // whose stream is keyed over REST (Binance's listenKey); the rest ignore it.
  std::unique_ptr<ExchangeSession> (*make_session)(const ExchangeConfig& exchange_config,
                                                const WebSocketConfig& ws_config,
                                                const RuntimeConfig& runtime,
                                                const net::TlsContext* tls, ExchangeRest* rest,
                                                ExchangeSessionHandlers handlers);

  // The order-entry-only connection, or null when orders ride the feed
  // session (Deribit, OKX).
  std::unique_ptr<ExchangeSession> (*make_trade_session)(const ExchangeConfig& exchange_config,
                                                      const WebSocketConfig& ws_config,
                                                      const RuntimeConfig& runtime,
                                                      const net::TlsContext* tls,
                                                      ExchangeSessionHandlers handlers);

  // Reconciliation snapshots, positions, tickers -- never order entry.
  std::unique_ptr<ExchangeRest> (*make_rest)(const ExchangeConfig& exchange_config,
                                          net::HttpClient* http);
};

// The exchange this exchange name maps to, or nullptr if this build has none.
const OmsExchange* oms_exchange_for(std::string_view name);

}  // namespace axon::oms
