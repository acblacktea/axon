// Binance order management: the exchange's private feed session, its REST
// snapshots, its order-entry session. Counterpart of
// exchanges/binance/binance_ems.h on the OMS side.
#pragma once

#include "axon/oms/oms_exchange.h"

namespace axon::exchanges::binance {

const oms::OmsExchange& oms_exchange();

}  // namespace axon::exchanges::binance
