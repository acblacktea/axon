// Binance order management: the venue's private feed session, its REST
// snapshots, its order-entry session. Counterpart of
// exchanges/binance/binance_ems.h on the OMS side.
#pragma once

#include "axon/oms/oms_venue.h"

namespace axon::oms::binance {

const OmsVenue& venue();

}  // namespace axon::oms::binance
