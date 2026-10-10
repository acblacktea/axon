// Bybit order management: the venue's private feed session, its REST
// snapshots, its order-entry session. Counterpart of
// exchanges/bybit/bybit_ems.h on the OMS side.
#pragma once

#include "axon/oms/oms_venue.h"

namespace axon::oms::bybit {

const OmsVenue& venue();

}  // namespace axon::oms::bybit
