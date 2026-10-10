// Deribit order management: the venue's private feed session, its REST
// snapshots. Counterpart of
// exchanges/deribit/deribit_ems.h on the OMS side.
#pragma once

#include "axon/oms/oms_venue.h"

namespace axon::oms::deribit {

const OmsVenue& venue();

}  // namespace axon::oms::deribit
