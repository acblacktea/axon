// OKX order management: the venue's private feed session, its REST
// snapshots. Counterpart of
// exchanges/okx/okx_ems.h on the OMS side.
#pragma once

#include "axon/oms/oms_venue.h"

namespace axon::oms::okx {

const OmsVenue& venue();

}  // namespace axon::oms::okx
