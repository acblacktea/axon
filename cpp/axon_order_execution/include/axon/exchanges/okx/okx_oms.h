// OKX order management: the exchange's private feed session, its REST
// snapshots. Counterpart of
// exchanges/okx/okx_ems.h on the OMS side.
#pragma once

#include "axon/oms/oms_exchange.h"

namespace axon::exchanges::okx {

const oms::OmsExchange& oms_exchange();

}  // namespace axon::exchanges::okx
