// Deribit order management: the exchange's private feed session, its REST
// snapshots. Counterpart of
// exchanges/deribit/deribit_ems.h on the OMS side.
#pragma once

#include "axon/oms/oms_exchange.h"

namespace axon::exchanges::deribit {

const oms::OmsExchange& oms_exchange();

}  // namespace axon::exchanges::deribit
