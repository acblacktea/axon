#include "axon/oms/oms_venue.h"

#include <array>

#include "axon/exchanges/binance/binance_oms.h"
#include "axon/exchanges/bybit/bybit_oms.h"
#include "axon/exchanges/deribit/deribit_oms.h"
#include "axon/exchanges/okx/okx_oms.h"

namespace axon::oms {

const OmsVenue* oms_venue_for(std::string_view exchange) {
  // Adding a venue is adding its module and one line here, as on the EMS side.
  static const std::array<const OmsVenue*, 4> kRegistry{
      &deribit::venue(), &okx::venue(), &bybit::venue(), &binance::venue(),
  };
  for (const OmsVenue* venue : kRegistry) {
    if (exchange == venue->name) {
      return venue;
    }
  }
  return nullptr;
}

}  // namespace axon::oms
