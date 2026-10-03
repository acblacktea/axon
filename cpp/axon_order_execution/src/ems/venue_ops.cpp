#include "axon/ems/venue_ops.h"

#include <array>

#include "axon/ems/binance/binance_ems.h"
#include "axon/ems/bybit/bybit_ems.h"
#include "axon/ems/deribit/deribit_ems.h"
#include "axon/ems/okx/okx_ems.h"

namespace axon::ems {

const VenueOps* ops_for(std::string_view exchange) {
  // Adding a venue is adding its module and one line here. Nothing else in the
  // EMS asks which venue a request is for.
  static const std::array<const VenueOps*, 4> kRegistry{
      &deribit::ops(), &okx::ops(), &bybit::ops(), &binance::ops(),
  };

  // Four entries: a linear scan beats hashing the string, and it runs once per
  // request rather than once per decision.
  for (const VenueOps* ops : kRegistry) {
    if (exchange == ops->name) {
      return ops;
    }
  }
  return nullptr;
}

}  // namespace axon::ems
