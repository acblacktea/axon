#include "axon/ems/exchange_ops.h"

#include <array>

#include "axon/exchanges/binance/binance_ems.h"
#include "axon/exchanges/bybit/bybit_ems.h"
#include "axon/exchanges/deribit/deribit_ems.h"
#include "axon/exchanges/okx/okx_ems.h"

namespace axon::ems {

const ExchangeOps* ops_for(std::string_view exchange) {
  // Adding an exchange is adding its module and one line here. Nothing else in the
  // EMS asks which exchange a request is for.
  static const std::array<const ExchangeOps*, 4> kRegistry{
      &exchanges::deribit::ems_ops(),
      &exchanges::okx::ems_ops(),
      &exchanges::bybit::ems_ops(),
      &exchanges::binance::ems_ops(),
  };

  // Four entries: a linear scan beats hashing the string, and it runs once per
  // request rather than once per decision.
  for (const ExchangeOps* ops : kRegistry) {
    if (exchange == ops->name) {
      return ops;
    }
  }
  return nullptr;
}

}  // namespace axon::ems
