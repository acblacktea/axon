#include "axon/oms/oms_exchange.h"

#include <array>

#include "axon/exchanges/binance/binance_oms.h"
#include "axon/exchanges/bybit/bybit_oms.h"
#include "axon/exchanges/deribit/deribit_oms.h"
#include "axon/exchanges/okx/okx_oms.h"

namespace axon::oms {

const OmsExchange* oms_exchange_for(std::string_view name) {
  // Adding an exchange is adding its module and one line here, as on the EMS side.
  static const std::array<const OmsExchange*, 4> kRegistry{
      &exchanges::deribit::oms_exchange(),
      &exchanges::okx::oms_exchange(),
      &exchanges::bybit::oms_exchange(),
      &exchanges::binance::oms_exchange(),
  };
  for (const OmsExchange* exchange : kRegistry) {
    if (name == exchange->name) {
      return exchange;
    }
  }
  return nullptr;
}

}  // namespace axon::oms
