// make_exchange_rest. Each exchange's REST client lives in exchanges/<exchange>/<exchange>_oms.cpp.

#include "axon/oms/exchange_rest.h"

#include "axon/oms/oms_exchange.h"
#include "axon/util/logging.h"

namespace axon::oms {

std::unique_ptr<ExchangeRest> make_exchange_rest(const ExchangeConfig& exchange_config,
                                           net::HttpClient* http) {
  const OmsExchange* exchange = oms_exchange_for(exchange_config.name);
  if (exchange == nullptr) {
    AXON_LOG_ERROR(util::get_logger("oms.rest"), "no REST implementation for exchange_config '{}'",
                   exchange_config.name);
    return nullptr;
  }
  return exchange->make_rest(exchange_config, http);
}

}  // namespace axon::oms
