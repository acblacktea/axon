// make_venue_rest. Each venue's REST client lives in exchanges/<venue>/<venue>_oms.cpp.

#include "axon/oms/venue_rest.h"

#include "axon/oms/oms_venue.h"
#include "axon/util/logging.h"

namespace axon::oms {

std::unique_ptr<VenueRest> make_venue_rest(const ExchangeConfig& exchange,
                                           net::HttpClient* http) {
  const OmsVenue* venue = oms_venue_for(exchange.name);
  if (venue == nullptr) {
    AXON_LOG_ERROR(util::get_logger("oms.rest"), "no REST implementation for exchange '{}'",
                   exchange.name);
    return nullptr;
  }
  return venue->make_rest(exchange, http);
}

}  // namespace axon::oms
