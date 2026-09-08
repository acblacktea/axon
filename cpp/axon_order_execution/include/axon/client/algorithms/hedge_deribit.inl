// Implementation of hedge_deribit_options. Included from hedge_deribit.h.

#pragma once

#include "axon/models/ids.h"
#include "axon/util/logging.h"

namespace axon::client::algorithms {

namespace detail {
inline auto& hedge_log() {
  static auto logger = util::get_logger("client.hedge_deribit");
  return logger;
}
}  // namespace detail

template <typename Client>
HedgeDeribitResult hedge_deribit_options(Client& client,
                                         const HedgeDeribitParams& params) {
  auto& log = detail::hedge_log();
  HedgeDeribitResult result;
  result.internal_order_id = params.internal_order_id.empty()
                                 ? models::generate_internal_id()
                                 : params.internal_order_id;

  if (params.batch_amount.raw() <= 0) {
    result.error = "batch_amount must be positive";
    AXON_LOG_ERROR(log, "{}", result.error);
    return result;
  }

  core::Qty remaining = params.amount;
  AXON_LOG_INFO(log, "hedge {}: {} (maker) / {} (taker), total={} batch={}",
                  result.internal_order_id, params.symbol1, params.symbol2,
                  params.amount.to_string(), params.batch_amount.to_string());

  while (remaining.raw() > 0) {
    const core::Qty batch =
        remaining < params.batch_amount ? remaining : params.batch_amount;

    ChaseMakerParams chase;
    chase.exchange = "deribit";
    chase.instrument = params.symbol1;
    chase.side = params.side1;
    chase.amount = batch;
    chase.internal_order_id = result.internal_order_id;
    chase.max_attempt_seconds = params.chase_max_attempt_seconds;
    // Matches the Python, which leaves the fallback on. It means a batch that
    // does not get filled passively crosses the spread rather than stalling
    // the hedge -- paying the taker fee on both legs to stay flat.
    chase.taker_fallback = true;

    const auto maker = chase_maker_fill(client, chase);
    ++result.batches;

    // When the chase crossed for its remainder, that market order is what
    // completes the batch: it is a taker order on a liquid instrument, so the
    // hedge is sized against the whole batch. chase_maker deliberately does
    // NOT fold it into `filled` (acceptance is not a fill), so it is added
    // here, where the batch semantics make it the right call.
    const core::Qty acquired = maker.used_taker ? batch : maker.filled;
    result.maker_filled = result.maker_filled + maker.filled;

    if (acquired.raw() <= 0) {
      // No fill and no fallback order. DIVERGENCE from the Python, which
      // subtracts zero and loops forever on exactly this case; stopping and
      // reporting is the only useful behaviour.
      result.error = maker.error.empty()
                         ? "maker leg made no progress; stopping the hedge"
                         : maker.error;
      AXON_LOG_ERROR(log, "hedge {}: {}", result.internal_order_id, result.error);
      break;
    }

    models::OrderRequest taker;
    taker.instrument = params.symbol2;
    taker.side = params.side2;
    taker.amount = acquired;
    taker.order_type = models::OrderType::kMarket;
    taker.internal_order_id = result.internal_order_id;

    std::string error;
    if (client.place_order("deribit", taker, error).has_value()) {
      result.taker_sent = result.taker_sent + acquired;
    } else {
      // The maker leg is already on. Stopping here leaves that batch unhedged
      // and says so, which beats acquiring more of an exposure we have just
      // shown we cannot lay off.
      result.error = "hedge leg failed with " + acquired.to_string() +
                     " unhedged: " + error;
      AXON_LOG_ERROR(log, "hedge {}: {}", result.internal_order_id, result.error);
      break;
    }

    remaining = remaining - acquired;
    AXON_LOG_INFO(log, "batch {} done: acquired {}, remaining {}", result.batches,
                    acquired.to_string(), remaining.to_string());
  }

  AXON_LOG_INFO(log, "hedge {} finished: {} batches, maker filled {}, taker sent {}",
                  result.internal_order_id, result.batches,
                  result.maker_filled.to_string(), result.taker_sent.to_string());
  return result;
}

}  // namespace axon::client::algorithms
