// Implementation of chase_maker_fill. Included from chase_maker.h.

#pragma once

#include <chrono>
#include <optional>
#include <thread>

#include "calais/core/clock.h"
#include "calais/util/logging.h"

namespace calais::client::algorithms {

namespace detail {
inline auto& chase_log() {
  static auto logger = util::get_logger("client.chase_maker");
  return logger;
}
}  // namespace detail

template <typename Client>
ChaseMakerResult chase_maker_fill(Client& client, const ChaseMakerParams& params) {
  auto& log = detail::chase_log();
  ChaseMakerResult result;

  const auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::duration<double>(params.max_attempt_seconds);
  std::optional<models::Order> current;
  std::string error;

  const auto sleep_a_bit = [&] {
    std::this_thread::sleep_for(
        std::chrono::duration<double>(params.poll_interval_seconds));
  };

  while (std::chrono::steady_clock::now() < deadline) {
    const auto ticker = client.get_ticker(params.exchange, params.instrument, error);
    if (!ticker.has_value()) {
      sleep_a_bit();
      continue;
    }

    // Quote at the FAR touch: a buy rests at the ask, a sell at the bid. That
    // is what makes it a maker order that is next in line rather than one
    // buried behind the queue.
    const core::Price price = params.side == models::OrderSide::kBuy
                                  ? ticker->best_ask_price
                                  : ticker->best_bid_price;
    if (price.raw() <= 0) {
      CALAIS_LOG_WARN(log, "invalid price for {}, retrying", params.instrument);
      sleep_a_bit();
      continue;
    }

    if (!current.has_value()) {
      models::OrderRequest request;
      request.instrument = params.instrument;
      request.side = params.side;
      request.amount = params.amount;
      request.order_type = models::OrderType::kLimit;
      request.price = price;
      request.internal_order_id = params.internal_order_id;

      current = client.place_order(params.exchange, request, error);
      if (!current.has_value()) {
        CALAIS_LOG_WARN(log, "order rejected: {}, retrying", error);
        sleep_a_bit();
        continue;
      }
      CALAIS_LOG_INFO(log, "placed maker order {}: {} {} {} @ {}",
                      current->order_id, models::to_string(params.side),
                      params.amount.to_string(), params.instrument,
                      price.to_string());
    } else if (!current->price.has_value() || *current->price != price) {
      // The book moved. Amend rather than cancel-and-replace: an amend usually
      // keeps queue position where a replacement always loses it.
      const auto amended = client.modify_order(params.exchange, current->order_id,
                                               std::nullopt, price, error);
      if (amended.has_value()) {
        current = amended;
        current->internal_order_id = params.internal_order_id;
        ++result.reprices;
      } else {
        // A failed amend is usually a fill or a cancel racing us; the status
        // check below settles it rather than guessing here.
        CALAIS_LOG_WARN(log, "could not amend order: {}", error);
      }
    }

    if (const auto updated = client.get_order(current->order_id, error);
        updated.has_value()) {
      current = updated;
      if (current->status == models::OrderStatus::kFilled) {
        CALAIS_LOG_INFO(log, "maker order filled: {}", current->order_id);
        result.filled = current->filled_amount;
        return result;
      }
      if (current->status == models::OrderStatus::kCancelled ||
          current->status == models::OrderStatus::kRejected) {
        CALAIS_LOG_INFO(log, "maker order {}: {}",
                        models::to_string(current->status), current->order_id);
        result.filled = current->filled_amount;
        return result;
      }
    }

    sleep_a_bit();
  }

  // Out of time. Cancel what is resting, then read the FINAL state -- a fill
  // can land between the cancel and the read, and assuming the cancel won
  // would double-count the remainder into the taker order.
  core::Qty filled;
  if (current.has_value()) {
    client.cancel_order(params.exchange, current->order_id, error);
    CALAIS_LOG_INFO(log, "cancelled unfilled maker order {}", current->order_id);
    if (const auto final_order = client.get_order(current->order_id, error);
        final_order.has_value()) {
      current = final_order;
    }
    filled = current->filled_amount;
  }
  result.filled = filled;

  const core::Qty remaining = params.amount - filled;
  if (params.taker_fallback && remaining.raw() > 0) {
    CALAIS_LOG_INFO(log, "chase timed out; crossing for the remaining {}",
                    remaining.to_string());
    models::OrderRequest taker;
    taker.instrument = params.instrument;
    taker.side = params.side;
    taker.amount = remaining;
    taker.order_type = models::OrderType::kMarket;
    taker.internal_order_id = params.internal_order_id;

    if (client.place_order(params.exchange, taker, error).has_value()) {
      // DIVERGENCE from the Python, which adds `remaining` to the total the
      // moment the market order is accepted. Acceptance is not a fill: the
      // real amount arrives on the feed. Reporting it as filled here would
      // overstate the position on any partial or rejected taker order.
      result.used_taker = true;
    } else {
      result.error = "taker fallback failed: " + error;
      CALAIS_LOG_ERROR(log, "{}", result.error);
    }
  }

  CALAIS_LOG_INFO(log, "chase ended: filled {}/{} after {}s ({} reprices)",
                  result.filled.to_string(), params.amount.to_string(),
                  params.max_attempt_seconds, result.reprices);
  return result;
}

}  // namespace calais::client::algorithms
