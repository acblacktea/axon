// Account and position models. Mirrors models/portfolio.py.
//
// Greeks are carried as fixed-point like everything else. They do not strictly
// need to be -- nobody reconciles a vega to the ninth decimal -- but a model
// where some numeric fields are Decimal and others are double invites exactly
// the implicit-conversion bugs the type is there to prevent.

#pragma once

#include <string>

#include "axon/core/decimal.h"
#include "axon/core/timestamp.h"

namespace axon::models {

using core::Price;
using core::Qty;
using core::Timestamp;

struct AccountSummary {
  std::string currency;
  std::string exchange;
  Price equity;
  Price balance;
  Price available_funds;
  Price initial_margin;
  Price maintenance_margin;
  Price margin_balance;
  Price delta_total;
  Price options_delta;
  Price options_gamma;
  Price options_vega;
  Price options_theta;
  Price futures_pl;
  Price options_pl;
  Price total_pl;
  Timestamp timestamp;
};

struct Position {
  std::string instrument;
  std::string exchange;
  std::string kind;       // "option" | "future"
  std::string direction;  // "buy" | "sell" | "zero"
  Qty size;
  Price average_price;
  Price mark_price;
  Price index_price;
  Price initial_margin;
  Price maintenance_margin;
  Price delta;
  Price gamma;
  Price vega;
  Price theta;
  Price total_profit_loss;
  Price floating_profit_loss;
  Price realized_profit_loss;
  Timestamp timestamp;

  bool is_flat() const noexcept { return size.is_zero(); }
};

}  // namespace axon::models
