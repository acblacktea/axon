#include "axon/oms/portfolio_store.h"

#include <algorithm>

#include "axon/util/logging.h"
#include "axon/util/metrics.h"

namespace axon::oms {
namespace {

auto& log() {
  static auto logger = util::get_logger("oms.portfolio");
  return logger;
}

}  // namespace

// ---------------------------------------------------------------------------
FillStore::FillStore(std::shared_ptr<FillRepository> repository)
    : repository_(repository ? std::move(repository)
                             : std::make_shared<InMemoryFillRepository>()) {}

bool FillStore::add_fill(const models::Fill& fill) {
  bool inserted = false;
  try {
    inserted = repository_->save(fill);
  } catch (const std::exception& e) {
    AXON_LOG_ERROR(log(), "failed to persist fill {}: {}", fill.trade_id, e.what());
    util::get_metrics().inc_db_write_failure("fills");
    return false;
  }

  if (!inserted) {
    // The same fill arriving from the feed AND from reconciliation is normal.
    // Silence here is correct; firing callbacks again would double a position.
    ++duplicates_;
    return false;
  }

  ++accepted_;
  AXON_LOG_INFO(log(), "fill {} order={} {} {} amount={} price={} {} fee={} {}",
                  fill.trade_id, fill.order_id, fill.instrument,
                  models::to_string(fill.side), fill.amount.to_string(),
                  fill.price.to_string(), models::to_string(fill.liquidity),
                  fill.fee.to_string(), fill.fee_currency);

  for (const auto& callback : callbacks_) {
    try {
      callback(fill);
    } catch (const std::exception& e) {
      AXON_LOG_ERROR(log(), "fill callback threw: {}", e.what());
    }
  }
  return true;
}

std::optional<models::Fill> FillStore::get(const std::string& trade_id) {
  return repository_->get(trade_id);
}
std::vector<models::Fill> FillStore::by_order(const std::string& order_id) {
  return repository_->by_order(order_id);
}
std::vector<models::Fill> FillStore::by_strategy(const std::string& strategy_id) {
  return repository_->by_strategy(strategy_id);
}
std::vector<models::Fill> FillStore::all() { return repository_->all(); }

// ---------------------------------------------------------------------------
PortfolioStore::PortfolioStore(std::shared_ptr<AccountRepository> accounts,
                               std::shared_ptr<PositionRepository> positions)
    : account_repo_(accounts ? std::move(accounts)
                             : std::make_shared<InMemoryAccountRepository>()),
      position_repo_(positions ? std::move(positions)
                               : std::make_shared<InMemoryPositionRepository>()) {}

void PortfolioStore::update_account(const models::AccountSummary& account) {
  account_cache_[account.exchange + ":" + account.currency] = account;
  try {
    account_repo_->save(account);
  } catch (const std::exception& e) {
    AXON_LOG_ERROR(log(), "failed to persist account {}/{}: {}", account.exchange,
                     account.currency, e.what());
    util::get_metrics().inc_db_write_failure("accounts");
  }

  // maintenance_margin / equity is the standard liquidation-distance proxy.
  // Non-positive equity means the account is already in trouble, so report 1.0
  // rather than skipping the sample -- a gap in the gauge looks like the
  // engine died, which is the wrong alert.
  const double ratio = account.equity.raw() > 0
                           ? account.maintenance_margin.to_double() /
                                 account.equity.to_double()
                           : 1.0;
  util::get_metrics().set_account_margin_ratio(account.exchange, account.currency,
                                               ratio);

  for (const auto& cb : account_callbacks_) {
    try {
      cb(account);
    } catch (const std::exception& e) {
      AXON_LOG_ERROR(log(), "account callback threw: {}", e.what());
    }
  }
}

void PortfolioStore::update_positions(const std::string& exchange,
                                      const std::vector<models::Position>& positions) {
  // REPLACE, do not merge. A position that has been closed simply vanishes
  // from the venue's snapshot; merging would leave it in the cache forever and
  // a strategy would keep hedging a position it no longer has.
  position_cache_[exchange] = positions;

  for (const auto& p : positions) {
    try {
      position_repo_->save(p);
    } catch (const std::exception& e) {
      AXON_LOG_ERROR(log(), "failed to persist position {}: {}", p.instrument,
                       e.what());
      util::get_metrics().inc_db_write_failure("positions");
    }
  }

  for (const auto& cb : position_callbacks_) {
    try {
      cb(positions);
    } catch (const std::exception& e) {
      AXON_LOG_ERROR(log(), "position callback threw: {}", e.what());
    }
  }
}

std::optional<models::AccountSummary> PortfolioStore::account(
    const std::string& exchange, const std::string& currency) const {
  const auto it = account_cache_.find(exchange + ":" + currency);
  return it == account_cache_.end()
             ? std::nullopt
             : std::optional<models::AccountSummary>(it->second);
}

std::vector<models::Position> PortfolioStore::positions(
    const std::string& exchange) const {
  const auto it = position_cache_.find(exchange);
  return it == position_cache_.end() ? std::vector<models::Position>{} : it->second;
}

}  // namespace axon::oms
