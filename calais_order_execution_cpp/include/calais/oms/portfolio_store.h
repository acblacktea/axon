// Account and position state. Ports oms/portfolio_manager.py and
// oms/fill_manager.py, plus the memory/Postgres repositories behind them.
//
// FILLS ARE IDEMPOTENT BY trade_id, and that is the whole design. The same
// fill legitimately arrives twice -- once on the WebSocket feed and once from
// reconciliation -- and add_fill() returns true only the FIRST time. Callbacks
// fire on that first insert alone, so a reconciler pass cannot re-report a
// fill a strategy already acted on, and cannot double the position.
//
// Single-threaded, like everything the engine loop owns: no locks, because
// nothing else touches these.

#pragma once

#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "calais/models/fill.h"
#include "calais/models/portfolio.h"

namespace calais::oms {

// --- persistence seams ------------------------------------------------------
class FillRepository {
 public:
  virtual ~FillRepository() = default;
  // Returns false if this trade_id was already stored. That return value is
  // the idempotency check, not a convenience.
  virtual bool save(const models::Fill& fill) = 0;
  virtual std::optional<models::Fill> get(const std::string& trade_id) = 0;
  virtual std::vector<models::Fill> by_order(const std::string& order_id) = 0;
  virtual std::vector<models::Fill> by_strategy(const std::string& strategy_id) = 0;
  virtual std::vector<models::Fill> all() = 0;
};

class AccountRepository {
 public:
  virtual ~AccountRepository() = default;
  virtual void save(const models::AccountSummary& account) = 0;
  virtual std::optional<models::AccountSummary> get(const std::string& exchange,
                                                    const std::string& currency) = 0;
};

class PositionRepository {
 public:
  virtual ~PositionRepository() = default;
  virtual void save(const models::Position& position) = 0;
  virtual std::vector<models::Position> by_exchange(const std::string& exchange) = 0;
};

// --- in-memory implementations, the default when no database is configured --
class InMemoryFillRepository final : public FillRepository {
 public:
  bool save(const models::Fill& fill) override {
    return fills_.emplace(fill.trade_id, fill).second;
  }
  std::optional<models::Fill> get(const std::string& trade_id) override {
    const auto it = fills_.find(trade_id);
    return it == fills_.end() ? std::nullopt : std::optional<models::Fill>(it->second);
  }
  std::vector<models::Fill> by_order(const std::string& order_id) override {
    return filter([&](const models::Fill& f) { return f.order_id == order_id; });
  }
  std::vector<models::Fill> by_strategy(const std::string& strategy_id) override {
    return filter([&](const models::Fill& f) {
      return f.strategy_id.value_or(std::string()) == strategy_id;
    });
  }
  std::vector<models::Fill> all() override {
    return filter([](const models::Fill&) { return true; });
  }

 private:
  template <typename Predicate>
  std::vector<models::Fill> filter(Predicate p) {
    std::vector<models::Fill> out;
    for (const auto& [_, fill] : fills_) {
      if (p(fill)) {
        out.push_back(fill);
      }
    }
    // Fills are facts on a timeline; every caller wants them in order.
    std::sort(out.begin(), out.end(),
              [](const models::Fill& a, const models::Fill& b) {
                return a.timestamp < b.timestamp;
              });
    return out;
  }
  std::unordered_map<std::string, models::Fill> fills_;
};

class InMemoryAccountRepository final : public AccountRepository {
 public:
  void save(const models::AccountSummary& account) override {
    accounts_[account.exchange + ":" + account.currency] = account;
  }
  std::optional<models::AccountSummary> get(const std::string& exchange,
                                            const std::string& currency) override {
    const auto it = accounts_.find(exchange + ":" + currency);
    return it == accounts_.end() ? std::nullopt
                                 : std::optional<models::AccountSummary>(it->second);
  }

 private:
  std::map<std::string, models::AccountSummary> accounts_;
};

class InMemoryPositionRepository final : public PositionRepository {
 public:
  void save(const models::Position& position) override {
    positions_[position.exchange + ":" + position.instrument] = position;
  }
  std::vector<models::Position> by_exchange(const std::string& exchange) override {
    std::vector<models::Position> out;
    for (const auto& [_, p] : positions_) {
      if (p.exchange == exchange) {
        out.push_back(p);
      }
    }
    return out;
  }

 private:
  std::map<std::string, models::Position> positions_;
};

// --- managers ---------------------------------------------------------------
class FillStore {
 public:
  using Callback = std::function<void(const models::Fill&)>;

  explicit FillStore(std::shared_ptr<FillRepository> repository = nullptr);

  // True only when the fill is NEW. A duplicate returns false and fires
  // nothing, which is what makes WebSocket-plus-reconciliation safe.
  bool add_fill(const models::Fill& fill);

  std::optional<models::Fill> get(const std::string& trade_id);
  std::vector<models::Fill> by_order(const std::string& order_id);
  std::vector<models::Fill> by_strategy(const std::string& strategy_id);
  std::vector<models::Fill> all();

  void register_callback(Callback callback) {
    callbacks_.push_back(std::move(callback));
  }

  std::uint64_t accepted() const noexcept { return accepted_; }
  std::uint64_t duplicates() const noexcept { return duplicates_; }

 private:
  std::shared_ptr<FillRepository> repository_;
  std::vector<Callback> callbacks_;
  std::uint64_t accepted_ = 0;
  std::uint64_t duplicates_ = 0;
};

class PortfolioStore {
 public:
  using AccountCallback = std::function<void(const models::AccountSummary&)>;
  using PositionCallback = std::function<void(const std::vector<models::Position>&)>;

  PortfolioStore(std::shared_ptr<AccountRepository> accounts = nullptr,
                 std::shared_ptr<PositionRepository> positions = nullptr);

  void update_account(const models::AccountSummary& account);
  // Replaces the whole snapshot for an exchange. Positions come from a
  // periodic REST pull, and a position that has closed simply disappears from
  // it -- merging instead of replacing would leave the closed one forever.
  void update_positions(const std::string& exchange,
                        const std::vector<models::Position>& positions);

  std::optional<models::AccountSummary> account(const std::string& exchange,
                                                const std::string& currency) const;
  std::vector<models::Position> positions(const std::string& exchange) const;

  void register_account_callback(AccountCallback cb) {
    account_callbacks_.push_back(std::move(cb));
  }
  void register_position_callback(PositionCallback cb) {
    position_callbacks_.push_back(std::move(cb));
  }

 private:
  std::shared_ptr<AccountRepository> account_repo_;
  std::shared_ptr<PositionRepository> position_repo_;
  std::map<std::string, models::AccountSummary> account_cache_;
  std::map<std::string, std::vector<models::Position>> position_cache_;
  std::vector<AccountCallback> account_callbacks_;
  std::vector<PositionCallback> position_callbacks_;
};

}  // namespace calais::oms
