// PostgreSQL persistence, on a dedicated writer thread.
//
// WHY A THREAD RATHER THAN ASYNC. libpqxx is synchronous, and making libpq
// asynchronous means driving PQsendQuery from the poll loop -- possible, but a
// database write is not on the critical path and does not deserve that
// complexity. Instead the engine thread pushes a row onto a lock-free ring and
// moves on; the writer thread does the blocking work.
//
// THE ENGINE THREAD NEVER BLOCKS ON THE DATABASE. Not on a slow query, not on
// a reconnect, not on a full disk. If the ring fills the row is DROPPED and a
// counter increments -- because the alternative, blocking the loop that feeds
// the order book, would turn a database problem into a trading outage.
//
// That trade means persistence is best-effort under load. The venue is the
// record of truth and reconciliation is what recovers state; this table is for
// analysis and post-trade, not for deciding whether an order is live.
//
// Schema is identical to repository/order_postgres.py and fill_postgres.py, so
// both implementations write the same rows and either can read the other's.

#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "calais/config.h"
#include "calais/models/fill.h"
#include "calais/models/order.h"
#include "calais/models/portfolio.h"
#include "calais/oms/order_store.h"
#include "calais/oms/portfolio_store.h"

namespace calais::repository {

struct PersistenceStats {
  std::uint64_t orders_written = 0;
  std::uint64_t fills_written = 0;
  std::uint64_t accounts_written = 0;
  std::uint64_t positions_written = 0;
  // Rows dropped because the ring was full. Alert on any sustained rate: it
  // means the writer cannot keep up and history is being lost.
  std::uint64_t dropped = 0;
  std::uint64_t write_errors = 0;
  bool connected = false;
};

class PostgresWriter {
 public:
  PostgresWriter();
  ~PostgresWriter();

  PostgresWriter(const PostgresWriter&) = delete;
  PostgresWriter& operator=(const PostgresWriter&) = delete;

  // Connects, creates the tables if they are missing, and starts the writer
  // thread. Throws if the initial connection fails -- an engine configured for
  // persistence that silently runs without it is worse than one that refuses
  // to start.
  void start(const DatabaseConfig& config);
  void stop();

  // All non-blocking. Return false if the row was dropped.
  bool write_order(const models::Order& order);
  bool write_fill(const models::Fill& fill);
  bool write_account(const models::AccountSummary& account);
  bool write_position(const models::Position& position);

  PersistenceStats stats() const;

 public:
  // Public so the free enqueue() helper in the .cpp can reach it. Callers
  // outside that file have no way to use it -- it is only forward-declared
  // here.
  struct Impl;

 private:
  std::unique_ptr<Impl> impl_;
};

// Adapter so OrderStore can persist through the writer without knowing it
// exists. Reads fall back to nothing: the cache is the read path, and a
// synchronous SELECT from the engine thread is exactly what this design
// exists to avoid.
class PostgresOrderRepository final : public oms::OrderRepository {
 public:
  explicit PostgresOrderRepository(PostgresWriter* writer) : writer_(writer) {}

  void save(const models::Order& order) override { writer_->write_order(order); }
  void update(const models::Order& order) override { writer_->write_order(order); }

  // Deliberately always empty. A blocking SELECT on the engine thread would
  // stall every venue feed; the in-memory cache answers reads, and anything
  // not in it is recovered by reconciliation rather than by the database.
  std::optional<models::Order> get(const std::string&) override {
    return std::nullopt;
  }
  std::vector<models::Order> all() override { return {}; }

 private:
  PostgresWriter* writer_;
};

// Fills, accounts and positions take the same treatment as orders: the write
// goes to the queue, the read comes from the in-memory cache the store keeps
// anyway. Each adapter therefore delegates reads to an in-memory twin rather
// than issuing a blocking SELECT.
class PostgresFillRepository final : public oms::FillRepository {
 public:
  explicit PostgresFillRepository(PostgresWriter* writer) : writer_(writer) {}

  // The in-memory twin decides novelty; Postgres would too (ON CONFLICT DO
  // NOTHING) but only after a round trip we are not willing to wait for.
  bool save(const models::Fill& fill) override {
    if (!cache_.save(fill)) {
      return false;
    }
    writer_->write_fill(fill);
    return true;
  }
  std::optional<models::Fill> get(const std::string& trade_id) override {
    return cache_.get(trade_id);
  }
  std::vector<models::Fill> by_order(const std::string& order_id) override {
    return cache_.by_order(order_id);
  }
  std::vector<models::Fill> by_strategy(const std::string& strategy_id) override {
    return cache_.by_strategy(strategy_id);
  }
  std::vector<models::Fill> all() override { return cache_.all(); }

 private:
  PostgresWriter* writer_;
  oms::InMemoryFillRepository cache_;
};

class PostgresAccountRepository final : public oms::AccountRepository {
 public:
  explicit PostgresAccountRepository(PostgresWriter* writer) : writer_(writer) {}

  void save(const models::AccountSummary& account) override {
    cache_.save(account);
    writer_->write_account(account);
  }
  std::optional<models::AccountSummary> get(const std::string& exchange,
                                            const std::string& currency) override {
    return cache_.get(exchange, currency);
  }

 private:
  PostgresWriter* writer_;
  oms::InMemoryAccountRepository cache_;
};

class PostgresPositionRepository final : public oms::PositionRepository {
 public:
  explicit PostgresPositionRepository(PostgresWriter* writer) : writer_(writer) {}

  void save(const models::Position& position) override {
    cache_.save(position);
    writer_->write_position(position);
  }
  std::vector<models::Position> by_exchange(const std::string& exchange) override {
    return cache_.by_exchange(exchange);
  }

 private:
  PostgresWriter* writer_;
  oms::InMemoryPositionRepository cache_;
};

}  // namespace calais::repository
