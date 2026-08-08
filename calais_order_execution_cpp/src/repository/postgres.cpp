#include "calais/repository/postgres.h"

#include <pqxx/pqxx>

#include <condition_variable>
#include <deque>
#include <mutex>
#include <variant>

#include "calais/util/logging.h"
#include "calais/util/metrics.h"

namespace calais::repository {
namespace {

auto& log() {
  static auto logger = util::get_logger("repository.postgres");
  return logger;
}

// Schema copied from repository/order_postgres.py and fill_postgres.py. The
// two implementations must produce interchangeable rows.
constexpr const char* kCreateOrders = R"SQL(
CREATE TABLE IF NOT EXISTS orders (
    order_id          TEXT PRIMARY KEY,
    exchange          TEXT NOT NULL,
    instrument        TEXT NOT NULL,
    side              TEXT NOT NULL,
    order_type        TEXT NOT NULL,
    amount            DOUBLE PRECISION NOT NULL,
    status            TEXT NOT NULL,
    internal_order_id TEXT,
    price             DOUBLE PRECISION,
    filled_amount     DOUBLE PRECISION DEFAULT 0,
    average_price     DOUBLE PRECISION,
    client_order_id   TEXT,
    label             TEXT,
    liquidity         TEXT DEFAULT 'maker',
    post_only         BOOLEAN DEFAULT FALSE,
    reject_post_only  BOOLEAN DEFAULT FALSE,
    strategy_id       TEXT,
    created_at        TIMESTAMPTZ NOT NULL,
    updated_at        TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orders_strategy_id ON orders(strategy_id);
CREATE INDEX IF NOT EXISTS idx_orders_status ON orders(status);
CREATE INDEX IF NOT EXISTS idx_orders_client_order_id ON orders(client_order_id);
CREATE INDEX IF NOT EXISTS idx_orders_instrument ON orders(instrument);
)SQL";

constexpr const char* kCreateFills = R"SQL(
CREATE TABLE IF NOT EXISTS fills (
    trade_id      TEXT PRIMARY KEY,
    order_id      TEXT NOT NULL,
    exchange      TEXT NOT NULL,
    instrument    TEXT NOT NULL,
    side          TEXT NOT NULL,
    amount        DOUBLE PRECISION NOT NULL,
    price         DOUBLE PRECISION NOT NULL,
    fee           DOUBLE PRECISION NOT NULL DEFAULT 0,
    fee_currency  TEXT NOT NULL DEFAULT '',
    liquidity     TEXT NOT NULL DEFAULT 'maker',
    timestamp     TIMESTAMPTZ NOT NULL,
    index_price   DOUBLE PRECISION,
    mark_price    DOUBLE PRECISION,
    iv            DOUBLE PRECISION,
    profit_loss   DOUBLE PRECISION,
    label         TEXT,
    strategy_id   TEXT
);
CREATE INDEX IF NOT EXISTS idx_fills_order_id ON fills(order_id);
CREATE INDEX IF NOT EXISTS idx_fills_strategy_id ON fills(strategy_id);
)SQL";

constexpr const char* kCreateAccounts = R"SQL(
CREATE TABLE IF NOT EXISTS account_summaries (
    exchange     TEXT NOT NULL,
    currency     TEXT NOT NULL,
    equity       DOUBLE PRECISION NOT NULL,
    balance      DOUBLE PRECISION NOT NULL,
    available_funds DOUBLE PRECISION NOT NULL,
    initial_margin  DOUBLE PRECISION NOT NULL,
    maintenance_margin DOUBLE PRECISION NOT NULL,
    margin_balance  DOUBLE PRECISION NOT NULL,
    delta_total  DOUBLE PRECISION NOT NULL,
    total_pl     DOUBLE PRECISION NOT NULL,
    timestamp    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, currency)
);
)SQL";

// Positions are a SNAPSHOT keyed by (exchange, instrument), not a log: the
// venue reports the current book on every pull, so the row is overwritten.
// Greeks are NOT NULL and zero for a future, matching the model, where they
// are plain Price rather than optional.
constexpr const char* kCreatePositions = R"SQL(
CREATE TABLE IF NOT EXISTS position_summaries (
    exchange      TEXT NOT NULL,
    instrument    TEXT NOT NULL,
    kind          TEXT NOT NULL DEFAULT '',
    direction     TEXT NOT NULL DEFAULT 'zero',
    size          DOUBLE PRECISION NOT NULL,
    average_price DOUBLE PRECISION NOT NULL,
    mark_price    DOUBLE PRECISION NOT NULL,
    index_price   DOUBLE PRECISION NOT NULL,
    initial_margin     DOUBLE PRECISION NOT NULL,
    maintenance_margin DOUBLE PRECISION NOT NULL,
    delta         DOUBLE PRECISION NOT NULL,
    gamma         DOUBLE PRECISION NOT NULL,
    vega          DOUBLE PRECISION NOT NULL,
    theta         DOUBLE PRECISION NOT NULL,
    total_profit_loss    DOUBLE PRECISION NOT NULL,
    floating_profit_loss DOUBLE PRECISION NOT NULL,
    realized_profit_loss DOUBLE PRECISION NOT NULL,
    timestamp     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (exchange, instrument)
);
CREATE INDEX IF NOT EXISTS idx_positions_exchange ON position_summaries(exchange);
)SQL";

// ON CONFLICT keeps an internal_order_id that a later venue update does not
// carry -- COALESCE, matching the Python. Losing it would orphan the order
// from the strategy that placed it.
constexpr const char* kUpsertOrder = R"SQL(
INSERT INTO orders (order_id, exchange, instrument, side, order_type, amount,
    status, internal_order_id, price, filled_amount, average_price,
    client_order_id, label, liquidity, post_only, reject_post_only,
    strategy_id, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
ON CONFLICT (order_id) DO UPDATE SET
    exchange = EXCLUDED.exchange,
    instrument = EXCLUDED.instrument,
    side = EXCLUDED.side,
    order_type = EXCLUDED.order_type,
    amount = EXCLUDED.amount,
    status = EXCLUDED.status,
    internal_order_id = COALESCE(EXCLUDED.internal_order_id, orders.internal_order_id),
    price = EXCLUDED.price,
    filled_amount = EXCLUDED.filled_amount,
    average_price = EXCLUDED.average_price,
    client_order_id = EXCLUDED.client_order_id,
    label = EXCLUDED.label,
    liquidity = EXCLUDED.liquidity,
    post_only = EXCLUDED.post_only,
    reject_post_only = EXCLUDED.reject_post_only,
    strategy_id = COALESCE(EXCLUDED.strategy_id, orders.strategy_id),
    updated_at = EXCLUDED.updated_at
)SQL";

// Fills are append-only facts keyed by the venue's trade_id, which is exactly
// what makes DO NOTHING correct: the same fill legitimately arrives twice,
// once on the feed and once from reconciliation.
constexpr const char* kInsertFill = R"SQL(
INSERT INTO fills (trade_id, order_id, exchange, instrument, side, amount,
    price, fee, fee_currency, liquidity, timestamp, index_price, mark_price,
    iv, profit_loss, label, strategy_id)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT (trade_id) DO NOTHING
)SQL";

constexpr const char* kUpsertAccount = R"SQL(
INSERT INTO account_summaries (exchange, currency, equity, balance,
    available_funds, initial_margin, maintenance_margin, margin_balance,
    delta_total, total_pl, timestamp)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
ON CONFLICT (exchange, currency) DO UPDATE SET
    equity = EXCLUDED.equity, balance = EXCLUDED.balance,
    available_funds = EXCLUDED.available_funds,
    initial_margin = EXCLUDED.initial_margin,
    maintenance_margin = EXCLUDED.maintenance_margin,
    margin_balance = EXCLUDED.margin_balance,
    delta_total = EXCLUDED.delta_total, total_pl = EXCLUDED.total_pl,
    timestamp = EXCLUDED.timestamp
)SQL";

constexpr const char* kUpsertPosition = R"SQL(
INSERT INTO position_summaries (exchange, instrument, kind, direction, size,
    average_price, mark_price, index_price, initial_margin, maintenance_margin,
    delta, gamma, vega, theta, total_profit_loss, floating_profit_loss,
    realized_profit_loss, timestamp)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
ON CONFLICT (exchange, instrument) DO UPDATE SET
    kind = EXCLUDED.kind, direction = EXCLUDED.direction,
    size = EXCLUDED.size, average_price = EXCLUDED.average_price,
    mark_price = EXCLUDED.mark_price, index_price = EXCLUDED.index_price,
    initial_margin = EXCLUDED.initial_margin,
    maintenance_margin = EXCLUDED.maintenance_margin,
    delta = EXCLUDED.delta, gamma = EXCLUDED.gamma, vega = EXCLUDED.vega,
    theta = EXCLUDED.theta,
    total_profit_loss = EXCLUDED.total_profit_loss,
    floating_profit_loss = EXCLUDED.floating_profit_loss,
    realized_profit_loss = EXCLUDED.realized_profit_loss,
    timestamp = EXCLUDED.timestamp
)SQL";

std::string iso(core::Timestamp t) { return t.to_iso8601() + "+00:00"; }

}  // namespace

using Row = std::variant<models::Order, models::Fill, models::AccountSummary,
                         models::Position>;

struct PostgresWriter::Impl {
  std::unique_ptr<pqxx::connection> conn;
  std::string dsn;

  std::thread thread;
  std::atomic<bool> running{false};

  // A mutex-and-condvar queue rather than the lock-free ring used elsewhere:
  // the writer thread SHOULD sleep when idle. Burning a core to poll a
  // database that gets a row per order would be the wrong trade entirely.
  mutable std::mutex mutex;
  std::condition_variable cv;
  std::deque<Row> queue;
  static constexpr std::size_t kMaxQueue = 100000;

  mutable std::mutex stats_mutex;
  PersistenceStats stats;

  void run();
  // Takes the enclosing transaction: the batch is ONE transaction, and
  // opening another inside it would need a pqxx subtransaction and buy nothing.
  void write_row(pqxx::work& tx, const Row& row);
};

PostgresWriter::PostgresWriter() : impl_(std::make_unique<Impl>()) {}

PostgresWriter::~PostgresWriter() { stop(); }

void PostgresWriter::start(const DatabaseConfig& config) {
  impl_->dsn = config.dsn;
  // Connect synchronously here, at startup, where blocking is fine and a
  // failure should stop the process rather than be discovered later.
  impl_->conn = std::make_unique<pqxx::connection>(config.dsn);

  pqxx::work tx(*impl_->conn);
  tx.exec(kCreateOrders);
  tx.exec(kCreateFills);
  tx.exec(kCreateAccounts);
  tx.exec(kCreatePositions);
  tx.commit();

  impl_->conn->prepare("upsert_order", kUpsertOrder);
  impl_->conn->prepare("insert_fill", kInsertFill);
  impl_->conn->prepare("upsert_account", kUpsertAccount);
  impl_->conn->prepare("upsert_position", kUpsertPosition);

  {
    std::lock_guard<std::mutex> lock(impl_->stats_mutex);
    impl_->stats.connected = true;
  }
  impl_->running.store(true, std::memory_order_release);
  impl_->thread = std::thread([this] { impl_->run(); });
  CALAIS_LOG_INFO(log(), "persistence connected, writer thread started");
}

void PostgresWriter::stop() {
  if (!impl_->running.exchange(false, std::memory_order_acq_rel)) {
    return;
  }
  impl_->cv.notify_all();
  if (impl_->thread.joinable()) {
    impl_->thread.join();
  }
  impl_->conn.reset();
  CALAIS_LOG_INFO(log(), "persistence stopped");
}

namespace {

template <typename T>
bool enqueue(PostgresWriter::Impl& impl, T&& row) {
  {
    std::lock_guard<std::mutex> lock(impl.mutex);
    if (impl.queue.size() >= PostgresWriter::Impl::kMaxQueue) {
      // DROP rather than block. Blocking here would stall the loop that feeds
      // the order book on a database problem, turning a persistence outage
      // into a trading outage.
      std::lock_guard<std::mutex> slock(impl.stats_mutex);
      ++impl.stats.dropped;
      return false;
    }
    impl.queue.emplace_back(std::forward<T>(row));
  }
  impl.cv.notify_one();
  return true;
}

}  // namespace

bool PostgresWriter::write_order(const models::Order& order) {
  return impl_->running.load(std::memory_order_acquire) && enqueue(*impl_, Row(order));
}
bool PostgresWriter::write_fill(const models::Fill& fill) {
  return impl_->running.load(std::memory_order_acquire) && enqueue(*impl_, Row(fill));
}
bool PostgresWriter::write_account(const models::AccountSummary& account) {
  return impl_->running.load(std::memory_order_acquire) && enqueue(*impl_, Row(account));
}
bool PostgresWriter::write_position(const models::Position& position) {
  return impl_->running.load(std::memory_order_acquire) &&
         enqueue(*impl_, Row(position));
}

PersistenceStats PostgresWriter::stats() const {
  std::lock_guard<std::mutex> lock(impl_->stats_mutex);
  return impl_->stats;
}

// ---------------------------------------------------------------------------
void PostgresWriter::Impl::run() {
  while (running.load(std::memory_order_acquire)) {
    std::deque<Row> batch;
    {
      std::unique_lock<std::mutex> lock(mutex);
      cv.wait_for(lock, std::chrono::milliseconds(100), [this] {
        return !queue.empty() || !running.load(std::memory_order_acquire);
      });
      // Take everything queued in one go: a batch inside a single transaction
      // is dramatically cheaper than a round trip per row, and under load the
      // batch grows exactly when that matters.
      batch.swap(queue);
    }
    if (batch.empty()) {
      continue;
    }

    try {
      pqxx::work tx(*conn);
      for (const auto& row : batch) {
        write_row(tx, row);
      }
      tx.commit();
    } catch (const std::exception& e) {
      // A failed batch is lost. Reconnecting and retrying it would risk
      // reordering rows against newer ones already queued; the venue plus
      // reconciliation is the recovery path, not this table.
      CALAIS_LOG_ERROR(log(), "batch write failed ({} rows): {}", batch.size(),
                       e.what());
      std::lock_guard<std::mutex> lock(stats_mutex);
      stats.write_errors += batch.size();
      util::get_metrics().inc_db_write_failure("batch");

      try {
        conn = std::make_unique<pqxx::connection>(dsn);
        conn->prepare("upsert_order", kUpsertOrder);
        conn->prepare("insert_fill", kInsertFill);
        conn->prepare("upsert_account", kUpsertAccount);
        conn->prepare("upsert_position", kUpsertPosition);
        stats.connected = true;
      } catch (const std::exception& reconnect_error) {
        stats.connected = false;
        CALAIS_LOG_ERROR(log(), "reconnect failed: {}", reconnect_error.what());
      }
    }
  }
}

void PostgresWriter::Impl::write_row(pqxx::work& tx, const Row& row) {
  std::visit(
      [&](const auto& value) {
        using T = std::decay_t<decltype(value)>;
        if constexpr (std::is_same_v<T, models::Order>) {
          tx.exec(pqxx::prepped{"upsert_order"}, pqxx::params{value.order_id, value.exchange, value.instrument,
              std::string(models::to_string(value.side)),
              std::string(models::to_string(value.order_type)),
              value.amount.to_double(),
              std::string(models::to_string(value.status)),
              value.internal_order_id, value.price.has_value()
                                           ? std::optional<double>(value.price->to_double())
                                           : std::nullopt,
              value.filled_amount.to_double(),
              value.average_price.has_value()
                  ? std::optional<double>(value.average_price->to_double())
                  : std::nullopt,
              value.client_order_id, value.label,
              std::string(models::to_string(value.liquidity)), value.post_only,
              value.reject_post_only, value.strategy_id, iso(value.created_at),
              iso(value.updated_at)});
          std::lock_guard<std::mutex> lock(stats_mutex);
          ++stats.orders_written;
        } else if constexpr (std::is_same_v<T, models::Fill>) {
          auto opt = [](const std::optional<core::Price>& p) {
            return p.has_value() ? std::optional<double>(p->to_double())
                                 : std::nullopt;
          };
          tx.exec(pqxx::prepped{"insert_fill"}, pqxx::params{value.trade_id, value.order_id,
                           value.exchange, value.instrument,
                           std::string(models::to_string(value.side)),
                           value.amount.to_double(), value.price.to_double(),
                           value.fee.to_double(), value.fee_currency,
                           std::string(models::to_string(value.liquidity)),
                           iso(value.timestamp), opt(value.index_price),
                           opt(value.mark_price), opt(value.iv),
                           opt(value.profit_loss), value.label, value.strategy_id});
          std::lock_guard<std::mutex> lock(stats_mutex);
          ++stats.fills_written;
        } else if constexpr (std::is_same_v<T, models::AccountSummary>) {
          tx.exec(pqxx::prepped{"upsert_account"}, pqxx::params{value.exchange, value.currency,
                           value.equity.to_double(), value.balance.to_double(),
                           value.available_funds.to_double(),
                           value.initial_margin.to_double(),
                           value.maintenance_margin.to_double(),
                           value.margin_balance.to_double(),
                           value.delta_total.to_double(),
                           value.total_pl.to_double(), iso(value.timestamp)});
          std::lock_guard<std::mutex> lock(stats_mutex);
          ++stats.accounts_written;
        } else {
          tx.exec(pqxx::prepped{"upsert_position"},
                  pqxx::params{value.exchange, value.instrument, value.kind,
                               value.direction, value.size.to_double(),
                               value.average_price.to_double(),
                               value.mark_price.to_double(),
                               value.index_price.to_double(),
                               value.initial_margin.to_double(),
                               value.maintenance_margin.to_double(),
                               value.delta.to_double(), value.gamma.to_double(),
                               value.vega.to_double(), value.theta.to_double(),
                               value.total_profit_loss.to_double(),
                               value.floating_profit_loss.to_double(),
                               value.realized_profit_loss.to_double(),
                               iso(value.timestamp)});
          std::lock_guard<std::mutex> lock(stats_mutex);
          ++stats.positions_written;
        }
      },
      row);
}

}  // namespace calais::repository
