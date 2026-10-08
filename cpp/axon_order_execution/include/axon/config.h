// Engine configuration. Mirrors config.py, including its YAML key names and
// defaults, so one config.yaml drives either implementation.
//
// Where the C++ needs a knob the Python has no equivalent for -- buffer sizes,
// core pinning -- it goes under a `cpp:` section that the Python loader
// ignores. Adding those at the top level would make a shared file fail to load
// on the Python side.

#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "axon/core/decimal.h"

namespace axon {

struct ExchangeConfig {
  std::string name;
  std::string env = "testnet";
  std::string api_key;
  std::string api_secret;
  std::string passphrase;  // OKX only

  bool is_testnet() const noexcept { return env == "testnet"; }
};

struct ReconciliationConfig {
  bool enabled = true;
  int interval_seconds = 30;
};

struct FillReconciliationConfig {
  bool enabled = true;
  int interval_seconds = 60;
  int lookback_seconds = 86400;
  int overlap_seconds = 30;
};

struct WebSocketConfig {
  int heartbeat_interval_seconds = 10;
  int reconnect_delay_seconds = 1;
  int max_reconnect_delay_seconds = 60;
  // Also the budget for each handshake stage: connect, authenticate,
  // subscribe. A stage that overruns it drops the connection and reconnects.
  int request_timeout_seconds = 30;
  // Read for config-file compatibility with the Python, which retries an
  // individual auth/subscribe RPC this many times. This implementation
  // reconnects the session instead -- see venue_session.cpp -- so the value is
  // accepted but not used.
  int max_request_retries = 3;
  double retry_delay_seconds = 1.0;
};

struct PortfolioConfig {
  std::vector<std::string> currencies{"BTC"};
  int position_refresh_interval_seconds = 10;
};

struct ZmqConfig {
  std::string router_endpoint = "tcp://*:5555";
  std::string pub_endpoint = "tcp://*:5556";
  std::string router_connect = "tcp://localhost:5555";
  std::string pub_connect = "tcp://localhost:5556";
};

struct DatabaseConfig {
  std::string dsn = "postgresql://localhost:5432/axon";
  int pool_min = 2;
  int pool_max = 10;
};

struct MetricsConfig {
  bool enabled = true;
  std::string host = "0.0.0.0";
  int port = 9100;
};

// C++-only knobs, read from the `cpp:` section. Absent from config.py, and
// under their own key so a shared config.yaml still loads there.
struct RuntimeConfig {
  std::size_t rx_buffer_bytes = 1u << 18;
  std::size_t tx_buffer_bytes = 1u << 16;
  std::size_t max_message_bytes = 8u << 20;

  // Pin the engine loop to this core. Negative means do not pin. Linux only;
  // on macOS it logs that it could not and carries on.
  int engine_core = -1;

  // Lock all pages into RAM so the hot path cannot take a page fault.
  bool lock_memory = false;

  // Orders in flight. The pool never grows, so this is a hard ceiling and
  // should be set from the worst case, not the average.
  std::uint32_t order_pool_size = 65536;

  // Verify exchange TLS certificates. Turning this off is for pointing a
  // diagnostic build at a proxy and nothing else.
  bool verify_tls = true;
  // Extra PEM trust anchor, for a corporate MITM proxy.
  std::string extra_ca_file;

  // Strategies given a shared-memory fast path. Empty means every strategy
  // goes through ZMQ, which is ~100x slower per order but needs no
  // co-location. Each named strategy gets its own ring pair.
  std::vector<std::string> shm_strategies;
  // /dev/shm on Linux (tmpfs). A real filesystem lets the kernel try to write
  // ring pages back to disk.
  std::string shm_directory = "/dev/shm";
  std::uint32_t shm_slots = 4096;
};

// Pre-trade limits for one instrument. Every limit is optional; an absent one
// is not checked. Quantities are in the VENUE'S units for that instrument --
// BTC on Binance's BTCUSDT, USD contracts on Deribit's BTC-PERPETUAL -- and
// notionals in its quote currency.
struct RiskLimits {
  std::optional<core::Qty> max_order_qty;
  // |price x quantity| of one order. A market order is valued at the
  // reference price, and refused when there is no fresh one.
  std::optional<core::Price> max_order_notional;
  // Worst-case |net position| if every working order filled: the venue's
  // position plus every open order on the side that grows it.
  std::optional<core::Qty> max_position;
  // How far a limit price may sit from the reference price, as a fraction
  // (0.05 = 5%). Catches a fat-fingered or mis-scaled price.
  std::optional<double> max_price_deviation;
};

// The engine's pre-trade risk layer, from `cpp.risk`. It sits in the EMS, so
// every order path -- ZMQ, shared memory, anything added later -- goes
// through it; strategies are not trusted to police themselves.
struct RiskConfig {
  // false skips the limit checks. The kill switch works regardless.
  bool enabled = true;
  RiskLimits defaults;
  // Keyed "exchange:instrument" (preferred) or "instrument". An entry
  // overrides `defaults` field by field.
  std::map<std::string, RiskLimits> instruments;
  // Orders a single strategy may submit per second. 0 = unlimited.
  int max_orders_per_strategy_per_second = 0;
  // A reference price older than this is treated as absent.
  double reference_price_max_age_seconds = 60.0;
  // How often the engine refreshes reference prices over REST for the
  // instruments named in `instruments` (as exchange:instrument).
  int reference_refresh_seconds = 5;
  // While this file exists, the kill switch is engaged. Empty disables it.
  // `touch` to stop trading, `rm` to resume. SIGUSR1 / SIGUSR2 do the same.
  std::string kill_switch_file;
  // Cancel every working order when the kill switch engages.
  bool cancel_all_on_kill = true;
};

struct Config {
  std::map<std::string, ExchangeConfig> exchanges;
  ReconciliationConfig reconciliation;
  FillReconciliationConfig fill_reconciliation;
  WebSocketConfig websocket;
  PortfolioConfig portfolio;
  ZmqConfig zmq;
  std::optional<DatabaseConfig> database;
  MetricsConfig metrics;
  RuntimeConfig runtime;
  RiskConfig risk;
};

// Throws std::runtime_error if the file is missing or malformed.
//
// A missing OPTIONAL section takes its defaults, exactly as the Python does.
// A malformed one is an error rather than a silent default: a typo'd
// `reconciliation` key that silently disables reconciliation is the kind of
// thing discovered during an incident.
Config load_config(const std::string& path);

// Credentials may come from the environment instead of the file, so a
// config.yaml can be committed. Reads AXON_<EXCHANGE>_API_KEY,
// _API_SECRET and _PASSPHRASE, overriding whatever the file said.
void apply_credentials_from_environment(Config& config);

}  // namespace axon
