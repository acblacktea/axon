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

namespace calais {

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
  std::string dsn = "postgresql://localhost:5432/calais";
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
};

// Throws std::runtime_error if the file is missing or malformed.
//
// A missing OPTIONAL section takes its defaults, exactly as the Python does.
// A malformed one is an error rather than a silent default: a typo'd
// `reconciliation` key that silently disables reconciliation is the kind of
// thing discovered during an incident.
Config load_config(const std::string& path);

// Credentials may come from the environment instead of the file, so a
// config.yaml can be committed. Reads CALAIS_<EXCHANGE>_API_KEY,
// _API_SECRET and _PASSPHRASE, overriding whatever the file said.
void apply_credentials_from_environment(Config& config);

}  // namespace calais
