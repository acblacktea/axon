#include "calais/config.h"

#include <yaml-cpp/yaml.h>

#include <cctype>
#include <cstdlib>
#include <stdexcept>

namespace calais {
namespace {

template <typename T>
T get_or(const YAML::Node& node, const char* key, T fallback) {
  if (!node || !node.IsMap()) {
    return fallback;
  }
  const YAML::Node child = node[key];
  if (!child || child.IsNull()) {
    return fallback;
  }
  return child.as<T>();
}

std::string upper(std::string s) {
  for (char& c : s) {
    c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
  }
  return s;
}

const char* env_or_null(const std::string& name) {
  return std::getenv(name.c_str());
}

}  // namespace

Config load_config(const std::string& path) {
  YAML::Node root;
  try {
    root = YAML::LoadFile(path);
  } catch (const YAML::Exception& e) {
    throw std::runtime_error("could not load config '" + path + "': " + e.what());
  }

  Config config;

  const YAML::Node exchanges = root["exchanges"];
  if (exchanges && exchanges.IsMap()) {
    for (const auto& entry : exchanges) {
      const auto name = entry.first.as<std::string>();
      const YAML::Node& data = entry.second;
      ExchangeConfig ex;
      ex.name = name;
      ex.env = get_or<std::string>(data, "env", "testnet");
      ex.api_key = get_or<std::string>(data, "api_key", "");
      ex.api_secret = get_or<std::string>(data, "api_secret", "");
      ex.passphrase = get_or<std::string>(data, "passphrase", "");
      config.exchanges.emplace(name, std::move(ex));
    }
  }

  const YAML::Node rec = root["reconciliation"];
  config.reconciliation.enabled = get_or(rec, "enabled", true);
  config.reconciliation.interval_seconds = get_or(rec, "interval_seconds", 30);

  const YAML::Node frec = root["fill_reconciliation"];
  config.fill_reconciliation.enabled = get_or(frec, "enabled", true);
  config.fill_reconciliation.interval_seconds = get_or(frec, "interval_seconds", 60);
  config.fill_reconciliation.lookback_seconds = get_or(frec, "lookback_seconds", 86400);
  config.fill_reconciliation.overlap_seconds = get_or(frec, "overlap_seconds", 30);

  const YAML::Node ws = root["websocket"];
  config.websocket.heartbeat_interval_seconds =
      get_or(ws, "heartbeat_interval_seconds", 10);
  config.websocket.reconnect_delay_seconds = get_or(ws, "reconnect_delay_seconds", 1);
  config.websocket.max_reconnect_delay_seconds =
      get_or(ws, "max_reconnect_delay_seconds", 60);
  config.websocket.request_timeout_seconds = get_or(ws, "request_timeout_seconds", 30);
  // NOTE: config.py defaults this to 20 in load_config while the dataclass says
  // 3. The loader wins there, so it wins here too -- matching behaviour, not
  // the declaration.
  config.websocket.max_request_retries = get_or(ws, "max_request_retries", 20);
  config.websocket.retry_delay_seconds = get_or(ws, "retry_delay_seconds", 1.0);

  const YAML::Node portfolio = root["portfolio"];
  if (portfolio && portfolio["currencies"] && portfolio["currencies"].IsSequence()) {
    config.portfolio.currencies.clear();
    for (const auto& c : portfolio["currencies"]) {
      config.portfolio.currencies.push_back(c.as<std::string>());
    }
  }
  config.portfolio.position_refresh_interval_seconds =
      get_or(portfolio, "position_refresh_interval_seconds", 10);

  const YAML::Node zmq = root["zmq"];
  config.zmq.router_endpoint = get_or<std::string>(zmq, "router_endpoint", "tcp://*:5555");
  config.zmq.pub_endpoint = get_or<std::string>(zmq, "pub_endpoint", "tcp://*:5556");
  config.zmq.router_connect =
      get_or<std::string>(zmq, "router_connect", "tcp://localhost:5555");
  config.zmq.pub_connect =
      get_or<std::string>(zmq, "pub_connect", "tcp://localhost:5556");

  const YAML::Node db = root["database"];
  if (db && !db.IsNull()) {
    DatabaseConfig d;
    d.dsn = get_or<std::string>(db, "dsn", "postgresql://localhost:5432/calais");
    d.pool_min = get_or(db, "pool_min", 2);
    d.pool_max = get_or(db, "pool_max", 10);
    config.database = d;
  }

  const YAML::Node metrics = root["metrics"];
  config.metrics.enabled = get_or(metrics, "enabled", true);
  config.metrics.host = get_or<std::string>(metrics, "host", "0.0.0.0");
  config.metrics.port = get_or(metrics, "port", 9100);

  // C++-only section; absent from a Python-authored file, which is fine.
  const YAML::Node cpp = root["cpp"];
  config.runtime.rx_buffer_bytes =
      get_or<std::size_t>(cpp, "rx_buffer_bytes", 1u << 18);
  config.runtime.tx_buffer_bytes =
      get_or<std::size_t>(cpp, "tx_buffer_bytes", 1u << 16);
  config.runtime.max_message_bytes =
      get_or<std::size_t>(cpp, "max_message_bytes", 8u << 20);
  config.runtime.engine_core = get_or(cpp, "engine_core", -1);
  config.runtime.lock_memory = get_or(cpp, "lock_memory", false);
  config.runtime.order_pool_size =
      get_or<std::uint32_t>(cpp, "order_pool_size", 65536);
  config.runtime.verify_tls = get_or(cpp, "verify_tls", true);
  config.runtime.extra_ca_file = get_or<std::string>(cpp, "extra_ca_file", "");
  if (cpp && cpp["shm_strategies"] && cpp["shm_strategies"].IsSequence()) {
    for (const auto& n : cpp["shm_strategies"]) {
      config.runtime.shm_strategies.push_back(n.as<std::string>());
    }
  }
  config.runtime.shm_directory =
      get_or<std::string>(cpp, "shm_directory", "/dev/shm");
  config.runtime.shm_slots = get_or<std::uint32_t>(cpp, "shm_slots", 4096);

  return config;
}

void apply_credentials_from_environment(Config& config) {
  for (auto& [name, exchange] : config.exchanges) {
    const std::string prefix = "CALAIS_" + upper(name) + "_";
    if (const char* v = env_or_null(prefix + "API_KEY")) {
      exchange.api_key = v;
    }
    if (const char* v = env_or_null(prefix + "API_SECRET")) {
      exchange.api_secret = v;
    }
    if (const char* v = env_or_null(prefix + "PASSPHRASE")) {
      exchange.passphrase = v;
    }
  }
}

}  // namespace calais
