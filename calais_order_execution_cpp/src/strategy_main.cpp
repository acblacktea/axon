// Example strategy process. There is no Python equivalent to port: on that
// side a strategy is a script that imports StrategyClient. This is the same
// thing in C++ -- a worked example of the client, and the only place the
// client-side algorithms are instantiated, so they are compiled by the build
// rather than only when someone first uses them.
//
//     calais_strategy --id my_strategy --ticker deribit:BTC-PERPETUAL
//     calais_strategy --id my_strategy --shm /dev/shm --watch
//
// The engine must already be running. With --shm the client attaches to the
// rings the engine created for this strategy id; without it, everything goes
// over ZMQ.

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <string>
#include <thread>

#include "calais/client/algorithms/chase_maker.h"
#include "calais/client/algorithms/hedge_deribit.h"
#include "calais/client/strategy_client.h"
#include "calais/util/logging.h"

namespace {

std::atomic<bool> g_stop{false};
void on_signal(int) { g_stop.store(true, std::memory_order_release); }

void usage() {
  std::printf(
      "usage: calais_strategy --id <strategy> [options]\n"
      "  --router <endpoint>          default tcp://localhost:5555\n"
      "  --pub <endpoint>             default tcp://localhost:5556\n"
      "  --shm <directory>            attach to the shared-memory fast path\n"
      "  --ticker <exchange:instrument>  print one ticker and exit\n"
      "  --orders                     print active orders and exit\n"
      "  --watch                      stream order and fill events\n"
      "  --hot-ping <exchange>        push a cancel of a nonexistent order\n"
      "                               through the shared-memory ring\n");
}

// Splits "deribit:BTC-PERPETUAL". Returns false when there is no colon.
bool split_pair(const std::string& in, std::string& left, std::string& right) {
  const auto pos = in.find(':');
  if (pos == std::string::npos) {
    return false;
  }
  left = in.substr(0, pos);
  right = in.substr(pos + 1);
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  calais::client::StrategyClientConfig config;
  std::string ticker_arg;
  bool list_orders = false;
  bool watch = false;
  std::string hot_ping;

  for (int i = 1; i < argc; ++i) {
    const auto next = [&](const char* what) -> std::string {
      if (i + 1 >= argc) {
        std::printf("%s needs a value\n", what);
        std::exit(2);
      }
      return argv[++i];
    };
    if (std::strcmp(argv[i], "--id") == 0) {
      config.strategy_id = next("--id");
    } else if (std::strcmp(argv[i], "--router") == 0) {
      config.router_endpoint = next("--router");
    } else if (std::strcmp(argv[i], "--pub") == 0) {
      config.pub_endpoint = next("--pub");
    } else if (std::strcmp(argv[i], "--shm") == 0) {
      config.shm_directory = next("--shm");
    } else if (std::strcmp(argv[i], "--ticker") == 0) {
      ticker_arg = next("--ticker");
    } else if (std::strcmp(argv[i], "--orders") == 0) {
      list_orders = true;
    } else if (std::strcmp(argv[i], "--watch") == 0) {
      watch = true;
    } else if (std::strcmp(argv[i], "--hot-ping") == 0) {
      hot_ping = next("--hot-ping");
    } else {
      usage();
      return argv[i] == std::string("--help") ? 0 : 2;
    }
  }

  if (config.strategy_id.empty()) {
    usage();
    return 2;
  }

  calais::util::LoggingOptions logging;
  logging.console = true;
  calais::util::init_logging(logging);
  auto log = calais::util::get_logger("strategy");

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);

  calais::client::StrategyClient client;
  calais::client::StrategyEvents events;
  events.on_order = [&log](const calais::models::Order& order) {
    CALAIS_LOG_INFO(log, "order {} {} {} {} filled {}/{}", order.order_id,
                    order.instrument, calais::models::to_string(order.side),
                    calais::models::to_string(order.status),
                    order.filled_amount.to_string(), order.amount.to_string());
  };
  events.on_fill = [&log](const calais::models::Fill& fill) {
    CALAIS_LOG_INFO(log, "fill {} {} {} @ {}", fill.trade_id, fill.instrument,
                    fill.amount.to_string(), fill.price.to_string());
  };

  try {
    client.connect(config, std::move(events));
  } catch (const std::exception& e) {
    CALAIS_LOG_ERROR(log, "could not connect: {}", e.what());
    return 1;
  }
  CALAIS_LOG_INFO(log, "connected as '{}' (fast path: {})", config.strategy_id,
                  client.fast_path_available() ? "yes" : "no");

  int exit_code = 0;
  std::string error;

  if (!ticker_arg.empty()) {
    std::string exchange;
    std::string instrument;
    if (!split_pair(ticker_arg, exchange, instrument)) {
      CALAIS_LOG_ERROR(log, "--ticker wants exchange:instrument");
      exit_code = 2;
    } else if (const auto ticker = client.get_ticker(exchange, instrument, error);
               ticker.has_value()) {
      CALAIS_LOG_INFO(log, "{} bid {} / ask {} last {}", instrument,
                      ticker->best_bid_price.to_string(),
                      ticker->best_ask_price.to_string(),
                      ticker->last_price.has_value()
                          ? ticker->last_price->to_string()
                          : std::string("-"));
    } else {
      CALAIS_LOG_ERROR(log, "ticker failed: {}", error);
      exit_code = 1;
    }
  }

  if (list_orders) {
    const auto orders = client.get_active_orders(error);
    if (!error.empty()) {
      CALAIS_LOG_ERROR(log, "get_active_orders failed: {}", error);
      exit_code = 1;
    }
    CALAIS_LOG_INFO(log, "{} active order(s)", orders.size());
    for (const auto& o : orders) {
      CALAIS_LOG_INFO(log, "  {} {} {} {}", o.order_id, o.instrument,
                      calais::models::to_string(o.side),
                      calais::models::to_string(o.status));
    }
  }

  if (!hot_ping.empty()) {
    // Deliberately a CANCEL of an id that cannot exist: it proves the ring,
    // the engine's dequeue and the EMS dispatch all work, and the venue
    // rejects it without anything being traded. Safe to run against live keys.
    if (!client.fast_path_available()) {
      CALAIS_LOG_ERROR(log, "--hot-ping needs --shm and an engine that created "
                            "a ring for this strategy id");
      exit_code = 1;
    } else if (client.cancel_order_fast(hot_ping, "PING", "calais-hot-ping")) {
      CALAIS_LOG_INFO(log, "hot ping pushed to the ring; the engine log should "
                           "show a rejected cancel");
    } else {
      CALAIS_LOG_ERROR(log, "hot ping could not be pushed: the ring is full");
      exit_code = 1;
    }
  }

  if (watch) {
    CALAIS_LOG_INFO(log, "watching; ctrl-c to stop");
    while (!g_stop.load(std::memory_order_acquire)) {
      // Busy-poll only when the shared-memory path is live, because that is
      // the only case where sleeping would throw away the latency it buys.
      // Over ZMQ alone, a millisecond of sleep costs nothing that matters and
      // keeps a core free.
      if (client.poll() == 0 && !client.fast_path_available()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }
  }

  client.close();
  calais::util::shutdown_logging();
  return exit_code;
}

// Not called: it exists so the client-side algorithm templates are
// instantiated and type-checked by every build, instead of failing to compile
// the first time somebody writes a strategy that uses them.
[[maybe_unused]] static void instantiate_algorithms(
    calais::client::StrategyClient& client) {
  calais::client::algorithms::chase_maker_fill(
      client, calais::client::algorithms::ChaseMakerParams{});
  calais::client::algorithms::hedge_deribit_options(
      client, calais::client::algorithms::HedgeDeribitParams{});
}
