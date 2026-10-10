// Engine process entry point. Port of engine.py + service.py.
//
//     axon_engine --config config.yaml
//
// ONE THREAD, BUSY-POLLED. The Python runs an asyncio loop with a task per
// venue and a task per command; this runs a single loop that polls every venue
// session and the ZMQ server in turn. That is the whole point of the design:
// no scheduler, no wakeups, no cross-thread hops on the path from a venue
// message to the order book.
//
// The cost is that CPU sits at 100% on the engine core. That is the entry fee
// for a bounded tail, and it is why `cpp.engine_core` exists.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <string>
#include <map>
#include <vector>

#include "axon/config.h"
#include "axon/core/clock.h"
#include "axon/core/platform.h"
#include "axon/ems/ems_service.h"
#include "axon/oms/oms_service.h"
#include "axon/risk/risk_feed.h"
#include "axon/risk/risk_manager.h"
#include "axon/oms/venue_connections.h"
#include "axon/transport/hot_messages.h"
#include "axon/transport/shm_bridge.h"
#include "axon/transport/wire.h"
#include "axon/transport/zmq_server.h"
#include "axon/util/logging.h"
#include "axon/util/metrics.h"

namespace {

std::atomic<bool> g_stop{false};
// SIGUSR1 engages the kill switch, SIGUSR2 releases it. Set here, acted on by
// the engine loop -- nothing else is safe to do inside a signal handler.
std::atomic<bool> g_kill_requested{false};
std::atomic<bool> g_release_requested{false};

void on_signal(int) { g_stop.store(true, std::memory_order_release); }
void on_kill_signal(int) { g_kill_requested.store(true, std::memory_order_release); }
void on_release_signal(int) { g_release_requested.store(true, std::memory_order_release); }

bool any_limit(const axon::RiskLimits& l) {
  return l.max_order_qty || l.max_order_notional || l.max_position || l.max_price_deviation;
}

// ---------------------------------------------------------------------------
// Turns what the OMS publishes into strategy-facing traffic: the shared-memory
// hot path for co-located strategies, ZMQ events for everyone.
// ---------------------------------------------------------------------------
class StrategyPublisher final : public axon::oms::OmsListener {
 public:
  StrategyPublisher(axon::transport::ZmqServer& zmq, axon::transport::ShmBridge& shm)
      : zmq_(zmq), shm_(shm) {}

  // Called before the OMS decodes anything: the ring gets the venue's bytes
  // ahead of every allocation on the control-plane side.
  void on_order_message(const axon::transport::OrderUpdateMsg& msg) override {
    shm_.publish_order(msg, msg.strategy_id.view());
  }

  void on_order_changed(const axon::models::Order& order) override {
    axon::transport::Event event;
    event.event_type = "order_update";
    event.data = axon::transport::to_json(order);
    event.strategy_id = order.strategy_id.value_or(std::string());
    zmq_.publish(event);
  }

  void on_fill(const axon::transport::FillMsg* msg, const axon::models::Fill& fill,
               bool) override {
    if (msg != nullptr) {
      shm_.publish_fill(*msg, msg->strategy_id.view());
    }
    axon::transport::Event event;
    event.event_type = "fill_update";
    event.data = axon::transport::to_json(fill);
    event.strategy_id = fill.strategy_id.value_or(std::string());
    zmq_.publish(event);
  }

 private:
  axon::transport::ZmqServer& zmq_;
  axon::transport::ShmBridge& shm_;
};

// ---------------------------------------------------------------------------
// The engine: wiring, the loop, and the strategy-facing command surface.
//
//   VenueConnections  every connection to every venue
//   OmsService        orders, fills, positions, reconciliation
//   EmsService        order entry
//   RiskManager       pre-trade checks; consulted by the EMS, fed by the OMS
//   ZMQ / ShmBridge   to and from strategies
// ---------------------------------------------------------------------------
class Engine {
 public:
  explicit Engine(axon::Config config)
      : config_(std::move(config)), log_(axon::util::get_logger("engine")) {}

  void start() {
    connections_ = std::make_unique<axon::oms::VenueConnections>(config_);
    oms_ = std::make_unique<axon::oms::OmsService>(config_);
    ems_ = std::make_unique<axon::ems::EmsService>(config_, &connections_->http());
    // Cancel and amend on the perpetual venues need the order's symbol, which
    // only the OMS knows.
    ems_->set_order_lookup([this](const std::string& order_id) {
      return oms_->get_order(order_id);
    });

    start_risk();

    // Publisher first: the hot path must see an update before anything else.
    publisher_ = std::make_unique<StrategyPublisher>(zmq_, shm_);
    risk_feed_ = std::make_unique<axon::risk::RiskFeed>(risk_, [] { return seconds(); });
    oms_->add_listener(publisher_.get());
    oms_->add_listener(risk_feed_.get());

    connections_->start(*oms_, [this](const std::string& exchange, std::int64_t id,
                                      bool ok, std::string_view payload,
                                      std::string_view error) {
      ems_->on_rpc_reply(exchange, id, ok, payload, error);
    });
    for (const auto& venue : connections_->venues()) {
      ems_->register_session(venue, connections_->session(venue));
      if (auto* trade = connections_->trade_session(venue)) {
        ems_->register_trade_session(venue, trade);
      }
    }
    oms_->start(connections_->rests());

    // --- hot path ---------------------------------------------------------
    // Opt-in per strategy, because the consumer must busy-poll a core to use
    // it. Nothing here changes the ZMQ path: a strategy with a ring gets both,
    // and the ring simply arrives first.
    if (!config_.runtime.shm_strategies.empty()) {
      shm_.start(config_.runtime.shm_directory, config_.runtime.shm_strategies,
                 config_.runtime.shm_slots);
      shm_.set_handlers(
          [this](const std::string& strategy, const axon::transport::PlaceOrderMsg& msg) {
            on_hot_place(strategy, msg);
          },
          [this](const std::string& strategy, const axon::transport::CancelOrderMsg& msg) {
            on_hot_cancel(strategy, msg);
          });
    }

    // --- control plane ----------------------------------------------------
    zmq_.start(config_.zmq, [this](const axon::transport::Command& c,
                                   axon::transport::ZmqServer::ResponseSink sink) {
      dispatch(c, std::move(sink));
    });
  }

  void run() {
    AXON_LOG_INFO(log_, "engine running; {} venue session(s)", connections_->venues().size());

    std::uint64_t iterations = 0;
    double last_report = seconds();

    while (!g_stop.load(std::memory_order_acquire)) {
      connections_->poll();
      ems_->poll();
      oms_->poll();
      shm_.poll();
      zmq_.poll();
      poll_risk();
      ++iterations;

      // A heartbeat line, so a silent engine is distinguishable from a wedged
      // one. Rate-limited to once a minute; it is not on any critical path.
      const double now = seconds();
      if (now - last_report > 60.0) {
        last_report = now;
        heartbeat();
      }

      axon::core::cpu_pause();
    }

    AXON_LOG_INFO(log_, "engine stopping after {} iterations", iterations);
  }

  void stop() {
    zmq_.stop();
    shm_.stop();
    // The OMS first: its reconcilers borrow the REST clients the connections
    // own. Then the connections, trade sessions before feeds.
    if (oms_) oms_->stop();
    if (connections_) connections_->stop();
  }

 private:
  static double seconds() {
    return static_cast<double>(axon::core::monotonic_ns()) / 1e9;
  }

  // --- risk ----------------------------------------------------------------
  void start_risk() {
    risk_ = axon::risk::RiskManager(config_.risk);
    ems_->set_risk_manager(&risk_);
    if (config_.risk.cancel_all_on_kill) {
      risk_.set_on_kill_engaged([this](const std::string&) { cancel_all_working_orders(); });
    }
    bool limits_configured = any_limit(config_.risk.defaults) ||
                             config_.risk.max_orders_per_strategy_per_second > 0;
    for (const auto& [_, l] : config_.risk.instruments) limits_configured |= any_limit(l);
    if (!config_.risk.enabled || !limits_configured) {
      // Loud: an engine with no limits sends whatever a strategy's bug says.
      AXON_LOG_WARN(log_,
                    "NO PRE-TRADE RISK LIMITS ARE CONFIGURED (cpp.risk). Every order a "
                    "strategy sends will reach the venue. Only the kill switch applies.");
    } else {
      AXON_LOG_INFO(log_, "pre-trade risk: {} instrument override(s), {} orders/s/strategy",
                    config_.risk.instruments.size(),
                    config_.risk.max_orders_per_strategy_per_second);
    }
  }

  // The engine's half of the risk layer: everything that touches the outside
  // world -- signals, the filesystem, the network. The decisions themselves
  // (who may release the kill switch, when a refresh is due) live in
  // RiskManager, where they are tested.
  void poll_risk() {
    if (g_kill_requested.exchange(false, std::memory_order_acq_rel)) {
      risk_.engage_kill_switch(axon::risk::KillSource::kSignal, "SIGUSR1");
    }
    if (g_release_requested.exchange(false, std::memory_order_acq_rel)) {
      risk_.release_kill_switch(axon::risk::KillSource::kSignal);
    }

    // Filesystem and network work is rate-limited: this runs on the hot loop.
    const double now = seconds();
    if (now - last_risk_tick_ < 1.0) {
      return;
    }
    last_risk_tick_ = now;

    if (!config_.risk.kill_switch_file.empty()) {
      std::error_code ec;
      const bool present = std::filesystem::exists(config_.risk.kill_switch_file, ec);
      if (ec) {
        // Fail closed: a check that could not run says nothing about the
        // file, and must never release a stop because of it.
        AXON_LOG_WARN(log_, "cannot check kill switch file {}: {}",
                      config_.risk.kill_switch_file, ec.message());
      } else {
        risk_.observe_kill_file(present, config_.risk.kill_switch_file);
      }
    }

    if (risk_.reference_refresh_due(now)) {
      refresh_reference_prices();
    }
  }

  // Wired to RiskManager::set_on_kill_engaged: stopping new orders is not
  // enough when working orders can still fill.
  void cancel_all_working_orders() {
    const auto working = oms_->active_orders();
    AXON_LOG_ERROR(log_, "kill switch: cancelling {} working order(s)", working.size());
    for (const auto& order : working) {
      ems_->cancel_order(order.exchange, order.order_id,
                         [this, id = order.order_id](bool ok, const std::string& error) {
                           if (!ok) {
                             AXON_LOG_ERROR(log_, "kill switch: cancel of {} failed: {}",
                                            id, error);
                           }
                         });
    }
  }

  // A ticker mid for each reference target. Positions and fills keep the
  // references fresh for instruments being traded; this covers the ones that
  // are not yet, which is exactly when a fat-fingered first order lands.
  void refresh_reference_prices() {
    for (const auto& target : risk_.reference_targets()) {
      auto* rest = connections_->rest(target.exchange);
      if (rest == nullptr) {
        continue;
      }
      rest->get_ticker(
          target.instrument,
          [this, exchange = target.exchange, instrument = target.instrument](
              std::optional<axon::models::Ticker> t, const std::string&) {
            if (t.has_value() && t->best_bid_price.raw() > 0 && t->best_ask_price.raw() > 0) {
              risk_.update_reference_price(exchange, instrument, t->mid(), seconds());
            }
          });
    }
  }

  // --- heartbeat -------------------------------------------------------------
  void heartbeat() {
    AXON_LOG_INFO(log_,
                  "alive: {} orders cached, {} fills ({} dup), {} commands, {} events, [{}]",
                  oms_->cached_order_count(), oms_->fills_accepted(), oms_->fills_duplicate(),
                  zmq_.commands_handled(), zmq_.events_published(),
                  connections_->state_summary());
    if (shm_.active()) {
      const auto hot = shm_.stats();
      AXON_LOG_INFO(log_, "hot path: {} commands, {} events ({} dropped)",
                    hot.commands_received, hot.events_published, hot.events_dropped);
      // Only worth printing once something has been through it; an empty
      // histogram table in the log is noise.
      if (hot.commands_received > 0) {
        AXON_LOG_INFO(log_, "hot command latency:\n{}", shm_.latency_report());
      }
      publish_hot_latency();
    }
  }

  // Publishes the closing latency window to Prometheus, then opens a new one.
  //
  // Called from the once-a-minute heartbeat, never from the message path: the
  // label lookups below are exactly what the note in util/metrics.h forbids on
  // a per-message path, and once a minute they cost nothing.
  void publish_hot_latency() {
    auto& metrics = axon::util::get_metrics();
    const auto& lat = shm_.command_latency();

    const auto publish = [&](const std::string& name, const axon::core::Histogram& h) {
      metrics.set_hot_stage(name, h.count(), h.p50(), h.p99(), h.p999(), h.max());
    };

    for (std::size_t i = 0; i < lat.segment_count(); ++i) {
      publish(lat.stage_name(i) + "->" + lat.stage_name(i + 1), lat.segment(i));
    }
    // The total is published under its own name rather than as another
    // segment, because it is not the sum of the segments: a per-segment p99
    // and the end-to-end p99 come from different messages.
    publish("total", lat.total());
    metrics.set_hot_stage_dropped(lat.dropped());

    // Reset last: everything above reads the window that just closed.
    shm_.reset_latency();
  }

  // --- order entry -------------------------------------------------------------
  // Every placement, from either transport, goes through here: the OMS
  // registers the request before the EMS sends it and records the verdict
  // after, so an update for it routes back even if the reply is lost.
  void place(const std::string& exchange, const axon::models::OrderRequest& request,
             std::function<void(const axon::ems::OrderResult&)> done) {
    using Outcome = axon::oms::OmsService::PlaceOutcome;
    const double started = seconds();
    oms_->before_place(exchange, request);
    ems_->place_order(
        exchange, request,
        [this, exchange, request, started, done = std::move(done)](
            const axon::ems::OrderResult& r) {
          axon::util::get_metrics().observe_order_submit_latency(exchange,
                                                                 seconds() - started);
          const Outcome outcome = r.success           ? Outcome::kAccepted
                                  : r.outcome_unknown ? Outcome::kUnknown
                                                      : Outcome::kRefused;
          if (outcome == Outcome::kRefused) {
            axon::util::get_metrics().inc_order_place_failure(exchange, "rejected");
          }
          oms_->after_place(exchange, request, outcome, r.order);
          done(r);
        });
  }

  // --- hot path commands ----------------------------------------------------
  // These come off the shared-memory ring, so there is no request_id and no
  // reply: the strategy learns the outcome from the order update it will get
  // back on the same ring. A synchronous reply would reintroduce the round
  // trip the ring exists to remove.
  void on_hot_place(const std::string& strategy, const axon::transport::PlaceOrderMsg& msg) {
    auto request = axon::transport::decode_place_order(msg);
    // The ring is per-strategy, so its identity is structural. Trusting the
    // field instead would let one strategy write another's id.
    request.strategy_id = strategy;
    const std::string exchange(msg.exchange.view());
    place(exchange, request, [this, exchange, id = request.internal_order_id](
                                 const axon::ems::OrderResult& r) {
      if (r.success) {
        return;
      }
      if (r.outcome_unknown) {
        // The update, if the order is live, is routed back to the strategy.
        AXON_LOG_WARN(log_, "[{}] hot place {} outcome unknown: {}", exchange, id, r.error);
      } else {
        AXON_LOG_WARN(log_, "[{}] hot place {} rejected: {}", exchange, id, r.error);
      }
    });
  }

  void on_hot_cancel(const std::string& strategy, const axon::transport::CancelOrderMsg& msg) {
    const std::string exchange(msg.exchange.view());
    const std::string order_id(msg.order_id.view());
    const double started = seconds();
    ems_->cancel_order(exchange, order_id,
                       [this, exchange, order_id, strategy, started](bool success,
                                                                     const std::string& error) {
                         axon::util::get_metrics().observe_order_cancel_latency(
                             exchange, seconds() - started);
                         if (!success) {
                           AXON_LOG_WARN(log_, "[{}] hot cancel of {} failed for {}: {}",
                                         exchange, order_id, strategy, error);
                         }
                       });
  }

  // --- control plane commands ---------------------------------------------
  void dispatch(const axon::transport::Command& command,
                axon::transport::ZmqServer::ResponseSink sink) {
    using axon::transport::Response;
    const auto typed = command.typed();
    if (!typed.has_value()) {
      sink(Response::fail(command.request_id, "unknown command type: " + command.command_type));
      return;
    }

    switch (*typed) {
      case axon::models::CommandType::kGetOrder: {
        const auto id = command.payload.value("order_id", std::string());
        const auto order = oms_->get_order(id);
        sink(Response::ok(command.request_id, order.has_value()
                                                  ? axon::transport::to_json(*order)
                                                  : axon::transport::Json()));
        return;
      }

      case axon::models::CommandType::kGetAllOrders:
      case axon::models::CommandType::kGetActiveOrders: {
        const bool active_only = *typed == axon::models::CommandType::kGetActiveOrders;
        auto orders = active_only ? oms_->active_orders() : oms_->all_orders();
        axon::transport::Json array = axon::transport::Json::array();
        for (const auto& o : orders) {
          // Strategies see only their own orders, matching the Python.
          if (!command.strategy_id.empty() &&
              o.strategy_id.value_or(std::string()) != command.strategy_id) {
            continue;
          }
          array.push_back(axon::transport::to_json(o));
        }
        sink(Response::ok(command.request_id, array));
        return;
      }

      case axon::models::CommandType::kGetAccountSummary: {
        const auto exchange = command.payload.value("exchange", std::string());
        const auto currency = command.payload.value("currency", std::string("BTC"));
        const auto account = oms_->account(exchange, currency);
        sink(Response::ok(command.request_id, account.has_value()
                                                  ? axon::transport::to_json(*account)
                                                  : axon::transport::Json()));
        return;
      }

      case axon::models::CommandType::kGetFillsByOrder:
      case axon::models::CommandType::kGetFillsByStrategy: {
        const bool by_order = *typed == axon::models::CommandType::kGetFillsByOrder;
        const auto key = by_order ? command.payload.value("order_id", std::string())
                                  : command.payload.value("strategy_id", std::string());
        const auto matches = by_order ? oms_->fills_by_order(key) : oms_->fills_by_strategy(key);
        axon::transport::Json array = axon::transport::Json::array();
        for (const auto& f : matches) {
          array.push_back(axon::transport::to_json(f));
        }
        sink(Response::ok(command.request_id, array));
        return;
      }

      case axon::models::CommandType::kPlaceOrder: {
        const auto exchange = command.payload.value("exchange", std::string());
        const auto request_json = command.payload.contains("request")
                                      ? command.payload["request"]
                                      : axon::transport::Json();
        auto request = axon::transport::order_request_from_json(request_json);
        if (!request.has_value()) {
          sink(Response::fail(command.request_id, "malformed order request"));
          return;
        }
        // The strategy_id comes from the envelope, not the payload, exactly as
        // in transport/server.py.
        if (!command.strategy_id.empty()) {
          request->strategy_id = command.strategy_id;
        }
        // DEFERRED. Placing an order is a network round trip; replying inline
        // would stall every venue feed for its duration.
        place(exchange, *request,
              [sink, request_id = command.request_id](const axon::ems::OrderResult& r) mutable {
                if (!r.success) {
                  // Unknown is not a rejection: the order may be live. The
                  // prefix tells the strategy not to resubmit.
                  sink(Response::fail(
                      request_id,
                      r.outcome_unknown
                          ? std::string(axon::transport::kOutcomeUnknownPrefix) + r.error
                          : r.error));
                  return;
                }
                // Accepted. When the reply carried no order, the authoritative
                // state arrives on the feed.
                sink(Response::ok(request_id, r.order.has_value()
                                                  ? axon::transport::to_json(*r.order)
                                                  : axon::transport::Json()));
              });
        return;
      }

      case axon::models::CommandType::kCancelOrder: {
        const auto exchange = command.payload.value("exchange", std::string());
        const auto order_id = command.payload.value("order_id", std::string());
        const std::string request_id = command.request_id;
        const double started = seconds();
        ems_->cancel_order(exchange, order_id,
                           [sink, request_id, exchange, started](
                               bool success, const std::string& error) mutable {
                             axon::util::get_metrics().observe_order_cancel_latency(
                                 exchange, seconds() - started);
                             if (!success) {
                               sink(Response::fail(request_id, error));
                               return;
                             }
                             sink(Response::ok(request_id, axon::transport::Json(true)));
                           });
        return;
      }

      case axon::models::CommandType::kModifyOrder: {
        const auto exchange = command.payload.value("exchange", std::string());
        const auto order_id = command.payload.value("order_id", std::string());
        std::optional<axon::core::Qty> amount;
        std::optional<axon::core::Price> price;
        if (command.payload.contains("amount") && !command.payload["amount"].is_null()) {
          amount = axon::core::Qty::from_double(command.payload["amount"].get<double>());
        }
        if (command.payload.contains("price") && !command.payload["price"].is_null()) {
          price = axon::core::Price::from_double(command.payload["price"].get<double>());
        }
        const std::string request_id = command.request_id;
        ems_->modify_order(exchange, order_id, amount, price,
                           [sink, request_id](const axon::ems::OrderResult& result) mutable {
                             if (!result.success) {
                               sink(Response::fail(request_id, result.error));
                               return;
                             }
                             sink(Response::ok(request_id,
                                               result.order.has_value()
                                                   ? axon::transport::to_json(*result.order)
                                                   : axon::transport::Json()));
                           });
        return;
      }

      case axon::models::CommandType::kGetTicker: {
        const auto exchange = command.payload.value("exchange", std::string());
        const auto instrument = command.payload.value("instrument", std::string());
        auto* rest = connections_->rest(exchange);
        if (rest == nullptr) {
          sink(Response::fail(command.request_id, "no REST client for " + exchange));
          return;
        }
        // A live REST read, deferred like order entry: it is a round trip and
        // must not be answered inline.
        rest->get_ticker(instrument, [sink, request_id = command.request_id](
                                         std::optional<axon::models::Ticker> ticker,
                                         const std::string& error) mutable {
          if (!ticker.has_value()) {
            sink(Response::fail(request_id, error));
            return;
          }
          sink(Response::ok(request_id, axon::transport::to_json(*ticker)));
        });
        return;
      }

      case axon::models::CommandType::kGetPositions: {
        const auto exchange = command.payload.value("exchange", std::string());
        const auto currency = command.payload.value("currency", std::string());
        axon::transport::Json array = axon::transport::Json::array();
        // Served from the cache the position reconciler refreshes, not from a
        // fresh REST call: a strategy polling positions should not add a round
        // trip to the venue each time.
        for (const auto& p : oms_->positions(exchange)) {
          if (!currency.empty() && p.instrument.rfind(currency, 0) != 0) {
            continue;
          }
          array.push_back(axon::transport::to_json(p));
        }
        sink(Response::ok(command.request_id, array));
        return;
      }
    }
    sink(Response::fail(command.request_id, "unhandled command"));
  }

  axon::Config config_;
  std::shared_ptr<spdlog::logger> log_;
  // Declaration order is destruction order in reverse: everything that holds
  // a pointer into the connections (EMS sessions, OMS reconcilers) is
  // declared after them and so destroyed first.
  std::unique_ptr<axon::oms::VenueConnections> connections_;
  std::unique_ptr<axon::oms::OmsService> oms_;
  std::unique_ptr<axon::ems::EmsService> ems_;
  axon::risk::RiskManager risk_;
  std::unique_ptr<axon::risk::RiskFeed> risk_feed_;
  axon::transport::ZmqServer zmq_;
  axon::transport::ShmBridge shm_;
  std::unique_ptr<StrategyPublisher> publisher_;
  double last_risk_tick_ = 0.0;
};

}  // namespace

int main(int argc, char** argv) {
  std::string config_path = "config.yaml";
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--config") == 0 && i + 1 < argc) {
      config_path = argv[++i];
    } else if (std::strcmp(argv[i], "--help") == 0) {
      std::printf("usage: axon_engine [--config config.yaml]\n");
      return 0;
    }
  }

  axon::util::LoggingOptions logging;
  logging.console = true;
  axon::util::init_logging(logging);
  auto log = axon::util::get_logger("main");

  axon::Config config;
  try {
    config = axon::load_config(config_path);
    axon::apply_credentials_from_environment(config);
  } catch (const std::exception& e) {
    AXON_LOG_ERROR(log, "{}", e.what());
    return 1;
  }

  try {
    axon::util::init_metrics(config.metrics);
    if (config.metrics.enabled) {
      AXON_LOG_INFO(log, "metrics on http://{}:{}/metrics", config.metrics.host,
                      config.metrics.port);
    }
  } catch (const std::exception& e) {
    AXON_LOG_ERROR(log, "{}", e.what());
    return 1;
  }

  // Startup tuning. Both are Linux-only and report why when they fail, rather
  // than pretending to have worked.
  if (config.runtime.lock_memory) {
    const auto r = axon::core::lock_all_memory();
    AXON_LOG_INFO(log, "mlockall: {}", r.detail);
  }
  if (config.runtime.engine_core >= 0) {
    const auto r =
        axon::core::pin_current_thread_to_core(config.runtime.engine_core);
    AXON_LOG_INFO(log, "core pinning: {}", r.detail);
  }
  const auto& clock = axon::core::clock_info();
  AXON_LOG_INFO(log, "clock: {} ({:.2f}ns resolution)", clock.source,
                  clock.resolution_ns);

  std::signal(SIGINT, on_signal);
  std::signal(SIGTERM, on_signal);
  std::signal(SIGUSR1, on_kill_signal);
  std::signal(SIGUSR2, on_release_signal);

  Engine engine(std::move(config));
  try {
    engine.start();
    engine.run();
  } catch (const std::exception& e) {
    AXON_LOG_ERROR(log, "fatal: {}", e.what());
    engine.stop();
    return 1;
  }

  engine.stop();
  axon::util::get_metrics().stop_server();
  AXON_LOG_INFO(log, "engine stopped");
  axon::util::shutdown_logging();
  return 0;
}
