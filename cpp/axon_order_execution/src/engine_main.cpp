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
#include <memory>
#include <string>
#include <map>
#include <vector>

#include "axon/config.h"
#include "axon/core/clock.h"
#include "axon/core/platform.h"
#include "axon/ems/ems_service.h"
#include "axon/net/http_client.h"
#include "axon/net/tls_stream.h"
#include "axon/oms/order_store.h"
#include "axon/oms/portfolio_store.h"
#include "axon/oms/reconciler.h"
#include "axon/oms/venue_rest.h"
#include "axon/repository/postgres.h"
#include "axon/oms/venue_session.h"
#include "axon/transport/hot_messages.h"
#include "axon/transport/shm_bridge.h"
#include "axon/transport/wire.h"
#include "axon/transport/zmq_server.h"
#include "axon/util/logging.h"
#include "axon/util/metrics.h"

namespace {

std::atomic<bool> g_stop{false};

void on_signal(int) { g_stop.store(true, std::memory_order_release); }

// ---------------------------------------------------------------------------
class Engine {
 public:
  explicit Engine(axon::Config config)
      : config_(std::move(config)), log_(axon::util::get_logger("engine")) {}

  void start() {
    // --- TLS -------------------------------------------------------------
    tls_ = std::make_unique<axon::net::TlsContext>();
    if (config_.runtime.verify_tls) {
      tls_->use_default_trust_store();
      if (!config_.runtime.extra_ca_file.empty()) {
        AXON_LOG_INFO(log_, "adding extra trust anchor {}",
                        config_.runtime.extra_ca_file);
      }
    } else {
      // Loud, because a silently unverified connection to an exchange looks
      // exactly like a verified one until it is not.
      AXON_LOG_WARN(log_,
                      "TLS VERIFICATION IS DISABLED. Never run this against a "
                      "live venue.");
      tls_->set_verify_peer(false);
    }

    // --- persistence ------------------------------------------------------
    // Every store takes a repository. With a database that repository writes
    // through to the writer thread; without one it is an in-memory twin. The
    // stores themselves cannot tell the difference, which is the point: no
    // `if (writer_)` scattered through the callbacks.
    if (config_.database.has_value()) {
      writer_ = std::make_unique<axon::repository::PostgresWriter>();
      writer_->start(*config_.database);
      // OrderStore persists through the writer; reads still come from the
      // in-memory cache, never from a blocking SELECT on this thread.
      orders_ = axon::oms::OrderStore(
          std::make_shared<axon::repository::PostgresOrderRepository>(writer_.get()));
      fills_ = axon::oms::FillStore(
          std::make_shared<axon::repository::PostgresFillRepository>(writer_.get()));
      portfolio_ = axon::oms::PortfolioStore(
          std::make_shared<axon::repository::PostgresAccountRepository>(
              writer_.get()),
          std::make_shared<axon::repository::PostgresPositionRepository>(
              writer_.get()));
      AXON_LOG_INFO(log_, "persistence enabled");
    } else {
      AXON_LOG_WARN(log_,
                      "no database configured: order and fill history lives only "
                      "in memory and is lost on restart");
    }

    http_ = std::make_unique<axon::net::HttpClient>(tls_.get());
    ems_ = std::make_unique<axon::ems::EmsService>(config_, http_.get());
    // Cancel and amend on the perpetual venues need the order's symbol, which
    // only the order store knows.
    ems_->set_order_lookup([this](const std::string& order_id) {
      return orders_.get_order(order_id);
    });

    // --- venue sessions ---------------------------------------------------
    for (const auto& entry : config_.exchanges) {
      const std::string& venue_name = entry.first;
      const auto& exchange = entry.second;
      axon::oms::VenueSessionHandlers handlers;
      handlers.on_order = [this](const axon::transport::OrderUpdateMsg& m) {
        on_order_update(m);
      };
      handlers.on_fill = [this](const axon::transport::FillMsg& m) {
        on_fill(m);
      };
      handlers.on_account = [this](const axon::models::AccountSummary& a) {
        // The store persists through its repository; nothing to do here.
        portfolio_.update_account(a);
        // Margin ratio is the one risk number worth a gauge: it is what an
        // alert fires on before a liquidation, not after.
        if (!a.equity.is_zero()) {
          axon::util::get_metrics().set_account_margin_ratio(
              a.exchange, a.currency,
              a.maintenance_margin.to_double() / a.equity.to_double());
        }
      };
      handlers.on_live = [this, venue_name]() {
        AXON_LOG_INFO(log_, "[{}] session live", venue_name);
        // Everything that happened while disconnected was missed; reconcile
        // now rather than waiting for the next tick.
        if (const auto it = reconcilers_.find(venue_name); it != reconcilers_.end()) {
          it->second->on_session_live();
        }
      };
      handlers.on_error = [this, venue_name](const std::string& what) {
        AXON_LOG_WARN(log_, "[{}] {}", venue_name, what);
      };
      handlers.on_rpc_reply = [this, venue_name](std::int64_t id, bool success,
                                                 std::string_view payload,
                                                 std::string_view error) {
        ems_->on_rpc_reply(venue_name, id, success, payload, error);
      };

      // One REST client per venue, shared by the session (Binance's listenKey),
      // the EMS (cancel/amend) and the reconciler (snapshots), so all three
      // reuse the same keep-alive connections.
      auto rest = axon::oms::make_venue_rest(exchange, http_.get());
      axon::oms::VenueRest* rest_ptr = rest.get();
      if (rest) {
        // Reconciliation and position snapshots only. Order entry does not go
        // through here -- it rides the WebSocket sessions below.
        rests_[venue_name] = std::move(rest);
      }

      auto session = axon::oms::make_venue_session(
          exchange, config_.websocket, config_.runtime, tls_.get(), rest_ptr,
          std::move(handlers));
      if (!session) {
        // Better a clear refusal at startup than a connection that never
        // authenticates for reasons nobody can see.
        AXON_LOG_ERROR(log_,
                         "exchange '{}' is configured but not supported by this "
                         "build; skipping it",
                         venue_name);
        continue;
      }
      session->start();
      ems_->register_session(venue_name, session.get());
      sessions_.push_back(std::move(session));

      // --- the order-entry connection ---------------------------------------
      //
      // Binance and Bybit each need a SECOND connection for order entry; OKX
      // sends orders on the session above and Deribit always has. There is no
      // REST order path, so on those two venues THIS CONNECTION IS ORDER
      // ENTRY: while it is down, the venue cannot trade.
      {
        axon::oms::VenueSessionHandlers trade_handlers;
        // Order and fill updates arrive on the FEED, never here: this
        // connection carries only replies to requests we sent.
        trade_handlers.on_rpc_reply =
            [this, venue_name](std::int64_t id, bool ok, std::string_view payload,
                               std::string_view error) {
              ems_->on_rpc_reply(venue_name, id, ok, payload, error);
            };
        trade_handlers.on_error = [this, venue_name](const std::string& what) {
          AXON_LOG_ERROR(log_, "[{}] ORDER ENTRY IS DOWN: {}", venue_name, what);
        };
        if (auto trade = axon::oms::make_trade_session(
                exchange, config_.websocket, config_.runtime, tls_.get(),
                std::move(trade_handlers))) {
          trade->start();
          ems_->register_trade_session(venue_name, trade.get());
          trade_sessions_.push_back(std::move(trade));
        }
      }

      if (rest_ptr != nullptr) {
        axon::oms::ReconcilerCallbacks rc;
        rc.on_recovered_fill = [this](const axon::models::Fill& fill) {
          record_recovered_fill(fill);
        };
        rc.on_positions = [this, venue_name](
                              const std::vector<axon::models::Position>& positions) {
          portfolio_.update_positions(venue_name, positions);
        };
        reconcilers_[venue_name] = std::make_unique<axon::oms::Reconciler>(
            config_, exchange, rest_ptr, &orders_, std::move(rc));
      } else {
        AXON_LOG_WARN(log_,
                        "[{}] no reconciler: a dropped feed message will not be "
                        "recovered on this venue",
                        venue_name);
      }
    }

    if (sessions_.empty()) {
      AXON_LOG_WARN(log_, "no venue sessions started");
    }

    // --- hot path ---------------------------------------------------------
    // Opt-in per strategy, because the consumer must busy-poll a core to use
    // it. Nothing here changes the ZMQ path: a strategy with a ring gets both,
    // and the ring simply arrives first.
    if (!config_.runtime.shm_strategies.empty()) {
      shm_.start(config_.runtime.shm_directory, config_.runtime.shm_strategies,
                 config_.runtime.shm_slots);
      shm_.set_handlers(
          [this](const std::string& strategy,
                 const axon::transport::PlaceOrderMsg& msg) {
            on_hot_place(strategy, msg);
          },
          [this](const std::string& strategy,
                 const axon::transport::CancelOrderMsg& msg) {
            on_hot_cancel(strategy, msg);
          });
    }

    // --- control plane ----------------------------------------------------
    zmq_.start(config_.zmq,
               [this](const axon::transport::Command& c,
                      axon::transport::ZmqServer::ResponseSink sink) {
                 dispatch(c, std::move(sink));
               });

    // Publish every order update to the strategies that care.
    orders_.register_update_callback([this](const axon::models::Order& order) {
      axon::transport::Event event;
      event.event_type = "order_update";
      event.data = axon::transport::to_json(order);
      event.strategy_id = order.strategy_id.value_or(std::string());
      zmq_.publish(event);
    });
  }

  void run() {
    AXON_LOG_INFO(log_, "engine running; {} venue session(s)", sessions_.size());

    std::uint64_t iterations = 0;
    double last_report = seconds();

    while (!g_stop.load(std::memory_order_acquire)) {
      for (auto& session : sessions_) {
        session->poll();
      }
      for (auto& session : trade_sessions_) {
        session->poll();
      }
      http_->poll();
      ems_->poll();
      for (auto& [_, reconciler] : reconcilers_) {
        reconciler->poll();
      }
      shm_.poll();
      zmq_.poll();
      ++iterations;

      // A heartbeat line, so a silent engine is distinguishable from a wedged
      // one. Rate-limited to once a minute; it is not on any critical path.
      const double now = seconds();
      if (now - last_report > 60.0) {
        last_report = now;
        std::string states;
        for (const auto& s : sessions_) {
          states += s->exchange_name();
          states += "=";
          states += axon::oms::to_string(s->state());
          states += " ";
        }
        AXON_LOG_INFO(log_,
                        "alive: {} orders cached, {} fills ({} dup), {} commands, "
                        "{} events, [{}]",
                        orders_.cached_order_count(), fills_.accepted(),
                        fills_.duplicates(), zmq_.commands_handled(),
                        zmq_.events_published(), states);
        if (shm_.active()) {
          const auto hot = shm_.stats();
          AXON_LOG_INFO(log_, "hot path: {} commands, {} events ({} dropped)",
                          hot.commands_received, hot.events_published,
                          hot.events_dropped);
          // Only worth printing once something has been through it; an empty
          // histogram table in the log is noise.
          if (hot.commands_received > 0) {
            AXON_LOG_INFO(log_, "hot command latency:\n{}", shm_.latency_report());
          }
        }
      }

      axon::core::cpu_pause();
    }

    AXON_LOG_INFO(log_, "engine stopping after {} iterations", iterations);
  }

  void stop() {
    zmq_.stop();
    shm_.stop();
    reconcilers_.clear();
    rests_.clear();
    for (auto& session : trade_sessions_) {
      session->stop();
    }
    trade_sessions_.clear();
    for (auto& session : sessions_) {
      session->stop();
    }
    sessions_.clear();
    if (writer_) {
      // Flush what is queued before the process exits; the writer thread
      // drains on stop().
      writer_->stop();
    }
  }

 private:
  static double seconds() {
    return static_cast<double>(axon::core::monotonic_ns()) / 1e9;
  }

  void on_order_update(const axon::transport::OrderUpdateMsg& msg) {
    // FAST PATH FIRST. The bytes go to the co-located strategies before
    // anything below allocates, because everything below is the control-plane
    // copy and none of it is on the strategy's critical path.
    shm_.publish_order(msg, msg.strategy_id.view());

    // decode_order_update allocates. It is on the receive path, which is a
    // known compromise: OrderStore is built around the domain Order the Python
    // uses, and converting to it here is what keeps the two implementations
    // behaviourally identical.
    orders_.update_from_ws(axon::transport::decode_order_update(msg));
  }

  void on_fill(const axon::transport::FillMsg& msg) {
    const auto fill = axon::transport::decode_fill(msg);
    // Tell the reconciler we have seen this trade_id, so its next pass does
    // not "recover" a fill that arrived normally and double-count it.
    if (const auto it = reconcilers_.find(fill.exchange); it != reconcilers_.end()) {
      it->second->note_fill(fill.trade_id);
    }
    // Deduplicate BEFORE publishing anything. A trade_id a strategy has already
    // acted on must not reach it twice; that guarantee is what lets the feed
    // and the reconciler both report fills without coordinating.
    if (!fills_.add_fill(fill)) {
      return;
    }
    // Forward the ORIGINAL bytes rather than re-encoding the decoded copy.
    shm_.publish_fill(msg, msg.strategy_id.view());
    publish_fill_event(fill);
  }

  // A fill the venue had but we never saw on the feed. Same idempotency check,
  // so a reconciler pass that re-reports a known trade does nothing.
  void record_recovered_fill(const axon::models::Fill& fill) {
    if (!fills_.add_fill(fill)) {
      return;
    }
    AXON_LOG_WARN(log_, "recovered fill {} for order {}", fill.trade_id,
                    fill.order_id);
    if (shm_.active()) {
      axon::transport::FillMsg msg;
      if (axon::transport::encode_fill(fill, ++hot_seq_, msg)) {
        shm_.publish_fill(msg, fill.strategy_id.value_or(std::string()));
      }
    }
    publish_fill_event(fill);
  }

  void publish_fill_event(const axon::models::Fill& fill) {
    axon::transport::Event event;
    event.event_type = "fill_update";
    event.data = axon::transport::to_json(fill);
    event.strategy_id = fill.strategy_id.value_or(std::string());
    zmq_.publish(event);
  }

  // --- hot path commands ----------------------------------------------------
  // These come off the shared-memory ring, so there is no request_id and no
  // reply: the strategy learns the outcome from the order update it will get
  // back on the same ring. A synchronous reply would reintroduce the round
  // trip the ring exists to remove.
  void on_hot_place(const std::string& strategy,
                    const axon::transport::PlaceOrderMsg& msg) {
    auto request = axon::transport::decode_place_order(msg);
    // The ring is per-strategy, so its identity is structural. Trusting the
    // field instead would let one strategy write another's id.
    request.strategy_id = strategy;
    const std::string exchange(msg.exchange.view());
    const double started = seconds();
    ems_->place_order(exchange, request,
                      [this, exchange, started](const axon::ems::OrderResult& r) {
                        axon::util::get_metrics().observe_order_submit_latency(
                            exchange, seconds() - started);
                        if (!r.success) {
                          axon::util::get_metrics().inc_order_place_failure(
                              exchange, "rejected");
                          AXON_LOG_WARN(log_, "[{}] hot place rejected: {}",
                                          exchange, r.error);
                          return;
                        }
                        if (r.order.has_value()) {
                          orders_.add_order(*r.order);
                        }
                      });
  }

  void on_hot_cancel(const std::string& strategy,
                     const axon::transport::CancelOrderMsg& msg) {
    const std::string exchange(msg.exchange.view());
    const std::string order_id(msg.order_id.view());
    const double started = seconds();
    ems_->cancel_order(exchange, order_id,
                       [this, exchange, order_id, strategy, started](
                           bool success, const std::string& error) {
                         axon::util::get_metrics().observe_order_cancel_latency(
                             exchange, seconds() - started);
                         if (!success) {
                           AXON_LOG_WARN(log_, "[{}] hot cancel of {} failed for "
                                                 "{}: {}",
                                           exchange, order_id, strategy, error);
                         }
                       });
  }

  void dispatch(const axon::transport::Command& command,
                axon::transport::ZmqServer::ResponseSink sink) {
    using axon::transport::Response;
    const auto typed = command.typed();
    if (!typed.has_value()) {
      sink(Response::fail(command.request_id,
                          "unknown command type: " + command.command_type));
      return;
    }

    switch (*typed) {
      case axon::models::CommandType::kGetOrder: {
        const auto id = command.payload.value("order_id", std::string());
        const auto order = orders_.get_order(id);
        sink(Response::ok(command.request_id,
                          order.has_value() ? axon::transport::to_json(*order)
                                            : axon::transport::Json()));
        return;
      }

      case axon::models::CommandType::kGetAllOrders:
      case axon::models::CommandType::kGetActiveOrders: {
        const bool active_only =
            *typed == axon::models::CommandType::kGetActiveOrders;
        auto orders = active_only ? orders_.active_orders() : orders_.all_orders();
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
        const auto account = portfolio_.account(exchange, currency);
        sink(Response::ok(command.request_id,
                          account.has_value()
                              ? axon::transport::to_json(*account)
                              : axon::transport::Json()));
        return;
      }

      case axon::models::CommandType::kGetFillsByOrder:
      case axon::models::CommandType::kGetFillsByStrategy: {
        const bool by_order =
            *typed == axon::models::CommandType::kGetFillsByOrder;
        const auto key = by_order
                             ? command.payload.value("order_id", std::string())
                             : command.payload.value("strategy_id", std::string());
        const auto matches = by_order ? fills_.by_order(key) : fills_.by_strategy(key);
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

        const std::string request_id = command.request_id;
        const double started = seconds();
        // DEFERRED. Placing an order is a network round trip; replying inline
        // would stall every venue feed for its duration.
        ems_->place_order(
            exchange, *request,
            [this, sink, request_id, exchange, started](
                const axon::ems::OrderResult& result) mutable {
              axon::util::get_metrics().observe_order_submit_latency(
                  exchange, seconds() - started);
              if (!result.success) {
                axon::util::get_metrics().inc_order_place_failure(exchange,
                                                                   "rejected");
                sink(Response::fail(request_id, result.error));
                return;
              }
              if (result.order.has_value()) {
                orders_.add_order(*result.order);
                sink(Response::ok(request_id,
                                  axon::transport::to_json(*result.order)));
              } else {
                // Accepted, but the venue's REST reply carries no order we can
                // trust; the authoritative state arrives on the feed.
                sink(Response::ok(request_id, axon::transport::Json()));
              }
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
                             axon::util::get_metrics()
                                 .observe_order_cancel_latency(exchange,
                                                               seconds() - started);
                             if (!success) {
                               sink(Response::fail(request_id, error));
                               return;
                             }
                             sink(Response::ok(request_id,
                                               axon::transport::Json(true)));
                           });
        return;
      }

      case axon::models::CommandType::kModifyOrder: {
        const auto exchange = command.payload.value("exchange", std::string());
        const auto order_id = command.payload.value("order_id", std::string());
        std::optional<axon::core::Qty> amount;
        std::optional<axon::core::Price> price;
        if (command.payload.contains("amount") && !command.payload["amount"].is_null()) {
          amount = axon::core::Qty::from_double(
              command.payload["amount"].get<double>());
        }
        if (command.payload.contains("price") && !command.payload["price"].is_null()) {
          price = axon::core::Price::from_double(
              command.payload["price"].get<double>());
        }
        const std::string request_id = command.request_id;
        ems_->modify_order(exchange, order_id, amount, price,
                           [sink, request_id](
                               const axon::ems::OrderResult& result) mutable {
                             if (!result.success) {
                               sink(Response::fail(request_id, result.error));
                               return;
                             }
                             sink(Response::ok(
                                 request_id,
                                 result.order.has_value()
                                     ? axon::transport::to_json(*result.order)
                                     : axon::transport::Json()));
                           });
        return;
      }

      case axon::models::CommandType::kGetTicker: {
        const auto exchange = command.payload.value("exchange", std::string());
        const auto instrument = command.payload.value("instrument", std::string());
        const auto it = rests_.find(exchange);
        if (it == rests_.end()) {
          sink(Response::fail(command.request_id,
                              "no REST client for " + exchange));
          return;
        }
        // A live REST read, deferred like order entry: it is a round trip and
        // must not be answered inline.
        const std::string request_id = command.request_id;
        it->second->get_ticker(instrument,
                        [sink, request_id](std::optional<axon::models::Ticker> ticker,
                                           const std::string& error) mutable {
                          if (!ticker.has_value()) {
                            sink(Response::fail(request_id, error));
                            return;
                          }
                          sink(Response::ok(request_id,
                                            axon::transport::to_json(*ticker)));
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
        for (const auto& p : portfolio_.positions(exchange)) {
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
  std::unique_ptr<axon::net::TlsContext> tls_;
  std::vector<std::unique_ptr<axon::oms::VenueSession>> sessions_;
  // The extra order-entry connections. Separate from sessions_ because they
  // carry no feed and must be torn down first -- a trade session outliving the
  // EMS pointer that references it would be a use-after-free on shutdown.
  std::vector<std::unique_ptr<axon::oms::VenueSession>> trade_sessions_;
  axon::oms::OrderStore orders_;
  axon::oms::FillStore fills_;
  axon::oms::PortfolioStore portfolio_;
  axon::transport::ZmqServer zmq_;
  axon::transport::ShmBridge shm_;
  // Sequence for messages this process originates. Venue-sourced messages
  // carry the parser's numbering; only recovered fills are minted here.
  std::uint64_t hot_seq_ = 0;
  std::unique_ptr<axon::net::HttpClient> http_;
  std::unique_ptr<axon::ems::EmsService> ems_;
  std::unique_ptr<axon::repository::PostgresWriter> writer_;
  std::map<std::string, std::unique_ptr<axon::oms::VenueRest>> rests_;
  std::map<std::string, std::unique_ptr<axon::oms::Reconciler>> reconcilers_;
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
