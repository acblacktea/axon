// StrategyClient's control-plane request/response matching, against a fake
// engine on a real ZMQ ROUTER socket.
//
// The case that matters: a request times out on the client, and the engine
// answers it anyway, later -- its own wait on the venue is 30s against the
// client's 5s. That late reply must not be taken as the answer to the NEXT
// request. Before replies were matched by request_id, it was, and every
// request after it got its predecessor's answer until the strategy restarted.

#include <gtest/gtest.h>

#include <zmq.hpp>
#include <zmq_addon.hpp>

#include <chrono>
#include <deque>
#include <functional>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "axon/client/strategy_client.h"
#include "axon/models/order.h"
#include "axon/transport/messages.h"
#include "axon/transport/wire.h"

namespace {

using namespace std::chrono_literals;
using axon::client::StrategyClient;
using axon::client::StrategyClientConfig;

// One request as the fake engine saw it: the DEALER's routing identity, and
// the decoded command.
struct Received {
  std::string identity;
  axon::transport::Command command;
};

// A ROUTER standing in for the engine. The test thread scripts it: take the
// next request, reply to any request seen so far, in any order.
class FakeEngine {
 public:
  FakeEngine() {
    router_.set(zmq::sockopt::linger, 0);
    router_.set(zmq::sockopt::rcvtimeo, 2000);
    router_.bind("tcp://127.0.0.1:*");
    endpoint_ = router_.get(zmq::sockopt::last_endpoint);
  }

  const std::string& endpoint() const { return endpoint_; }

  std::optional<Received> next_request() {
    std::vector<zmq::message_t> parts;
    if (!zmq::recv_multipart(router_, std::back_inserter(parts))) return std::nullopt;
    // [identity, empty delimiter, body]
    auto command = axon::transport::deserialize_command(parts.back().to_string_view());
    if (!command) return std::nullopt;
    return Received{parts.front().to_string(), *command};
  }

  // Answers `r` with an order whose id is `order_id`.
  void reply_with_order(const Received& r, const std::string& order_id) {
    axon::models::Order o;
    o.order_id = order_id;
    o.exchange = "binance";
    o.instrument = "BTCUSDT";
    send(r, axon::transport::Response::ok(r.command.request_id, axon::transport::to_json(o)));
  }

  void reply_raw(const Received& r, const std::string& body) {
    router_.send(zmq::buffer(r.identity), zmq::send_flags::sndmore);
    router_.send(zmq::message_t(), zmq::send_flags::sndmore);
    router_.send(zmq::buffer(body), zmq::send_flags::none);
  }

 private:
  void send(const Received& r, const axon::transport::Response& resp) {
    reply_raw(r, axon::transport::serialize_response(resp));
  }

  zmq::context_t ctx_{1};
  zmq::socket_t router_{ctx_, zmq::socket_type::router};
  std::string endpoint_;
};

class StrategyClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    StrategyClientConfig cfg;
    cfg.strategy_id = "test";
    cfg.router_endpoint = engine.endpoint();
    cfg.pub_endpoint = "tcp://127.0.0.1:1";  // events are not under test
    cfg.request_timeout_ms = 300;
    client.connect(cfg, {});
  }

  // Runs `fn` on the client while the fake engine is driven by `script` on
  // this thread.
  template <typename Fn>
  auto with_engine(Fn fn, const std::function<void()>& script) {
    std::optional<decltype(fn())> result;
    std::thread t([&] { result = fn(); });
    script();
    t.join();
    return *result;
  }

  FakeEngine engine;
  StrategyClient client;
};

TEST_F(StrategyClientTest, ReplyIsReturnedToItsRequest) {
  std::string error;
  auto order = with_engine([&] { return client.get_order("A", error); }, [&] {
    auto r = engine.next_request();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->command.command_type, "get_order");
    engine.reply_with_order(*r, "A");
  });
  ASSERT_TRUE(order.has_value()) << error;
  EXPECT_EQ(order->order_id, "A");
  EXPECT_EQ(client.stats().stale_replies, 0u);
}

TEST_F(StrategyClientTest, UnansweredRequestTimesOut) {
  std::string error;
  const auto t0 = std::chrono::steady_clock::now();
  auto order = with_engine([&] { return client.get_order("A", error); },
                           [&] { ASSERT_TRUE(engine.next_request().has_value()); });
  EXPECT_FALSE(order.has_value());
  EXPECT_EQ(error, "request timed out");
  EXPECT_GE(std::chrono::steady_clock::now() - t0, 290ms);
}

// The bug this file exists for.
TEST_F(StrategyClientTest, LateReplyToATimedOutRequestIsNotTakenByTheNextOne) {
  // Request A times out: the engine sees it but does not answer in time.
  std::string error;
  std::optional<Received> a;
  auto first = with_engine([&] { return client.get_order("A", error); },
                           [&] { a = engine.next_request(); });
  ASSERT_TRUE(a.has_value());
  EXPECT_FALSE(first.has_value());
  EXPECT_EQ(error, "request timed out");

  // Request B goes out; the engine finally answers A, THEN answers B.
  error.clear();
  auto second = with_engine([&] { return client.get_order("B", error); }, [&] {
    auto b = engine.next_request();
    ASSERT_TRUE(b.has_value());
    EXPECT_NE(b->command.request_id, a->command.request_id);
    engine.reply_with_order(*a, "A");  // late
    engine.reply_with_order(*b, "B");
  });

  ASSERT_TRUE(second.has_value()) << error;
  EXPECT_EQ(second->order_id, "B") << "request B was handed request A's reply";
  EXPECT_EQ(client.stats().stale_replies, 1u);

  // And the request after that is not shifted by one either.
  auto third = with_engine([&] { return client.get_order("C", error); }, [&] {
    auto c = engine.next_request();
    ASSERT_TRUE(c.has_value());
    engine.reply_with_order(*c, "C");
  });
  ASSERT_TRUE(third.has_value()) << error;
  EXPECT_EQ(third->order_id, "C");
}

// Discarding stale replies must not stretch a request past its own timeout:
// the deadline covers the whole wait, not each receive.
TEST_F(StrategyClientTest, StaleRepliesDoNotExtendTheTimeout) {
  std::string error;
  std::optional<Received> a;
  with_engine([&] { return client.get_order("A", error); },
              [&] { a = engine.next_request(); });
  ASSERT_TRUE(a.has_value());

  // Timed on the client's own thread: the engine script below outlives it.
  std::chrono::steady_clock::duration elapsed{};
  auto second = with_engine([&] {
    const auto t0 = std::chrono::steady_clock::now();
    auto r = client.get_order("B", error);
    elapsed = std::chrono::steady_clock::now() - t0;
    return r;
  }, [&] {
    ASSERT_TRUE(engine.next_request().has_value());
    // Keep feeding A's reply and never answer B.
    for (int i = 0; i < 5; ++i) {
      engine.reply_with_order(*a, "A");
      std::this_thread::sleep_for(100ms);
    }
  });

  EXPECT_FALSE(second.has_value());
  EXPECT_EQ(error, "request timed out");
  EXPECT_LT(elapsed, 450ms) << "the timeout restarted on every stale reply";
  EXPECT_GE(client.stats().stale_replies, 1u);
}

// A reply that is not a response at all is a protocol failure, not something
// to skip past.
TEST_F(StrategyClientTest, MalformedReplyFailsTheRequest) {
  std::string error;
  auto order = with_engine([&] { return client.get_order("A", error); }, [&] {
    auto r = engine.next_request();
    ASSERT_TRUE(r.has_value());
    engine.reply_raw(*r, "not json");
  });
  EXPECT_FALSE(order.has_value());
  EXPECT_EQ(error, "malformed response");
}

TEST_F(StrategyClientTest, EngineErrorIsReported) {
  std::string error;
  auto order = with_engine([&] { return client.get_order("A", error); }, [&] {
    auto r = engine.next_request();
    ASSERT_TRUE(r.has_value());
    engine.reply_raw(*r, axon::transport::serialize_response(
                             axon::transport::Response::fail(r->command.request_id,
                                                             "order not found")));
  });
  EXPECT_FALSE(order.has_value());
  EXPECT_EQ(error, "order not found");
}

// ===========================================================================
// submit_order: a refusal and a timeout are different answers
// ===========================================================================

axon::models::OrderRequest limit_buy() {
  axon::models::OrderRequest r;
  r.instrument = "BTCUSDT";
  r.side = axon::models::OrderSide::kBuy;
  r.order_type = axon::models::OrderType::kLimit;
  r.amount = *axon::core::Qty::from_string("0.002");
  r.price = axon::core::Price::from_string("60000");
  return r;
}

TEST_F(StrategyClientTest, AcceptedPlacementReturnsTheOrder) {
  const auto req = limit_buy();
  auto result = with_engine([&] { return client.submit_order("binance", req); }, [&] {
    auto r = engine.next_request();
    ASSERT_TRUE(r.has_value());
    EXPECT_EQ(r->command.payload["request"].value("internal_order_id", ""),
              req.internal_order_id);
    engine.reply_with_order(*r, "28618940365");
  });
  EXPECT_EQ(result.outcome, axon::client::PlaceOutcome::kAccepted);
  ASSERT_TRUE(result.order.has_value());
  EXPECT_EQ(result.order->order_id, "28618940365");
  EXPECT_EQ(result.internal_order_id, req.internal_order_id);
}

TEST_F(StrategyClientTest, RefusedPlacementIsRejected) {
  auto result = with_engine([&] { return client.submit_order("binance", limit_buy()); }, [&] {
    auto r = engine.next_request();
    ASSERT_TRUE(r.has_value());
    engine.reply_raw(*r, axon::transport::serialize_response(axon::transport::Response::fail(
                             r->command.request_id, "Margin is insufficient.")));
  });
  EXPECT_EQ(result.outcome, axon::client::PlaceOutcome::kRejected);
  EXPECT_EQ(result.error, "Margin is insufficient.");
}

// The engine sent it and the venue never answered.
TEST_F(StrategyClientTest, EngineReportedTimeoutIsUnknown) {
  const auto req = limit_buy();
  auto result = with_engine([&] { return client.submit_order("binance", req); }, [&] {
    auto r = engine.next_request();
    ASSERT_TRUE(r.has_value());
    engine.reply_raw(*r, axon::transport::serialize_response(axon::transport::Response::fail(
                             r->command.request_id,
                             std::string(axon::transport::kOutcomeUnknownPrefix) +
                                 "the venue did not reply in time")));
  });
  EXPECT_EQ(result.outcome, axon::client::PlaceOutcome::kUnknown);
  EXPECT_EQ(result.internal_order_id, req.internal_order_id);
}

// Our own timeout says nothing about whether the engine acted on it.
TEST_F(StrategyClientTest, ClientTimeoutIsUnknownNotRejected) {
  auto result = with_engine([&] { return client.submit_order("binance", limit_buy()); },
                            [&] { ASSERT_TRUE(engine.next_request().has_value()); });
  EXPECT_EQ(result.outcome, axon::client::PlaceOutcome::kUnknown);
  EXPECT_EQ(result.error.rfind(axon::transport::kOutcomeUnknownPrefix, 0), 0u) << result.error;
}

// The older entry point folds both failures into `error`, but keeps them
// distinguishable by the prefix.
TEST_F(StrategyClientTest, PlaceOrderKeepsUnknownDistinguishable) {
  std::string error;
  auto order = with_engine([&] { return client.place_order("binance", limit_buy(), error); },
                           [&] { ASSERT_TRUE(engine.next_request().has_value()); });
  EXPECT_FALSE(order.has_value());
  EXPECT_EQ(error.rfind(axon::transport::kOutcomeUnknownPrefix, 0), 0u) << error;
}

}  // namespace
