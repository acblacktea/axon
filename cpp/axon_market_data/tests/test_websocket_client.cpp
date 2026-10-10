// WebSocketClient against a real TLS WebSocket server on loopback: delivery,
// sending, heartbeats, reconnection after every way a link can end, backoff,
// timeouts, and the connection-health metrics.
//
// Every adapter's stability rests on this class: if it fails to notice a dead
// link or reconnects badly, every topic on that venue goes quiet at once.

#include "test_support.h"
#include "ws_test_server.h"

namespace mds_test {
namespace {

using namespace std::chrono_literals;

class WsClientTest : public MetricsTest {
protected:
    WebSocketClient::Config config(unsigned short port) {
        WebSocketClient::Config c;
        c.host                 = "127.0.0.1";
        c.port                 = std::to_string(port);
        c.path                 = "/ws";
        c.tag                  = "test_ws";
        c.ping_interval        = 30s;
        c.connect_timeout      = 2s;
        c.reconnect_base_delay = 0.05;
        c.reconnect_max_delay  = 0.4;
        return c;
    }

    std::shared_ptr<WebSocketClient> make(WebSocketClient::Config c) {
        client = std::make_shared<WebSocketClient>(
            ioc, std::move(c), [this](std::string_view m) { messages.emplace_back(m); },
            [this](bool up) { states.push_back({up, steady_seconds()}); }, log.logger);
        return client;
    }

    std::shared_ptr<WebSocketClient> connect() {
        make(config(server.port()))->start();
        EXPECT_TRUE(run_until(ioc, [&] { return client->is_connected(); }));
        return client;
    }

    // A port with nothing listening on it.
    static unsigned short closed_port() {
        net::io_context tmp;
        tcp::acceptor a(tmp, {net::ip::make_address("127.0.0.1"), 0});
        return a.local_endpoint().port();
    }

    size_t ups() const {
        return std::count_if(states.begin(), states.end(), [](auto& s) { return s.first; });
    }
    size_t downs() const { return states.size() - ups(); }

    // The context is deliberately NOT run after stop(): stopping mid-connect
    // leaves an operation pending on a stream stop() has already destroyed
    // (see DISABLED_StopWhileConnectingIsMemorySafe). Pending handlers are
    // destroyed with the io_context instead, never resumed.
    void TearDown() override {
        if (client) client->stop();
    }

    const std::map<std::string, std::string> tag{{"exchange", "test_ws"}};

    WsTestServer                         server;
    net::io_context                      ioc;
    CapturedLogger                       log;
    std::vector<std::string>             messages;
    std::vector<std::pair<bool, double>> states;
    std::shared_ptr<WebSocketClient>     client;
};

// ===========================================================================
// Delivery
// ===========================================================================

TEST_F(WsClientTest, DeliversFramesInOrder) {
    connect();
    for (auto m : {"one", "two", "three"}) server.send(m);
    ASSERT_TRUE(run_until(ioc, [&] { return messages.size() == 3; }));
    EXPECT_EQ(messages, (std::vector<std::string>{"one", "two", "three"}));
}

TEST_F(WsClientTest, LargeFrameArrivesWhole) {
    connect();
    std::string big(2 * 1024 * 1024, 'x');
    big.front() = '{';
    big.back()  = '}';
    server.send(big);
    ASSERT_TRUE(run_until(ioc, [&] { return messages.size() == 1; }));
    EXPECT_EQ(messages[0].size(), big.size());
    EXPECT_EQ(messages[0], big);
}

TEST_F(WsClientTest, SendsAreQueuedAndKeepTheirOrder) {
    connect();
    // Beast allows one outstanding write; these overlap and must queue.
    for (auto m : {"a", "b", "c", "d"})
        net::co_spawn(ioc, client->send(m), net::detached);
    ASSERT_TRUE(run_until(ioc, [&] { return server.received().size() == 4; }));
    EXPECT_EQ(server.received(), (std::vector<std::string>{"a", "b", "c", "d"}));
}

// Messages sent while disconnected are dropped, not replayed on reconnect --
// subscriptions are re-sent by the adapter's on-connect hook instead.
TEST_F(WsClientTest, SendWhileDisconnectedIsDropped) {
    make(config(server.port()));
    net::co_spawn(ioc, client->send("early"), net::detached);
    drain(ioc);
    client->start();
    ASSERT_TRUE(run_until(ioc, [&] { return client->is_connected(); }));
    net::co_spawn(ioc, client->send("late"), net::detached);
    ASSERT_TRUE(run_until(ioc, [&] { return server.received().size() == 1; }));
    EXPECT_EQ(server.received(), (std::vector<std::string>{"late"}));
}

// Venues that want an application-level heartbeat (OKX "ping", Bybit
// {"op":"ping"}, Hyperliquid {"method":"ping"}) get it as a text frame.
TEST_F(WsClientTest, TextHeartbeatIsSentOnTheInterval) {
    auto c          = config(server.port());
    c.ping_interval = 1s;
    c.ping_text     = R"({"op":"ping"})";
    make(c)->start();
    ASSERT_TRUE(run_until(ioc, [&] { return server.received().size() >= 2; }, 3500ms));
    for (auto& m : server.received()) EXPECT_EQ(m, R"({"op":"ping"})");
}

// Without ping_text a WebSocket control ping is sent; Beast answers it on the
// server, so the link stays up and no data frame is seen.
TEST_F(WsClientTest, ControlPingKeepsTheLinkUpWithoutDataFrames) {
    auto c          = config(server.port());
    c.ping_interval = 1s;
    make(c)->start();
    run_for(ioc, 2300ms);
    EXPECT_TRUE(client->is_connected());
    EXPECT_TRUE(server.received().empty());
    EXPECT_EQ(server.handshakes(), 1);
}

// ===========================================================================
// Connection-health metrics
// ===========================================================================

TEST_F(WsClientTest, ConnectIsMeasuredAndTheGaugeRaised) {
    connect();
    EXPECT_EQ(metric("axon_mds_ws_connected", tag), 1.0);
    EXPECT_EQ(metric("axon_mds_ws_connect_duration_seconds_count", tag), 1.0);
    EXPECT_GT(*metric("axon_mds_ws_connect_duration_seconds_sum", tag), 0.0);
    EXPECT_EQ(metric_or_zero("axon_mds_ws_connect_failure_total", tag), 0.0);
}

// A series that only appears on the first successful connect is ABSENT while
// the feed is down, and absent reads as "not configured" rather than "down".
TEST_F(WsClientTest, GaugeIsPublishedAsZeroBeforeTheFirstConnect) {
    make(config(closed_port()))->start();
    EXPECT_EQ(metric("axon_mds_ws_connected", tag), 0.0);
}

// The cost of the whole receive path for one frame: parse, book, publish.
TEST_F(WsClientTest, HandlerDurationIsRecordedPerFrame) {
    connect();
    for (int i = 0; i < 5; ++i) server.send("{}");
    ASSERT_TRUE(run_until(ioc, [&] { return messages.size() == 5; }));
    EXPECT_EQ(metric("axon_mds_handler_duration_seconds_count", tag), 5.0);
}

// ===========================================================================
// Reconnection
// ===========================================================================

TEST_F(WsClientTest, ReconnectsAfterTheServerClosesCleanly) {
    connect();
    server.close_all();
    ASSERT_TRUE(run_until(ioc, [&] { return ups() == 2; }));

    EXPECT_EQ(states.size(), 3u);  // up, down, up
    EXPECT_FALSE(states[1].first);
    EXPECT_EQ(server.handshakes(), 2);
    EXPECT_EQ(metric("axon_mds_ws_reconnect_total", tag), 1.0);
    EXPECT_EQ(metric("axon_mds_ws_connected", tag), 1.0);

    // And the new connection carries data.
    server.send("after");
    ASSERT_TRUE(run_until(ioc, [&] { return !messages.empty(); }));
    EXPECT_EQ(messages.back(), "after");
}

TEST_F(WsClientTest, ReconnectsAfterTheConnectionIsReset) {
    connect();
    server.drop_all();
    ASSERT_TRUE(run_until(ioc, [&] { return ups() == 2; }));
    EXPECT_EQ(server.handshakes(), 2);
    EXPECT_EQ(metric("axon_mds_ws_connected", tag), 1.0);
}

TEST_F(WsClientTest, RecoversFromRepeatedDrops) {
    connect();
    for (int i = 0; i < 3; ++i) {
        server.drop_all();
        ASSERT_TRUE(run_until(ioc, [&] { return ups() == size_t(i) + 2; }, 8s)) << i;
    }
    EXPECT_EQ(server.handshakes(), 4);
    EXPECT_EQ(metric("axon_mds_ws_session_duration_seconds_count", tag), 3.0);
}

// A flapping link and a daily reconnect have the same reconnect rate over a
// week; only the session length separates them.
TEST_F(WsClientTest, SessionLengthIsRecordedOncePerDrop) {
    connect();
    run_for(ioc, 300ms);
    server.close_all();
    ASSERT_TRUE(run_until(ioc, [&] { return ups() == 2; }));

    EXPECT_EQ(metric("axon_mds_ws_session_duration_seconds_count", tag), 1.0);
    EXPECT_GE(*metric("axon_mds_ws_session_duration_seconds_sum", tag), 0.3);
}

TEST_F(WsClientTest, StateCallbackSeesEveryTransition) {
    connect();
    server.drop_all();
    ASSERT_TRUE(run_until(ioc, [&] { return ups() == 2; }));
    ASSERT_GE(states.size(), 3u);
    EXPECT_TRUE(states[0].first);
    EXPECT_FALSE(states[1].first);
    EXPECT_TRUE(states.back().first);
}

// ===========================================================================
// Backoff
// ===========================================================================

TEST_F(WsClientTest, RefusedConnectionsBackOffExponentiallyToTheCap) {
    make(config(closed_port()))->start();
    run_for(ioc, 1700ms);

    // One failure notification per attempt: the first try, then each retry
    // after delays of 0.05, 0.1, 0.2, 0.4 (cap), 0.4, ...
    ASSERT_GE(downs(), 6u);
    std::vector<double> gaps;
    for (size_t i = 1; i < states.size(); ++i) gaps.push_back(states[i].second - states[i - 1].second);

    const double expected[] = {0.05, 0.1, 0.2, 0.4, 0.4};
    for (size_t i = 0; i < 5; ++i) {
        EXPECT_GE(gaps[i], expected[i] * 0.95) << "retry " << i << " came too early";
        EXPECT_LE(gaps[i], expected[i] + 0.25) << "retry " << i << " came too late";
    }
    EXPECT_EQ(metric("axon_mds_ws_connect_failure_total", tag), double(downs()));
    // Each retry is counted before it waits, so the latest may still be pending.
    const double retries = metric_or_zero("axon_mds_ws_reconnect_total", tag);
    EXPECT_GE(retries, double(downs() - 1));
    EXPECT_LE(retries, double(downs()));
    EXPECT_EQ(metric("axon_mds_ws_connected", tag), 0.0);
}

TEST_F(WsClientTest, RetriesReachAServerThatComesBack) {
    // Point at a port, fail a few times, then have something listen there.
    const auto port = closed_port();
    make(config(port))->start();
    run_for(ioc, 200ms);
    EXPECT_GE(downs(), 2u);

    tcp::acceptor raw(ioc, {net::ip::make_address("127.0.0.1"), port});
    std::optional<tcp::socket> accepted;
    raw.async_accept([&](boost::system::error_code ec, tcp::socket s) {
        if (!ec) accepted.emplace(std::move(s));
    });
    ASSERT_TRUE(run_until(ioc, [&] { return accepted.has_value(); }, 3s));
}

// A peer that accepts TCP and then says nothing must not hold the client
// forever: the TLS handshake is bounded by connect_timeout.
TEST_F(WsClientTest, StalledHandshakeTimesOutAndRetries) {
    tcp::acceptor            raw(ioc, {net::ip::make_address("127.0.0.1"), 0});
    std::vector<tcp::socket> held;
    std::function<void()>    accept_next = [&] {
        raw.async_accept([&](boost::system::error_code ec, tcp::socket s) {
            if (ec) return;
            held.push_back(std::move(s));  // accept, then never speak TLS
            accept_next();
        });
    };
    accept_next();

    auto c            = config(raw.local_endpoint().port());
    c.connect_timeout = 1s;
    make(c)->start();

    ASSERT_TRUE(run_until(ioc, [&] { return held.size() >= 2; }, 4s));
    EXPECT_GE(metric_or_zero("axon_mds_ws_connect_failure_total", tag), 1.0);
    EXPECT_EQ(ups(), 0u);
    raw.close();
}

// ===========================================================================
// Stop
// ===========================================================================

TEST_F(WsClientTest, StopClosesTheConnection) {
    connect();
    ASSERT_EQ(server.open_connections(), 1);
    client->stop();
    run_for(ioc, 200ms);
    EXPECT_FALSE(client->is_connected());
    EXPECT_EQ(server.open_connections(), 0);
    EXPECT_EQ(server.handshakes(), 1);  // and did not come back
}

TEST_F(WsClientTest, StopEndsTheRetryLoop) {
    make(config(closed_port()))->start();
    run_for(ioc, 120ms);
    client->stop();
    const auto attempts = downs();
    run_for(ioc, 800ms);
    EXPECT_LE(downs(), attempts + 1);  // at most the attempt already in flight
}

TEST_F(WsClientTest, StartIsIdempotent) {
    make(config(server.port()));
    client->start();
    client->start();
    ASSERT_TRUE(run_until(ioc, [&] { return client->is_connected(); }));
    run_for(ioc, 200ms);
    EXPECT_EQ(server.handshakes(), 1);
}

// ===========================================================================
// Known defects
// ===========================================================================

// BUG (use-after-free, found by ASan): stop() destroys the stream with
// ws_.reset() while a connect or TLS handshake on it is still in flight. The
// aborted operation then completes against freed memory. Service::stop()
// happens to stop the io_context straight afterwards, so the handler never
// runs today; stopping one client while the loop keeps going -- restarting a
// single venue, say -- would corrupt the heap. Only an ASan build reports it.
TEST_F(WsClientTest, DISABLED_StopWhileConnectingIsMemorySafe) {
    tcp::acceptor              raw(ioc, {net::ip::make_address("127.0.0.1"), 0});
    std::optional<tcp::socket> held;
    raw.async_accept([&](boost::system::error_code ec, tcp::socket s) {
        if (!ec) held.emplace(std::move(s));  // accept, then never speak TLS
    });
    make(config(raw.local_endpoint().port()))->start();
    ASSERT_TRUE(run_until(ioc, [&] { return held.has_value(); }));
    run_for(ioc, 50ms);  // the client is now inside the TLS handshake

    client->stop();
    run_for(ioc, 200ms);  // the aborted handshake completes here
    EXPECT_FALSE(client->is_connected());
}

// BUG: the backoff attempt counter lives for the whole reconnect() loop and is
// never reset after a connection succeeds. Every drop after a healthy session
// therefore waits longer than the last -- base, 2x, 4x, ... up to the cap -- so
// a venue that resets its connections daily (several do) ends up with the
// maximum delay, 60s in production, as its normal reconnect time. There is
// also no jitter, so every connection that drops together retries in lockstep.
TEST_F(WsClientTest, DISABLED_BackoffResetsAfterAHealthySession) {
    auto c                 = config(server.port());
    c.reconnect_base_delay = 0.05;
    c.reconnect_max_delay  = 5.0;
    make(c)->start();
    ASSERT_TRUE(run_until(ioc, [&] { return client->is_connected(); }));

    for (int i = 0; i < 5; ++i) {
        run_for(ioc, 100ms);  // a healthy session
        const double dropped = steady_seconds();
        server.drop_all();
        ASSERT_TRUE(run_until(ioc, [&] { return ups() == size_t(i) + 2; }, 10s));
        EXPECT_LT(states.back().second - dropped, 0.3)
            << "reconnect #" << i + 1 << " waited as if it were a consecutive failure";
    }
}

// BUG: Config::pong_timeout exists but nothing reads it. With a text heartbeat
// nothing ever checks that the venue answers, and Beast's suggested client
// timeouts disable idle pings, so a half-open TCP connection -- the venue gone,
// no RST -- is never detected. The client sits "connected" forever while every
// topic on it is silent.
TEST_F(WsClientTest, DISABLED_SilentPeerIsDetectedWithinPingPlusPongTimeout) {
    auto c          = config(server.port());
    c.ping_interval = 1s;
    c.pong_timeout  = 1s;
    c.ping_text     = "ping";  // the test server never answers it
    make(c)->start();
    ASSERT_TRUE(run_until(ioc, [&] { return client->is_connected(); }));

    EXPECT_TRUE(run_until(ioc, [&] { return downs() >= 1; }, 3500ms))
        << "a peer that stopped answering heartbeats was never noticed";
}

}  // namespace
}  // namespace mds_test
