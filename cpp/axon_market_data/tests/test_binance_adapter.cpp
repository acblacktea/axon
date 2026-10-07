// Binance: bookTicker, kline_1m and the REST-snapshot + diff-stream orderbook,
// for spot and both futures markets.
//
// The orderbook is the part that matters. Binance publishes diffs, and a book
// is only correct if it is seeded from a REST snapshot and every diff after it
// chains on without a hole -- with a join rule that differs between spot and
// futures. Each rule below is a way the book silently goes wrong if broken.

#include "test_support.hpp"

#include <deque>

namespace mds_test {
namespace {

using A = AdapterTestAccess;

std::string depth_update(const std::string& sym, int64_t U, int64_t u,
                         const std::string& bids, const std::string& asks,
                         std::optional<int64_t> pu = std::nullopt,
                         int64_t E = 0) {
    if (E == 0) E = now_ms() - 25;
    std::string s = R"({"e":"depthUpdate","E":)" + std::to_string(E) +
                    R"(,"s":")" + sym + R"(","U":)" + std::to_string(U) +
                    R"(,"u":)" + std::to_string(u);
    if (pu) s += R"(,"pu":)" + std::to_string(*pu);
    s += R"(,"b":)" + bids + R"(,"a":)" + asks + "}";
    return s;
}

std::string snapshot_body(int64_t last_update_id, const std::string& bids,
                          const std::string& asks) {
    return R"({"lastUpdateId":)" + std::to_string(last_update_id) +
           R"(,"bids":)" + bids + R"(,"asks":)" + asks + "}";
}

// Stands in for the REST endpoint. Records what was asked for and answers
// from a queue; an empty queue means the request fails.
struct FakeRest {
    std::vector<std::string> hosts;
    std::vector<std::string> targets;
    std::deque<std::string>  bodies;
};

class BinanceTest : public MetricsTest {
protected:
    std::shared_ptr<BinanceAdapter> make(MarketType mt, std::vector<std::string> depth,
                                         std::vector<std::string> ticker = {},
                                         std::vector<std::string> kline  = {},
                                         size_t depth_levels = 400) {
        auto a = std::make_shared<BinanceAdapter>(
            ioc, exchange("binance", mt, depth, ticker, kline, depth_levels),
            sink.callback(), log.logger);
        A::set_fetcher(*a, [rest = rest](std::string host, std::string target)
                               -> net::awaitable<HttpResponse> {
            rest->hosts.push_back(host);
            rest->targets.push_back(target);
            if (rest->bodies.empty()) throw std::runtime_error("connection refused");
            std::string body = std::move(rest->bodies.front());
            rest->bodies.pop_front();
            co_return HttpResponse{200, std::move(body), {}};
        });
        return a;
    }

    // Brings a symbol to a synced book at `snapshot_id` with no buffered
    // diffs, then lets the first live diff join it.
    void sync(BinanceAdapter& a, const std::string& sym, int64_t snapshot_id,
              const std::string& bids = R"([["2000.00","1.0"],["1999.00","2.0"]])",
              const std::string& asks = R"([["2001.00","1.5"],["2002.00","3.0"]])") {
        rest->bodies.push_back(snapshot_body(snapshot_id, bids, asks));
        A::schedule_sync(a, sym);
        drain(ioc);
    }

    net::io_context           ioc;
    EventSink                 sink;
    CapturedLogger            log;
    std::shared_ptr<FakeRest> rest = std::make_shared<FakeRest>();
};

// ===========================================================================
// Configuration: names, streams, endpoints
// ===========================================================================

TEST_F(BinanceTest, ExchangeNameCarriesTheMarket) {
    EXPECT_EQ(make(MarketType::Spot, {"ETH_USDT_SPOT"})->exchange_name(), "binance_spot");
    EXPECT_EQ(make(MarketType::UsdtFutures, {"ETH_USDT_PERP"})->exchange_name(),
              "binance_usdt_futures");
    EXPECT_EQ(make(MarketType::CoinFutures, {"BTC_USD_PERP"})->exchange_name(),
              "binance_coin_futures");
}

TEST_F(BinanceTest, StreamNamesAreLowercaseAndOnePerTopic) {
    auto cfg = exchange("binance", MarketType::Spot, {"ETH_USDT_SPOT"}, {"BTC_USDT_SPOT"},
                        {"SOL_USDT_SPOT"});
    cfg.update_speed = "1000ms";
    BinanceAdapter a(ioc, cfg, sink.callback(), log.logger);

    EXPECT_EQ(A::streams(a), (std::vector<std::string>{
                                 "ethusdt@depth@1000ms", "btcusdt@bookTicker",
                                 "solusdt@kline_1m"}));
}

TEST_F(BinanceTest, EveryConfiguredTopicIsDeclaredAsASubscription) {
    make(MarketType::Spot, {"ETH_USDT_SPOT"}, {"ETH_USDT_SPOT"}, {"BTC_USDT_SPOT"});

    for (auto [dt, sym] : {std::pair{"depth", "ETH_USDT_SPOT"},
                           std::pair{"ticker", "ETH_USDT_SPOT"},
                           std::pair{"kline", "BTC_USDT_SPOT"}}) {
        EXPECT_EQ(metric("axon_mds_subscription",
                         {{"exchange", "binance_spot"}, {"data_type", dt}, {"symbol", sym}}),
                  1.0)
            << dt << " " << sym;
    }
}

// The diff stream only reports levels that change, so a level deeper than the
// snapshot stays invisible until it moves. The adapter asks for twice the
// published depth, within what each endpoint accepts.
TEST_F(BinanceTest, SpotSnapshotLimitIsDoubledAndClamped) {
    EXPECT_EQ(A::rest_limit(*make(MarketType::Spot, {}, {}, {}, 400)), 800);
    EXPECT_EQ(A::rest_limit(*make(MarketType::Spot, {}, {}, {}, 1)), 5);
    EXPECT_EQ(A::rest_limit(*make(MarketType::Spot, {}, {}, {}, 4000)), 5000);
}

// Futures reject anything off their fixed ladder: limit=400 returns -4021.
TEST_F(BinanceTest, FuturesSnapshotLimitRoundsUpToTheAcceptedLadder) {
    EXPECT_EQ(A::rest_limit(*make(MarketType::UsdtFutures, {}, {}, {}, 2)), 5);
    EXPECT_EQ(A::rest_limit(*make(MarketType::UsdtFutures, {}, {}, {}, 20)), 50);
    EXPECT_EQ(A::rest_limit(*make(MarketType::UsdtFutures, {}, {}, {}, 200)), 500);
    EXPECT_EQ(A::rest_limit(*make(MarketType::UsdtFutures, {}, {}, {}, 400)), 1000);
    EXPECT_EQ(A::rest_limit(*make(MarketType::CoinFutures, {}, {}, {}, 5000)), 1000);
}

TEST_F(BinanceTest, SnapshotIsRequestedFromTheMarketsOwnEndpoint) {
    struct Case { MarketType mt; const char* sym; const char* host; const char* target; };
    for (auto c : {Case{MarketType::Spot, "ETHUSDT", "api.binance.com",
                        "/api/v3/depth?symbol=ETHUSDT&limit=800"},
                   Case{MarketType::UsdtFutures, "ETHUSDT", "fapi.binance.com",
                        "/fapi/v1/depth?symbol=ETHUSDT&limit=1000"},
                   Case{MarketType::CoinFutures, "BTCUSD", "dapi.binance.com",
                        "/dapi/v1/depth?symbol=BTCUSD&limit=1000"}}) {
        rest = std::make_shared<FakeRest>();
        auto a = make(c.mt, {});
        sync(*a, c.sym, 100);
        ASSERT_EQ(rest->targets.size(), 1u);
        EXPECT_EQ(rest->hosts[0], c.host);
        EXPECT_EQ(rest->targets[0], c.target);
    }
}

// ===========================================================================
// bookTicker
// ===========================================================================

TEST_F(BinanceTest, BookTickerBecomesATickerEvent) {
    auto a = make(MarketType::Spot, {}, {"BNB_USDT_SPOT"});
    A::feed(*a, R"({"u":400900217,"s":"BNBUSDT","b":"25.35190000","B":"31.21000000",)"
                R"("a":"25.36520000","A":"40.66000000"})");

    ASSERT_EQ(sink.size(), 1u);
    const auto& e = sink.last();
    EXPECT_EQ(e.data_type, DataType::Ticker);
    EXPECT_EQ(e.event_type, "update");
    EXPECT_EQ(e.exchange, "binance_spot");
    EXPECT_EQ(e.symbol, "BNB_USDT_SPOT");
    const auto& t = sink.ticker(0);
    EXPECT_EQ(t.symbol, "BNB_USDT_SPOT");
    EXPECT_DOUBLE_EQ(t.best_bid_price, 25.3519);
    EXPECT_DOUBLE_EQ(t.best_bid_qty, 31.21);
    EXPECT_DOUBLE_EQ(t.best_ask_price, 25.3652);
    EXPECT_DOUBLE_EQ(t.best_ask_qty, 40.66);
}

// Spot bookTicker carries no venue time, so its age cannot be measured. It
// must leave a gap in the age histogram, not a sample of zero.
TEST_F(BinanceTest, BookTickerRecordsNoMessageAge) {
    auto a = make(MarketType::Spot, {}, {"BNB_USDT_SPOT"});
    A::feed(*a, R"({"u":1,"s":"BNBUSDT","b":"1","B":"1","a":"2","A":"1"})");

    EXPECT_EQ(metric("axon_mds_events_total", {{"exchange", "binance_spot"},
                                               {"data_type", "ticker"}}), 1.0);
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count",
                     {{"exchange", "binance_spot"}, {"data_type", "ticker"}}), 0.0);
}

// ===========================================================================
// kline_1m
// ===========================================================================

TEST_F(BinanceTest, KlineMapsEveryField) {
    auto a = make(MarketType::Spot, {}, {}, {"BNB_BTC_SPOT"});
    A::feed(*a, R"({"e":"kline","E":1672515782136,"s":"BNBBTC","k":{"t":1672515780000,)"
                R"("T":1672515839999,"s":"BNBBTC","i":"1m","f":100,"L":200,"o":"0.0010",)"
                R"("c":"0.0020","h":"0.0025","l":"0.0015","v":"1000","n":100,"x":false,)"
                R"("q":"1.0000","V":"500","Q":"0.500","B":"123456"}})");

    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().data_type, DataType::Kline);
    EXPECT_EQ(sink.last().symbol, "BNB_BTC_SPOT");
    const auto& k = sink.kline(0);
    EXPECT_EQ(k.interval, "1m");
    EXPECT_EQ(k.open_time, 1672515780000);
    EXPECT_EQ(k.close_time, 1672515839999);
    EXPECT_DOUBLE_EQ(k.open, 0.001);
    EXPECT_DOUBLE_EQ(k.close, 0.002);
    EXPECT_DOUBLE_EQ(k.high, 0.0025);
    EXPECT_DOUBLE_EQ(k.low, 0.0015);
    EXPECT_DOUBLE_EQ(k.volume, 1000);
    EXPECT_DOUBLE_EQ(k.quote_volume, 1.0);
    EXPECT_EQ(k.num_trades, 100);
    EXPECT_FALSE(k.is_closed);
    EXPECT_EQ(k.timestamp, 1672515782136);
}

TEST_F(BinanceTest, ClosedKlineIsFlaggedClosed) {
    auto a = make(MarketType::UsdtFutures, {}, {}, {"BTC_USDT_PERP"});
    A::feed(*a, R"({"e":"kline","E":2,"s":"BTCUSDT","k":{"t":0,"T":59999,"i":"1m","o":"1",)"
                R"("c":"1","h":"1","l":"1","v":"0","n":0,"x":true,"q":"0"}})");
    ASSERT_EQ(sink.size(), 1u);
    EXPECT_TRUE(sink.kline(0).is_closed);
    EXPECT_EQ(sink.last().exchange, "binance_usdt_futures");
    EXPECT_EQ(sink.last().symbol, "BTC_USDT_PERP");
}

// ===========================================================================
// Control frames, malformed input, unknown symbols
// ===========================================================================

TEST_F(BinanceTest, SubscribeAcknowledgementIsNotAnEvent) {
    auto a = make(MarketType::Spot, {}, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"result":null,"id":1})");
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric_or_zero("axon_mds_parse_error_total"), 0.0);
}

TEST_F(BinanceTest, EmptyFrameIsCountedAsAParseError) {
    auto a = make(MarketType::Spot, {}, {"ETH_USDT_SPOT"});
    A::feed(*a, "");
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "binance_spot"}}), 1.0);
}

// BUG: simdjson On-Demand's iterate() only rejects an empty document or an
// unterminated string; any other truncated or garbled frame "parses" and fails
// later, at the first field lookup -- which the handler reads as "field absent"
// and silently drops. So axon_mds_parse_error_total misses most malformed
// input, on every venue, and a feed sending garbage looks like a quiet one.
TEST_F(BinanceTest, DISABLED_MalformedJsonIsCountedAsAParseError) {
    auto a = make(MarketType::Spot, {}, {"ETH_USDT_SPOT"});
    A::feed(*a, "{not json");
    A::feed(*a, R"({"u":1,"s":"ETHUSDT","b":"1")");  // truncated mid-object
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "binance_spot"}}), 2.0);
}

// A venue sending a symbol we never asked for is counted per exchange, not
// given its own label -- an unbounded label would let a misbehaving feed blow
// up the series count. The event still flows, under the venue's own name.
TEST_F(BinanceTest, UndeclaredSymbolIsCountedWithoutItsOwnSeries) {
    auto a = make(MarketType::Spot, {}, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"u":1,"s":"DOGEUSDT","b":"1","B":"1","a":"2","A":"1"})");

    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().symbol, "DOGEUSDT");
    EXPECT_EQ(metric("axon_mds_undeclared_event_total", {{"exchange", "binance_spot"}}), 1.0);
    EXPECT_FALSE(metric("axon_mds_events_total", {{"symbol", "DOGEUSDT"}}).has_value());
}

// BUG: a frame that is valid JSON but missing a field the handler reads throws
// a simdjson_error out of handle_message. In production that propagates out
// of the WebSocket read loop and is treated as a connection failure: one odd
// frame tears down the socket and every topic on it, then reconnects. It
// should be counted as a parse error and dropped, like invalid JSON is.
TEST_F(BinanceTest, DISABLED_FrameMissingAFieldIsDroppedNotThrown) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"}, {"ETH_USDT_SPOT"});
    EXPECT_NO_THROW(A::feed(*a, R"({"e":"depthUpdate","s":"ETHUSDT"})"));
    EXPECT_NO_THROW(A::feed(*a, R"({"u":1,"s":"ETHUSDT","b":"not-a-number","B":"1","a":"2","A":"1"})"));
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "binance_spot"}}), 2.0);
}

// ===========================================================================
// Orderbook: snapshot + buffered diffs (spot)
//
//   drop u <= lastUpdateId; first kept event needs U <= lastUpdateId+1 <= u;
//   then U == previous u + 1.
// ===========================================================================

TEST_F(BinanceTest, SpotSnapshotWithNoBufferedDiffsPublishesTheSnapshot) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    sync(*a, "ETHUSDT", 100);

    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().event_type, "snapshot");
    EXPECT_EQ(sink.last().data_type, DataType::Depth);
    const auto& b = sink.last_book();
    EXPECT_EQ(b.symbol, "ETH_USDT_SPOT");
    EXPECT_EQ(b.exchange, "binance_spot");
    EXPECT_EQ(b.sequence, 100);
    ASSERT_EQ(b.bids.size(), 2u);
    ASSERT_EQ(b.asks.size(), 2u);
    EXPECT_DOUBLE_EQ(b.bids[0].price, 2000.0);  // best bid first
    EXPECT_DOUBLE_EQ(b.bids[1].price, 1999.0);
    EXPECT_DOUBLE_EQ(b.asks[0].price, 2001.0);  // best ask first
    EXPECT_DOUBLE_EQ(b.asks[1].price, 2002.0);
    EXPECT_FALSE(A::syncing(*a, "ETHUSDT"));
    EXPECT_EQ(metric("axon_mds_rest_snapshot_duration_seconds_count",
                     {{"exchange", "binance_spot"}}), 1.0);
}

TEST_F(BinanceTest, SpotDiffsArrivingDuringTheSnapshotAreReplayedInOrder) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    rest->bodies.push_back(snapshot_body(100, R"([["2000.00","1.0"]])", R"([["2001.00","1.0"]])"));
    A::schedule_sync(*a, "ETHUSDT");

    // The REST call is in flight; these buffer rather than apply.
    A::feed(*a, depth_update("ETHUSDT", 95, 100, R"([["1990.00","9.0"]])", "[]"));  // u <= id: stale
    A::feed(*a, depth_update("ETHUSDT", 99, 102, R"([["2000.00","5.0"]])", "[]"));  // straddles 101
    A::feed(*a, depth_update("ETHUSDT", 103, 104, "[]", R"([["2001.00","0"]])"));   // removes level
    EXPECT_EQ(sink.size(), 0u);

    drain(ioc);

    ASSERT_EQ(sink.size(), 1u);
    const auto& b = sink.last_book();
    EXPECT_EQ(sink.last().event_type, "snapshot");
    EXPECT_EQ(b.sequence, 104);
    ASSERT_EQ(b.bids.size(), 1u);  // the stale 1990 diff was not applied
    EXPECT_DOUBLE_EQ(b.bids[0].quantity, 5.0);
    EXPECT_TRUE(b.asks.empty());

    // And the live stream chains on from the replayed tail.
    A::feed(*a, depth_update("ETHUSDT", 105, 106, "[]", R"([["2003.00","1.0"]])"));
    ASSERT_EQ(sink.size(), 2u);
    EXPECT_EQ(sink.last().event_type, "update");
    EXPECT_EQ(sink.last_book().sequence, 106);
}

TEST_F(BinanceTest, SpotBufferStartingPastTheSnapshotForcesAResync) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    rest->bodies.push_back(snapshot_body(100, "[]", "[]"));
    A::schedule_sync(*a, "ETHUSDT");
    A::feed(*a, depth_update("ETHUSDT", 103, 104, "[]", "[]"));  // 101 and 102 missing
    drain(ioc);

    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "binance_spot"}, {"reason", "snapshot_too_old"}}), 1.0);
    // Still syncing: live diffs keep buffering until the retry lands.
    EXPECT_TRUE(A::syncing(*a, "ETHUSDT"));
}

TEST_F(BinanceTest, SpotBufferWithAHoleForcesAResync) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    rest->bodies.push_back(snapshot_body(100, "[]", "[]"));
    A::schedule_sync(*a, "ETHUSDT");
    A::feed(*a, depth_update("ETHUSDT", 99, 102, "[]", "[]"));
    A::feed(*a, depth_update("ETHUSDT", 104, 105, "[]", "[]"));  // 103 missing
    drain(ioc);

    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "binance_spot"}, {"reason", "sequence_gap"}}), 1.0);
}

TEST_F(BinanceTest, BrokenReplayRetriesTheSnapshotWhileRunning) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::set_running(*a, true);
    rest->bodies.push_back(snapshot_body(100, "[]", "[]"));
    A::schedule_sync(*a, "ETHUSDT");
    A::feed(*a, depth_update("ETHUSDT", 103, 104, "[]", "[]"));
    rest->bodies.push_back(snapshot_body(110, "[]", "[]"));

    ASSERT_TRUE(run_until(ioc, [&] { return sink.size() == 1; }, std::chrono::seconds(3)));
    EXPECT_EQ(rest->targets.size(), 2u);
    EXPECT_EQ(sink.last_book().sequence, 110);
    A::set_running(*a, false);
}

// After a snapshot that no buffered diff bridged, the first LIVE diff has to
// straddle the snapshot id rather than chain onto it.
TEST_F(BinanceTest, SpotFirstLiveDiffMustStraddleTheSnapshot) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    sync(*a, "ETHUSDT", 100);
    sink.clear();

    A::feed(*a, depth_update("ETHUSDT", 98, 100, R"([["1.0","1.0"]])", "[]"));  // wholly older
    EXPECT_EQ(sink.size(), 0u);

    A::feed(*a, depth_update("ETHUSDT", 100, 103, "[]", "[]"));  // U <= 101 <= u
    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last_book().sequence, 103);
}

TEST_F(BinanceTest, SpotFirstLiveDiffPastTheSnapshotResyncs) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    sync(*a, "ETHUSDT", 100);
    sink.clear();

    A::feed(*a, depth_update("ETHUSDT", 102, 103, "[]", "[]"));
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "binance_spot"}, {"reason", "snapshot_too_old"}}), 1.0);
    EXPECT_TRUE(A::syncing(*a, "ETHUSDT"));

    drain(ioc);
    EXPECT_EQ(rest->targets.size(), 2u);  // the resync asked for a fresh snapshot
}

TEST_F(BinanceTest, SpotSteadyStateGapResyncs) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    sync(*a, "ETHUSDT", 100);
    A::feed(*a, depth_update("ETHUSDT", 101, 105, "[]", "[]"));
    sink.clear();

    A::feed(*a, depth_update("ETHUSDT", 107, 110, "[]", "[]"));  // 106 missing

    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "binance_spot"}, {"reason", "sequence_gap"}}), 1.0);
    EXPECT_TRUE(A::syncing(*a, "ETHUSDT"));
}

TEST_F(BinanceTest, SpotContiguousDiffsUpdateTheBook) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    sync(*a, "ETHUSDT", 100);
    A::feed(*a, depth_update("ETHUSDT", 101, 101, R"([["2000.00","0"],["2000.50","4.0"]])",
                             R"([["2001.00","7.0"]])"));

    const auto& b = sink.last_book();
    EXPECT_EQ(sink.last().event_type, "update");
    ASSERT_EQ(b.bids.size(), 2u);
    EXPECT_DOUBLE_EQ(b.bids[0].price, 2000.5);  // new best bid
    EXPECT_DOUBLE_EQ(b.bids[1].price, 1999.0);  // 2000.00 removed by qty 0
    EXPECT_DOUBLE_EQ(b.asks[0].quantity, 7.0);  // replaced, not added
}

TEST_F(BinanceTest, PublishedDepthIsCappedAtDepthLevels) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"}, {}, {}, 2);
    sync(*a, "ETHUSDT", 100, R"([["5","1"],["4","1"],["3","1"],["2","1"]])",
         R"([["6","1"],["7","1"],["8","1"]])");
    EXPECT_EQ(sink.last_book().bids.size(), 2u);
    EXPECT_EQ(sink.last_book().asks.size(), 2u);
    EXPECT_DOUBLE_EQ(sink.last_book().bids[1].price, 4.0);
}

// ===========================================================================
// Orderbook: futures
//
//   drop u < lastUpdateId; first kept event needs U <= lastUpdateId <= u;
//   then pu == previous u.
// ===========================================================================

TEST_F(BinanceTest, FuturesReplayJoinsOnTheSnapshotIdAndChainsOnPu) {
    auto a = make(MarketType::UsdtFutures, {"ETH_USDT_PERP"});
    rest->bodies.push_back(snapshot_body(100, R"([["2000.00","1.0"]])", "[]"));
    A::schedule_sync(*a, "ETHUSDT");
    A::feed(*a, depth_update("ETHUSDT", 95, 99, "[]", "[]", 94));    // u < 100: stale
    A::feed(*a, depth_update("ETHUSDT", 98, 100, R"([["2000.00","3.0"]])", "[]", 97));  // straddles
    // U jumps past 101 -- legal on futures, where only pu has to chain.
    A::feed(*a, depth_update("ETHUSDT", 103, 105, "[]", R"([["2001.00","1.0"]])", 100));
    drain(ioc);

    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().exchange, "binance_usdt_futures");
    EXPECT_EQ(sink.last().symbol, "ETH_USDT_PERP");
    EXPECT_EQ(sink.last_book().sequence, 105);
    EXPECT_DOUBLE_EQ(sink.last_book().bids[0].quantity, 3.0);
    ASSERT_EQ(sink.last_book().asks.size(), 1u);

    // Futures chain on pu, not on U: U can jump as long as pu matches.
    A::feed(*a, depth_update("ETHUSDT", 120, 125, "[]", "[]", 105));
    ASSERT_EQ(sink.size(), 2u);
    EXPECT_EQ(sink.last_book().sequence, 125);
}

TEST_F(BinanceTest, FuturesPuMismatchInTheBufferResyncs) {
    auto a = make(MarketType::UsdtFutures, {"ETH_USDT_PERP"});
    rest->bodies.push_back(snapshot_body(100, "[]", "[]"));
    A::schedule_sync(*a, "ETHUSDT");
    A::feed(*a, depth_update("ETHUSDT", 98, 101, "[]", "[]", 97));
    A::feed(*a, depth_update("ETHUSDT", 103, 105, "[]", "[]", 102));  // pu should be 101
    drain(ioc);

    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total", {{"exchange", "binance_usdt_futures"},
                                                         {"reason", "sequence_gap"}}), 1.0);
}

TEST_F(BinanceTest, FuturesFirstLiveDiffJoinsOnTheSnapshotIdItself) {
    auto a = make(MarketType::UsdtFutures, {"ETH_USDT_PERP"});
    sync(*a, "ETHUSDT", 100);
    sink.clear();

    // Spot would demand U <= 101; futures demands U <= 100 <= u.
    A::feed(*a, depth_update("ETHUSDT", 100, 104, "[]", "[]", 99));
    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last_book().sequence, 104);
}

TEST_F(BinanceTest, FuturesSteadyStatePuMismatchResyncs) {
    auto a = make(MarketType::UsdtFutures, {"ETH_USDT_PERP"});
    sync(*a, "ETHUSDT", 100);
    A::feed(*a, depth_update("ETHUSDT", 100, 104, "[]", "[]", 99));
    sink.clear();

    A::feed(*a, depth_update("ETHUSDT", 105, 108, "[]", "[]", 103));
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total", {{"exchange", "binance_usdt_futures"},
                                                         {"reason", "sequence_gap"}}), 1.0);
    EXPECT_TRUE(A::syncing(*a, "ETHUSDT"));
}

// ===========================================================================
// Orderbook: failures and connection loss
// ===========================================================================

TEST_F(BinanceTest, FailedSnapshotIsCountedAndRetried) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::set_running(*a, true);
    A::schedule_sync(*a, "ETHUSDT");  // no body queued: the request fails
    drain(ioc);

    EXPECT_EQ(metric("axon_mds_rest_snapshot_failure_total", {{"exchange", "binance_spot"}}), 1.0);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "binance_spot"}, {"reason", "snapshot_failed"}}), 1.0);

    rest->bodies.push_back(snapshot_body(100, "[]", "[]"));
    ASSERT_TRUE(run_until(ioc, [&] { return sink.size() == 1; }, std::chrono::seconds(3)));
    EXPECT_EQ(rest->targets.size(), 2u);
    A::set_running(*a, false);
}

TEST_F(BinanceTest, ErrorBodyFromTheVenueCountsAsAFailedSnapshot) {
    auto a = make(MarketType::UsdtFutures, {"ETH_USDT_PERP"});
    rest->bodies.push_back(R"({"code":-4021,"msg":"400 is not valid depth limit"})");
    A::schedule_sync(*a, "ETHUSDT");
    drain(ioc);

    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_rest_snapshot_failure_total",
                     {{"exchange", "binance_usdt_futures"}}), 1.0);
}

// BUG: when the snapshot request fails, the symbol leaves the syncing set
// while it waits a second to retry. Live diffs arriving in that window are
// applied to an empty book -- no snapshot, no sequence check -- and published
// as a real orderbook. The replay-failure path keeps the symbol syncing for
// exactly this reason; the request-failure path should too.
TEST_F(BinanceTest, DISABLED_NoBookIsPublishedBetweenAFailedSnapshotAndItsRetry) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    A::schedule_sync(*a, "ETHUSDT");  // fails
    drain(ioc);

    A::feed(*a, depth_update("ETHUSDT", 500, 501, R"([["2000.00","1.0"]])", "[]"));
    EXPECT_EQ(sink.size(), 0u) << "a diff-only book was published as an orderbook";
}

TEST_F(BinanceTest, DisconnectCountsOneResyncPerDepthBookAndDropsSyncState) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT", "BTC_USDT_SPOT"}, {"SOL_USDT_SPOT"});
    rest->bodies.push_back(snapshot_body(100, "[]", "[]"));
    A::schedule_sync(*a, "ETHUSDT");
    A::schedule_sync(*a, "BTCUSDT");
    EXPECT_TRUE(A::syncing(*a, "BTCUSDT"));

    A::connection_state(*a, false);

    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "binance_spot"}, {"reason", "disconnect"}}), 2.0);
    EXPECT_FALSE(A::syncing(*a, "ETHUSDT"));
    EXPECT_FALSE(A::syncing(*a, "BTCUSDT"));
}

// ===========================================================================
// Latency instrumentation
// ===========================================================================

TEST_F(BinanceTest, DepthAgeIsMeasuredFromTheVenueEventTime) {
    auto a = make(MarketType::Spot, {"ETH_USDT_SPOT"});
    sync(*a, "ETHUSDT", 100);
    // REST snapshots carry no venue time either.
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count",
                     {{"exchange", "binance_spot"}, {"data_type", "depth"}}), 0.0);

    A::feed(*a, depth_update("ETHUSDT", 100, 101, "[]", "[]", std::nullopt, now_ms() - 1500));

    const std::map<std::string, std::string> l{{"exchange", "binance_spot"},
                                               {"data_type", "depth"}};
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count", l), 1.0);
    EXPECT_GE(*metric("axon_mds_message_age_seconds_sum", l), 1.5);
    EXPECT_EQ(bucket("axon_mds_message_age_seconds", l, 1.0), 0.0);
    EXPECT_EQ(bucket("axon_mds_message_age_seconds", l, 5.0), 1.0);
}

TEST_F(BinanceTest, EveryEventRecordsItsPublishDurationAndLastEventTime) {
    auto a = make(MarketType::Spot, {}, {"ETH_USDT_SPOT"});
    const double before = static_cast<double>(now_ms()) / 1000.0;
    for (int i = 0; i < 3; ++i)
        A::feed(*a, R"({"u":1,"s":"ETHUSDT","b":"1","B":"1","a":"2","A":"1"})");

    EXPECT_EQ(metric("axon_mds_publish_duration_seconds_count",
                     {{"exchange", "binance_spot"}, {"data_type", "ticker"}}), 3.0);
    EXPECT_EQ(metric("axon_mds_events_total", {{"symbol", "ETH_USDT_SPOT"}}), 3.0);
    EXPECT_GE(*metric("axon_mds_last_event_timestamp_seconds", {{"symbol", "ETH_USDT_SPOT"}}),
              before - 0.001);
}

}  // namespace
}  // namespace mds_test
