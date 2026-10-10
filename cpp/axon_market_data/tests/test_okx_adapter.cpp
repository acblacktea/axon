// OKX: books / books5 orderbooks with prevSeqId chaining and CRC32 checksums,
// bbo-tbt tickers, and candle1m klines on a second (business) connection.

#include "test_support.h"

namespace mds_test {
namespace {

using A = AdapterTestAccess;

std::string books(const std::string& action, const std::string& inst, const std::string& asks,
                  const std::string& bids, int64_t prev_seq, int64_t seq,
                  int64_t checksum = 0, int64_t ts = 0) {
    if (ts == 0) ts = now_ms() - 40;
    return R"({"arg":{"channel":"books","instId":")" + inst + R"("},"action":")" + action +
           R"(","data":[{"asks":)" + asks + R"(,"bids":)" + bids + R"(,"ts":")" +
           std::to_string(ts) + R"(","checksum":)" + std::to_string(checksum) +
           R"(,"prevSeqId":)" + std::to_string(prev_seq) + R"(,"seqId":)" +
           std::to_string(seq) + "}]}";
}

// The reference book used by the checksum tests, with its CRC32 computed
// independently (Python zlib.crc32) over "2000.1:1:2000.2:3:2000.0:2".
const char* kAsks = R"([["2000.2","3","0","2"]])";
const char* kBids = R"([["2000.1","1","0","1"],["2000.0","2","0","1"]])";
constexpr int64_t kChecksum = -1600554681;

class OkxTest : public MetricsTest {
protected:
    std::shared_ptr<OkxAdapter> make(std::vector<std::string> depth,
                                     std::vector<std::string> ticker = {},
                                     std::vector<std::string> kline  = {},
                                     size_t depth_levels = 400, bool verify_checksum = false) {
        auto cfg            = exchange("okx", MarketType::Spot, depth, ticker, kline, depth_levels);
        cfg.verify_checksum = verify_checksum;
        return std::make_shared<OkxAdapter>(ioc, cfg, sink.callback(), log.logger);
    }

    net::io_context ioc;
    EventSink       sink;
    CapturedLogger  log;
};

// ===========================================================================
// Configuration
// ===========================================================================

TEST_F(OkxTest, SymbolSuffixDecidesTheInstrument) {
    auto a = make({"ETH_USDT_SPOT", "ETH_USDT_PERP"});
    EXPECT_EQ(A::public_channels(*a),
              (std::vector<std::string>{"books:ETH-USDT", "books:ETH-USDT-SWAP"}));
    EXPECT_EQ(a->exchange_name(), "okx");
}

// Candles are only served on /ws/v5/business, so klines ride a second
// connection; depth and tickers stay on /ws/v5/public.
TEST_F(OkxTest, KlinesGoToTheBusinessConnection) {
    auto a = make({"ETH_USDT_SPOT"}, {"BTC_USDT_SPOT"}, {"SOL_USDT_PERP"});
    EXPECT_EQ(A::public_channels(*a),
              (std::vector<std::string>{"books:ETH-USDT", "bbo-tbt:BTC-USDT"}));
    EXPECT_EQ(A::business_channels(*a), (std::vector<std::string>{"candle1m:SOL-USDT-SWAP"}));
}

// books5 is 5 levels per message instead of 400 -- two orders of magnitude
// less traffic -- so a shallow request gets it. Nothing between 5 and 400.
TEST_F(OkxTest, DepthLevelsPicksTheCheapestSufficientChannel) {
    struct Case { size_t want; const char* channel; size_t published; };
    for (auto c : {Case{1, "books5", 1}, Case{5, "books5", 5}, Case{6, "books", 6},
                   Case{400, "books", 400}, Case{1000, "books", 400}}) {
        auto a = make({"ETH_USDT_SPOT"}, {}, {}, c.want);
        EXPECT_EQ(A::book_channel(*a), c.channel) << c.want;
        EXPECT_EQ(A::depth_levels(*a), c.published) << c.want;
    }
}

// ===========================================================================
// books: snapshot, updates, sequence
// ===========================================================================

TEST_F(OkxTest, SnapshotSeedsTheBook) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100));

    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().event_type, "snapshot");
    EXPECT_EQ(sink.last().symbol, "ETH_USDT_SPOT");
    const auto& b = sink.last_book();
    EXPECT_EQ(b.sequence, 100);
    ASSERT_EQ(b.bids.size(), 2u);
    EXPECT_DOUBLE_EQ(b.bids[0].price, 2000.1);
    EXPECT_DOUBLE_EQ(b.bids[1].price, 2000.0);
    ASSERT_EQ(b.asks.size(), 1u);
    EXPECT_DOUBLE_EQ(b.asks[0].quantity, 3.0);
}

TEST_F(OkxTest, UpdateChainingOnPrevSeqIdIsApplied) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100));
    A::feed(*a, books("update", "ETH-USDT", R"([["2000.2","0","0","0"],["2000.4","5","0","1"]])",
                      R"([["2000.1","9","0","1"]])", 100, 101));

    ASSERT_EQ(sink.size(), 2u);
    EXPECT_EQ(sink.last().event_type, "update");
    const auto& b = sink.last_book();
    EXPECT_EQ(b.sequence, 101);
    EXPECT_DOUBLE_EQ(b.bids[0].quantity, 9.0);
    ASSERT_EQ(b.asks.size(), 1u);  // 2000.2 removed, 2000.4 added
    EXPECT_DOUBLE_EQ(b.asks[0].price, 2000.4);
}

// seqId == prevSeqId with no levels is OKX's "nothing changed" heartbeat.
TEST_F(OkxTest, KeepAliveUpdateIsAccepted) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100));
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 100, 100));
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 100, 101));

    EXPECT_EQ(sink.size(), 3u);
    EXPECT_EQ(sink.last_book().sequence, 101);
    EXPECT_EQ(metric_or_zero("axon_mds_orderbook_resync_total"), 0.0);
}

TEST_F(OkxTest, SequenceGapDropsTheBookUntilTheNextSnapshot) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100));
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 105, 106));  // expected prev 100
    drain(ioc);  // the re-subscribe is a no-op without a connection

    EXPECT_EQ(sink.size(), 1u);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "okx"}, {"reason", "sequence_gap"}}), 1.0);

    // Updates are ignored until a fresh snapshot arrives...
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 106, 107));
    EXPECT_EQ(sink.size(), 1u);

    // ...and the snapshot restores the book.
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 200));
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 200, 201));
    EXPECT_EQ(sink.size(), 3u);
    EXPECT_EQ(sink.last_book().sequence, 201);
}

TEST_F(OkxTest, UpdateBeforeAnySnapshotIsIgnored) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, books("update", "ETH-USDT", kAsks, kBids, 99, 100));
    EXPECT_EQ(sink.size(), 0u);
}

TEST_F(OkxTest, BooksAreKeptPerInstrument) {
    auto a = make({"ETH_USDT_SPOT", "BTC_USDT_SPOT"});
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100));
    A::feed(*a, books("snapshot", "BTC-USDT", R"([["60000","1","0","1"]])",
                      R"([["59999","1","0","1"]])", -1, 7));
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 100, 101));

    ASSERT_EQ(sink.size(), 3u);
    EXPECT_EQ(sink.last().symbol, "ETH_USDT_SPOT");
    EXPECT_EQ(sink.book(1).symbol, "BTC_USDT_SPOT");
    EXPECT_EQ(metric_or_zero("axon_mds_orderbook_resync_total"), 0.0);
}

TEST_F(OkxTest, PublishedDepthIsCapped) {
    auto a = make({"ETH_USDT_SPOT"}, {}, {}, 1);  // books5 channel, 1 level
    A::feed(*a, R"({"arg":{"channel":"books5","instId":"ETH-USDT"},"data":[{"asks":)"
                R"([["3","1","0","1"],["4","1","0","1"]],"bids":[["2","1","0","1"],)"
                R"(["1","1","0","1"]],"instId":"ETH-USDT","ts":"1700000000000","seqId":5}]})");
    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last_book().bids.size(), 1u);
    EXPECT_EQ(sink.last_book().asks.size(), 1u);
}

// books5 is a full 5-level book every message: no action, no prevSeqId, so
// every message replaces the last rather than updating it.
TEST_F(OkxTest, Books5MessagesAreSnapshots) {
    auto a = make({"ETH_USDT_SPOT"}, {}, {}, 5);
    for (int seq : {1, 2}) {
        A::feed(*a, R"({"arg":{"channel":"books5","instId":"ETH-USDT"},"data":[{"asks":)"
                    R"([[")" + std::to_string(2000 + seq) + R"(","1","0","1"]],"bids":[],)"
                    R"("instId":"ETH-USDT","ts":"1700000000000","seqId":)" +
                        std::to_string(seq) + "}]}");
    }
    ASSERT_EQ(sink.size(), 2u);
    EXPECT_EQ(sink.last().event_type, "snapshot");
    ASSERT_EQ(sink.last_book().asks.size(), 1u);  // replaced, not merged
    EXPECT_DOUBLE_EQ(sink.last_book().asks[0].price, 2002.0);
    EXPECT_EQ(sink.last_book().sequence, 2);
}

// ===========================================================================
// Checksum
// ===========================================================================

TEST(OkxChecksum, InterleavesBidAndAskLevels) {
    Orderbook b;
    b.apply_bid(2000.1, 1, "2000.1:1");
    b.apply_bid(2000.0, 2, "2000.0:2");
    b.apply_ask(2000.2, 3, "2000.2:3");
    // When one side runs out the other continues alone.
    EXPECT_EQ(okx_checksum_string(b, 25), "2000.1:1:2000.2:3:2000.0:2");
    EXPECT_EQ(okx_checksum(b, 25), kChecksum);
}

// The worked example from OKX's API documentation.
TEST(OkxChecksum, MatchesTheVenuesDocumentedExample) {
    Orderbook b;
    b.apply_bid(3366.1, 7, "3366.1:7");
    b.apply_bid(3366.0, 6, "3366:6");
    b.apply_ask(3366.8, 9, "3366.8:9");
    b.apply_ask(3368.0, 8, "3368:8");
    EXPECT_EQ(okx_checksum_string(b, 25), "3366.1:7:3366.8:9:3366:6:3368:8");
    EXPECT_EQ(okx_checksum(b, 25), -1881014294);
}

TEST(OkxChecksum, OnlyTheTopLevelsCount) {
    Orderbook b;
    for (int i = 0; i < 30; ++i) {
        b.apply_bid(100.0 - i, 1, std::to_string(100 - i) + ":1");
        b.apply_ask(200.0 + i, 1, std::to_string(200 + i) + ":1");
    }
    auto s = okx_checksum_string(b, 25);
    EXPECT_NE(s.find("76:1"), std::string::npos);   // 25th bid
    EXPECT_EQ(s.find("75:1"), std::string::npos);   // 26th bid excluded
    EXPECT_NE(s.find("224:1"), std::string::npos);  // 25th ask
    EXPECT_EQ(s.find("225:1"), std::string::npos);
}

// The checksum hashes the venue's own decimal text, not a reformatted double:
// "2000.0" and "2000" are the same price but different checksums.
TEST_F(OkxTest, ChecksumUsesTheVenuesOriginalText) {
    auto a = make({"ETH_USDT_SPOT"}, {}, {}, 400, true);
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100, kChecksum));
    EXPECT_EQ(sink.size(), 1u);
    EXPECT_EQ(metric_or_zero("axon_mds_checksum_failure_total"), 0.0);
}

TEST_F(OkxTest, UpdateWithAMatchingChecksumIsApplied) {
    auto a = make({"ETH_USDT_SPOT"}, {}, {}, 400, true);
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100, kChecksum));
    A::feed(*a, books("update", "ETH-USDT", R"([["2000.3","4","0","1"]])", "[]", 100, 101,
                      -1615101977));  // crc32("2000.1:1:2000.2:3:2000.0:2:2000.3:4")
    EXPECT_EQ(sink.size(), 2u);
    EXPECT_EQ(metric_or_zero("axon_mds_checksum_failure_total"), 0.0);
}

TEST_F(OkxTest, SnapshotChecksumMismatchDiscardsTheBook) {
    auto a = make({"ETH_USDT_SPOT"}, {}, {}, 400, true);
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100, 12345));
    drain(ioc);

    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_checksum_failure_total", {{"exchange", "okx"}}), 1.0);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "okx"}, {"reason", "checksum_mismatch"}}), 1.0);
    // Nothing to apply updates to.
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 100, 101));
    EXPECT_EQ(sink.size(), 0u);
}

TEST_F(OkxTest, UpdateChecksumMismatchDiscardsTheBook) {
    auto a = make({"ETH_USDT_SPOT"}, {}, {}, 400, true);
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100, kChecksum));
    A::feed(*a, books("update", "ETH-USDT", R"([["2000.3","4","0","1"]])", "[]", 100, 101, 777));
    drain(ioc);

    EXPECT_EQ(sink.size(), 1u);  // only the snapshot
    EXPECT_EQ(metric("axon_mds_checksum_failure_total", {{"exchange", "okx"}}), 1.0);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "okx"}, {"reason", "checksum_mismatch"}}), 1.0);
}

// OKX sends "checksum":0 today, meaning "not computed". Treating it as a real
// value would reject every message.
TEST_F(OkxTest, ZeroChecksumMeansNotProvided) {
    auto a = make({"ETH_USDT_SPOT"}, {}, {}, 400, true);
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100, 0));
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 100, 101, 0));
    EXPECT_EQ(sink.size(), 2u);
    EXPECT_EQ(metric_or_zero("axon_mds_checksum_failure_total"), 0.0);
}

TEST_F(OkxTest, ChecksumIsIgnoredWhenVerificationIsOff) {
    auto a = make({"ETH_USDT_SPOT"}, {}, {}, 400, false);
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 100, 12345));
    EXPECT_EQ(sink.size(), 1u);
    EXPECT_EQ(metric_or_zero("axon_mds_checksum_failure_total"), 0.0);
}

// ===========================================================================
// bbo-tbt
// ===========================================================================

TEST_F(OkxTest, BboBecomesATicker) {
    auto a = make({}, {"ETH_USDT_PERP"});
    const int64_t ts = now_ms() - 30;
    A::feed(*a, R"({"arg":{"channel":"bbo-tbt","instId":"ETH-USDT-SWAP"},"data":[{"asks":)"
                R"([["2000.2","3","0","2"]],"bids":[["2000.1","1","0","1"]],"ts":")" +
                    std::to_string(ts) + R"(","seqId":1}]})");

    ASSERT_EQ(sink.size(), 1u);
    EXPECT_EQ(sink.last().data_type, DataType::Ticker);
    EXPECT_EQ(sink.last().symbol, "ETH_USDT_PERP");
    const auto& t = sink.ticker(0);
    EXPECT_DOUBLE_EQ(t.best_bid_price, 2000.1);
    EXPECT_DOUBLE_EQ(t.best_bid_qty, 1);
    EXPECT_DOUBLE_EQ(t.best_ask_price, 2000.2);
    EXPECT_DOUBLE_EQ(t.best_ask_qty, 3);
    EXPECT_EQ(t.timestamp, ts);
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count",
                     {{"exchange", "okx"}, {"data_type", "ticker"}}), 1.0);
}

// ===========================================================================
// candle1m
// ===========================================================================

TEST_F(OkxTest, CandleMapsTheArrayLayout) {
    auto a = make({}, {}, {"ETH_USDT_SPOT"});
    // [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    A::feed(*a, R"({"arg":{"channel":"candle1m","instId":"ETH-USDT"},"data":[["1700000000000",)"
                R"("2000","2010","1990","2005","100","0.05","200500","0"]]})");

    ASSERT_EQ(sink.size(), 1u);
    const auto& k = sink.kline(0);
    EXPECT_EQ(sink.last().symbol, "ETH_USDT_SPOT");
    EXPECT_EQ(k.interval, "1m");
    EXPECT_EQ(k.open_time, 1700000000000);
    EXPECT_EQ(k.close_time, 1700000059999);
    EXPECT_DOUBLE_EQ(k.open, 2000);
    EXPECT_DOUBLE_EQ(k.high, 2010);
    EXPECT_DOUBLE_EQ(k.low, 1990);
    EXPECT_DOUBLE_EQ(k.close, 2005);
    EXPECT_DOUBLE_EQ(k.volume, 100);
    EXPECT_DOUBLE_EQ(k.quote_volume, 200500);
    EXPECT_FALSE(k.is_closed);
}

TEST_F(OkxTest, ConfirmedCandleIsClosed) {
    auto a = make({}, {}, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"arg":{"channel":"candle1m","instId":"ETH-USDT"},"data":[["1700000000000",)"
                R"("1","1","1","1","1","1","1","1"]]})");
    ASSERT_EQ(sink.size(), 1u);
    EXPECT_TRUE(sink.kline(0).is_closed);
}

// candle1m carries no event time, so no age can be measured for it.
TEST_F(OkxTest, CandleRecordsNoMessageAge) {
    auto a = make({}, {}, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"arg":{"channel":"candle1m","instId":"ETH-USDT"},"data":[["1700000000000",)"
                R"("1","1","1","1","1","1","1","0"]]})");
    EXPECT_EQ(metric("axon_mds_message_age_seconds_count",
                     {{"exchange", "okx"}, {"data_type", "kline"}}), 0.0);
}

TEST_F(OkxTest, ShortCandleArrayIsSkipped) {
    auto a = make({}, {}, {"ETH_USDT_SPOT"});
    A::feed(*a, R"({"arg":{"channel":"candle1m","instId":"ETH-USDT"},"data":[["1700000000000","1"]]})");
    EXPECT_EQ(sink.size(), 0u);
}

// ===========================================================================
// Control frames and connection loss
// ===========================================================================

// OKX's heartbeat reply is plain text, not JSON.
TEST_F(OkxTest, TextPongIsNotAParseError) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, "pong");
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric_or_zero("axon_mds_parse_error_total"), 0.0);
}

TEST_F(OkxTest, SubscribeEventsAreNotDataAndErrorsAreLogged) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, R"({"event":"subscribe","arg":{"channel":"books","instId":"ETH-USDT"},"connId":"a"})");
    A::feed(*a, R"({"event":"error","code":"60012","msg":"Invalid request: bad instId","connId":"a"})");
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(log.count_containing("Invalid request: bad instId"), 1u);
}

TEST_F(OkxTest, EmptyFrameIsCountedAsAParseError) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, "");
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "okx"}}), 1.0);
}

// BUG -- CRASHES THE PROCESS. A frame truncated after a complete value makes
// doc["event"] fail with INCOMPLETE_ARRAY_OR_OBJECT, which simdjson defines as
// fatal and unrecoverable: the document must not be touched again. The
// handler reads it as "no event field" and goes on to doc["arg"]
// (okx_adapter.cpp), which trips an internal SIMDJSON_ASSUME -- UBSan reports
// "execution reached an unreachable program point" and a release build
// segfaults. Every adapter follows the same pattern of continuing after a
// lookup error, so the others are exposed to the same undefined behaviour;
// OKX is the one this input reliably brings down.
TEST_F(OkxTest, DISABLED_TruncatedFrameIsCountedNotFatal) {
    auto a = make({"ETH_USDT_SPOT"});
    A::feed(*a, R"({"arg":{"channel":"books","instId":"ETH-USDT"},"action":"snapshot")");
    EXPECT_EQ(sink.size(), 0u);
    EXPECT_EQ(metric("axon_mds_parse_error_total", {{"exchange", "okx"}}), 1.0);
}

// BUG: see BinanceTest.DISABLED_FrameMissingAFieldIsDroppedNotThrown.
TEST_F(OkxTest, DISABLED_FrameMissingAFieldIsDroppedNotThrown) {
    auto a = make({"ETH_USDT_SPOT"});
    EXPECT_NO_THROW(A::feed(*a, R"({"arg":{"channel":"books","instId":"ETH-USDT"},)"
                                R"("action":"snapshot","data":[{"asks":[],"bids":[]}]})"));
}

TEST_F(OkxTest, PublicDisconnectCountsOneResyncPerBookHeld) {
    auto a = make({"ETH_USDT_SPOT", "BTC_USDT_SPOT", "SOL_USDT_SPOT"});
    A::feed(*a, books("snapshot", "ETH-USDT", kAsks, kBids, -1, 1));
    A::feed(*a, books("snapshot", "BTC-USDT", kAsks, kBids, -1, 1));

    A::public_state(*a, false);
    EXPECT_EQ(metric("axon_mds_orderbook_resync_total",
                     {{"exchange", "okx"}, {"reason", "disconnect"}}), 2.0);

    // The books are gone: updates wait for the post-reconnect snapshot.
    A::feed(*a, books("update", "ETH-USDT", "[]", "[]", 1, 2));
    EXPECT_EQ(sink.size(), 2u);
}

}  // namespace
}  // namespace mds_test
