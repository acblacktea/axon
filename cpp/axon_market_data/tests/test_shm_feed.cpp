// The shared-memory feed: the broadcast ring, and the publisher that fills it
// from market data events.
//
// The properties that matter, each pinned below: every reader sees every
// record; a reader never receives a torn one, even while the writer laps it;
// a reader that falls behind finds out, and how much it missed; and a writer
// restart is noticed rather than silently starting the sequence over.

#include <gtest/gtest.h>

#include <unistd.h>

#include <atomic>
#include <cstring>
#include <string>
#include <thread>
#include <vector>

#include "axon/market_data/shm_feed.h"
#include "axon/market_data/shm_publisher.h"

using namespace axon::market_data;
namespace shm = axon::market_data::shm;

namespace {

// A segment name no other test (or process) is using, removed afterwards.
struct Segment {
    std::string name;
    Segment() {
        static std::atomic<int> n{0};
        name = "axon_mds_test_" + std::to_string(::getpid()) + "_" + std::to_string(n++);
    }
    ~Segment() { shm::unlink(name); }
};

constexpr std::size_t kSmall = 4096;  // the smallest ring: wraps quickly

template <std::size_t N>
void put(char (&field)[N], const char* text) {
    std::strncpy(field, text, N - 1);
}

shm::EventHeader header(const char* symbol, std::uint16_t bids = 0, std::uint16_t asks = 0) {
    shm::EventHeader h{};
    put(h.exchange, "binance_usdt_futures");
    put(h.symbol, symbol);
    h.bid_count = bids;
    h.ask_count = asks;
    h.publish_ns = shm::steady_ns();
    return h;
}

template <typename T>
std::span<const std::byte> bytes(const T& v) {
    return std::as_bytes(std::span<const T>(&v, 1));
}

template <typename T>
std::span<const std::byte> bytes(const std::vector<T>& v) {
    return std::as_bytes(std::span<const T>(v));
}

bool write_ticker(shm::Writer& w, const char* symbol, double bid) {
    const auto h = header(symbol);
    const shm::Ticker t{bid, 1.0, bid + 0.5, 2.0};
    return w.publish(shm::RecordType::kTicker, {bytes(h), bytes(t)});
}

std::vector<double> drain_bids(shm::Reader& r) {
    std::vector<double> out;
    while (r.poll([&](const shm::RecordView& v) { out.push_back(v.ticker().bid_price); }) > 0) {
    }
    return out;
}

}  // namespace

// ===========================================================================
// The ring
// ===========================================================================

TEST(ShmFeed, EachRecordTypeRoundTrips) {
    Segment seg;
    shm::Writer w(seg.name, kSmall);
    shm::Reader r(seg.name);

    auto h = header("BTC_USDT_PERP", 2, 1);
    h.book_sequence = 42;
    h.kind = shm::EventKind::kSnapshot;
    const std::vector<shm::Level> bids{{100.0, 1.0}, {99.5, 2.0}};
    const std::vector<shm::Level> asks{{100.5, 3.0}};
    ASSERT_TRUE(w.publish(shm::RecordType::kDepth, {bytes(h), bytes(bids), bytes(asks)}));
    ASSERT_TRUE(write_ticker(w, "ETH_USDT_PERP", 3000.0));
    shm::Kline k{};
    put(k.interval, "1m");
    k.close = 64000.0;
    k.is_closed = 1;
    const auto kh = header("BTC_USDT_PERP");
    ASSERT_TRUE(w.publish(shm::RecordType::kKline, {bytes(kh), bytes(k)}));

    std::vector<std::string> seen;
    r.poll([&](const shm::RecordView& v) {
        switch (v.type()) {
            case shm::RecordType::kDepth:
                EXPECT_EQ(v.exchange(), "binance_usdt_futures");
                EXPECT_EQ(v.symbol(), "BTC_USDT_PERP");
                EXPECT_EQ(v.kind(), shm::EventKind::kSnapshot);
                EXPECT_EQ(v.event().book_sequence, 42);
                ASSERT_EQ(v.bids().size(), 2u);
                ASSERT_EQ(v.asks().size(), 1u);
                EXPECT_DOUBLE_EQ(v.bids()[1].price, 99.5);
                EXPECT_DOUBLE_EQ(v.asks()[0].quantity, 3.0);
                seen.push_back("depth");
                break;
            case shm::RecordType::kTicker:
                EXPECT_EQ(v.symbol(), "ETH_USDT_PERP");
                EXPECT_DOUBLE_EQ(v.ticker().ask_price, 3000.5);
                seen.push_back("ticker");
                break;
            case shm::RecordType::kKline:
                EXPECT_EQ(std::string(v.kline().interval), "1m");
                EXPECT_DOUBLE_EQ(v.kline().close, 64000.0);
                EXPECT_EQ(v.kline().is_closed, 1);
                seen.push_back("kline");
                break;
            default:
                ADD_FAILURE() << "unexpected record type";
        }
    });
    EXPECT_EQ(seen, (std::vector<std::string>{"depth", "ticker", "kline"}));
    EXPECT_EQ(r.stats().records, 3u);
    EXPECT_EQ(r.stats().lost, 0u);
}

// What was written before a reader attached is history, not news.
TEST(ShmFeed, AReaderStartsAtThePresent) {
    Segment seg;
    shm::Writer w(seg.name, kSmall);
    write_ticker(w, "OLD", 1.0);
    shm::Reader r(seg.name);
    write_ticker(w, "NEW", 2.0);
    EXPECT_EQ(drain_bids(r), std::vector<double>{2.0});
}

// Records never straddle the end of the ring: the writer pads to the end and
// starts again at 0, and the reader steps over the padding.
TEST(ShmFeed, RecordsWrapAroundTheRingIntact) {
    Segment seg;
    shm::Writer w(seg.name, kSmall);
    shm::Reader r(seg.name);
    std::vector<double> expected;
    std::vector<double> got;
    for (int i = 0; i < 1000; ++i) {  // ~170 KB through a 4 KB ring
        ASSERT_TRUE(write_ticker(w, "BTC_USDT_PERP", i));
        expected.push_back(i);
        const auto batch = drain_bids(r);
        got.insert(got.end(), batch.begin(), batch.end());
    }
    EXPECT_EQ(got, expected);
    EXPECT_EQ(r.stats().lapped, 0u);
}

TEST(ShmFeed, EveryReaderSeesEveryRecord) {
    Segment seg;
    shm::Writer w(seg.name, kSmall);
    shm::Reader a(seg.name);
    shm::Reader b(seg.name);
    for (int i = 0; i < 5; ++i) write_ticker(w, "BTC_USDT_PERP", i);
    EXPECT_EQ(drain_bids(a), (std::vector<double>{0, 1, 2, 3, 4}));
    EXPECT_EQ(drain_bids(b), (std::vector<double>{0, 1, 2, 3, 4}));
}

// A reader that falls a whole ring behind jumps to the present and is told
// how much it missed -- and the writer was never slowed down by it.
TEST(ShmFeed, ALappedReaderResyncsAndCountsWhatItMissed) {
    Segment seg;
    shm::Writer w(seg.name, kSmall);
    shm::Reader r(seg.name);
    for (int i = 0; i < 500; ++i) ASSERT_TRUE(write_ticker(w, "BTC_USDT_PERP", i));

    EXPECT_TRUE(drain_bids(r).empty()) << "nothing that old is still valid";
    EXPECT_GE(r.stats().lapped, 1u);

    write_ticker(w, "BTC_USDT_PERP", 500);
    write_ticker(w, "BTC_USDT_PERP", 501);
    EXPECT_EQ(drain_bids(r), (std::vector<double>{500, 501}));
    EXPECT_EQ(r.stats().lost, 0u) << "the sequence restarts its count at the resync point";
}

TEST(ShmFeed, ASequenceGapIsCountedAsLost) {
    Segment seg;
    shm::Writer w(seg.name, 1 << 16);
    shm::Reader r(seg.name);
    write_ticker(w, "BTC_USDT_PERP", 1);
    drain_bids(r);
    // Lapped in the middle of a burst: the gap shows in the next sequence.
    for (int i = 0; i < 2000; ++i) write_ticker(w, "BTC_USDT_PERP", i);
    drain_bids(r);
    EXPECT_GE(r.stats().lapped, 1u);
    write_ticker(w, "BTC_USDT_PERP", 9);
    drain_bids(r);
    EXPECT_EQ(r.stats().records, 2u);
}

TEST(ShmFeed, ARecordTooLargeIsRefused) {
    Segment seg;
    shm::Writer w(seg.name, 1 << 20);
    const std::vector<shm::Level> deep(shm::kMaxRecordBytes / sizeof(shm::Level));
    const auto h = header("BTC_USDT_PERP", static_cast<std::uint16_t>(deep.size()), 0);
    EXPECT_FALSE(w.publish(shm::RecordType::kDepth, {bytes(h), bytes(deep)}));
    EXPECT_EQ(w.published(), 0u);
}

// A writer that restarts reuses the segment in place: attached readers keep
// reading, notice the restart, and do not count the new sequence as a loss.
TEST(ShmFeed, AWriterRestartIsNoticedAndReadingContinues) {
    Segment seg;
    auto w = std::make_unique<shm::Writer>(seg.name, kSmall);
    shm::Reader r(seg.name);
    write_ticker(*w, "BTC_USDT_PERP", 1);
    EXPECT_EQ(drain_bids(r), std::vector<double>{1});

    w = std::make_unique<shm::Writer>(seg.name, kSmall);
    write_ticker(*w, "BTC_USDT_PERP", 2);
    write_ticker(*w, "BTC_USDT_PERP", 3);
    EXPECT_EQ(drain_bids(r), (std::vector<double>{2, 3})) << "nothing the new writer wrote is skipped";
    EXPECT_EQ(r.stats().restarts, 1u);
    EXPECT_EQ(r.stats().lost, 0u);
}

// A writer that died mid-record leaves half of it in the ring. The next
// writer must cover it, or a reader would take the remains for a record.
TEST(ShmFeed, HalfARecordFromADeadWriterIsNeverDelivered) {
    Segment seg;
    {
        shm::Writer w(seg.name, kSmall);
        write_ticker(w, "BTC_USDT_PERP", 1);
    }
    {
        // Simulate the crash: claim a record's bytes, scribble, never commit.
        const int fd = ::shm_open(("/" + seg.name).c_str(), O_RDWR, 0);
        ASSERT_GE(fd, 0);
        const std::size_t size = sizeof(shm::SegmentHeader) + kSmall;
        void* p = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
        ::close(fd);
        auto* h = static_cast<shm::SegmentHeader*>(p);
        const auto tail = h->tail.load();
        auto* data = static_cast<std::byte*>(p) + sizeof(shm::SegmentHeader);
        const shm::RecordHeader garbage{64, shm::RecordType::kTicker, 0, 999};
        std::memcpy(data + (tail & (kSmall - 1)), &garbage, sizeof garbage);
        h->tail_intent.store(tail + 64);
        ::munmap(p, size);
    }
    shm::Reader r(seg.name);
    shm::Writer w(seg.name, kSmall);
    write_ticker(w, "BTC_USDT_PERP", 2);
    EXPECT_EQ(drain_bids(r), std::vector<double>{2});
    EXPECT_EQ(r.stats().malformed, 0u);
}

TEST(ShmFeed, ADifferentlySizedSegmentIsReplaced) {
    Segment seg;
    { shm::Writer w(seg.name, kSmall); }
    shm::Writer w(seg.name, kSmall * 4);
    shm::Reader r(seg.name);
    EXPECT_EQ(r.capacity(), kSmall * 4);
}

TEST(ShmFeed, AMissingSegmentIsAClearError) {
    try {
        shm::Reader r("axon_mds_test_does_not_exist");
        FAIL() << "expected an exception";
    } catch (const std::exception& e) {
        EXPECT_NE(std::string(e.what()).find("market data service"), std::string::npos)
            << e.what();
    }
}

TEST(ShmFeed, TheHeartbeatSaysWhetherTheWriterIsAlive) {
    Segment seg;
    shm::Writer w(seg.name, kSmall);
    shm::Reader r(seg.name);
    EXPECT_TRUE(r.writer_alive(std::chrono::seconds(5)));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    EXPECT_FALSE(r.writer_alive(std::chrono::milliseconds(10)));
    w.heartbeat();
    EXPECT_TRUE(r.writer_alive(std::chrono::milliseconds(10)));
}

// The property the whole design rests on, under real concurrency: a writer
// lapping a slow reader as fast as it can, and the reader never delivering a
// record whose contents disagree with themselves.
TEST(ShmFeed, ConcurrentReadingNeverDeliversATornRecord) {
    Segment seg;
    constexpr int kRecords = 500'000;
    constexpr std::size_t kLevels = 20;
    // ~9 records to a ring: the writer overwrites what the reader is copying
    // on most reads, which is exactly the case under test.
    shm::Writer w(seg.name, kSmall);
    shm::Reader r(seg.name);

    std::atomic<bool> done{false};
    std::thread writer([&] {
        std::vector<shm::Level> levels(kLevels);
        for (int i = 1; i <= kRecords; ++i) {
            // Every field derives from i, so a mix of two records shows.
            for (std::size_t j = 0; j < kLevels; ++j) {
                levels[j] = {static_cast<double>(i), static_cast<double>(i) * 2 + j};
            }
            auto h = header("BTC_USDT_PERP", kLevels / 2, kLevels / 2);
            h.book_sequence = i;
            w.publish(shm::RecordType::kDepth, {bytes(h), bytes(levels)});
        }
        done.store(true, std::memory_order_release);
    });

    std::uint64_t torn = 0;
    std::int64_t last = 0;
    std::uint64_t out_of_order = 0;
    auto check = [&](const shm::RecordView& v) {
        const auto i = v.event().book_sequence;
        if (i <= last) ++out_of_order;
        last = i;
        std::size_t j = 0;
        for (const auto& l : v.bids()) {
            if (l.price != static_cast<double>(i) || l.quantity != i * 2.0 + j) ++torn;
            ++j;
        }
        for (const auto& l : v.asks()) {
            if (l.price != static_cast<double>(i) || l.quantity != i * 2.0 + j) ++torn;
            ++j;
        }
    };
    while (!done.load(std::memory_order_acquire)) r.poll(check);
    while (r.poll(check) > 0) {
    }
    writer.join();

    EXPECT_EQ(torn, 0u);
    EXPECT_EQ(out_of_order, 0u);
    EXPECT_GT(r.stats().records, 0u);
    EXPECT_GT(r.stats().lapped, 0u) << "the race this test is about never happened";
    // Every record is accounted for: delivered or counted as lost.
    EXPECT_LE(r.stats().records + r.stats().lost, static_cast<std::uint64_t>(kRecords));
}

// ===========================================================================
// The publisher
// ===========================================================================

MarketDataEvent depth_event(std::string symbol) {
    OrderbookSnapshot s;
    s.symbol = symbol;
    s.exchange = "binance_usdt_futures";
    s.bids = {{100.0, 1.0}, {99.0, 2.0}};
    s.asks = {{101.0, 3.0}};
    s.sequence = 7;
    s.timestamp = 1'700'000'000'000;
    s.local_timestamp = 1'700'000'000'005;
    MarketDataEvent e;
    e.data_type = DataType::Depth;
    e.event_type = "update";
    e.symbol = std::move(symbol);
    e.exchange = "binance_usdt_futures";
    e.data = s;
    return e;
}

TEST(ShmPublisher, ADepthEventBecomesADepthRecord) {
    Segment seg;
    ShmConfig cfg;
    cfg.name = seg.name;
    cfg.capacity_mb = 1;
    ShmPublisher pub(cfg);
    shm::Reader r(seg.name);

    ASSERT_TRUE(pub.publish(depth_event("BTC_USDT_PERP")));
    int n = 0;
    r.poll([&](const shm::RecordView& v) {
        ++n;
        EXPECT_EQ(v.type(), shm::RecordType::kDepth);
        EXPECT_EQ(v.symbol(), "BTC_USDT_PERP");
        EXPECT_EQ(v.kind(), shm::EventKind::kUpdate);
        EXPECT_EQ(v.event().book_sequence, 7);
        EXPECT_EQ(v.event().exchange_ts_ms, 1'700'000'000'000);
        EXPECT_EQ(v.event().local_ts_ms, 1'700'000'000'005);
        ASSERT_EQ(v.bids().size(), 2u);
        EXPECT_DOUBLE_EQ(v.bids()[1].quantity, 2.0);
        EXPECT_DOUBLE_EQ(v.asks()[0].price, 101.0);
        EXPECT_GT(v.event().publish_ns, 0);
    });
    EXPECT_EQ(n, 1);
}

TEST(ShmPublisher, TickerAndKlineEvents) {
    Segment seg;
    ShmConfig cfg;
    cfg.name = seg.name;
    cfg.capacity_mb = 1;
    ShmPublisher pub(cfg);
    shm::Reader r(seg.name);

    MarketDataEvent t;
    t.data_type = DataType::Ticker;
    t.event_type = "update";
    t.symbol = "ETH_USDT_PERP";
    t.exchange = "okx";
    t.data = TickerData{"ETH_USDT_PERP", "okx", 3000.0, 1.0, 3000.5, 2.0, 1, 2};
    ASSERT_TRUE(pub.publish(t));

    KlineData kd;
    kd.interval = "1m";
    kd.close = 3001.0;
    kd.num_trades = 12;
    kd.is_closed = true;
    MarketDataEvent k;
    k.data_type = DataType::Kline;
    k.event_type = "update";
    k.symbol = "ETH_USDT_PERP";
    k.exchange = "okx";
    k.data = kd;
    ASSERT_TRUE(pub.publish(k));

    std::vector<shm::RecordType> types;
    r.poll([&](const shm::RecordView& v) {
        types.push_back(v.type());
        if (v.type() == shm::RecordType::kTicker) {
            EXPECT_DOUBLE_EQ(v.ticker().bid_price, 3000.0);
            EXPECT_DOUBLE_EQ(v.ticker().ask_quantity, 2.0);
        } else {
            EXPECT_EQ(std::string(v.kline().interval), "1m");
            EXPECT_EQ(v.kline().num_trades, 12);
            EXPECT_EQ(v.kline().is_closed, 1);
        }
    });
    EXPECT_EQ(types, (std::vector<shm::RecordType>{shm::RecordType::kTicker,
                                                   shm::RecordType::kKline}));
}

// Truncating would turn one symbol into another; refusing keeps it on ZMQ.
TEST(ShmPublisher, ASymbolTooLongForTheFieldIsRefusedNotTruncated) {
    Segment seg;
    ShmConfig cfg;
    cfg.name = seg.name;
    cfg.capacity_mb = 1;
    ShmPublisher pub(cfg);
    EXPECT_FALSE(pub.publish(depth_event(std::string(40, 'X'))));
    EXPECT_EQ(pub.published(), 0u);
}
