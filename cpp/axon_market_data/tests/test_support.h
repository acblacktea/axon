// Shared scaffolding for the market data unit tests.
//
// The adapters are driven the way the WebSocket client drives them in
// production -- one raw venue frame at a time -- but without a socket, so each
// venue's protocol handling can be pinned down against captured payloads.

#pragma once

#include <chrono>
#include <cstdlib>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gtest/gtest.h>
#include <spdlog/sinks/ringbuffer_sink.h>
#include <spdlog/spdlog.h>

#include "axon/market_data/binance_adapter.h"
#include "axon/market_data/bybit_adapter.h"
#include "axon/market_data/hyperliquid_adapter.h"
#include "axon/market_data/metrics.h"
#include "axon/market_data/okx_adapter.h"

namespace axon::market_data {

// Befriended by every adapter. The only window the tests have into adapter
// internals; everything else goes through the public interface.
struct AdapterTestAccess {
    template <class Adapter>
    static void feed(Adapter& a, std::string_view raw) { a.handle_message(raw); }

    // Binance, Bybit and Hyperliquid have one connection; OKX has two.
    template <class Adapter>
    static void connection_state(Adapter& a, bool connected) { a.on_ws_state_change(connected); }
    static void public_state(OkxAdapter& a, bool connected) { a.on_public_state_change(connected); }

    // Retry timers only re-arm while the adapter is running; start() itself
    // would open a real connection to the venue.
    template <class Adapter>
    static void set_running(Adapter& a, bool running) { a.running_ = running; }

    template <class Adapter>
    static size_t depth_levels(const Adapter& a) { return a.depth_levels_; }

    // --- Binance ---
    static void set_fetcher(BinanceAdapter& a, BinanceAdapter::SnapshotFetcher f) {
        a.fetch_snapshot_ = std::move(f);
    }
    static void schedule_sync(BinanceAdapter& a, std::string symbol) {
        a.schedule_sync(std::move(symbol));
    }
    static const std::vector<std::string>& streams(const BinanceAdapter& a) { return a.all_streams_; }
    static int rest_limit(const BinanceAdapter& a) { return a.rest_depth_limit_; }
    static bool syncing(const BinanceAdapter& a, const std::string& sym) { return a.syncing_.contains(sym); }

    // --- OKX ---
    static std::vector<std::string> public_channels(const OkxAdapter& a) {
        std::vector<std::string> out;
        for (auto& c : a.public_channels_) out.push_back(c.channel + ":" + c.inst_id);
        return out;
    }
    static std::vector<std::string> business_channels(const OkxAdapter& a) {
        std::vector<std::string> out;
        for (auto& c : a.business_channels_) out.push_back(c.channel + ":" + c.inst_id);
        return out;
    }
    static std::string book_channel(const OkxAdapter& a) { return a.book_channel_; }

    // --- Bybit ---
    static const std::vector<std::string>& topics(const BybitAdapter& a) { return a.topics_; }

    // --- Hyperliquid ---
    static std::vector<std::string> subscriptions(const HyperliquidAdapter& a) {
        std::vector<std::string> out;
        for (auto& s : a.subscriptions_) out.push_back(s.type + ":" + s.coin);
        return out;
    }
    static bool fast_book(const HyperliquidAdapter& a) { return a.fast_book_; }
};

} // namespace axon::market_data

namespace mds_test {

using namespace axon::market_data;

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

struct EventSink {
    std::vector<MarketDataEvent> events;

    EventCallback callback() {
        return [this](const MarketDataEvent& e) { events.push_back(e); };
    }

    const MarketDataEvent& last() const { return events.back(); }
    size_t size() const { return events.size(); }
    void clear() { events.clear(); }

    const OrderbookSnapshot& book(size_t i) const { return std::get<OrderbookSnapshot>(events.at(i).data); }
    const TickerData& ticker(size_t i) const { return std::get<TickerData>(events.at(i).data); }
    const KlineData& kline(size_t i) const { return std::get<KlineData>(events.at(i).data); }
    const OrderbookSnapshot& last_book() const { return std::get<OrderbookSnapshot>(last().data); }
};

// ---------------------------------------------------------------------------
// Logs
// ---------------------------------------------------------------------------

struct CapturedLogger {
    std::shared_ptr<spdlog::sinks::ringbuffer_sink_mt> sink =
        std::make_shared<spdlog::sinks::ringbuffer_sink_mt>(1024);
    std::shared_ptr<spdlog::logger> logger;

    CapturedLogger() : logger(std::make_shared<spdlog::logger>("test", sink)) {
        sink->set_pattern("%l %v");
        logger->set_level(spdlog::level::trace);
    }

    std::vector<std::string> lines() const { return sink->last_formatted(); }

    size_t count_containing(std::string_view needle) const {
        size_t n = 0;
        for (auto& l : lines())
            if (l.find(needle) != std::string::npos) ++n;
        return n;
    }
};

// ---------------------------------------------------------------------------
// Metrics: the Prometheus text exposition, parsed back into samples.
// ---------------------------------------------------------------------------

struct Sample {
    std::string                        name;
    std::map<std::string, std::string> labels;
    double                             value = 0;
};

inline std::vector<Sample> parse_exposition(const std::string& text) {
    std::vector<Sample> out;
    std::istringstream in(text);
    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;
        Sample s;
        size_t pos = line.find_first_of("{ ");
        s.name     = line.substr(0, pos);
        if (line[pos] == '{') {
            size_t end = line.find('}', pos);
            std::string body = line.substr(pos + 1, end - pos - 1);
            size_t i = 0;
            while (i < body.size()) {
                size_t eq    = body.find('=', i);
                size_t open  = body.find('"', eq);
                size_t close = body.find('"', open + 1);
                s.labels[body.substr(i, eq - i)] = body.substr(open + 1, close - open - 1);
                i = close + 1;
                if (i < body.size() && body[i] == ',') ++i;
            }
            pos = end + 1;
        }
        s.value = std::stod(line.substr(line.find_first_not_of(' ', pos)));
        out.push_back(std::move(s));
    }
    return out;
}

// Sum of every sample called `name` whose labels include all of `labels`, or
// nullopt when there is no such series at all. Absent and zero are different
// answers here on purpose -- several metrics exist precisely so that "never
// reported" and "reported as zero" can be told apart.
inline std::optional<double> metric(const std::string& name,
                                    const std::map<std::string, std::string>& labels = {}) {
    std::optional<double> total;
    for (auto& s : parse_exposition(get_metrics().serialize())) {
        if (s.name != name) continue;
        bool match = true;
        for (auto& [k, v] : labels) {
            auto it = s.labels.find(k);
            if (it == s.labels.end() || it->second != v) { match = false; break; }
        }
        if (!match) continue;
        total = total.value_or(0) + s.value;
    }
    return total;
}

inline double metric_or_zero(const std::string& name,
                             const std::map<std::string, std::string>& labels = {}) {
    return metric(name, labels).value_or(0.0);
}

// Cumulative count in the histogram bucket whose upper bound is `le`.
inline std::optional<double> bucket(const std::string& name,
                                    std::map<std::string, std::string> labels, double le) {
    for (auto& s : parse_exposition(get_metrics().serialize())) {
        if (s.name != name + "_bucket") continue;
        bool match = true;
        for (auto& [k, v] : labels) {
            auto it = s.labels.find(k);
            if (it == s.labels.end() || it->second != v) { match = false; break; }
        }
        if (!match) continue;
        auto le_it = s.labels.find("le");
        if (le_it == s.labels.end() || le_it->second == "+Inf") continue;
        if (std::abs(std::stod(le_it->second) - le) < 1e-12) return s.value;
    }
    return std::nullopt;
}

// Every test starts from a fresh, enabled registry. Port 0 lets the exposer
// take any free port, so tests can run in parallel.
class MetricsTest : public ::testing::Test {
protected:
    void SetUp() override { init_metrics(MetricsConfig{true, "127.0.0.1", 0}); }
};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

inline ExchangeConfig exchange(std::string name, MarketType mt = MarketType::Spot,
                               std::vector<std::string> depth  = {},
                               std::vector<std::string> ticker = {},
                               std::vector<std::string> kline  = {},
                               size_t depth_levels = 400) {
    ExchangeConfig c;
    c.name                 = std::move(name);
    c.market_type          = mt;
    c.depth_levels         = depth_levels;
    c.subscriptions.depth  = std::move(depth);
    c.subscriptions.ticker = std::move(ticker);
    c.subscriptions.kline  = std::move(kline);
    return c;
}

// ---------------------------------------------------------------------------
// io_context
// ---------------------------------------------------------------------------

// Runs ready handlers only -- what an adapter has co_spawned, without ever
// waiting on a timer.
inline void drain(boost::asio::io_context& ioc) {
    ioc.restart();
    ioc.poll();
}

// Runs the context until `done` holds or `timeout` passes.
inline bool run_until(boost::asio::io_context& ioc, const std::function<bool()>& done,
                      std::chrono::milliseconds timeout = std::chrono::seconds(5)) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!done()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        if (ioc.stopped()) ioc.restart();
        ioc.run_for(std::chrono::milliseconds(5));
    }
    return true;
}

inline void run_for(boost::asio::io_context& ioc, std::chrono::milliseconds d) {
    run_until(ioc, [] { return false; }, d);
}

} // namespace mds_test
