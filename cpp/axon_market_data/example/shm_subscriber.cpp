/**
 * axon_market_data shared-memory subscriber example
 *
 * Reads the feed the market data service writes to shared memory -- the
 * same-host counterpart of mds_subscriber, which reads ZMQ. Needs nothing but
 * axon/market_data/shm_feed.h: no ZMQ, no JSON.
 *
 * Usage:
 *   ./mds_shm_subscriber                                   # every topic, segment axon_mds
 *   ./mds_shm_subscriber axon_mds binance_usdt_futures.depth
 *   ./mds_shm_subscriber axon_mds binance_usdt_futures.depth.BTC_USDT_PERP
 *   ./mds_shm_subscriber axon_mds "" --stats               # rates and one-way latency only
 *
 * The filter is a topic prefix, "{exchange}.{data_type}.{symbol}", exactly as
 * on ZMQ -- matched here, by the reader, since a broadcast ring has no
 * per-subscriber routing.
 */

#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include "axon/market_data/shm_feed.h"

namespace shm = axon::market_data::shm;

namespace {

volatile std::sig_atomic_t g_stop = 0;

std::string_view type_name(shm::RecordType t) {
    switch (t) {
        case shm::RecordType::kDepth:  return "depth";
        case shm::RecordType::kTicker: return "ticker";
        case shm::RecordType::kKline:  return "kline";
        default:                       return "?";
    }
}

std::string topic_of(const shm::RecordView& v) {
    std::string t(v.exchange());
    t += '.';
    t += type_name(v.type());
    t += '.';
    t += v.symbol();
    return t;
}

void print(const shm::RecordView& v, std::uint64_t n, double latency_us) {
    std::printf("[%llu] %s  (%.1f us)\n", static_cast<unsigned long long>(n), topic_of(v).c_str(),
                latency_us);
    switch (v.type()) {
        case shm::RecordType::kDepth: {
            std::printf("  seq: %lld  asks: ", static_cast<long long>(v.event().book_sequence));
            for (std::size_t i = 0; i < std::min<std::size_t>(5, v.asks().size()); ++i)
                std::printf("%gx%g  ", v.asks()[i].price, v.asks()[i].quantity);
            std::printf("\n  bids: ");
            for (std::size_t i = 0; i < std::min<std::size_t>(5, v.bids().size()); ++i)
                std::printf("%gx%g  ", v.bids()[i].price, v.bids()[i].quantity);
            std::printf("\n");
            break;
        }
        case shm::RecordType::kTicker: {
            const auto& t = v.ticker();
            std::printf("  bid=%g  ask=%g  spread=%g\n", t.bid_price, t.ask_price,
                        t.ask_price - t.bid_price);
            break;
        }
        case shm::RecordType::kKline: {
            const auto& k = v.kline();
            std::printf("  %s  O=%g H=%g L=%g C=%g  vol=%g%s\n", k.interval, k.open, k.high, k.low,
                        k.close, k.volume, k.is_closed ? " [CLOSED]" : "");
            break;
        }
        default:
            break;
    }
}

double percentile(std::vector<double>& v, double p) {
    if (v.empty()) return 0.0;
    const auto at = static_cast<std::size_t>(p * static_cast<double>(v.size() - 1));
    std::nth_element(v.begin(), v.begin() + static_cast<std::ptrdiff_t>(at), v.end());
    return v[at];
}

}  // namespace

int main(int argc, char* argv[]) {
    const std::string name   = argc > 1 ? argv[1] : "axon_mds";
    const std::string filter = argc > 2 ? argv[2] : "";
    const bool stats_only    = argc > 3 && std::string(argv[3]) == "--stats";

    std::signal(SIGINT, [](int) { g_stop = 1; });
    std::signal(SIGTERM, [](int) { g_stop = 1; });

    try {
        shm::Reader reader(name);
        std::printf("Reading /dev/shm/%s (%zu MiB ring)\nTopic filter: %s\n---\n", name.c_str(),
                    reader.capacity() >> 20, filter.empty() ? "(all)" : filter.c_str());

        std::uint64_t matched = 0;
        std::vector<double> latencies_us;
        auto window_start = std::chrono::steady_clock::now();
        std::uint64_t window_records = 0;
        bool warned_dead = false;

        while (!g_stop) {
            const std::size_t n = reader.poll([&](const shm::RecordView& v) {
                const double latency_us =
                    static_cast<double>(shm::steady_ns() - v.event().publish_ns) / 1e3;
                if (!filter.empty() && topic_of(v).rfind(filter, 0) != 0) return;
                ++matched;
                ++window_records;
                if (stats_only) {
                    latencies_us.push_back(latency_us);
                } else {
                    print(v, matched, latency_us);
                }
            });

            const auto now = std::chrono::steady_clock::now();
            if (stats_only && now - window_start >= std::chrono::seconds(5)) {
                const double secs = std::chrono::duration<double>(now - window_start).count();
                const auto& s = reader.stats();
                std::printf("%.0f rec/s  latency us p50=%.2f p99=%.2f p99.9=%.2f max=%.2f  "
                            "lost=%llu lapped=%llu restarts=%llu\n",
                            static_cast<double>(window_records) / secs,
                            percentile(latencies_us, 0.50), percentile(latencies_us, 0.99),
                            percentile(latencies_us, 0.999),
                            latencies_us.empty()
                                ? 0.0
                                : *std::max_element(latencies_us.begin(), latencies_us.end()),
                            static_cast<unsigned long long>(s.lost),
                            static_cast<unsigned long long>(s.lapped),
                            static_cast<unsigned long long>(s.restarts));
                latencies_us.clear();
                window_records = 0;
                window_start = now;
            }

            if (n == 0) {
                if (!reader.writer_alive() && !warned_dead) {
                    std::printf("(no heartbeat from the market data service)\n");
                    warned_dead = true;
                } else if (reader.writer_alive()) {
                    warned_dead = false;
                }
                // A latency-critical strategy spins here instead; an example
                // that pins a core at 100% is a poor default.
                if (!stats_only) std::this_thread::yield();
            }
        }

        const auto& s = reader.stats();
        std::printf("---\n%llu records, %llu lost, %llu lapped, %llu writer restarts\n",
                    static_cast<unsigned long long>(s.records),
                    static_cast<unsigned long long>(s.lost),
                    static_cast<unsigned long long>(s.lapped),
                    static_cast<unsigned long long>(s.restarts));
    } catch (const std::exception& e) {
        std::fprintf(stderr, "error: %s\n", e.what());
        return 1;
    }
    return 0;
}
