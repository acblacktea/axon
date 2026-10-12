#include "axon/market_data/shm_publisher.h"

#include <cstddef>
#include <cstring>
#include <limits>
#include <span>
#include <type_traits>

#include "axon/market_data/metrics.h"

namespace axon::market_data {
namespace {

// The record's levels are the snapshot's levels, byte for byte, so a depth
// record is two memcpys out of the snapshot's vectors.
static_assert(sizeof(PriceLevel) == sizeof(shm::Level));
static_assert(offsetof(PriceLevel, price) == offsetof(shm::Level, price));
static_assert(offsetof(PriceLevel, quantity) == offsetof(shm::Level, quantity));
static_assert(std::is_trivially_copyable_v<PriceLevel>);

// Copies `text` into a fixed field, NUL-padded. False if it does not fit with
// room for the terminator.
template <std::size_t N>
bool put(char (&field)[N], std::string_view text) {
    if (text.size() >= N) return false;
    std::memset(field, 0, N);
    std::memcpy(field, text.data(), text.size());
    return true;
}

template <typename T>
std::span<const std::byte> bytes_of(const T& v) {
    return std::as_bytes(std::span<const T>(&v, 1));
}

template <typename T>
std::span<const std::byte> bytes_of(const std::vector<T>& v) {
    return std::as_bytes(std::span<const T>(v));
}

} // namespace

ShmPublisher::ShmPublisher(const ShmConfig& config, std::shared_ptr<spdlog::logger> logger)
    : logger_(logger ? std::move(logger) : spdlog::default_logger())
    , writer_(config.name, config.capacity_mb << 20) {
    logger_->info("shared-memory feed: /dev/shm/{} ({} MiB ring)", config.name,
                  config.capacity_mb);
}

bool ShmPublisher::publish(const MarketDataEvent& event) {
    shm::EventHeader h{};
    if (!put(h.exchange, event.exchange)) {
        refuse(event, "exchange name too long");
        return false;
    }
    if (!put(h.symbol, event.symbol)) {
        refuse(event, "symbol too long");
        return false;
    }
    h.kind = event.event_type == "snapshot" ? shm::EventKind::kSnapshot
                                            : shm::EventKind::kUpdate;

    bool ok = false;
    std::visit(
        [&](const auto& d) {
            using T = std::decay_t<decltype(d)>;
            h.exchange_ts_ms = d.timestamp;
            h.local_ts_ms    = d.local_timestamp;
            if constexpr (std::is_same_v<T, OrderbookSnapshot>) {
                constexpr auto kMaxSide = std::numeric_limits<std::uint16_t>::max();
                if (d.bids.size() > kMaxSide || d.asks.size() > kMaxSide) return;
                h.book_sequence = d.sequence;
                h.bid_count     = static_cast<std::uint16_t>(d.bids.size());
                h.ask_count     = static_cast<std::uint16_t>(d.asks.size());
                h.publish_ns    = shm::steady_ns();
                ok = writer_.publish(shm::RecordType::kDepth,
                                     {bytes_of(h), bytes_of(d.bids), bytes_of(d.asks)});
            } else if constexpr (std::is_same_v<T, TickerData>) {
                const shm::Ticker t{d.best_bid_price, d.best_bid_qty, d.best_ask_price,
                                    d.best_ask_qty};
                h.publish_ns = shm::steady_ns();
                ok = writer_.publish(shm::RecordType::kTicker, {bytes_of(h), bytes_of(t)});
            } else {
                shm::Kline k{};
                if (!put(k.interval, d.interval)) return;
                k.open_time    = d.open_time;
                k.close_time   = d.close_time;
                k.open         = d.open;
                k.high         = d.high;
                k.low          = d.low;
                k.close        = d.close;
                k.volume       = d.volume;
                k.quote_volume = d.quote_volume;
                k.num_trades   = d.num_trades;
                k.is_closed    = d.is_closed ? 1 : 0;
                h.publish_ns   = shm::steady_ns();
                ok = writer_.publish(shm::RecordType::kKline, {bytes_of(h), bytes_of(k)});
            }
        },
        event.data);

    if (!ok) refuse(event, "does not fit one record");
    return ok;
}

void ShmPublisher::tick() {
    writer_.heartbeat();
    get_metrics().set_shm_records(writer_.published());
}

void ShmPublisher::refuse(const MarketDataEvent& event, const char* why) {
    get_metrics().inc_shm_refused(event.exchange);
    // The first few, not every one: a misconfigured symbol would otherwise
    // log on every message.
    if (++refused_ <= 10) {
        logger_->warn("shm: {}.{}.{} not written to shared memory ({}); ZMQ still has it",
                      event.exchange, to_string(event.data_type), event.symbol, why);
    }
}

} // namespace axon::market_data
