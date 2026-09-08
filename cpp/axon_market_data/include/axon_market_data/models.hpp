#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

#include <glaze/glaze.hpp>

namespace axon_market_data {

inline int64_t now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

enum class DataType { Depth, Ticker, Kline };

constexpr std::string_view to_string(DataType dt) {
    switch (dt) {
    case DataType::Depth:  return "depth";
    case DataType::Ticker: return "ticker";
    case DataType::Kline:  return "kline";
    }
    return "unknown";
}

enum class MarketType { Spot, UsdtFutures, CoinFutures };

constexpr std::string_view to_string(MarketType mt) {
    switch (mt) {
    case MarketType::Spot:        return "spot";
    case MarketType::UsdtFutures: return "usdt_futures";
    case MarketType::CoinFutures: return "coin_futures";
    }
    return "unknown";
}

inline MarketType parse_market_type(std::string_view s) {
    if (s == "usdt_futures") return MarketType::UsdtFutures;
    if (s == "coin_futures") return MarketType::CoinFutures;
    return MarketType::Spot;
}

// ---------------------------------------------------------------------------
// Price level
// ---------------------------------------------------------------------------

struct PriceLevel {
    double price    = 0.0;
    double quantity = 0.0;
};

// ---------------------------------------------------------------------------
// Orderbook (mutable, maintained locally) — shared by every exchange adapter
// ---------------------------------------------------------------------------

struct OrderbookSnapshot;

struct BookLevel {
    double      quantity = 0.0;
    // The exchange's own "<price>:<quantity>" text, kept verbatim. Only
    // populated by adapters that must reproduce it byte-for-byte (OKX's CRC32
    // checksum); empty everywhere else.
    std::string raw;
};

struct Orderbook {
    std::string symbol;   // unified symbol
    std::string exchange;
    std::map<double, BookLevel, std::greater<>> bids; // descending
    std::map<double, BookLevel>                 asks; // ascending
    int64_t sequence        = 0;
    int64_t timestamp       = 0;
    int64_t local_timestamp = 0;

    // A zero quantity removes the level, which is how every supported exchange
    // encodes a deletion.
    void apply_bid(double price, double quantity, std::string raw = {}) {
        if (quantity == 0.0) bids.erase(price);
        else                 bids[price] = BookLevel{quantity, std::move(raw)};
    }

    void apply_ask(double price, double quantity, std::string raw = {}) {
        if (quantity == 0.0) asks.erase(price);
        else                 asks[price] = BookLevel{quantity, std::move(raw)};
    }

    void clear() {
        bids.clear();
        asks.clear();
        sequence  = 0;
        timestamp = 0;
    }

    bool empty() const { return bids.empty() && asks.empty(); }

    std::optional<PriceLevel> best_bid() const {
        if (bids.empty()) return std::nullopt;
        auto it = bids.begin();
        return PriceLevel{it->first, it->second.quantity};
    }

    std::optional<PriceLevel> best_ask() const {
        if (asks.empty()) return std::nullopt;
        auto it = asks.begin();
        return PriceLevel{it->first, it->second.quantity};
    }

    std::optional<double> mid_price() const {
        auto bb = best_bid();
        auto ba = best_ask();
        if (bb && ba) return (bb->price + ba->price) / 2.0;
        return std::nullopt;
    }

    std::optional<double> spread() const {
        auto bb = best_bid();
        auto ba = best_ask();
        if (bb && ba) return ba->price - bb->price;
        return std::nullopt;
    }

    std::vector<PriceLevel> top_bids(size_t n) const {
        std::vector<PriceLevel> out;
        out.reserve(n);
        size_t i = 0;
        for (auto it = bids.begin(); it != bids.end() && i < n; ++it, ++i)
            out.push_back({it->first, it->second.quantity});
        return out;
    }

    std::vector<PriceLevel> top_asks(size_t n) const {
        std::vector<PriceLevel> out;
        out.reserve(n);
        size_t i = 0;
        for (auto it = asks.begin(); it != asks.end() && i < n; ++it, ++i)
            out.push_back({it->first, it->second.quantity});
        return out;
    }

    // The top-N view every adapter publishes.
    OrderbookSnapshot snapshot(size_t depth) const;
};

// ---------------------------------------------------------------------------
// Orderbook snapshot (immutable, for ZMQ publishing)
// ---------------------------------------------------------------------------

struct OrderbookSnapshot {
    std::string           symbol;
    std::string           exchange;
    std::vector<PriceLevel> bids;
    std::vector<PriceLevel> asks;
    int64_t               sequence        = 0;
    int64_t               timestamp       = 0;
    int64_t               local_timestamp = 0;
};

inline OrderbookSnapshot Orderbook::snapshot(size_t depth) const {
    OrderbookSnapshot snap;
    snap.symbol          = symbol;
    snap.exchange        = exchange;
    snap.bids            = top_bids(depth);
    snap.asks            = top_asks(depth);
    snap.sequence        = sequence;
    snap.timestamp       = timestamp;
    snap.local_timestamp = local_timestamp;
    return snap;
}

// ---------------------------------------------------------------------------
// Ticker (bookTicker)
// ---------------------------------------------------------------------------

struct TickerData {
    std::string symbol;
    std::string exchange;
    double      best_bid_price = 0.0;
    double      best_bid_qty   = 0.0;
    double      best_ask_price = 0.0;
    double      best_ask_qty   = 0.0;
    int64_t     timestamp       = 0;
    int64_t     local_timestamp = 0;
};

// ---------------------------------------------------------------------------
// Kline
// ---------------------------------------------------------------------------

struct KlineData {
    std::string symbol;
    std::string exchange;
    std::string interval;
    int64_t     open_time    = 0;
    int64_t     close_time   = 0;
    double      open         = 0.0;
    double      high         = 0.0;
    double      low          = 0.0;
    double      close        = 0.0;
    double      volume       = 0.0;
    double      quote_volume = 0.0;
    int64_t     num_trades   = 0;
    bool        is_closed    = false;
    int64_t     timestamp       = 0;
    int64_t     local_timestamp = 0;
};

// ---------------------------------------------------------------------------
// Unified market data event
// ---------------------------------------------------------------------------

using EventData = std::variant<OrderbookSnapshot, TickerData, KlineData>;

struct MarketDataEvent {
    DataType    data_type;
    std::string event_type; // "snapshot", "update"
    std::string symbol;     // unified symbol (e.g. ETH_USDT_SPOT)
    std::string exchange;
    EventData   data;
    int64_t     timestamp = 0;
};

// ---------------------------------------------------------------------------
// Glaze metadata for JSON serialization (ZMQ output)
// ---------------------------------------------------------------------------

} // namespace axon_market_data

template <>
struct glz::meta<axon_market_data::PriceLevel> {
    using T = axon_market_data::PriceLevel;
    static constexpr auto value = object("price", &T::price, "quantity", &T::quantity);
};

template <>
struct glz::meta<axon_market_data::OrderbookSnapshot> {
    using T = axon_market_data::OrderbookSnapshot;
    static constexpr auto value = object(
        "symbol", &T::symbol,
        "exchange", &T::exchange,
        "bids", &T::bids,
        "asks", &T::asks,
        "sequence", &T::sequence,
        "timestamp", &T::timestamp,
        "local_timestamp", &T::local_timestamp);
};

template <>
struct glz::meta<axon_market_data::TickerData> {
    using T = axon_market_data::TickerData;
    static constexpr auto value = object(
        "symbol", &T::symbol,
        "exchange", &T::exchange,
        "best_bid_price", &T::best_bid_price,
        "best_bid_qty", &T::best_bid_qty,
        "best_ask_price", &T::best_ask_price,
        "best_ask_qty", &T::best_ask_qty,
        "timestamp", &T::timestamp,
        "local_timestamp", &T::local_timestamp);
};

template <>
struct glz::meta<axon_market_data::KlineData> {
    using T = axon_market_data::KlineData;
    static constexpr auto value = object(
        "symbol", &T::symbol,
        "exchange", &T::exchange,
        "interval", &T::interval,
        "open_time", &T::open_time,
        "close_time", &T::close_time,
        "open", &T::open,
        "high", &T::high,
        "low", &T::low,
        "close", &T::close,
        "volume", &T::volume,
        "quote_volume", &T::quote_volume,
        "num_trades", &T::num_trades,
        "is_closed", &T::is_closed,
        "timestamp", &T::timestamp,
        "local_timestamp", &T::local_timestamp);
};
