#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "logging.hpp"
#include "models.hpp"

namespace axon_market_data {

struct SubscriptionConfig {
    std::vector<std::string> depth;
    std::vector<std::string> ticker;
    std::vector<std::string> kline;
};

struct ExchangeConfig {
    std::string        name;
    MarketType         market_type  = MarketType::Spot;
    std::string        update_speed = "100ms";
    bool               verify_checksum = false; // OKX only: CRC32 orderbook check
    // Levels per side to publish. Each exchange caps this at what it serves:
    // Hyperliquid tops out at 20, OKX at 400, Bybit rounds up to 50/200/1000.
    size_t             depth_levels = 400;
    SubscriptionConfig subscriptions;
};

struct MetricsConfig {
    bool        enabled = true;
    std::string host    = "0.0.0.0";
    int         port    = 9101;  // 9100 is node_exporter's, and the two are
                                 // routinely scraped from the same host
};

struct AppConfig {
    std::string                pub_address = "tcp://*:5558";
    LogConfig                  logging;
    MetricsConfig              metrics;
    std::vector<ExchangeConfig> exchanges;
};

AppConfig load_config(const std::string& path);

} // namespace axon_market_data
