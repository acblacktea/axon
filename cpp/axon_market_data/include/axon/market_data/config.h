#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "axon/market_data/logging.h"
#include "axon/market_data/models.h"

namespace axon::market_data {

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

// The same-host transport, alongside ZMQ. A strategy on this machine reads
// the feed straight out of shared memory; one on another machine uses ZMQ.
struct ShmConfig {
    bool        enabled     = true;
    // POSIX shared memory object, under /dev/shm on Linux.
    std::string name        = "axon_mds";
    // Ring size. A power of two; the deepest books are ~13 KB a record.
    std::size_t capacity_mb = 64;
};

struct AppConfig {
    std::string                pub_address = "tcp://*:5558";
    ShmConfig                  shm;
    // Seconds after start at which every subscribed-but-silent topic is
    // logged. 0 disables the check. A one-shot, not a recurring monitor:
    // it catches a topic that never worked, which is a deploy-time mistake.
    // A topic that worked and then went quiet is a runtime condition, and
    // the Prometheus query in the README is the right tool for that.
    int                        silent_topic_check_seconds = 30;
    LogConfig                  logging;
    MetricsConfig              metrics;
    std::vector<ExchangeConfig> exchanges;
};

AppConfig load_config(const std::string& path);

} // namespace axon::market_data
