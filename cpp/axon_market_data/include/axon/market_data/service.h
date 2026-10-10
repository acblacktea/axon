#pragma once

#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <spdlog/spdlog.h>

#include "axon/market_data/adapter.h"
#include "axon/market_data/binance_adapter.h"
#include "axon/market_data/bybit_adapter.h"
#include "axon/market_data/config.h"
#include "axon/market_data/hyperliquid_adapter.h"
#include "axon/market_data/okx_adapter.h"
#include "axon/market_data/zmq_publisher.h"

namespace axon::market_data {

class Service {
public:
    explicit Service(const AppConfig& cfg,
                     std::shared_ptr<spdlog::logger> logger = nullptr);
    ~Service();

    // Blocking – runs the io_context event loop.
    void run();
    void stop();

private:
    void on_event(const MarketDataEvent& event);

    AppConfig                    cfg_;
    std::shared_ptr<spdlog::logger> logger_;
    boost::asio::io_context      ioc_;
    boost::asio::steady_timer    silent_check_;
    ZmqPublisher                 publisher_;
    std::vector<std::shared_ptr<Adapter>> adapters_;
    bool                         running_ = false;
};

} // namespace axon::market_data
