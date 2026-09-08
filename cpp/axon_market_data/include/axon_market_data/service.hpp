#pragma once

#include <memory>
#include <string>
#include <vector>

#include <boost/asio.hpp>
#include <spdlog/spdlog.h>

#include "adapter.hpp"
#include "binance_adapter.hpp"
#include "bybit_adapter.hpp"
#include "config.hpp"
#include "hyperliquid_adapter.hpp"
#include "okx_adapter.hpp"
#include "zmq_publisher.hpp"

namespace axon_market_data {

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
    ZmqPublisher                 publisher_;
    std::vector<std::shared_ptr<Adapter>> adapters_;
    bool                         running_ = false;
};

} // namespace axon_market_data
