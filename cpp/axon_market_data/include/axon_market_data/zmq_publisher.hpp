#pragma once

#include <string>
#include <memory>

#include <zmq.hpp>
#include <spdlog/spdlog.h>

#include "models.hpp"

namespace axon_market_data {

class ZmqPublisher {
public:
    explicit ZmqPublisher(const std::string& address,
                          std::shared_ptr<spdlog::logger> logger = nullptr);
    ~ZmqPublisher();

    ZmqPublisher(const ZmqPublisher&)            = delete;
    ZmqPublisher& operator=(const ZmqPublisher&) = delete;

    void start();
    void stop();

    // Publish a market data event.
    // Topic format: "{exchange}.{data_type}.{symbol}"
    void publish(const MarketDataEvent& event);

private:
    std::string                  address_;
    std::shared_ptr<spdlog::logger> logger_;
    std::unique_ptr<zmq::context_t> ctx_;
    std::unique_ptr<zmq::socket_t>  pub_;
    bool running_ = false;
};

} // namespace axon_market_data
