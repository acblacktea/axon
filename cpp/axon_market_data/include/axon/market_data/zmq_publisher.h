#pragma once

#include <string>
#include <string_view>
#include <memory>

#include <zmq.hpp>
#include <spdlog/spdlog.h>

#include "axon/market_data/models.h"

namespace axon::market_data {

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
    // Sends one frame without blocking. False if it did not fit some
    // subscriber's queue (it still went to every subscriber with room).
    bool send_frame(std::string_view data, zmq::send_flags flags);

    std::string                  address_;
    std::shared_ptr<spdlog::logger> logger_;
    std::unique_ptr<zmq::context_t> ctx_;
    std::unique_ptr<zmq::socket_t>  pub_;
    bool running_ = false;
};

} // namespace axon::market_data
