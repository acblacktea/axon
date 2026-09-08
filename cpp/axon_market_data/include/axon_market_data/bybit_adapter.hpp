#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/asio.hpp>
#include <simdjson.h>
#include <spdlog/spdlog.h>

#include "adapter.hpp"
#include "config.hpp"
#include "models.hpp"
#include "websocket_client.hpp"

namespace axon_market_data {

namespace net = boost::asio;

class BybitAdapter : public BaseAdapter,
                     public std::enable_shared_from_this<BybitAdapter> {
public:
    BybitAdapter(net::io_context& ioc,
                 const ExchangeConfig& cfg,
                 EventCallback on_event,
                 std::shared_ptr<spdlog::logger> logger = nullptr);
    ~BybitAdapter();

    void start() override;
    void stop() override;

private:
    // ----- WebSocket callbacks -----
    void on_ws_message(std::string_view raw);
    void on_ws_state_change(bool connected);

    net::awaitable<void> subscribe_topics();

    // ----- message handling -----
    void handle_message(std::string_view raw);
    void handle_orderbook(simdjson::ondemand::document& doc,
                          const std::string& symbol, bool is_bbo);
    void handle_kline(simdjson::ondemand::document& doc,
                      const std::string& symbol);

    // ----- symbol helpers -----
    std::string to_exchange_symbol(const std::string& unified) const;

    // ----- core -----
    net::io_context&                 ioc_;
    ExchangeConfig                   cfg_;
    std::shared_ptr<WebSocketClient> ws_client_;
    bool                             running_ = false;

    // ----- subscriptions -----
    std::vector<std::string> topics_;

    // ----- state -----
    std::unordered_map<std::string, Orderbook>  books_;
    std::unordered_map<std::string, TickerData> tickers_;

    simdjson::ondemand::parser json_parser_;

    size_t depth_levels_ = 400;
    int    book_depth_   = 1000; // resolved orderbook.{50,200,1000} topic

    static constexpr size_t SUBSCRIBE_BATCH = 10;
};

} // namespace axon_market_data
