#pragma once

#include <cstdint>
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

class HyperliquidAdapter : public BaseAdapter,
                           public std::enable_shared_from_this<HyperliquidAdapter> {
public:
    HyperliquidAdapter(net::io_context& ioc,
                       const ExchangeConfig& cfg,
                       EventCallback on_event,
                       std::shared_ptr<spdlog::logger> logger = nullptr);
    ~HyperliquidAdapter();

    void start() override;
    void stop() override;

private:
    struct Subscription {
        std::string type; // "l2Book" / "bbo" / "candle"
        std::string coin;
    };

    // ----- WebSocket callbacks -----
    void on_ws_message(std::string_view raw);
    void on_ws_state_change(bool connected);

    net::awaitable<void> subscribe_all();

    // ----- message handling -----
    void handle_message(std::string_view raw);
    void handle_l2_book(simdjson::ondemand::value data);
    void handle_bbo(simdjson::ondemand::value data);
    void handle_candle(simdjson::ondemand::value data);

    // ----- symbol helpers -----
    std::string to_exchange_symbol(const std::string& unified) const;

    // ----- core -----
    net::io_context&                 ioc_;
    ExchangeConfig                   cfg_;
    std::shared_ptr<WebSocketClient> ws_client_;
    bool                             running_ = false;

    // ----- subscriptions -----
    std::vector<Subscription> subscriptions_;

    // ----- state -----
    // Only the BBO needs carrying forward: a side is null when it is empty.
    std::unordered_map<std::string, TickerData> tickers_;

    simdjson::ondemand::parser json_parser_;

    size_t depth_levels_ = 400;
    bool   fast_book_    = false; // l2Book "fast": 5 levels at ~2/s

    // l2Book serves 5 levels in fast mode, 20 otherwise. Nothing deeper exists.
    static constexpr size_t FAST_LEVELS = 5;
    static constexpr size_t SLOW_LEVELS = 20;
};

} // namespace axon_market_data
