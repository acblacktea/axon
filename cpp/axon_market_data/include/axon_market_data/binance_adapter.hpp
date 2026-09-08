#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <simdjson.h>
#include <spdlog/spdlog.h>

#include "adapter.hpp"
#include "config.hpp"
#include "http_client.hpp"
#include "models.hpp"
#include "websocket_client.hpp"

namespace axon_market_data {

namespace net = boost::asio;
using tcp     = net::ip::tcp;

class BinanceAdapter : public BaseAdapter,
                       public std::enable_shared_from_this<BinanceAdapter> {
public:
    BinanceAdapter(net::io_context& ioc,
                   const ExchangeConfig& cfg,
                   EventCallback on_event,
                   std::shared_ptr<spdlog::logger> logger = nullptr);
    ~BinanceAdapter();

    void start() override;
    void stop() override;

private:
    // ----- WebSocket callbacks -----
    void on_ws_message(std::string_view raw);
    void on_ws_state_change(bool connected);

    // ----- subscribe after (re)connect -----
    net::awaitable<void> subscribe_streams();

    // ----- REST (for depth snapshot) -----
    // Both take the symbol BY VALUE: a coroutine frame stores a reference
    // parameter as a reference, so a caller's local string would dangle the
    // moment the caller returns.
    net::awaitable<void> sync_orderbook(std::string exchange_symbol);
    net::awaitable<void> retry_sync(std::string exchange_symbol);

    void schedule_sync(std::string exchange_symbol);

    // ----- message handling -----
    void handle_message(std::string_view raw);
    void handle_depth_update(simdjson::ondemand::document& doc);
    void handle_book_ticker(simdjson::ondemand::document& doc);
    void handle_kline(simdjson::ondemand::document& doc);

    // ----- symbol helpers -----
    std::string to_exchange_symbol(const std::string& unified) const;
    std::string stream_name(const std::string& exchange_sym, DataType dt) const;

    // ----- core -----
    net::io_context&                ioc_;
    ExchangeConfig                  cfg_;
    std::shared_ptr<WebSocketClient> ws_client_;
    bool                            running_ = false;
    int                             request_id_ = 0;

    // ----- endpoints -----
    std::string rest_host_;
    std::string depth_path_;
    int         rest_depth_limit_ = 1000; // REST snapshot "limit" parameter

    // ----- subscriptions -----
    std::vector<std::string>     all_streams_;
    std::set<std::string>        depth_exchange_symbols_;

    // ----- symbol mapping -----
    std::unordered_map<std::string, std::string> sym_to_exchange_;

    // ----- orderbook state -----
    std::unordered_map<std::string, Orderbook>                orderbooks_;
    std::unordered_map<std::string, int64_t>                  last_update_ids_;
    std::set<std::string>                                     syncing_;
    // Symbols holding a fresh REST snapshot that no diff event has chained onto
    // yet. Their next live event is validated with Binance's join condition
    // rather than the steady-state one.
    std::set<std::string>                                     awaiting_first_;
    std::unordered_map<std::string, std::vector<std::string>> event_buffers_;

    // Futures carry "pu" and join the snapshot one id earlier than spot.
    bool    is_futures() const { return cfg_.market_type != MarketType::Spot; }
    int64_t join_target(int64_t snapshot_id) const {
        return is_futures() ? snapshot_id : snapshot_id + 1;
    }

    // ----- simdjson parser (reused) -----
    simdjson::ondemand::parser json_parser_;

    size_t depth_levels_ = 400;
};

} // namespace axon_market_data
