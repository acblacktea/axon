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

// OKX's book checksum, exposed for testing: the exchange only ever sends
// "checksum":0 today, so this path has no live coverage.
// Builds "bid1px:bid1sz:ask1px:ask1sz:..." from BookLevel::raw over the top
// `levels` levels, then CRC32s it as a signed 32-bit value.
std::string okx_checksum_string(const Orderbook& book, size_t levels);
int32_t     okx_checksum(const Orderbook& book, size_t levels);

class OkxAdapter : public BaseAdapter,
                   public std::enable_shared_from_this<OkxAdapter> {
public:
    OkxAdapter(net::io_context& ioc,
               const ExchangeConfig& cfg,
               EventCallback on_event,
               std::shared_ptr<spdlog::logger> logger = nullptr);
    ~OkxAdapter();

    void start() override;
    void stop() override;

private:
    struct Channel {
        std::string channel; // "books" / "bbo-tbt" / "candle1m"
        std::string inst_id;
    };

    // ----- WebSocket callbacks -----
    void on_ws_message(std::string_view raw);
    void on_public_state_change(bool connected);
    void on_business_state_change(bool connected);

    std::shared_ptr<WebSocketClient> make_client(const std::string& path,
                                                 const std::string& tag,
                                                 WebSocketClient::StateCallback on_state);
    net::awaitable<void> subscribe(std::shared_ptr<WebSocketClient> client,
                                   const std::vector<Channel>& channels);
    net::awaitable<void> resubscribe_book(std::string inst_id);

    // ----- message handling -----
    void handle_message(std::string_view raw);
    // `books5` is a snapshot-only channel: no action / prevSeqId / checksum.
    void handle_books(simdjson::ondemand::document& doc,
                      const std::string& inst_id, bool snapshot_only);
    void handle_bbo(simdjson::ondemand::document& doc,
                    const std::string& inst_id);
    void handle_candle(simdjson::ondemand::document& doc,
                       const std::string& inst_id);

    // ----- orderbook helpers -----
    void apply_levels(Orderbook& book, simdjson::ondemand::array levels, bool is_bid);
    bool verify_checksum(const Orderbook& book, int64_t expected) const;

    // ----- symbol helpers -----
    std::string to_exchange_symbol(const std::string& unified) const;

    // ----- core -----
    net::io_context&                 ioc_;
    ExchangeConfig                   cfg_;
    // OKX serves candles from a separate endpoint, so market data and klines
    // ride on two connections.
    std::shared_ptr<WebSocketClient> ws_public_;
    std::shared_ptr<WebSocketClient> ws_business_;
    bool                             running_ = false;

    // ----- subscriptions -----
    std::vector<Channel> public_channels_;   // books, bbo-tbt
    std::vector<Channel> business_channels_; // candle1m

    // ----- orderbook state -----
    std::unordered_map<std::string, Orderbook> books_;

    simdjson::ondemand::parser json_parser_;

    size_t      depth_levels_ = 400;
    const char* book_channel_  = "books";

    // books5 serves 5 levels; books serves 400. Nothing in between exists.
    static constexpr size_t BOOKS5_LEVELS = 5;

    static constexpr size_t CHECKSUM_LEVELS = 25;
};

} // namespace axon_market_data
