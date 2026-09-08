#include "axon_market_data/hyperliquid_adapter.hpp"

#include <algorithm>

namespace axon_market_data {

// ---------------------------------------------------------------------------
// Ctor / Dtor
// ---------------------------------------------------------------------------

HyperliquidAdapter::HyperliquidAdapter(net::io_context& ioc,
                                       const ExchangeConfig& cfg,
                                       EventCallback on_event,
                                       std::shared_ptr<spdlog::logger> logger)
    : BaseAdapter("hyperliquid", std::move(on_event), std::move(logger))
    , ioc_(ioc)
    , cfg_(cfg)
{
    // "fast" trades depth for rate: 5 levels at ~2 msg/s against 20 at ~0.24.
    // Only ask for it when the configured depth actually fits in 5 levels.
    fast_book_    = cfg_.depth_levels <= FAST_LEVELS;
    depth_levels_ = std::min(cfg_.depth_levels, fast_book_ ? FAST_LEVELS : SLOW_LEVELS);
    if (cfg_.depth_levels > SLOW_LEVELS)
        logger_->info("[{}] depth_levels {} capped to {} — l2Book serves no more",
                      exchange_name_, cfg_.depth_levels, SLOW_LEVELS);

    // One endpoint serves perps and spot; the coin name carries the market, so
    // market_type is unused here.
    auto add_symbols = [&](const std::vector<std::string>& syms, const char* type) {
        for (auto& unified : syms) {
            auto coin = to_exchange_symbol(unified);
            sym_to_unified_[coin] = unified;
            subscriptions_.push_back({type, coin});
        }
    };
    add_symbols(cfg_.subscriptions.depth,  "l2Book");
    add_symbols(cfg_.subscriptions.ticker, "bbo");
    add_symbols(cfg_.subscriptions.kline,  "candle");

    WebSocketClient::Config ws_cfg;
    ws_cfg.host          = "api.hyperliquid.xyz";
    ws_cfg.port          = "443";
    ws_cfg.path          = "/ws";
    ws_cfg.tag           = exchange_name_;
    ws_cfg.ping_interval = std::chrono::seconds(30); // server drops idle links at 60s
    ws_cfg.ping_text     = R"({"method":"ping"})";

    ws_client_ = std::make_shared<WebSocketClient>(
        ioc_, std::move(ws_cfg),
        [this](std::string_view msg) { on_ws_message(msg); },
        [this](bool connected)       { on_ws_state_change(connected); },
        logger_);
}

HyperliquidAdapter::~HyperliquidAdapter() { stop(); }

// ---------------------------------------------------------------------------
// Symbol helpers
// ---------------------------------------------------------------------------

// Perps are named by their base asset (ETH_USD_PERP -> ETH); spot pairs keep
// both sides (PURR_USDC_SPOT -> PURR/USDC).
std::string HyperliquidAdapter::to_exchange_symbol(const std::string& unified) const {
    std::string s = unified;

    if (s.ends_with("_PERP")) {
        s.resize(s.size() - 5);
        auto pos = s.find('_');
        return pos == std::string::npos ? s : s.substr(0, pos);
    }
    if (s.ends_with("_SPOT")) {
        s.resize(s.size() - 5);
        std::replace(s.begin(), s.end(), '_', '/');
    }
    return s;
}

// ---------------------------------------------------------------------------
// Start / Stop
// ---------------------------------------------------------------------------

void HyperliquidAdapter::start() {
    if (running_) return;
    running_ = true;
    logger_->info("[{}] starting ({} subscriptions, depth_levels={} via l2Book fast={})",
                  exchange_name_, subscriptions_.size(), depth_levels_,
                  fast_book_ ? "true" : "false");
    ws_client_->start();
}

void HyperliquidAdapter::stop() {
    running_ = false;
    if (ws_client_) ws_client_->stop();
}

// ---------------------------------------------------------------------------
// WebSocket callbacks
// ---------------------------------------------------------------------------

void HyperliquidAdapter::on_ws_message(std::string_view raw) {
    handle_message(raw);
}

void HyperliquidAdapter::on_ws_state_change(bool connected) {
    if (connected) {
        net::co_spawn(ioc_, subscribe_all(), net::detached);
    } else {
        tickers_.clear();
    }
}

// ---------------------------------------------------------------------------
// Subscribe — Hyperliquid takes one subscription per message
// ---------------------------------------------------------------------------

net::awaitable<void> HyperliquidAdapter::subscribe_all() {
    if (subscriptions_.empty() || !ws_client_->is_connected()) co_return;

    for (auto& sub : subscriptions_) {
        std::string msg = R"({"method":"subscribe","subscription":{"type":")";
        msg += sub.type;
        msg += R"(","coin":")";
        msg += sub.coin;
        msg += '"';
        if (sub.type == "l2Book" && fast_book_) msg += R"(,"fast":true)";
        else if (sub.type == "candle")          msg += R"(,"interval":"1m")";
        msg += "}}";

        co_await ws_client_->send(std::move(msg));
    }

    logger_->info("[{}] subscribed to {} channels", exchange_name_,
                  subscriptions_.size());
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

void HyperliquidAdapter::handle_message(std::string_view raw) {
    simdjson::padded_string padded(raw);
    auto doc_result = json_parser_.iterate(padded);
    if (doc_result.error()) {
        logger_->warn("[{}] JSON parse error", exchange_name_);
        return;
    }
    auto doc = std::move(doc_result).value();

    auto channel_field = doc["channel"];
    if (channel_field.error()) return;
    std::string channel(channel_field.get_string().value());

    if (channel == "pong") return;
    if (channel == "error") {
        auto data = doc["data"];
        logger_->error("[{}] server error: {}", exchange_name_,
                       data.error() ? "unknown" : data.get_string().value());
        return;
    }
    if (channel == "subscriptionResponse") return;

    auto data = doc["data"];
    if (data.error()) return;

    if (channel == "l2Book")      handle_l2_book(data.value());
    else if (channel == "bbo")    handle_bbo(data.value());
    else if (channel == "candle") handle_candle(data.value());
}

// ---------------------------------------------------------------------------
// Orderbook — every l2Book message is a complete book, so there is no local
// state to maintain and no sequence number to track.
// ---------------------------------------------------------------------------

void HyperliquidAdapter::handle_l2_book(simdjson::ondemand::value data) {
    auto obj = data.get_object().value();

    std::string coin(obj["coin"].get_string().value());
    int64_t     time = obj["time"].get_int64().value();

    OrderbookSnapshot snap;
    snap.symbol   = to_unified_symbol(coin);
    snap.exchange = exchange_name_;

    // levels[0] = bids (descending), levels[1] = asks (ascending)
    size_t side = 0;
    for (auto side_levels : obj["levels"].get_array()) {
        auto& out = (side == 0) ? snap.bids : snap.asks;
        for (auto level : side_levels.get_array().value()) {
            if (out.size() >= depth_levels_) break;
            auto lvl = level.get_object().value();
            double px = std::stod(std::string(lvl["px"].get_string().value()));
            double sz = std::stod(std::string(lvl["sz"].get_string().value()));
            out.push_back({px, sz});
        }
        if (++side == 2) break;
    }

    snap.sequence        = 0; // Hyperliquid does not publish one
    snap.timestamp       = time;
    snap.local_timestamp = now_ms();

    emit(DataType::Depth, coin, "snapshot", std::move(snap));
}

// ---------------------------------------------------------------------------
// Ticker (bbo channel) — either side is null when that side of the book is empty
// ---------------------------------------------------------------------------

void HyperliquidAdapter::handle_bbo(simdjson::ondemand::value data) {
    auto obj = data.get_object().value();

    std::string coin(obj["coin"].get_string().value());
    int64_t     time = obj["time"].get_int64().value();

    auto& tick    = tickers_[coin];
    tick.symbol   = to_unified_symbol(coin);
    tick.exchange = exchange_name_;

    size_t side = 0;
    for (auto level : obj["bbo"].get_array()) {
        if (!level.is_null()) {
            auto lvl = level.get_object().value();
            double px = std::stod(std::string(lvl["px"].get_string().value()));
            double sz = std::stod(std::string(lvl["sz"].get_string().value()));
            if (side == 0) { tick.best_bid_price = px; tick.best_bid_qty = sz; }
            else           { tick.best_ask_price = px; tick.best_ask_qty = sz; }
        }
        if (++side == 2) break;
    }

    tick.timestamp       = time;
    tick.local_timestamp = now_ms();

    emit(DataType::Ticker, coin, "update", tick);
}

// ---------------------------------------------------------------------------
// Kline (candle channel)
// ---------------------------------------------------------------------------

void HyperliquidAdapter::handle_candle(simdjson::ondemand::value data) {
    auto obj = data.get_object().value();

    int64_t open_time  = obj["t"].get_int64().value();
    int64_t close_time = obj["T"].get_int64().value();
    std::string coin(obj["s"].get_string().value());

    KlineData kline;
    kline.symbol       = to_unified_symbol(coin);
    kline.exchange     = exchange_name_;
    kline.interval     = std::string(obj["i"].get_string().value());
    kline.open_time    = open_time;
    kline.close_time   = close_time;
    kline.open         = std::stod(std::string(obj["o"].get_string().value()));
    kline.close        = std::stod(std::string(obj["c"].get_string().value()));
    kline.high         = std::stod(std::string(obj["h"].get_string().value()));
    kline.low          = std::stod(std::string(obj["l"].get_string().value()));
    kline.volume       = std::stod(std::string(obj["v"].get_string().value()));
    kline.quote_volume = 0.0; // not provided by Hyperliquid
    kline.num_trades   = obj["n"].get_int64().value();
    kline.is_closed    = (now_ms() > close_time);
    kline.timestamp    = now_ms();
    kline.local_timestamp = kline.timestamp;

    emit(DataType::Kline, coin, "update", std::move(kline));
}

} // namespace axon_market_data
