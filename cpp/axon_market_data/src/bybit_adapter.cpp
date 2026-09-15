#include "axon_market_data/bybit_adapter.hpp"

#include "axon_market_data/metrics.hpp"

#include <algorithm>

namespace axon_market_data {

// ---------------------------------------------------------------------------
// Market type mapping (Bybit V5 categories)
// ---------------------------------------------------------------------------

// Bybit only serves orderbook.1 / .50 / .200 / .1000, so round the requested
// depth up to the next available topic. Level 1 is reserved for the ticker.
static int bybit_topic_depth(size_t wanted) {
    if (wanted <= 50)  return 50;
    if (wanted <= 200) return 200;
    return 1000;
}

static const char* bybit_category(MarketType mt) {
    switch (mt) {
    case MarketType::UsdtFutures: return "linear";
    case MarketType::CoinFutures: return "inverse";
    default:                      return "spot";
    }
}

// ---------------------------------------------------------------------------
// Ctor / Dtor
// ---------------------------------------------------------------------------

BybitAdapter::BybitAdapter(net::io_context& ioc,
                           const ExchangeConfig& cfg,
                           EventCallback on_event,
                           std::shared_ptr<spdlog::logger> logger)
    : BaseAdapter("bybit_" + std::string(bybit_category(cfg.market_type)),
                  std::move(on_event), std::move(logger))
    , ioc_(ioc)
    , cfg_(cfg)
{
    const std::string category   = bybit_category(cfg_.market_type);
    book_depth_   = bybit_topic_depth(cfg_.depth_levels);
    depth_levels_ = std::min<size_t>(cfg_.depth_levels, book_depth_);

    auto add_symbols = [&](const std::vector<std::string>& syms, DataType dt,
                           const std::string& prefix) {
        for (auto& unified : syms) {
            auto exch = to_exchange_symbol(unified);
            sym_to_unified_[exch] = unified;
            declare_subscription(dt, exch);
            topics_.push_back(prefix + exch);
        }
    };
    add_symbols(cfg_.subscriptions.depth, DataType::Depth,
                "orderbook." + std::to_string(book_depth_) + ".");
    // Bybit's spot `tickers` topic carries no bid/ask, so the level-1 book is
    // the BBO source for every category.
    add_symbols(cfg_.subscriptions.ticker, DataType::Ticker, "orderbook.1.");
    add_symbols(cfg_.subscriptions.kline,  DataType::Kline,  "kline.1.");

    WebSocketClient::Config ws_cfg;
    ws_cfg.host          = "stream.bybit.com";
    ws_cfg.port          = "443";
    ws_cfg.path          = "/v5/public/" + category;
    ws_cfg.tag           = exchange_name_;
    ws_cfg.ping_interval = std::chrono::seconds(20); // Bybit requires 20s pings
    ws_cfg.ping_text     = R"({"op":"ping"})";

    ws_client_ = std::make_shared<WebSocketClient>(
        ioc_, std::move(ws_cfg),
        [this](std::string_view msg) { on_ws_message(msg); },
        [this](bool connected)       { on_ws_state_change(connected); },
        logger_);
}

BybitAdapter::~BybitAdapter() { stop(); }

// ---------------------------------------------------------------------------
// Symbol helpers
// ---------------------------------------------------------------------------

// ETH_USDT_SPOT / ETH_USDT_PERP -> ETHUSDT
std::string BybitAdapter::to_exchange_symbol(const std::string& unified) const {
    std::string s = unified;
    if (s.ends_with("_SPOT") || s.ends_with("_PERP"))
        s.resize(s.size() - 5);
    std::erase(s, '_');
    return s;
}

// ---------------------------------------------------------------------------
// Start / Stop
// ---------------------------------------------------------------------------

void BybitAdapter::start() {
    if (running_) return;
    running_ = true;
    logger_->info("[{}] starting ({} topics, depth_levels={} via orderbook.{})",
                  exchange_name_, topics_.size(), depth_levels_, book_depth_);
    ws_client_->start();
}

void BybitAdapter::stop() {
    running_ = false;
    if (ws_client_) ws_client_->stop();
}

// ---------------------------------------------------------------------------
// WebSocket callbacks
// ---------------------------------------------------------------------------

void BybitAdapter::on_ws_message(std::string_view raw) {
    handle_message(raw);
}

void BybitAdapter::on_ws_state_change(bool connected) {
    if (connected) {
        net::co_spawn(ioc_, subscribe_topics(), net::detached);
    } else {
        logger_->warn("[{}] disconnected, dropping local books", exchange_name_);
        for (size_t i = 0; i < books_.size(); ++i)
            get_metrics().inc_resync(exchange_name_, ResyncReason::Disconnect);
        books_.clear();
        tickers_.clear();
    }
}

// ---------------------------------------------------------------------------
// Subscribe (batched — Bybit caps the args array per message)
// ---------------------------------------------------------------------------

net::awaitable<void> BybitAdapter::subscribe_topics() {
    if (topics_.empty() || !ws_client_->is_connected()) co_return;

    for (size_t start = 0; start < topics_.size(); start += SUBSCRIBE_BATCH) {
        size_t end = std::min(start + SUBSCRIBE_BATCH, topics_.size());

        std::string msg = R"({"op":"subscribe","args":[)";
        for (size_t i = start; i < end; ++i) {
            if (i > start) msg += ',';
            msg += '"';
            msg += topics_[i];
            msg += '"';
        }
        msg += "]}";

        co_await ws_client_->send(std::move(msg));
    }

    logger_->info("[{}] subscribed to {} topics", exchange_name_, topics_.size());
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

void BybitAdapter::handle_message(std::string_view raw) {
    simdjson::padded_string padded(raw);
    auto doc_result = json_parser_.iterate(padded);
    if (doc_result.error()) {
        get_metrics().inc_parse_error(exchange_name_);
        logger_->warn("[{}] JSON parse error", exchange_name_);
        return;
    }
    auto doc = std::move(doc_result).value();

    auto topic_field = doc["topic"];
    if (topic_field.error()) {
        // Control response: subscribe ack / pong
        auto success = doc["success"];
        if (!success.error() && !success.get_bool().value()) {
            auto msg = doc["ret_msg"];
            logger_->error("[{}] subscription failed: {}", exchange_name_,
                           msg.error() ? "unknown" : msg.get_string().value());
        }
        return;
    }

    // Topic layout: orderbook.{depth}.{symbol} or kline.{interval}.{symbol}
    std::string topic(topic_field.get_string().value());
    auto first = topic.find('.');
    if (first == std::string::npos) return;
    auto second = topic.find('.', first + 1);
    if (second == std::string::npos) return;

    std::string kind   = topic.substr(0, first);
    std::string middle = topic.substr(first + 1, second - first - 1);
    std::string symbol = topic.substr(second + 1);

    if (kind == "orderbook")
        handle_orderbook(doc, symbol, middle == "1");
    else if (kind == "kline")
        handle_kline(doc, symbol);
}

// ---------------------------------------------------------------------------
// Orderbook — depth (BOOK_DEPTH levels) and BBO (level 1, emitted as a ticker)
// ---------------------------------------------------------------------------

void BybitAdapter::handle_orderbook(simdjson::ondemand::document& doc,
                                    const std::string& symbol, bool is_bbo) {
    auto type_field = doc["type"];
    if (type_field.error()) return;
    std::string type(type_field.get_string().value());
    const bool is_snapshot = (type == "snapshot");

    int64_t event_time = 0;
    auto ts_field = doc["ts"];
    if (!ts_field.error()) event_time = ts_field.get_int64().value();

    auto data = doc["data"].get_object();
    if (data.error()) return;
    auto obj = data.value();

    std::vector<std::pair<double, double>> bid_updates, ask_updates;
    for (auto level : obj["b"].get_array().value()) {
        auto arr = level.get_array().value();
        auto it  = arr.begin();
        double price = std::stod(std::string((*it).get_string().value()));
        ++it;
        double qty   = std::stod(std::string((*it).get_string().value()));
        bid_updates.emplace_back(price, qty);
    }
    for (auto level : obj["a"].get_array().value()) {
        auto arr = level.get_array().value();
        auto it  = arr.begin();
        double price = std::stod(std::string((*it).get_string().value()));
        ++it;
        double qty   = std::stod(std::string((*it).get_string().value()));
        ask_updates.emplace_back(price, qty);
    }

    int64_t update_id = 0;
    auto u_field = obj["u"];
    if (!u_field.error()) update_id = u_field.get_int64().value();

    // The level-1 stream feeds TickerData: a delta omits the side that did not
    // change, so the last known best price/qty is carried forward.
    if (is_bbo) {
        auto& tick = tickers_[symbol];
        if (is_snapshot) tick = TickerData{};
        tick.symbol   = to_unified_symbol(symbol);
        tick.exchange = exchange_name_;
        if (!bid_updates.empty()) {
            tick.best_bid_price = bid_updates.front().first;
            tick.best_bid_qty   = bid_updates.front().second;
        }
        if (!ask_updates.empty()) {
            tick.best_ask_price = ask_updates.front().first;
            tick.best_ask_qty   = ask_updates.front().second;
        }
        tick.timestamp       = event_time;
        tick.local_timestamp = now_ms();

        emit(DataType::Ticker, symbol, "update", tick);
        return;
    }

    if (is_snapshot) {
        Orderbook ob;
        ob.symbol   = to_unified_symbol(symbol);
        ob.exchange = exchange_name_;
        for (auto& [px, qty] : bid_updates) ob.apply_bid(px, qty);
        for (auto& [px, qty] : ask_updates) ob.apply_ask(px, qty);
        ob.sequence        = update_id;
        ob.timestamp       = event_time;
        ob.local_timestamp = now_ms();
        books_[symbol] = std::move(ob);
    } else {
        // u == 1 means Bybit restarted the stream; a fresh snapshot follows.
        if (update_id == 1) {
            get_metrics().inc_resync(exchange_name_, ResyncReason::StreamRestart);
            logger_->warn("[{}] {} stream restarted, waiting for snapshot",
                          exchange_name_, symbol);
            books_.erase(symbol);
            return;
        }
        auto it = books_.find(symbol);
        if (it == books_.end()) return; // delta before the first snapshot
        auto& ob = it->second;
        for (auto& [px, qty] : bid_updates) ob.apply_bid(px, qty);
        for (auto& [px, qty] : ask_updates) ob.apply_ask(px, qty);
        ob.sequence        = update_id;
        ob.timestamp       = event_time;
        ob.local_timestamp = now_ms();
    }

    emit(DataType::Depth, symbol, is_snapshot ? "snapshot" : "update",
         books_[symbol].snapshot(depth_levels_));
}

// ---------------------------------------------------------------------------
// Kline
// ---------------------------------------------------------------------------

void BybitAdapter::handle_kline(simdjson::ondemand::document& doc,
                                const std::string& symbol) {
    auto data = doc["data"].get_array();
    if (data.error()) return;

    for (auto entry : data.value()) {
        auto obj = entry.get_object().value();

        KlineData kline;
        kline.symbol     = to_unified_symbol(symbol);
        kline.exchange   = exchange_name_;
        kline.interval   = "1m";
        kline.open_time  = obj["start"].get_int64().value();
        kline.close_time = obj["end"].get_int64().value();
        kline.open       = std::stod(std::string(obj["open"].get_string().value()));
        kline.high       = std::stod(std::string(obj["high"].get_string().value()));
        kline.low        = std::stod(std::string(obj["low"].get_string().value()));
        kline.close      = std::stod(std::string(obj["close"].get_string().value()));
        kline.volume     = std::stod(std::string(obj["volume"].get_string().value()));
        kline.quote_volume = std::stod(std::string(obj["turnover"].get_string().value()));
        kline.num_trades   = 0; // not provided by Bybit
        kline.is_closed    = obj["confirm"].get_bool().value();
        kline.timestamp    = obj["timestamp"].get_int64().value();
        kline.local_timestamp = now_ms();

        emit(DataType::Kline, symbol, "update", std::move(kline));
    }
}

} // namespace axon_market_data
