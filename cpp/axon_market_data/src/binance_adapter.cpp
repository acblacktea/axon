#include "axon_market_data/binance_adapter.hpp"

#include <algorithm>

namespace axon_market_data {

namespace beast = boost::beast;

// ---------------------------------------------------------------------------
// Endpoint tables
// ---------------------------------------------------------------------------

struct Endpoints {
    const char* ws_host;
    const char* ws_port;
    const char* rest_host;
    const char* depth_path;
};

// The REST snapshot only seeds the book; the diff stream then reports just the
// levels that change, so a level deeper than the snapshot stays invisible until
// it moves. Seeding at exactly `depth_levels` therefore lets the published book
// erode below it, and the snapshot is a one-off per sync — so ask for double
// the headroom, capped at what the venue allows.
//
// Spot accepts any limit up to 5000; the futures endpoints reject anything
// outside their fixed ladder (limit=400 -> "-4021 400 is not valid depth limit").
static int rest_depth_limit_for(MarketType mt, size_t wanted) {
    const size_t with_headroom = wanted * 2;

    if (mt == MarketType::Spot)
        return static_cast<int>(std::clamp<size_t>(with_headroom, 5, 5000));

    for (int allowed : {5, 10, 20, 50, 100, 500, 1000})
        if (with_headroom <= static_cast<size_t>(allowed)) return allowed;
    return 1000;
}

static const Endpoints& endpoints_for(MarketType mt) {
    static const Endpoints spot         {"stream.binance.com", "9443", "api.binance.com",  "/api/v3/depth"};
    static const Endpoints usdt_futures {"fstream.binance.com","443",  "fapi.binance.com", "/fapi/v1/depth"};
    static const Endpoints coin_futures {"dstream.binance.com","443",  "dapi.binance.com", "/dapi/v1/depth"};
    switch (mt) {
    case MarketType::UsdtFutures: return usdt_futures;
    case MarketType::CoinFutures: return coin_futures;
    default:                      return spot;
    }
}

// ---------------------------------------------------------------------------
// Ctor / Dtor
// ---------------------------------------------------------------------------

BinanceAdapter::BinanceAdapter(net::io_context& ioc,
                               const ExchangeConfig& cfg,
                               EventCallback on_event,
                               std::shared_ptr<spdlog::logger> logger)
    : BaseAdapter("binance_" + std::string(to_string(cfg.market_type)),
                  std::move(on_event), std::move(logger))
    , ioc_(ioc)
    , cfg_(cfg)
{
    depth_levels_     = cfg_.depth_levels;
    rest_depth_limit_ = rest_depth_limit_for(cfg_.market_type, cfg_.depth_levels);

    auto& ep    = endpoints_for(cfg_.market_type);
    rest_host_  = ep.rest_host;
    depth_path_ = ep.depth_path;

    // Build stream list and symbol mappings
    auto add_symbols = [&](const std::vector<std::string>& syms, DataType dt) {
        for (auto& unified : syms) {
            auto exch = to_exchange_symbol(unified);
            sym_to_unified_[exch]    = unified;
            sym_to_exchange_[unified] = exch;
            all_streams_.push_back(stream_name(exch, dt));
            if (dt == DataType::Depth)
                depth_exchange_symbols_.insert(exch);
        }
    };
    add_symbols(cfg_.subscriptions.depth,  DataType::Depth);
    add_symbols(cfg_.subscriptions.ticker, DataType::Ticker);
    add_symbols(cfg_.subscriptions.kline,  DataType::Kline);

    // Create WebSocket client
    WebSocketClient::Config ws_cfg;
    ws_cfg.host = ep.ws_host;
    ws_cfg.port = ep.ws_port;
    ws_cfg.path = "/ws";
    ws_cfg.tag  = exchange_name_;
    ws_cfg.ping_interval = std::chrono::seconds(20);

    ws_client_ = std::make_shared<WebSocketClient>(
        ioc_, std::move(ws_cfg),
        [this](std::string_view msg) { on_ws_message(msg); },
        [this](bool connected)       { on_ws_state_change(connected); },
        logger_);
}

BinanceAdapter::~BinanceAdapter() { stop(); }

// ---------------------------------------------------------------------------
// Symbol helpers
// ---------------------------------------------------------------------------

std::string BinanceAdapter::to_exchange_symbol(const std::string& unified) const {
    std::string s = unified;
    for (auto suffix : {"_SPOT", "_PERP"}) {
        if (s.size() > 5 && s.ends_with(suffix)) {
            s.resize(s.size() - std::strlen(suffix));
            break;
        }
    }
    std::erase(s, '_');
    return s;
}

std::string BinanceAdapter::stream_name(const std::string& exch, DataType dt) const {
    std::string lower;
    lower.reserve(exch.size());
    for (char c : exch) lower += static_cast<char>(std::tolower(c));

    switch (dt) {
    case DataType::Depth:  return lower + "@depth@" + cfg_.update_speed;
    case DataType::Ticker: return lower + "@bookTicker";
    case DataType::Kline:  return lower + "@kline_1m";
    }
    return lower;
}

// ---------------------------------------------------------------------------
// Start / Stop
// ---------------------------------------------------------------------------

void BinanceAdapter::start() {
    if (running_) return;
    running_ = true;
    logger_->info("[{}] starting ({} streams, depth_levels={} via REST limit={})",
                  exchange_name_, all_streams_.size(), depth_levels_, rest_depth_limit_);
    ws_client_->start();
}

void BinanceAdapter::stop() {
    running_ = false;
    if (ws_client_) ws_client_->stop();
}

// ---------------------------------------------------------------------------
// WebSocket callbacks
// ---------------------------------------------------------------------------

void BinanceAdapter::on_ws_message(std::string_view raw) {
    handle_message(raw);
}

void BinanceAdapter::on_ws_state_change(bool connected) {
    if (connected) {
        // (Re)subscribe after connect
        net::co_spawn(ioc_, subscribe_streams(), net::detached);
    } else {
        logger_->warn("[{}] disconnected, clearing sync state", exchange_name_);
        syncing_.clear();
        awaiting_first_.clear();
        event_buffers_.clear();
    }
}

// ---------------------------------------------------------------------------
// Subscribe to streams (called after each connect/reconnect)
// ---------------------------------------------------------------------------

net::awaitable<void> BinanceAdapter::subscribe_streams() {
    if (all_streams_.empty() || !ws_client_->is_connected()) co_return;

    ++request_id_;
    std::string msg = R"({"method":"SUBSCRIBE","params":[)";
    for (size_t i = 0; i < all_streams_.size(); ++i) {
        if (i > 0) msg += ',';
        msg += '"';
        msg += all_streams_[i];
        msg += '"';
    }
    msg += R"(],"id":)";
    msg += std::to_string(request_id_);
    msg += '}';

    co_await ws_client_->send(std::move(msg));
    logger_->info("[{}] subscribed to {} streams", exchange_name_, all_streams_.size());

    // Sync orderbooks
    for (auto& sym : depth_exchange_symbols_)
        schedule_sync(sym);
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

void BinanceAdapter::handle_message(std::string_view raw) {
    simdjson::padded_string padded(raw);
    auto doc_result = json_parser_.iterate(padded);
    if (doc_result.error()) {
        logger_->warn("[{}] JSON parse error", exchange_name_);
        return;
    }
    auto doc = std::move(doc_result).value();

    // Skip subscription confirmations
    auto result_field = doc["result"];
    if (!result_field.error()) return;

    // Check event type field
    auto e_field = doc["e"];
    if (!e_field.error()) {
        auto etype = e_field.get_string().value();
        if (etype == "depthUpdate") {
            handle_depth_update(doc);
            return;
        }
        if (etype == "kline") {
            handle_kline(doc);
            return;
        }
    }

    // bookTicker has no "e" field
    auto u_field = doc["u"];
    auto b_field = doc["b"];
    if (!u_field.error() && !b_field.error()) {
        handle_book_ticker(doc);
    }
}

// ---------------------------------------------------------------------------
// Depth update
// ---------------------------------------------------------------------------

void BinanceAdapter::handle_depth_update(simdjson::ondemand::document& doc) {
    auto symbol_sv = doc["s"].get_string().value();
    std::string symbol(symbol_sv);

    int64_t first_update_id = doc["U"].get_int64().value();
    int64_t final_update_id = doc["u"].get_int64().value();
    int64_t event_time      = doc["E"].get_int64().value();

    // If syncing, buffer raw JSON for replay
    if (syncing_.contains(symbol)) {
        // Re-read raw from the WebSocket client's read buffer
        auto& buf = ws_client_->read_buffer();
        auto  ptr = static_cast<const char*>(buf.data().data());
        auto  len = beast::buffer_bytes(buf.data());
        event_buffers_[symbol].emplace_back(ptr, len);
        return;
    }

    // Sequence validation
    auto last_it = last_update_ids_.find(symbol);
    if (last_it != last_update_ids_.end()) {
        int64_t last_id = last_it->second;

        if (awaiting_first_.contains(symbol)) {
            // A REST snapshot id is not part of the diff chain, so neither "pu"
            // nor "U" can chain onto it. The first event after a snapshot has to
            // straddle it instead: U <= target <= u.
            int64_t target = join_target(last_id);
            if (final_update_id < target) return; // wholly older than the snapshot
            if (first_update_id > target) {
                logger_->warn("[{}] {} snapshot too old: U={} > target={}",
                              exchange_name_, symbol, first_update_id, target);
                schedule_sync(symbol);
                return;
            }
            awaiting_first_.erase(symbol);
        } else {
            auto pu_field = doc["pu"];
            if (!pu_field.error()) {
                int64_t prev_id = pu_field.get_int64().value();
                if (prev_id != last_id) {
                    logger_->warn("[{}] {} sequence gap: pu={} expected={}",
                                  exchange_name_, symbol, prev_id, last_id);
                    schedule_sync(symbol);
                    return;
                }
            } else {
                if (first_update_id > last_id + 1) {
                    logger_->warn("[{}] {} sequence gap: U={} expected<={}",
                                  exchange_name_, symbol, first_update_id, last_id + 1);
                    schedule_sync(symbol);
                    return;
                }
            }
        }
    }

    // Parse bids / asks
    auto& ob = orderbooks_[symbol];
    if (ob.symbol.empty()) {
        ob.symbol   = to_unified_symbol(symbol);
        ob.exchange = exchange_name_;
    }

    for (auto arr : doc["b"].get_array()) {
        auto a  = arr.get_array().value();
        auto it = a.begin();
        double price = std::stod(std::string((*it).get_string().value()));
        ++it;
        double qty   = std::stod(std::string((*it).get_string().value()));
        ob.apply_bid(price, qty);
    }
    for (auto arr : doc["a"].get_array()) {
        auto a  = arr.get_array().value();
        auto it = a.begin();
        double price = std::stod(std::string((*it).get_string().value()));
        ++it;
        double qty   = std::stod(std::string((*it).get_string().value()));
        ob.apply_ask(price, qty);
    }

    ob.sequence        = final_update_id;
    ob.timestamp       = event_time;
    ob.local_timestamp = now_ms();

    last_update_ids_[symbol] = final_update_id;

    emit(DataType::Depth, symbol, "update", ob.snapshot(depth_levels_));
}

// ---------------------------------------------------------------------------
// BookTicker
// ---------------------------------------------------------------------------

void BinanceAdapter::handle_book_ticker(simdjson::ondemand::document& doc) {
    auto symbol_sv = doc["s"].get_string().value();
    std::string symbol(symbol_sv);

    TickerData tick;
    tick.symbol          = to_unified_symbol(symbol);
    tick.exchange        = exchange_name_;
    tick.best_bid_price  = std::stod(std::string(doc["b"].get_string().value()));
    tick.best_bid_qty    = std::stod(std::string(doc["B"].get_string().value()));
    tick.best_ask_price  = std::stod(std::string(doc["a"].get_string().value()));
    tick.best_ask_qty    = std::stod(std::string(doc["A"].get_string().value()));
    tick.timestamp       = now_ms();
    tick.local_timestamp = tick.timestamp;

    emit(DataType::Ticker, symbol, "update", std::move(tick));
}

// ---------------------------------------------------------------------------
// Kline
// ---------------------------------------------------------------------------

void BinanceAdapter::handle_kline(simdjson::ondemand::document& doc) {
    auto symbol_sv = doc["s"].get_string().value();
    std::string symbol(symbol_sv);
    int64_t event_time = doc["E"].get_int64().value();

    auto k = doc["k"].get_object().value();

    KlineData kline;
    kline.symbol          = to_unified_symbol(symbol);
    kline.exchange        = exchange_name_;
    kline.interval        = std::string(k["i"].get_string().value());
    kline.open_time       = k["t"].get_int64().value();
    kline.close_time      = k["T"].get_int64().value();
    kline.open            = std::stod(std::string(k["o"].get_string().value()));
    kline.close           = std::stod(std::string(k["c"].get_string().value()));
    kline.high            = std::stod(std::string(k["h"].get_string().value()));
    kline.low             = std::stod(std::string(k["l"].get_string().value()));
    kline.volume          = std::stod(std::string(k["v"].get_string().value()));
    kline.quote_volume    = std::stod(std::string(k["q"].get_string().value()));
    kline.num_trades      = k["n"].get_int64().value();
    kline.is_closed       = k["x"].get_bool().value();
    kline.timestamp       = event_time;
    kline.local_timestamp = now_ms();

    emit(DataType::Kline, symbol, "update", std::move(kline));
}

// ---------------------------------------------------------------------------
// Sync orderbook via REST snapshot + buffered events
// ---------------------------------------------------------------------------

void BinanceAdapter::schedule_sync(std::string exchange_symbol) {
    syncing_.insert(exchange_symbol);
    awaiting_first_.erase(exchange_symbol);
    event_buffers_[exchange_symbol] = {};
    net::co_spawn(ioc_, sync_orderbook(std::move(exchange_symbol)), net::detached);
}

net::awaitable<void> BinanceAdapter::retry_sync(std::string exchange_symbol) {
    net::steady_timer timer(ioc_);
    timer.expires_after(std::chrono::seconds(1));
    co_await timer.async_wait(net::use_awaitable);
    if (running_) schedule_sync(std::move(exchange_symbol));
}

net::awaitable<void> BinanceAdapter::sync_orderbook(std::string exchange_symbol) {
    try {
        logger_->info("[{}] syncing orderbook for {}", exchange_name_, exchange_symbol);

        std::string target = depth_path_;
        target += "?symbol=";
        target += exchange_symbol;
        target += "&limit=";
        target += std::to_string(rest_depth_limit_);

        auto resp = co_await axon_market_data::http_get(rest_host_, target);
        auto& body = resp.body;

        simdjson::padded_string padded(body);
        simdjson::ondemand::parser parser;
        auto doc = parser.iterate(padded).value();

        int64_t snapshot_update_id = doc["lastUpdateId"].get_int64().value();

        Orderbook ob;
        ob.symbol   = to_unified_symbol(exchange_symbol);
        ob.exchange = exchange_name_;

        for (auto bid : doc["bids"].get_array()) {
            auto arr = bid.get_array().value();
            auto it  = arr.begin();
            double price = std::stod(std::string((*it).get_string().value()));
            ++it;
            double qty   = std::stod(std::string((*it).get_string().value()));
            ob.apply_bid(price, qty);
        }
        for (auto ask : doc["asks"].get_array()) {
            auto arr = ask.get_array().value();
            auto it  = arr.begin();
            double price = std::stod(std::string((*it).get_string().value()));
            ++it;
            double qty   = std::stod(std::string((*it).get_string().value()));
            ob.apply_ask(price, qty);
        }

        ob.sequence        = snapshot_update_id;
        ob.timestamp       = now_ms();
        ob.local_timestamp = ob.timestamp;

        // Replay buffered events
        auto buffered = std::move(event_buffers_[exchange_symbol]);
        event_buffers_.erase(exchange_symbol);

        orderbooks_[exchange_symbol] = std::move(ob);
        last_update_ids_[exchange_symbol] = snapshot_update_id;

        // Binance's own recipe, which differs by market:
        //   spot    – drop u <= lastUpdateId, first kept event needs
        //             U <= lastUpdateId+1 <= u, then U == previous u + 1
        //   futures – drop u <  lastUpdateId, first kept event needs
        //             U <= lastUpdateId <= u, then pu == previous u
        // Anything that breaks the chain means the snapshot and the buffer do
        // not overlap; silently skipping such an event would leave a hole.
        const int64_t join_id    = join_target(snapshot_update_id);
        int64_t       chained_id = snapshot_update_id;
        bool          chained    = false;
        bool          broken     = false;

        for (auto& raw_json : buffered) {
            simdjson::padded_string buf_padded(raw_json);
            auto buf_doc = parser.iterate(buf_padded).value();

            auto e_field = buf_doc["e"];
            if (e_field.error()) continue;
            if (e_field.get_string().value() != "depthUpdate") continue;
            if (buf_doc["s"].get_string().value() != exchange_symbol) continue;

            int64_t first_id = buf_doc["U"].get_int64().value();
            int64_t final_id = buf_doc["u"].get_int64().value();
            int64_t prev_id  = -1;
            auto    pu_field = buf_doc["pu"];
            if (!pu_field.error()) prev_id = pu_field.get_int64().value();

            if (final_id < join_id) continue; // wholly older than the snapshot

            if (!chained) {
                if (first_id > join_id) {
                    logger_->warn("[{}] {} buffer starts past the snapshot "
                                  "(U={} > join={}), resyncing",
                                  exchange_name_, exchange_symbol, first_id, join_id);
                    broken = true;
                    break;
                }
            } else {
                bool contiguous = is_futures() ? (prev_id == chained_id)
                                               : (first_id == chained_id + 1);
                if (!contiguous) {
                    logger_->warn("[{}] {} buffered events not contiguous "
                                  "(U={} pu={} after u={}), resyncing",
                                  exchange_name_, exchange_symbol,
                                  first_id, prev_id, chained_id);
                    broken = true;
                    break;
                }
            }

            auto& local_ob = orderbooks_[exchange_symbol];
            for (auto arr : buf_doc["b"].get_array()) {
                auto a = arr.get_array().value();
                auto it = a.begin();
                double price = std::stod(std::string((*it).get_string().value()));
                ++it;
                double qty   = std::stod(std::string((*it).get_string().value()));
                local_ob.apply_bid(price, qty);
            }
            for (auto arr : buf_doc["a"].get_array()) {
                auto a = arr.get_array().value();
                auto it = a.begin();
                double price = std::stod(std::string((*it).get_string().value()));
                ++it;
                double qty   = std::stod(std::string((*it).get_string().value()));
                local_ob.apply_ask(price, qty);
            }
            local_ob.sequence = final_id;
            chained    = true;
            chained_id = final_id;
        }

        if (broken) {
            // Leave `syncing_` set so live events keep buffering harmlessly;
            // schedule_sync() resets the buffer when the retry fires.
            orderbooks_.erase(exchange_symbol);
            last_update_ids_.erase(exchange_symbol);
            if (running_)
                net::co_spawn(ioc_, retry_sync(exchange_symbol), net::detached);
            co_return;
        }

        last_update_ids_[exchange_symbol] = chained_id;
        // Nothing bridged the snapshot, so the next live event still has to
        // join it rather than chain onto it.
        if (!chained) awaiting_first_.insert(exchange_symbol);

        syncing_.erase(exchange_symbol);

        auto& synced_ob = orderbooks_[exchange_symbol];
        emit(DataType::Depth, exchange_symbol, "snapshot",
             synced_ob.snapshot(depth_levels_));

        logger_->info("[{}] orderbook synced for {} (seq={})",
                      exchange_name_, exchange_symbol, synced_ob.sequence);

    } catch (const std::exception& e) {
        logger_->error("[{}] sync_orderbook {} failed: {}",
                       exchange_name_, exchange_symbol, e.what());
        syncing_.erase(exchange_symbol);

        if (running_)
            net::co_spawn(ioc_, retry_sync(exchange_symbol), net::detached);
    }
}

} // namespace axon_market_data
