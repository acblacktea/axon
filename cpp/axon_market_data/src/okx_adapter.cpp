#include "axon_market_data/okx_adapter.hpp"

#include <algorithm>
#include <array>

namespace axon_market_data {

// ---------------------------------------------------------------------------
// CRC32 (IEEE) — OKX compares the result as a signed 32-bit integer
// ---------------------------------------------------------------------------

static uint32_t crc32_of(std::string_view s) {
    static const std::array<uint32_t, 256> table = [] {
        std::array<uint32_t, 256> t{};
        for (uint32_t i = 0; i < 256; ++i) {
            uint32_t c = i;
            for (int k = 0; k < 8; ++k)
                c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
            t[i] = c;
        }
        return t;
    }();

    uint32_t crc = 0xFFFFFFFFu;
    for (unsigned char ch : s)
        crc = table[(crc ^ ch) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFu;
}

// ---------------------------------------------------------------------------
// Ctor / Dtor
// ---------------------------------------------------------------------------

OkxAdapter::OkxAdapter(net::io_context& ioc,
                       const ExchangeConfig& cfg,
                       EventCallback on_event,
                       std::shared_ptr<spdlog::logger> logger)
    : BaseAdapter("okx", std::move(on_event), std::move(logger))
    , ioc_(ioc)
    , cfg_(cfg)
{
    // A shallow request gets the cheap channel: books5 carries 5 levels per
    // message instead of 400, which is two orders of magnitude less traffic.
    book_channel_ = (cfg_.depth_levels <= BOOKS5_LEVELS) ? "books5" : "books";
    depth_levels_ = std::min<size_t>(cfg_.depth_levels,
                                     (book_channel_ == std::string("books5")) ? BOOKS5_LEVELS : 400);

    // OKX exposes every instrument type on one endpoint; the instId carries
    // the market type, so there is no per-market host to pick.
    auto add_symbols = [&](const std::vector<std::string>& syms,
                           const char* channel, std::vector<Channel>& out) {
        for (auto& unified : syms) {
            auto inst = to_exchange_symbol(unified);
            sym_to_unified_[inst] = unified;
            out.push_back({channel, inst});
        }
    };
    add_symbols(cfg_.subscriptions.depth,  book_channel_, public_channels_);
    add_symbols(cfg_.subscriptions.ticker, "bbo-tbt",  public_channels_);
    add_symbols(cfg_.subscriptions.kline,  "candle1m", business_channels_);

    if (!public_channels_.empty()) {
        ws_public_ = make_client("/ws/v5/public", exchange_name_,
                                 [this](bool c) { on_public_state_change(c); });
    }
    if (!business_channels_.empty()) {
        ws_business_ = make_client("/ws/v5/business", exchange_name_ + "_business",
                                   [this](bool c) { on_business_state_change(c); });
    }
}

std::shared_ptr<WebSocketClient> OkxAdapter::make_client(
    const std::string& path, const std::string& tag,
    WebSocketClient::StateCallback on_state) {
    WebSocketClient::Config ws_cfg;
    ws_cfg.host          = "ws.okx.com";
    ws_cfg.port          = "8443";
    ws_cfg.path          = path;
    ws_cfg.tag           = tag;
    ws_cfg.ping_interval = std::chrono::seconds(20); // OKX cuts idle links at 30s
    ws_cfg.ping_text     = "ping";

    return std::make_shared<WebSocketClient>(
        ioc_, std::move(ws_cfg),
        [this](std::string_view msg) { on_ws_message(msg); },
        std::move(on_state),
        logger_);
}

OkxAdapter::~OkxAdapter() { stop(); }

// ---------------------------------------------------------------------------
// Symbol helpers
// ---------------------------------------------------------------------------

// ETH_USDT_SPOT -> ETH-USDT, ETH_USDT_PERP -> ETH-USDT-SWAP
std::string OkxAdapter::to_exchange_symbol(const std::string& unified) const {
    std::string s = unified;
    bool is_perp  = false;

    if (s.ends_with("_SPOT")) {
        s.resize(s.size() - 5);
    } else if (s.ends_with("_PERP")) {
        s.resize(s.size() - 5);
        is_perp = true;
    }

    std::replace(s.begin(), s.end(), '_', '-');
    if (is_perp) s += "-SWAP";
    return s;
}

// ---------------------------------------------------------------------------
// Start / Stop
// ---------------------------------------------------------------------------

void OkxAdapter::start() {
    if (running_) return;
    running_ = true;
    logger_->info("[{}] starting ({} public + {} business channels, "
                  "depth_levels={} via channel \"{}\")",
                  exchange_name_, public_channels_.size(), business_channels_.size(),
                  depth_levels_, book_channel_);
    if (ws_public_)   ws_public_->start();
    if (ws_business_) ws_business_->start();
}

void OkxAdapter::stop() {
    running_ = false;
    if (ws_public_)   ws_public_->stop();
    if (ws_business_) ws_business_->stop();
}

// ---------------------------------------------------------------------------
// WebSocket callbacks
// ---------------------------------------------------------------------------

void OkxAdapter::on_ws_message(std::string_view raw) {
    handle_message(raw);
}

void OkxAdapter::on_public_state_change(bool connected) {
    if (connected) {
        net::co_spawn(ioc_, subscribe(ws_public_, public_channels_), net::detached);
    } else {
        logger_->warn("[{}] disconnected, dropping local books", exchange_name_);
        books_.clear();
    }
}

void OkxAdapter::on_business_state_change(bool connected) {
    if (connected)
        net::co_spawn(ioc_, subscribe(ws_business_, business_channels_), net::detached);
}

// ---------------------------------------------------------------------------
// Subscribe
// ---------------------------------------------------------------------------

net::awaitable<void> OkxAdapter::subscribe(std::shared_ptr<WebSocketClient> client,
                                           const std::vector<Channel>& channels) {
    if (!client || channels.empty() || !client->is_connected()) co_return;

    std::string msg = R"({"op":"subscribe","args":[)";
    for (size_t i = 0; i < channels.size(); ++i) {
        if (i > 0) msg += ',';
        msg += R"({"channel":")";
        msg += channels[i].channel;
        msg += R"(","instId":")";
        msg += channels[i].inst_id;
        msg += R"("})";
    }
    msg += "]}";

    co_await client->send(std::move(msg));
    logger_->info("[{}] subscribed to {} channels", exchange_name_, channels.size());
}

// OKX never re-pushes a snapshot on its own; re-subscribing the books channel
// is the documented way to recover from a sequence gap.
net::awaitable<void> OkxAdapter::resubscribe_book(std::string inst_id) {
    if (!ws_public_ || !ws_public_->is_connected()) co_return;

    std::string args = R"([{"channel":")" + std::string(book_channel_) +
                       R"(","instId":")" + inst_id + R"("}])";
    co_await ws_public_->send(R"({"op":"unsubscribe","args":)" + args + "}");
    co_await ws_public_->send(R"({"op":"subscribe","args":)" + args + "}");
    logger_->info("[{}] re-subscribed books for {}", exchange_name_, inst_id);
}

// ---------------------------------------------------------------------------
// Message dispatch
// ---------------------------------------------------------------------------

void OkxAdapter::handle_message(std::string_view raw) {
    if (raw == "pong") return;

    simdjson::padded_string padded(raw);
    auto doc_result = json_parser_.iterate(padded);
    if (doc_result.error()) {
        logger_->warn("[{}] JSON parse error", exchange_name_);
        return;
    }
    auto doc = std::move(doc_result).value();

    // Control frames: {"event":"subscribe"|"unsubscribe"|"error", ...}
    auto event_field = doc["event"];
    if (!event_field.error()) {
        auto event = event_field.get_string().value();
        if (event == "error") {
            auto msg = doc["msg"];
            logger_->error("[{}] subscription error: {}", exchange_name_,
                           msg.error() ? "unknown" : msg.get_string().value());
        }
        return;
    }

    auto arg = doc["arg"];
    if (arg.error()) return;

    std::string channel(arg["channel"].get_string().value());
    std::string inst_id(arg["instId"].get_string().value());

    if (channel == "books")         handle_books(doc, inst_id, false);
    else if (channel == "books5")   handle_books(doc, inst_id, true);
    else if (channel == "bbo-tbt")  handle_bbo(doc, inst_id);
    else if (channel.starts_with("candle")) handle_candle(doc, inst_id);
}

// ---------------------------------------------------------------------------
// Orderbook (books channel: one snapshot then incremental updates)
// ---------------------------------------------------------------------------

void OkxAdapter::apply_levels(Orderbook& book, simdjson::ondemand::array levels,
                              bool is_bid) {
    for (auto level : levels) {
        auto arr = level.get_array().value();
        auto it  = arr.begin();

        std::string price_str((*it).get_string().value());
        ++it;
        std::string qty_str((*it).get_string().value());

        double price = std::stod(price_str);
        double qty   = std::stod(qty_str);

        // The checksum consumes "<price>:<qty>" pairs, so store that exact text.
        std::string raw;
        if (qty != 0.0 && cfg_.verify_checksum) {
            raw.reserve(price_str.size() + 1 + qty_str.size());
            raw += price_str;
            raw += ':';
            raw += qty_str;
        }

        if (is_bid) book.apply_bid(price, qty, std::move(raw));
        else        book.apply_ask(price, qty, std::move(raw));
    }
}

void OkxAdapter::handle_books(simdjson::ondemand::document& doc,
                              const std::string& inst_id, bool snapshot_only) {
    std::string action = "snapshot";
    if (!snapshot_only) {
        auto action_field = doc["action"];
        if (action_field.error()) return;
        action = std::string(action_field.get_string().value());
    }

    auto data = doc["data"].get_array();
    if (data.error()) return;

    for (auto entry : data.value()) {
        auto obj = entry.get_object().value();

        Orderbook  fresh;
        Orderbook& book = (action == "snapshot") ? fresh : books_[inst_id];

        if (action != "snapshot" && book.empty()) {
            // Update before the first snapshot — nothing to apply it to.
            return;
        }

        int64_t seq_id      = 0;
        int64_t prev_seq_id = -1;
        int64_t timestamp   = 0;
        int64_t checksum    = 0;
        bool    has_checksum = false;

        // Field order in OKX payloads: asks, bids, ts, checksum, prevSeqId, seqId
        apply_levels(book, obj["asks"].get_array().value(), false);
        apply_levels(book, obj["bids"].get_array().value(), true);

        timestamp = std::stoll(std::string(obj["ts"].get_string().value()));

        auto checksum_field = snapshot_only ? simdjson::simdjson_result<simdjson::ondemand::value>{simdjson::NO_SUCH_FIELD}
                                            : obj["checksum"];
        if (!checksum_field.error() &&
            checksum_field.type() == simdjson::ondemand::json_type::number) {
            checksum = checksum_field.get_int64().value();
            // OKX leaves this at 0 when it does not compute a checksum.
            has_checksum = (checksum != 0);
        }
        auto prev_field = snapshot_only ? simdjson::simdjson_result<simdjson::ondemand::value>{simdjson::NO_SUCH_FIELD}
                                        : obj["prevSeqId"];
        if (!prev_field.error()) prev_seq_id = prev_field.get_int64().value();
        auto seq_field = obj["seqId"];
        if (!seq_field.error()) seq_id = seq_field.get_int64().value();

        if (action == "snapshot") {
            fresh.symbol          = to_unified_symbol(inst_id);
            fresh.exchange        = exchange_name_;
            fresh.sequence        = seq_id;
            fresh.timestamp       = timestamp;
            fresh.local_timestamp = now_ms();

            if (cfg_.verify_checksum && has_checksum && !verify_checksum(fresh, checksum)) {
                logger_->warn("[{}] {} snapshot checksum mismatch, re-subscribing",
                              exchange_name_, inst_id);
                books_.erase(inst_id);
                net::co_spawn(ioc_, resubscribe_book(inst_id), net::detached);
                return;
            }

            books_[inst_id] = std::move(fresh);
            emit(DataType::Depth, inst_id, "snapshot",
                 books_[inst_id].snapshot(depth_levels_));
            continue;
        }

        // Sequence validation: prevSeqId must match the last seqId we applied.
        // seqId == prevSeqId is OKX's "nothing changed" keep-alive.
        if (prev_seq_id != book.sequence) {
            logger_->warn("[{}] {} sequence gap: prevSeqId={} expected={}",
                          exchange_name_, inst_id, prev_seq_id, book.sequence);
            books_.erase(inst_id);
            net::co_spawn(ioc_, resubscribe_book(inst_id), net::detached);
            return;
        }

        book.sequence        = seq_id;
        book.timestamp       = timestamp;
        book.local_timestamp = now_ms();

        if (cfg_.verify_checksum && has_checksum && !verify_checksum(book, checksum)) {
            logger_->warn("[{}] {} checksum mismatch after update, re-subscribing",
                          exchange_name_, inst_id);
            books_.erase(inst_id);
            net::co_spawn(ioc_, resubscribe_book(inst_id), net::detached);
            return;
        }

        emit(DataType::Depth, inst_id, "update", book.snapshot(depth_levels_));
    }
}

// Checksum string: bid1p:bid1q:ask1p:ask1q:... over the top 25 levels, using
// the exchange's original decimal formatting.
std::string okx_checksum_string(const Orderbook& book, size_t levels) {
    std::string s;
    s.reserve(levels * 40);

    auto bid_it = book.bids.begin();
    auto ask_it = book.asks.begin();
    for (size_t i = 0; i < levels; ++i) {
        if (bid_it != book.bids.end()) {
            if (!s.empty()) s += ':';
            s += bid_it->second.raw;
            ++bid_it;
        }
        if (ask_it != book.asks.end()) {
            if (!s.empty()) s += ':';
            s += ask_it->second.raw;
            ++ask_it;
        }
    }
    return s;
}

int32_t okx_checksum(const Orderbook& book, size_t levels) {
    return static_cast<int32_t>(crc32_of(okx_checksum_string(book, levels)));
}

bool OkxAdapter::verify_checksum(const Orderbook& book, int64_t expected) const {
    return okx_checksum(book, CHECKSUM_LEVELS) == expected;
}

// ---------------------------------------------------------------------------
// Ticker (bbo-tbt channel)
// ---------------------------------------------------------------------------

void OkxAdapter::handle_bbo(simdjson::ondemand::document& doc,
                            const std::string& inst_id) {
    auto data = doc["data"].get_array();
    if (data.error()) return;

    for (auto entry : data.value()) {
        auto obj = entry.get_object().value();

        TickerData tick;
        tick.symbol   = to_unified_symbol(inst_id);
        tick.exchange = exchange_name_;

        for (auto ask : obj["asks"].get_array().value()) {
            auto arr = ask.get_array().value();
            auto it  = arr.begin();
            tick.best_ask_price = std::stod(std::string((*it).get_string().value()));
            ++it;
            tick.best_ask_qty   = std::stod(std::string((*it).get_string().value()));
            break;
        }
        for (auto bid : obj["bids"].get_array().value()) {
            auto arr = bid.get_array().value();
            auto it  = arr.begin();
            tick.best_bid_price = std::stod(std::string((*it).get_string().value()));
            ++it;
            tick.best_bid_qty   = std::stod(std::string((*it).get_string().value()));
            break;
        }

        tick.timestamp       = std::stoll(std::string(obj["ts"].get_string().value()));
        tick.local_timestamp = now_ms();

        emit(DataType::Ticker, inst_id, "update", std::move(tick));
    }
}

// ---------------------------------------------------------------------------
// Kline (candle1m channel)
// ---------------------------------------------------------------------------

void OkxAdapter::handle_candle(simdjson::ondemand::document& doc,
                               const std::string& inst_id) {
    auto data = doc["data"].get_array();
    if (data.error()) return;

    // Each entry: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
    for (auto entry : data.value()) {
        auto arr = entry.get_array().value();

        std::vector<std::string> f;
        f.reserve(9);
        for (auto item : arr)
            f.emplace_back(item.get_string().value());
        if (f.size() < 9) continue;

        KlineData kline;
        kline.symbol          = to_unified_symbol(inst_id);
        kline.exchange        = exchange_name_;
        kline.interval        = "1m";
        kline.open_time       = std::stoll(f[0]);
        kline.close_time      = kline.open_time + 60'000 - 1;
        kline.open            = std::stod(f[1]);
        kline.high            = std::stod(f[2]);
        kline.low             = std::stod(f[3]);
        kline.close           = std::stod(f[4]);
        kline.volume          = std::stod(f[5]);
        kline.quote_volume    = std::stod(f[7]);
        kline.num_trades      = 0; // not provided by OKX
        kline.is_closed       = (f[8] == "1");
        kline.timestamp       = now_ms();
        kline.local_timestamp = kline.timestamp;

        emit(DataType::Kline, inst_id, "update", std::move(kline));
    }
}

} // namespace axon_market_data
