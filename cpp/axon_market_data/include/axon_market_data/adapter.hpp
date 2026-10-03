#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <spdlog/spdlog.h>

#include "metrics.hpp"
#include "models.hpp"

namespace axon_market_data {

using EventCallback = std::function<void(const MarketDataEvent&)>;

// Common interface for all exchange adapters.
class Adapter {
public:
    virtual ~Adapter() = default;

    virtual void start() = 0;
    virtual void stop()  = 0;

    virtual const std::string& exchange_name() const = 0;

    // Logs every topic that was subscribed but has never delivered an event.
    // Called once, a grace period after start.
    //
    // This exists because a venue's subscribe acknowledgement cannot be
    // trusted. Binance answers {"result":null} to a SUBSCRIBE for a stream
    // name that does not exist at all, so "subscribed to N streams" in the log
    // means only that the bytes were sent. A topic that is misspelled,
    // withdrawn, or restricted for this network looks exactly like a quiet
    // market: nothing happens, and nothing says so.
    virtual void warn_if_silent() = 0;
};

// Everything every adapter needs: the symbol table, the event callback, and
// the wrapping of a payload into a MarketDataEvent.
class BaseAdapter : public Adapter {
public:
    const std::string& exchange_name() const override { return exchange_name_; }

protected:
    BaseAdapter(std::string name,
                EventCallback on_event,
                std::shared_ptr<spdlog::logger> logger)
        : exchange_name_(std::move(name))
        , on_event_(std::move(on_event))
        , logger_(logger ? std::move(logger) : spdlog::default_logger()) {}

    // Per topic: the Prometheus handle, plus a plain local counter. The
    // counter is not redundant with the Prometheus one -- reading a count back
    // out of a Counter family means resolving its labels again, and this is
    // one increment on a cache line the emit path has already touched.
    struct TopicSlot {
        TopicMetrics  metrics;
        std::uint64_t events   = 0;
        bool          declared = false;
    };

    // exchange symbol -> one slot per DataType, indexed by the enum.
    using TopicSlots = std::array<TopicSlot, 3>;

    // Falls back to the exchange's own symbol when it was never subscribed.
    const std::string& to_unified_symbol(const std::string& exchange_sym) const {
        auto it = sym_to_unified_.find(exchange_sym);
        return it != sym_to_unified_.end() ? it->second : exchange_sym;
    }

    // Registers one topic with the metrics registry and caches its handle.
    // Called once per subscription while the adapter is being constructed, so
    // that nothing on the receive path ever resolves a Prometheus label set.
    //
    // Adapters call this from the same loop that builds their stream list, so
    // what is declared here is exactly what was asked for on the wire.
    void declare_subscription(DataType dt, const std::string& exchange_sym) {
        auto& slot = topic_metrics_[exchange_sym][static_cast<std::size_t>(dt)];
        slot.declared = true;
        slot.metrics  = get_metrics().declare_topic(
            exchange_name_, dt, to_unified_symbol(exchange_sym));
    }

public:
    void warn_if_silent() override {
        std::size_t silent = 0, total = 0;
        for (const auto& [exchange_sym, slots] : topic_metrics_) {
            for (std::size_t i = 0; i < slots.size(); ++i) {
                if (!slots[i].declared) continue;
                ++total;
                if (slots[i].events != 0) continue;
                ++silent;
                logger_->warn(
                    "[{}] SILENT: subscribed to {}.{} but no data has arrived. "
                    "The venue acknowledged the subscription -- that proves "
                    "nothing. Check the stream name, and whether this network "
                    "is served that stream.",
                    exchange_name_, to_string(static_cast<DataType>(i)),
                    to_unified_symbol(exchange_sym));
            }
        }
        if (silent != 0)
            logger_->warn("[{}] {} of {} subscribed topics are silent",
                          exchange_name_, silent, total);
    }

protected:

    void emit(DataType dt, const std::string& exchange_sym,
              const std::string& event_type, EventData data) {
        MarketDataEvent ev;
        ev.data_type  = dt;
        ev.event_type = event_type;
        ev.symbol     = to_unified_symbol(exchange_sym);
        ev.exchange   = exchange_name_;
        ev.data       = std::move(data);
        ev.timestamp  = now_ms();

        TopicSlot& topic = topic_metrics_for(dt, exchange_sym);
        ++topic.events;
        topic.metrics.on_event(venue_timestamp(ev.data), ev.timestamp);

        const double t0 = steady_seconds();
        on_event_(ev);
        topic.metrics.on_published(steady_seconds() - t0);
    }

    // One hash of a short string, against the three-label map hash plus string
    // building that prometheus-cpp would do per observation. A symbol that was
    // never declared gets the null handle and is counted in aggregate rather
    // than given its own series -- see inc_undeclared_event().
    TopicSlot& topic_metrics_for(DataType dt, const std::string& exchange_sym) {
        auto it = topic_metrics_.find(exchange_sym);
        if (it == topic_metrics_.end()) {
            get_metrics().inc_undeclared_event(exchange_name_);
            return undeclared_;
        }
        return it->second[static_cast<std::size_t>(dt)];
    }

    std::string                     exchange_name_;
    EventCallback                   on_event_;
    std::shared_ptr<spdlog::logger> logger_;

    // exchange symbol -> unified symbol
    std::unordered_map<std::string, std::string> sym_to_unified_;

    std::unordered_map<std::string, TopicSlots> topic_metrics_;

    // Handed out for anything undeclared; its handle's methods all no-op.
    TopicSlot undeclared_;
};

} // namespace axon_market_data
