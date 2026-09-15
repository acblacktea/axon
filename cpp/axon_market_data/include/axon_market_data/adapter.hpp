#pragma once

#include <array>
#include <cstddef>
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
        auto& slots = topic_metrics_[exchange_sym];
        slots[static_cast<std::size_t>(dt)] = get_metrics().declare_topic(
            exchange_name_, dt, to_unified_symbol(exchange_sym));
    }

    void emit(DataType dt, const std::string& exchange_sym,
              const std::string& event_type, EventData data) {
        MarketDataEvent ev;
        ev.data_type  = dt;
        ev.event_type = event_type;
        ev.symbol     = to_unified_symbol(exchange_sym);
        ev.exchange   = exchange_name_;
        ev.data       = std::move(data);
        ev.timestamp  = now_ms();

        TopicMetrics& topic = topic_metrics_for(dt, exchange_sym);
        topic.on_event(venue_timestamp(ev.data), ev.timestamp);

        const double t0 = steady_seconds();
        on_event_(ev);
        topic.on_published(steady_seconds() - t0);
    }

    // One hash of a short string, against the three-label map hash plus string
    // building that prometheus-cpp would do per observation. A symbol that was
    // never declared gets the null handle and is counted in aggregate rather
    // than given its own series -- see inc_undeclared_event().
    TopicMetrics& topic_metrics_for(DataType dt, const std::string& exchange_sym) {
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

    // exchange symbol -> one handle per DataType, indexed by the enum.
    using TopicSlots = std::array<TopicMetrics, 3>;
    std::unordered_map<std::string, TopicSlots> topic_metrics_;

    // Null handle handed out for anything undeclared; all its methods no-op.
    TopicMetrics undeclared_;
};

} // namespace axon_market_data
