#pragma once

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include <spdlog/spdlog.h>

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

    void emit(DataType dt, const std::string& exchange_sym,
              const std::string& event_type, EventData data) {
        MarketDataEvent ev;
        ev.data_type  = dt;
        ev.event_type = event_type;
        ev.symbol     = to_unified_symbol(exchange_sym);
        ev.exchange   = exchange_name_;
        ev.data       = std::move(data);
        ev.timestamp  = now_ms();
        on_event_(ev);
    }

    std::string                     exchange_name_;
    EventCallback                   on_event_;
    std::shared_ptr<spdlog::logger> logger_;

    // exchange symbol -> unified symbol
    std::unordered_map<std::string, std::string> sym_to_unified_;
};

} // namespace axon_market_data
