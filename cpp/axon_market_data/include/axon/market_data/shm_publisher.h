#pragma once

#include <cstdint>
#include <memory>
#include <string>

#include <spdlog/spdlog.h>

#include "axon/market_data/config.h"
#include "axon/market_data/models.h"
#include "axon/market_data/shm_feed.h"

namespace axon::market_data {

// Publishes market data events to the shared-memory feed (shm_feed.h), next
// to the ZMQ publisher: same events, binary records instead of JSON.
//
// Called from the service's one event-loop thread -- the segment's single
// writer.
class ShmPublisher {
public:
    ShmPublisher(const ShmConfig& config, std::shared_ptr<spdlog::logger> logger = nullptr);

    ShmPublisher(const ShmPublisher&)            = delete;
    ShmPublisher& operator=(const ShmPublisher&) = delete;

    // False when the event cannot be written as a record: a name longer than
    // its field (never truncated -- a truncated symbol could be another
    // symbol), or a book too deep for one record. Counted and logged once per
    // exchange; ZMQ still carries it.
    bool publish(const MarketDataEvent& event);

    // Marks the writer alive for readers and refreshes the record gauge.
    // Call about once a second.
    void tick();

    std::uint64_t published() const noexcept { return writer_.published(); }
    const shm::Writer& writer() const noexcept { return writer_; }

private:
    void refuse(const MarketDataEvent& event, const char* why);

    std::shared_ptr<spdlog::logger> logger_;
    shm::Writer                     writer_;
    std::uint64_t                   refused_ = 0;
};

} // namespace axon::market_data
