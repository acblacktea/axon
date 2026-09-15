#include "axon_market_data/zmq_publisher.hpp"

#include <glaze/glaze.hpp>

#include "axon_market_data/metrics.hpp"

namespace axon_market_data {

ZmqPublisher::ZmqPublisher(const std::string& address,
                           std::shared_ptr<spdlog::logger> logger)
    : address_(address)
    , logger_(logger ? std::move(logger) : spdlog::default_logger()) {}

ZmqPublisher::~ZmqPublisher() { stop(); }

void ZmqPublisher::start() {
    if (running_) return;
    ctx_ = std::make_unique<zmq::context_t>(1);
    pub_ = std::make_unique<zmq::socket_t>(*ctx_, zmq::socket_type::pub);

    // Set high water mark – drop if consumer is slow
    int hwm = 100000;
    pub_->set(zmq::sockopt::sndhwm, hwm);
    pub_->bind(address_);

    running_ = true;
    logger_->info("ZMQ PUB bound on {}", address_);
}

void ZmqPublisher::stop() {
    if (!running_) return;
    running_ = false;
    if (pub_) {
        pub_->close();
        pub_.reset();
    }
    if (ctx_) {
        ctx_->close();
        ctx_.reset();
    }
    logger_->info("ZMQ PUB stopped");
}

void ZmqPublisher::publish(const MarketDataEvent& event) {
    if (!running_) return;

    // Build topic: exchange.data_type.symbol
    std::string topic;
    topic.reserve(64);
    topic += event.exchange;
    topic += '.';
    topic += to_string(event.data_type);
    topic += '.';
    topic += event.symbol;

    // Serialize payload to JSON via glaze
    std::string json;
    std::visit(
        [&json](const auto& d) { json = glz::write_json(d).value_or("{}"); },
        event.data);

    // Send multi-part: [topic, json]
    try {
        // cppzmq returns an empty optional on EAGAIN rather than throwing, so
        // a full high water mark is not an exception -- it is a silent drop.
        // Dropping is the right behaviour for a PUB socket (one stalled
        // subscriber must not stall the feed), but silent is not: without this
        // counter a subscriber that stopped draining looks identical to a
        // venue that went quiet.
        auto topic_sent = pub_->send(
            zmq::buffer(topic), zmq::send_flags::sndmore | zmq::send_flags::dontwait);
        if (!topic_sent) {
            get_metrics().inc_publish_failure(event.exchange);
            return;
        }
        if (!pub_->send(zmq::buffer(json), zmq::send_flags::dontwait))
            get_metrics().inc_publish_failure(event.exchange);
    } catch (const zmq::error_t& e) {
        get_metrics().inc_publish_failure(event.exchange);
        if (e.num() != EAGAIN)
            logger_->warn("ZMQ send failed: {}", e.what());
    }
}

} // namespace axon_market_data
