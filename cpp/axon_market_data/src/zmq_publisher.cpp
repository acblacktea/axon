#include "axon/market_data/zmq_publisher.h"

#include <glaze/glaze.hpp>

#include "axon/market_data/metrics.h"

namespace axon::market_data {

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
    // A plain PUB socket drops at the HWM and reports success, so a full
    // subscriber queue is invisible. With NODROP a send that would overflow
    // any subscriber fails with EAGAIN instead -- which send_frame() turns
    // into a count, then a lossy resend so everyone else still gets it.
    pub_->set(zmq::sockopt::xpub_nodrop, true);
    // Discard whatever is still queued on stop(). The default (-1) makes the
    // context shutdown wait until every queued frame is delivered -- forever,
    // with a subscriber that has stopped reading -- and stale market data is
    // worth nothing to a subscriber anyway.
    pub_->set(zmq::sockopt::linger, 0);
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
        // Never blocks the feed: a frame that does not fit some subscriber's
        // queue is dropped for that subscriber only, and counted. Without the
        // counter a subscriber that stopped draining looks identical to a
        // venue that went quiet.
        bool delivered = send_frame(topic, zmq::send_flags::sndmore);
        delivered      = send_frame(json, zmq::send_flags::none) && delivered;
        if (!delivered) get_metrics().inc_publish_failure(event.exchange);
    } catch (const zmq::error_t& e) {
        get_metrics().inc_publish_failure(event.exchange);
        logger_->warn("ZMQ send failed: {}", e.what());
    }
}

bool ZmqPublisher::send_frame(std::string_view data, zmq::send_flags flags) {
    flags = flags | zmq::send_flags::dontwait;
    if (pub_->send(zmq::buffer(data), flags)) return true;

    // EAGAIN under NODROP: at least one subscriber is at its HWM, and libzmq
    // sent the frame to nobody. Resend it lossy, the way a plain PUB socket
    // would have: subscribers with room get it, the full one drops it. A
    // lossy send cannot return EAGAIN.
    //
    // libzmq then stops offering frames to the full subscriber until it has
    // drained down to its low-water mark, so a subscriber that stalls for
    // good is counted when its queue fills, not once per frame it misses.
    pub_->set(zmq::sockopt::xpub_nodrop, false);
    try {
        (void)pub_->send(zmq::buffer(data), flags);
    } catch (...) {
        pub_->set(zmq::sockopt::xpub_nodrop, true);
        throw;
    }
    pub_->set(zmq::sockopt::xpub_nodrop, true);
    return false;
}

} // namespace axon::market_data
