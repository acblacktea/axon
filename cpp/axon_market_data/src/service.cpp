#include "axon/market_data/service.h"

#include <boost/asio/signal_set.hpp>

namespace axon::market_data {

Service::Service(const AppConfig& cfg, std::shared_ptr<spdlog::logger> logger)
    : cfg_(cfg)
    , logger_(logger ? std::move(logger) : spdlog::default_logger())
    , silent_check_(ioc_)
    , publisher_(cfg.pub_address, logger_)
    , shm_tick_(ioc_) {
    if (cfg_.shm.enabled) {
        try {
            shm_.emplace(cfg_.shm, logger_);
        } catch (const std::exception& e) {
            // ZMQ alone still serves every subscriber, so this is not fatal --
            // but a strategy configured for shared memory will see nothing.
            logger_->error("shared-memory feed disabled: {}", e.what());
        }
    }
}

Service::~Service() { stop(); }

void Service::run() {
    publisher_.start();

    // Create adapters from config
    for (auto& ex_cfg : cfg_.exchanges) {
        auto on_ev = [this](const MarketDataEvent& ev) { on_event(ev); };

        std::shared_ptr<Adapter> adapter;
        if (ex_cfg.name == "binance")
            adapter = std::make_shared<BinanceAdapter>(ioc_, ex_cfg, on_ev, logger_);
        else if (ex_cfg.name == "okx")
            adapter = std::make_shared<OkxAdapter>(ioc_, ex_cfg, on_ev, logger_);
        else if (ex_cfg.name == "bybit")
            adapter = std::make_shared<BybitAdapter>(ioc_, ex_cfg, on_ev, logger_);
        else if (ex_cfg.name == "hyperliquid")
            adapter = std::make_shared<HyperliquidAdapter>(ioc_, ex_cfg, on_ev, logger_);
        else {
            logger_->warn("Unknown exchange: {}", ex_cfg.name);
            continue;
        }

        adapter->start();
        adapters_.push_back(std::move(adapter));
    }

    // A venue's subscribe acknowledgement does not mean data will follow, so
    // check once, after every stream has had time to deliver something.
    if (cfg_.silent_topic_check_seconds > 0) {
        silent_check_.expires_after(
            std::chrono::seconds(cfg_.silent_topic_check_seconds));
        silent_check_.async_wait([this](const boost::system::error_code& ec) {
            if (ec) return; // cancelled by shutdown
            for (auto& adapter : adapters_)
                adapter->warn_if_silent();
        });
    }

    if (shm_) schedule_shm_tick();

    // Graceful shutdown on SIGINT / SIGTERM
    boost::asio::signal_set signals(ioc_, SIGINT, SIGTERM);
    signals.async_wait([this](const boost::system::error_code&, int sig) {
        logger_->info("Received signal {}, shutting down...", sig);
        stop();
    });

    running_ = true;
    logger_->info("Service running – press Ctrl+C to stop");
    ioc_.run();
}

void Service::stop() {
    if (!running_) return;
    running_ = false;

    silent_check_.cancel();
    shm_tick_.cancel();

    for (auto& adapter : adapters_)
        adapter->stop();

    ioc_.stop();
    publisher_.stop();
    logger_->info("Service stopped");
}

void Service::on_event(const MarketDataEvent& event) {
    // Shared memory first: it is the cheaper of the two and the one a
    // same-host strategy is waiting on.
    if (shm_) shm_->publish(event);
    publisher_.publish(event);
}

void Service::schedule_shm_tick() {
    shm_tick_.expires_after(std::chrono::seconds(1));
    shm_tick_.async_wait([this](const boost::system::error_code& ec) {
        if (ec) return; // cancelled by shutdown
        shm_->tick();
        schedule_shm_tick();
    });
}

} // namespace axon::market_data
