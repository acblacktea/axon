#include "axon_market_data/service.hpp"

#include <boost/asio/signal_set.hpp>

namespace axon_market_data {

Service::Service(const AppConfig& cfg, std::shared_ptr<spdlog::logger> logger)
    : cfg_(cfg)
    , logger_(logger ? std::move(logger) : spdlog::default_logger())
    , publisher_(cfg.pub_address, logger_) {}

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

    for (auto& adapter : adapters_)
        adapter->stop();

    ioc_.stop();
    publisher_.stop();
    logger_->info("Service stopped");
}

void Service::on_event(const MarketDataEvent& event) {
    publisher_.publish(event);
}

} // namespace axon_market_data
