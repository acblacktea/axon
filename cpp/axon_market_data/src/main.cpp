#include <iostream>
#include <string>

#include <spdlog/spdlog.h>

#include "axon_market_data/config.hpp"
#include "axon_market_data/logging.hpp"
#include "axon_market_data/service.hpp"

int main(int argc, char* argv[]) {
    std::string config_path = "config.yml";
    if (argc > 1) config_path = argv[1];

    // Bootstrap logger: the config names the real one, but load_config() can
    // throw before we get there.
    auto logger = axon_market_data::make_logger({.file = "", .level = "info"});

    std::cout << "start" << std::endl;
    try {
        auto cfg = axon_market_data::load_config(config_path);

        spdlog::drop("mds");
        logger = axon_market_data::make_logger(cfg.logging);

        logger->info("Loaded config: {} exchange(s), pub={}",
                     cfg.exchanges.size(), cfg.pub_address);
        if (!cfg.logging.file.empty())
            logger->info("Logging to {} (daily, keeping {} files), level={}",
                         cfg.logging.file, cfg.logging.max_files, cfg.logging.level);
        for (auto& ex : cfg.exchanges) {
            logger->info("  {} ({}) – depth:{} ticker:{} kline:{}",
                         ex.name, axon_market_data::to_string(ex.market_type),
                         ex.subscriptions.depth.size(),
                         ex.subscriptions.ticker.size(),
                         ex.subscriptions.kline.size());
        }

        axon_market_data::Service service(cfg, logger);
        service.run();

    } catch (const std::exception& e) {
        logger->critical("Fatal: {}", e.what());
        return 1;
    }

    return 0;
}
