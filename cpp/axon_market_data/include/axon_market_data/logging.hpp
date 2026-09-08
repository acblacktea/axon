#pragma once

#include <memory>
#include <string>

#include <spdlog/spdlog.h>

namespace axon_market_data {

struct LogConfig {
    // Daily-rotated log file. spdlog appends the date to the stem, so
    // "logs/axon_market_data.log" becomes "logs/axon_market_data_2026-08-28.log".
    // Empty disables file output and leaves only the console.
    std::string file      = "logs/axon_market_data.log";
    std::string level     = "info";
    // Days of history to keep; 0 keeps everything.
    unsigned    max_files = 30;
};

// Console + daily file, both with the level name colourised.
std::shared_ptr<spdlog::logger> make_logger(const LogConfig& cfg);

} // namespace axon_market_data
