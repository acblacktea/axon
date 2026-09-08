#include "axon_market_data/logging.hpp"

#include <algorithm>
#include <filesystem>
#include <regex>
#include <vector>

#include <spdlog/pattern_formatter.h>
#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace axon_market_data {

namespace {

// File sinks ignore spdlog's %^/%$ colour range — only console sinks act on it.
// A custom flag lets the file carry the same highlighting. The escapes wrap the
// level name alone, so line-oriented tools still work: `grep error app.log`
// matches, and `less -R` / `tail` render the colour.
class colour_level_flag : public spdlog::custom_flag_formatter {
public:
    void format(const spdlog::details::log_msg& msg, const std::tm&,
                spdlog::memory_buf_t& dest) override {
        const char* colour = "";
        switch (msg.level) {
        case spdlog::level::trace:    colour = "\033[37m";   break; // white
        case spdlog::level::debug:    colour = "\033[36m";   break; // cyan
        case spdlog::level::info:     colour = "\033[32m";   break; // green
        case spdlog::level::warn:     colour = "\033[33;1m"; break; // bold yellow
        case spdlog::level::err:      colour = "\033[31;1m"; break; // bold red
        case spdlog::level::critical: colour = "\033[97;41;1m"; break; // white on red
        default: break;
        }
        static constexpr std::string_view reset = "\033[m";
        auto name = spdlog::level::to_string_view(msg.level);

        dest.append(colour, colour + std::char_traits<char>::length(colour));
        dest.append(name.data(), name.data() + name.size());
        dest.append(reset.data(), reset.data() + reset.size());
    }

    std::unique_ptr<custom_flag_formatter> clone() const override {
        return std::make_unique<colour_level_flag>();
    }
};

// spdlog's own retention is not enough on its own: daily_file_sink only prunes
// when it rotates, it never touches a backlog that already exists at startup,
// and its lookback stops at the first missing day — so a single day of downtime
// orphans everything older than the gap. Sweep the directory ourselves instead.
void prune_old_logs(const std::string& file_path, unsigned max_files,
                    const std::shared_ptr<spdlog::logger>& logger) {
    if (max_files == 0) return; // keep everything

    namespace fs = std::filesystem;
    try {
        fs::path path(file_path);
        fs::path dir = path.has_parent_path() ? path.parent_path() : fs::path(".");
        if (!fs::exists(dir)) return;

        // daily_filename_calculator writes "<stem>_YYYY-MM-DD<ext>"
        const std::regex pattern(
            "^" + path.stem().string() + R"(_(\d{4}-\d{2}-\d{2}))" +
            path.extension().string() + "$");

        std::vector<fs::path> logs;
        for (const auto& entry : fs::directory_iterator(dir)) {
            if (!entry.is_regular_file()) continue;
            auto name = entry.path().filename().string();
            if (std::regex_match(name, pattern)) logs.push_back(entry.path());
        }
        if (logs.size() <= max_files) return;

        // ISO dates sort lexicographically, so plain name order is date order.
        std::sort(logs.begin(), logs.end());

        size_t to_remove = logs.size() - max_files;
        for (size_t i = 0; i < to_remove; ++i) {
            std::error_code ec;
            fs::remove(logs[i], ec);
            if (ec)
                logger->warn("Could not remove old log {}: {}",
                             logs[i].string(), ec.message());
        }
        logger->info("Pruned {} log file(s) older than the newest {}",
                     to_remove, max_files);
    } catch (const std::exception& e) {
        logger->warn("Log retention sweep failed: {}", e.what());
    }
}

std::unique_ptr<spdlog::formatter> make_file_formatter() {
    auto f = std::make_unique<spdlog::pattern_formatter>();
    f->add_flag<colour_level_flag>('*');
    f->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%*] %v");
    return f;
}

} // namespace

std::shared_ptr<spdlog::logger> make_logger(const LogConfig& cfg) {
    std::vector<spdlog::sink_ptr> sinks;

    auto console = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    console->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%n] [%^%l%$] %v");
    sinks.push_back(std::move(console));

    if (!cfg.file.empty()) {
        // Rotates at 00:00; spdlog creates the parent directory as needed.
        auto file = std::make_shared<spdlog::sinks::daily_file_sink_mt>(
            cfg.file, 0, 0, false, static_cast<uint16_t>(cfg.max_files));
        file->set_formatter(make_file_formatter());
        sinks.push_back(std::move(file));
    }

    auto logger = std::make_shared<spdlog::logger>("mds", sinks.begin(), sinks.end());
    logger->set_level(spdlog::level::from_str(cfg.level));
    // Anything at warn or above reaches disk immediately; the rest is flushed
    // on a timer so a crash loses at most a second of info lines.
    logger->flush_on(spdlog::level::warn);

    spdlog::register_logger(logger);
    spdlog::flush_every(std::chrono::seconds(1));

    if (!cfg.file.empty()) prune_old_logs(cfg.file, cfg.max_files, logger);
    return logger;
}

} // namespace axon_market_data
