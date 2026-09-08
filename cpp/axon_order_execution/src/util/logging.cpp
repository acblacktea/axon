#include "axon/util/logging.h"

#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <mutex>
#include <unordered_map>
#include <vector>

namespace axon::util {
namespace {

spdlog::level::level_enum to_spdlog(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return spdlog::level::trace;
    case LogLevel::kDebug:
      return spdlog::level::debug;
    case LogLevel::kInfo:
      return spdlog::level::info;
    case LogLevel::kWarn:
      return spdlog::level::warn;
    case LogLevel::kError:
      return spdlog::level::err;
    case LogLevel::kCritical:
      return spdlog::level::critical;
  }
  return spdlog::level::info;
}

std::vector<spdlog::sink_ptr>& sinks() {
  static std::vector<spdlog::sink_ptr> s;
  return s;
}

spdlog::level::level_enum& configured_level() {
  static spdlog::level::level_enum level = spdlog::level::info;
  return level;
}

std::mutex& registry_mutex() {
  static std::mutex m;
  return m;
}

}  // namespace

void init_logging(const LoggingOptions& options) {
  std::lock_guard<std::mutex> lock(registry_mutex());
  sinks().clear();

  if (options.console) {
    sinks().push_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
  }
  if (!options.file_path.empty()) {
    sinks().push_back(std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        options.file_path, options.max_file_bytes, options.max_files));
  }

  configured_level() = to_spdlog(options.level);

  // Timestamps in UTC with microseconds. Local time in a log that will be read
  // next to exchange timestamps is a reliable way to waste an hour during an
  // incident.
  for (auto& sink : sinks()) {
    sink->set_pattern("%Y-%m-%dT%H:%M:%S.%f %^%-5l%$ [%n] %v");
    sink->set_level(configured_level());
  }
  spdlog::set_level(configured_level());
  // Flush warnings and worse immediately; anything less can sit in a buffer
  // through a crash, which is exactly when the last few lines matter most.
  spdlog::flush_on(spdlog::level::warn);
}

std::shared_ptr<spdlog::logger> get_logger(const std::string& name) {
  std::lock_guard<std::mutex> lock(registry_mutex());

  if (auto existing = spdlog::get(name)) {
    return existing;
  }
  if (sinks().empty()) {
    // init_logging was never called. Fall back to a console logger rather than
    // returning null -- a missing log line during startup should not be a
    // segfault.
    sinks().push_back(std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
  }
  auto logger = std::make_shared<spdlog::logger>(name, sinks().begin(), sinks().end());
  logger->set_level(configured_level());
  logger->flush_on(spdlog::level::warn);
  spdlog::register_logger(logger);
  return logger;
}

void shutdown_logging() { spdlog::shutdown(); }

}  // namespace axon::util
