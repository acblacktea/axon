// Logging.
//
// spdlog for now, wrapped so the backend can change. It is NOT hot-path safe:
// even async spdlog formats on the calling thread and touches a mutex, which
// is fine per order and far too slow per market-data message.
//
// The intended end state is Quill (or NanoLog), which stores binary arguments
// on the hot path at ~10-30ns and formats them in a background thread. That
// swap is the reason this wrapper exists; nothing outside logging.cpp should
// know which library is underneath.
//
// RULE: no logging inside the receive loop or between a decision and a send.
// Log the decision after the order is on the wire, not before.

#pragma once

#include <memory>
#include <string>
#include <string_view>

#include <spdlog/spdlog.h>

namespace axon::util {

enum class LogLevel { kTrace, kDebug, kInfo, kWarn, kError, kCritical };

struct LoggingOptions {
  LogLevel level = LogLevel::kInfo;
  bool console = true;
  // Empty disables file output.
  std::string file_path;
  // Rotate at this size, keeping this many files.
  std::size_t max_file_bytes = 64u << 20;
  std::size_t max_files = 8;
};

void init_logging(const LoggingOptions& options);

// Named logger, one per subsystem, matching get_logger(__name__) in the Python.
std::shared_ptr<spdlog::logger> get_logger(const std::string& name);

void shutdown_logging();

}  // namespace axon::util

// Convenience macros. They take a logger so a subsystem's lines are
// attributable, and they compile to nothing more than the underlying call.
#define AXON_LOG_TRACE(logger, ...) SPDLOG_LOGGER_TRACE(logger, __VA_ARGS__)
#define AXON_LOG_DEBUG(logger, ...) SPDLOG_LOGGER_DEBUG(logger, __VA_ARGS__)
#define AXON_LOG_INFO(logger, ...) SPDLOG_LOGGER_INFO(logger, __VA_ARGS__)
#define AXON_LOG_WARN(logger, ...) SPDLOG_LOGGER_WARN(logger, __VA_ARGS__)
#define AXON_LOG_ERROR(logger, ...) SPDLOG_LOGGER_ERROR(logger, __VA_ARGS__)
