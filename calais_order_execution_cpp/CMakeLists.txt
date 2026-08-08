cmake_minimum_required(VERSION 3.24)
project(calais_order_execution
        VERSION 0.1.0
        DESCRIPTION "Low-latency C++ engine for Calais order execution"
        LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "" FORCE)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")

# ---------------------------------------------------------------------------
# Options
# ---------------------------------------------------------------------------
option(CALAIS_BUILD_TESTS      "Build unit tests"                      ON)
option(CALAIS_BUILD_BENCH      "Build latency benchmarks"              ON)
option(CALAIS_NATIVE_ARCH      "Compile with -march=native"            OFF)
option(CALAIS_WARNINGS_AS_ERRORS "Treat compiler warnings as errors"   ON)
option(CALAIS_ASAN             "Enable address+UB sanitizers"          OFF)
option(CALAIS_TSAN             "Enable thread sanitizer"               OFF)
option(CALAIS_BENCH_JSON_LIBS  "Fetch rapidjson+simdjson for the JSON comparison bench" OFF)

if(CALAIS_ASAN AND CALAIS_TSAN)
  message(FATAL_ERROR "CALAIS_ASAN and CALAIS_TSAN are mutually exclusive")
endif()

include(CalaisCompilerFlags)
include(CalaisDependencies)

# ---------------------------------------------------------------------------
# calais_core -- the foundation layer.
#
# Deliberately dependency-free (libc++/pthread only) so it can be unit tested
# and benchmarked in isolation, and so nothing on the hot path can drag in a
# third-party allocator or lock.
# ---------------------------------------------------------------------------
add_library(calais_core
  src/core/clock.cpp
  src/core/histogram.cpp
  src/core/latency.cpp
  src/core/shm_ring.cpp
  src/core/platform.cpp
)
add_library(calais::core ALIAS calais_core)

target_include_directories(calais_core PUBLIC
  $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
  $<INSTALL_INTERFACE:include>
)
target_link_libraries(calais_core PUBLIC calais_compiler_flags Threads::Threads)

# ---------------------------------------------------------------------------
# calais_models -- domain types + enum<->string tables.
# Depends only on calais_core.
# ---------------------------------------------------------------------------
add_library(calais_models
  src/models/enums.cpp
  src/models/ids.cpp
)
add_library(calais::models ALIAS calais_models)
target_link_libraries(calais_models PUBLIC calais_core)

# ---------------------------------------------------------------------------
# calais_transport -- wire codecs.
#
# `wire` is the CONTROL plane: JSON, byte-compatible with the Python
# implementation's transport/serialization.py. It is explicitly NOT hot path,
# which is why depending on nlohmann::json here is acceptable.
#
# `hot_messages` is the DATA plane: fixed-size POD, memcpy-only, zero parse.
# It lives in calais_core's dependency-free world (header-only).
# ---------------------------------------------------------------------------
add_library(calais_transport
  src/transport/wire.cpp
)
add_library(calais::transport ALIAS calais_transport)
# nlohmann is PUBLIC, not PRIVATE: Command::payload and Response::data are
# arbitrary JSON, so the type is part of the interface and every consumer of
# transport/messages.h needs the header. Only calais_transport links it --
# calais_core and calais_models stay JSON-free so hot-path code cannot reach a
# DOM parser by accident.
target_link_libraries(calais_transport PUBLIC calais_models nlohmann_json::nlohmann_json)

# ---------------------------------------------------------------------------
# calais_net -- WebSocket protocol layer.
#
# Hand-rolled RFC 6455 rather than Boost.Beast. Beast is the right call at a
# ~100us internal budget; at the 10us target this project is aimed at, its
# dynamic_buffer allocations put an unbounded malloc tail on the receive path.
# A client needs only a fraction of the RFC, and that fraction is small enough
# to own and to test exhaustively.
#
# SHA-1/SHA-256/HMAC/base64 are implemented locally rather than via OpenSSL:
# they run once per connection and all have authoritative published vectors, so
# keeping them here means the protocol layer stays trivially buildable. OpenSSL
# is used ONLY for TLS itself, which is not something to reimplement.
# ---------------------------------------------------------------------------
find_package(OpenSSL REQUIRED)

add_library(calais_net
  src/net/crypto_lite.cpp
  src/net/http_client.cpp
  src/net/tcp_socket.cpp
  src/net/tls_stream.cpp
  src/net/ws_connection.cpp
  src/net/ws_frame.cpp
  src/net/ws_handshake.cpp
  src/net/ws_message.cpp
)
add_library(calais::net ALIAS calais_net)
# OpenSSL is PRIVATE: tls_stream.h forward-declares the few OpenSSL types it
# exposes, so nothing downstream has to include <openssl/ssl.h>.
target_link_libraries(calais_net PUBLIC calais_core PRIVATE OpenSSL::SSL OpenSSL::Crypto)

# ---------------------------------------------------------------------------
# calais_venue -- per-exchange protocol.
#
# Inbound parsing (simdjson On-Demand) and outbound message construction (byte
# templates, no JSON library). This is the only place simdjson appears; the
# venue parsers are the only hot-path JSON in the system.
#
# Depends on calais_models for the domain enums and on calais_transport for the
# fixed-layout hot messages the parsers emit. It does NOT depend on calais_net:
# a venue parser transforms bytes to messages and has no idea they arrived over
# a WebSocket, which is what makes it testable against captured payloads.
# ---------------------------------------------------------------------------
add_library(calais_venue
  src/venue/json_writer.cpp
  src/venue/deribit/deribit_builder.cpp
  src/venue/binance/binance_builder.cpp
  src/venue/bybit/bybit_builder.cpp
  src/venue/okx/okx_builder.cpp
)
add_library(calais::venue ALIAS calais_venue)
# calais_net is needed for HMAC-SHA256 and base64: Bybit and OKX sign their
# WebSocket logins, and all three perp venues sign their REST requests.
target_link_libraries(calais_venue PUBLIC calais_transport calais_simdjson calais_net)

# ---------------------------------------------------------------------------
# calais_app -- config, logging, metrics, order state, venue sessions and the
# ZMQ control plane. Everything above the protocol layers.
#
# This is where the third-party surface widens: yaml-cpp, spdlog,
# prometheus-cpp and cppzmq all appear here and nowhere below. The layers
# underneath stay buildable with nothing but a compiler and OpenSSL, which is
# what keeps them testable in isolation.
# ---------------------------------------------------------------------------
# Homebrew installs these outside CMake's default search path on macOS, so
# point the search at their prefixes rather than requiring the caller to set
# CMAKE_PREFIX_PATH by hand.
if(APPLE)
  find_program(CALAIS_BREW brew)
  if(CALAIS_BREW)
    foreach(_pkg yaml-cpp spdlog prometheus-cpp cppzmq zeromq fmt libpqxx libpq)
      execute_process(COMMAND "${CALAIS_BREW}" --prefix ${_pkg}
                      OUTPUT_VARIABLE _prefix
                      OUTPUT_STRIP_TRAILING_WHITESPACE
                      ERROR_QUIET)
      if(_prefix)
        list(APPEND CMAKE_PREFIX_PATH "${_prefix}")
      endif()
    endforeach()
  endif()
endif()

find_package(yaml-cpp REQUIRED)
find_package(spdlog REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(cppzmq REQUIRED)
find_package(libpqxx REQUIRED)

add_library(calais_app
  src/config.cpp
  src/util/logging.cpp
  src/util/metrics.cpp
  src/ems/ems_service.cpp
  src/repository/postgres.cpp
  src/client/strategy_client.cpp
  src/oms/order_store.cpp
  src/oms/portfolio_store.cpp
  src/oms/reconciler.cpp
  src/oms/venue_rest.cpp
  src/oms/venue_session.cpp
  src/transport/shm_bridge.cpp
  src/transport/zmq_server.cpp
)
add_library(calais::app ALIAS calais_app)
target_link_libraries(calais_app
  PUBLIC calais_venue calais_net spdlog::spdlog
  PRIVATE yaml-cpp::yaml-cpp prometheus-cpp::pull cppzmq libpqxx::pqxx
)

add_executable(calais_engine src/engine_main.cpp)
target_link_libraries(calais_engine PRIVATE calais::app)

# A worked example of the client, and the only translation unit that
# instantiates the client-side algorithm templates -- so they are type-checked
# by every build rather than by whoever writes the first strategy.
add_executable(calais_strategy src/strategy_main.cpp)
target_link_libraries(calais_strategy PRIVATE calais::app)

# ---------------------------------------------------------------------------
if(CALAIS_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(CALAIS_BUILD_BENCH)
  add_subdirectory(bench)
endif()

message(STATUS "")
message(STATUS "calais_order_execution ${PROJECT_VERSION}")
message(STATUS "  build type ......... ${CMAKE_BUILD_TYPE}")
message(STATUS "  compiler ........... ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
message(STATUS "  target arch ........ ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "  -march=native ...... ${CALAIS_NATIVE_ARCH}")
message(STATUS "  tests .............. ${CALAIS_BUILD_TESTS}")
message(STATUS "  benchmarks ......... ${CALAIS_BUILD_BENCH}")
message(STATUS "  asan/tsan .......... ${CALAIS_ASAN}/${CALAIS_TSAN}")
message(STATUS "")
