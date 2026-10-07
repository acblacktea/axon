cmake_minimum_required(VERSION 3.25)
project(axon_market_data CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Dependencies
# Beast, Asio and System are all header-only. Boost >= 1.89 no longer ships a
# boost_system component at all, so asking for it fails to configure there.
find_package(Boost CONFIG REQUIRED)
find_package(simdjson CONFIG REQUIRED)
find_package(glaze CONFIG REQUIRED)
find_package(cppzmq CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

option(AXON_MDS_BUILD_TESTS "Build unit tests" ON)

# Everything but main(), so the unit tests link exactly what ships.
add_library(axon_market_data_core STATIC
    src/config.cpp
    src/logging.cpp
    src/metrics.cpp
    src/websocket_client.cpp
    src/http_client.cpp
    src/binance_adapter.cpp
    src/okx_adapter.cpp
    src/bybit_adapter.cpp
    src/hyperliquid_adapter.cpp
    src/zmq_publisher.cpp
    src/service.cpp
)

target_include_directories(axon_market_data_core PUBLIC include)

target_link_libraries(axon_market_data_core PUBLIC
    Boost::headers
    simdjson::simdjson
    glaze::glaze
    cppzmq
    yaml-cpp::yaml-cpp
    spdlog::spdlog
    prometheus-cpp::pull
    OpenSSL::SSL
    OpenSSL::Crypto
)

add_executable(axon_market_data src/main.cpp)
target_link_libraries(axon_market_data PRIVATE axon_market_data_core)

if(AXON_MDS_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# --- Examples ---
add_executable(mds_subscriber example/subscriber.cpp)
target_link_libraries(mds_subscriber PRIVATE
    cppzmq
    simdjson::simdjson
)
