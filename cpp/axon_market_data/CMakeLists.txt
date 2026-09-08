cmake_minimum_required(VERSION 3.25)
project(axon_market_data CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Dependencies
find_package(Boost REQUIRED COMPONENTS system)
find_package(simdjson CONFIG REQUIRED)
find_package(glaze CONFIG REQUIRED)
find_package(cppzmq CONFIG REQUIRED)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(OpenSSL REQUIRED)

add_executable(axon_market_data
    src/main.cpp
    src/config.cpp
    src/logging.cpp
    src/websocket_client.cpp
    src/http_client.cpp
    src/binance_adapter.cpp
    src/okx_adapter.cpp
    src/bybit_adapter.cpp
    src/hyperliquid_adapter.cpp
    src/zmq_publisher.cpp
    src/service.cpp
)

target_include_directories(axon_market_data PRIVATE include)

target_link_libraries(axon_market_data PRIVATE
    Boost::system
    simdjson::simdjson
    glaze::glaze
    cppzmq
    yaml-cpp::yaml-cpp
    spdlog::spdlog
    OpenSSL::SSL
    OpenSSL::Crypto
)

# --- Examples ---
add_executable(mds_subscriber example/subscriber.cpp)
target_link_libraries(mds_subscriber PRIVATE
    cppzmq
    simdjson::simdjson
)
