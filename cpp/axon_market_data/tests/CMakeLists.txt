find_package(GTest REQUIRED)
include(GoogleTest)

add_executable(mds_tests
    test_models.cpp
    test_config.cpp
    test_metrics.cpp
    test_logging.cpp
    test_adapter_base.cpp
    test_binance_adapter.cpp
    test_okx_adapter.cpp
    test_bybit_adapter.cpp
    test_hyperliquid_adapter.cpp
    test_zmq_publisher.cpp
    test_websocket_client.cpp
)

# So a test can load the config.yml shipped next to the sources.
target_compile_definitions(mds_tests PRIVATE MDS_SOURCE_DIR="${PROJECT_SOURCE_DIR}")

target_link_libraries(mds_tests PRIVATE
    axon_market_data_core
    GTest::gtest
    GTest::gtest_main
)

# One process per test: the metrics registry and spdlog's logger registry are
# process-wide, so isolating tests also isolates their global state.
gtest_discover_tests(mds_tests
    DISCOVERY_TIMEOUT 60
    PROPERTIES TIMEOUT 120
)
