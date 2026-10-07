// config.yml loading: defaults, overrides, and the shipped example.

#include "test_support.hpp"

#include <filesystem>
#include <fstream>

#include "axon_market_data/config.hpp"

namespace mds_test {
namespace {

namespace fs = std::filesystem;

class ConfigTest : public ::testing::Test {
protected:
    std::string write(const std::string& yaml) {
        std::ofstream(path_) << yaml;
        return path_.string();
    }
    void TearDown() override { fs::remove(path_); }

    fs::path path_ = fs::temp_directory_path() /
                     ("mds_config_" + std::to_string(::getpid()) + ".yml");
};

TEST_F(ConfigTest, MinimalConfigTakesTheDefaults) {
    auto cfg = load_config(write(R"(
exchanges:
  - name: binance
)"));
    EXPECT_EQ(cfg.pub_address, "tcp://*:5558");
    EXPECT_EQ(cfg.silent_topic_check_seconds, 30);
    EXPECT_TRUE(cfg.metrics.enabled);
    EXPECT_EQ(cfg.metrics.port, 9101);
    EXPECT_EQ(cfg.logging.level, "info");
    EXPECT_EQ(cfg.logging.max_files, 30u);
    ASSERT_EQ(cfg.exchanges.size(), 1u);
    const auto& ex = cfg.exchanges[0];
    EXPECT_EQ(ex.name, "binance");
    EXPECT_EQ(ex.market_type, MarketType::Spot);
    EXPECT_EQ(ex.update_speed, "100ms");
    EXPECT_EQ(ex.depth_levels, 400u);
    EXPECT_FALSE(ex.verify_checksum);
    EXPECT_TRUE(ex.subscriptions.depth.empty());
}

TEST_F(ConfigTest, EveryFieldCanBeOverridden) {
    auto cfg = load_config(write(R"(
zmq:
  pub_address: "ipc:///tmp/mds"
logging:
  file: ""
  level: debug
  max_files: 0
silent_topic_check_seconds: 0
metrics:
  enabled: false
  host: "127.0.0.1"
  port: 9200
exchanges:
  - name: okx
    market_type: usdt_futures
    update_speed: "1000ms"
    verify_checksum: true
    depth_levels: 5
    subscriptions:
      depth: [ETH_USDT_PERP, BTC_USDT_PERP]
      ticker: [ETH_USDT_SPOT]
      kline: [SOL_USDT_SPOT]
  - name: bybit
    market_type: coin_futures
)"));
    EXPECT_EQ(cfg.pub_address, "ipc:///tmp/mds");
    EXPECT_EQ(cfg.logging.file, "");
    EXPECT_EQ(cfg.logging.level, "debug");
    EXPECT_EQ(cfg.logging.max_files, 0u);
    EXPECT_EQ(cfg.silent_topic_check_seconds, 0);
    EXPECT_FALSE(cfg.metrics.enabled);
    EXPECT_EQ(cfg.metrics.host, "127.0.0.1");
    EXPECT_EQ(cfg.metrics.port, 9200);

    ASSERT_EQ(cfg.exchanges.size(), 2u);
    const auto& okx = cfg.exchanges[0];
    EXPECT_EQ(okx.market_type, MarketType::UsdtFutures);
    EXPECT_EQ(okx.update_speed, "1000ms");
    EXPECT_TRUE(okx.verify_checksum);
    EXPECT_EQ(okx.depth_levels, 5u);
    EXPECT_EQ(okx.subscriptions.depth, (std::vector<std::string>{"ETH_USDT_PERP", "BTC_USDT_PERP"}));
    EXPECT_EQ(okx.subscriptions.ticker, (std::vector<std::string>{"ETH_USDT_SPOT"}));
    EXPECT_EQ(okx.subscriptions.kline, (std::vector<std::string>{"SOL_USDT_SPOT"}));
    EXPECT_EQ(cfg.exchanges[1].market_type, MarketType::CoinFutures);
}

// A service with nothing to subscribe to would start, publish nothing, and
// look healthy. Refuse it instead.
TEST_F(ConfigTest, NoExchangesIsAnError) {
    EXPECT_THROW(load_config(write("zmq:\n  pub_address: tcp://*:1\n")), std::runtime_error);
}

TEST_F(ConfigTest, MissingFileIsAnError) {
    EXPECT_ANY_THROW(load_config("/nonexistent/axon/config.yml"));
}

TEST_F(ConfigTest, ExchangeWithoutANameIsAnError) {
    EXPECT_ANY_THROW(load_config(write("exchanges:\n  - market_type: spot\n")));
}

TEST_F(ConfigTest, TheShippedExampleLoads) {
    auto cfg = load_config(std::string(MDS_SOURCE_DIR) + "/config.yml");
    EXPECT_EQ(cfg.exchanges.size(), 5u);
    EXPECT_EQ(cfg.metrics.port, 9101);
}

}  // namespace
}  // namespace mds_test
