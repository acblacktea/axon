#include "axon_market_data/config.hpp"

#include <stdexcept>

#include <yaml-cpp/yaml.h>

namespace axon_market_data {

static std::vector<std::string> read_string_list(const YAML::Node& node) {
    std::vector<std::string> out;
    if (!node || !node.IsSequence()) return out;
    for (const auto& item : node)
        out.push_back(item.as<std::string>());
    return out;
}

AppConfig load_config(const std::string& path) {
    YAML::Node root = YAML::LoadFile(path);
    AppConfig cfg;

    if (auto zmq = root["zmq"]) {
        if (zmq["pub_address"])
            cfg.pub_address = zmq["pub_address"].as<std::string>();
    }

    if (auto log = root["logging"]) {
        if (log["file"])      cfg.logging.file      = log["file"].as<std::string>();
        if (log["level"])     cfg.logging.level     = log["level"].as<std::string>();
        if (log["max_files"]) cfg.logging.max_files = log["max_files"].as<unsigned>();
    }

    if (auto metrics = root["metrics"]) {
        if (metrics["enabled"]) cfg.metrics.enabled = metrics["enabled"].as<bool>();
        if (metrics["host"])    cfg.metrics.host    = metrics["host"].as<std::string>();
        if (metrics["port"])    cfg.metrics.port    = metrics["port"].as<int>();
    }

    if (auto exchanges = root["exchanges"]) {
        for (const auto& ex : exchanges) {
            ExchangeConfig ec;
            ec.name        = ex["name"].as<std::string>();
            ec.market_type = parse_market_type(
                ex["market_type"] ? ex["market_type"].as<std::string>() : "spot");
            if (ex["update_speed"])
                ec.update_speed = ex["update_speed"].as<std::string>();
            if (ex["verify_checksum"])
                ec.verify_checksum = ex["verify_checksum"].as<bool>();
            if (ex["depth_levels"])
                ec.depth_levels = ex["depth_levels"].as<size_t>();

            if (auto subs = ex["subscriptions"]) {
                ec.subscriptions.depth  = read_string_list(subs["depth"]);
                ec.subscriptions.ticker = read_string_list(subs["ticker"]);
                ec.subscriptions.kline  = read_string_list(subs["kline"]);
            }
            cfg.exchanges.push_back(std::move(ec));
        }
    }

    if (cfg.exchanges.empty())
        throw std::runtime_error("No exchanges configured in " + path);

    return cfg;
}

} // namespace axon_market_data
