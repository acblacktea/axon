/**
 * axon_market_data ZMQ subscriber example
 *
 * Connects to the market data service and prints received events.
 *
 * Usage:
 *   ./mds_subscriber                          # subscribe all from localhost:5558
 *   ./mds_subscriber tcp://localhost:5558 binance_spot.depth
 *   ./mds_subscriber tcp://localhost:5558 binance_spot.depth.ETH_USDT_SPOT
 */

#include <iostream>
#include <string>

#include <zmq.hpp>
#include <simdjson.h>

static void print_orderbook(simdjson::ondemand::document& doc) {
    auto symbol = doc["symbol"].get_string().value();
    auto seq    = doc["sequence"].get_int64().value();

    std::cout << "  symbol: " << symbol << "  seq: " << seq << '\n';

    std::cout << "  asks: ";
    int i = 0;
    for (auto level : doc["asks"].get_array()) {
        if (i >= 5) { std::cout << "..."; break; }
        auto obj = level.get_object().value();
        auto p = obj["price"].get_double().value();
        auto q = obj["quantity"].get_double().value();
        std::cout << p << "x" << q << "  ";
        ++i;
    }
    std::cout << '\n';

    std::cout << "  bids: ";
    i = 0;
    for (auto level : doc["bids"].get_array()) {
        if (i >= 5) { std::cout << "..."; break; }
        auto obj = level.get_object().value();
        auto p = obj["price"].get_double().value();
        auto q = obj["quantity"].get_double().value();
        std::cout << p << "x" << q << "  ";
        ++i;
    }
    std::cout << '\n';
}

static void print_ticker(simdjson::ondemand::document& doc) {
    auto symbol = doc["symbol"].get_string().value();
    auto bid    = doc["best_bid_price"].get_double().value();
    auto ask    = doc["best_ask_price"].get_double().value();

    std::cout << "  " << symbol
              << "  bid=" << bid
              << "  ask=" << ask
              << "  spread=" << (ask - bid) << '\n';
}

static void print_kline(simdjson::ondemand::document& doc) {
    auto symbol   = doc["symbol"].get_string().value();
    auto interval = doc["interval"].get_string().value();
    auto o = doc["open"].get_double().value();
    auto h = doc["high"].get_double().value();
    auto l = doc["low"].get_double().value();
    auto c = doc["close"].get_double().value();
    auto v = doc["volume"].get_double().value();
    auto closed = doc["is_closed"].get_bool().value();

    std::cout << "  " << symbol << " " << interval
              << "  O=" << o << " H=" << h << " L=" << l << " C=" << c
              << "  vol=" << v
              << (closed ? " [CLOSED]" : "") << '\n';
}

int main(int argc, char* argv[]) {
    std::string address = "tcp://localhost:5558";
    std::string filter  = "";  // empty = subscribe all

    if (argc > 1) address = argv[1];
    if (argc > 2) filter  = argv[2];

    std::cout << "Connecting to " << address << '\n';
    std::cout << "Topic filter: " << (filter.empty() ? "(all)" : filter) << '\n';
    std::cout << "---\n";

    zmq::context_t ctx(1);
    zmq::socket_t sub(ctx, zmq::socket_type::sub);
    sub.connect(address);
    sub.set(zmq::sockopt::subscribe, filter);

    simdjson::ondemand::parser parser;
    uint64_t count = 0;

    while (true) {
        // Receive topic frame
        zmq::message_t topic_msg;
        auto res = sub.recv(topic_msg, zmq::recv_flags::none);
        if (!res) continue;

        std::string topic(static_cast<char*>(topic_msg.data()), topic_msg.size());

        // Receive data frame
        zmq::message_t data_msg;
        res = sub.recv(data_msg, zmq::recv_flags::none);
        if (!res) continue;

        std::string_view json(static_cast<char*>(data_msg.data()), data_msg.size());

        ++count;
        std::cout << "[" << count << "] " << topic << '\n';

        try {
            simdjson::padded_string padded(json);
            auto doc = parser.iterate(padded).value();

            if (topic.find(".depth.") != std::string::npos) {
                print_orderbook(doc);
            } else if (topic.find(".ticker.") != std::string::npos) {
                print_ticker(doc);
            } else if (topic.find(".kline.") != std::string::npos) {
                print_kline(doc);
            } else {
                std::cout << "  " << json << '\n';
            }
        } catch (const std::exception& e) {
            std::cout << "  parse error: " << e.what() << '\n';
            std::cout << "  raw: " << json << '\n';
        }

        std::cout << '\n';
    }

    return 0;
}
