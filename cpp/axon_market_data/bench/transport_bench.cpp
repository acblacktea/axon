// One hop, two transports: the same 20-level book event from the service's
// publish call to a strategy holding the prices, over shared memory and over
// ZMQ. Writer and reader are threads pinned to separate cores, the reader
// busy-polling, as a latency-sensitive strategy would.
//
//   shm: binary record into the ring -> reader copies it out
//   zmq: glaze JSON encode -> PUB/SUB over tcp loopback -> simdjson decode
//
// Events are paced, not blasted: this measures latency, not throughput, and
// a queue that is always full measures the queue.
//
// Usage: mds_transport_bench [events=20000] [gap_us=20]

#include <pthread.h>
#include <sched.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <string>
#include <thread>
#include <vector>

#include <simdjson.h>
#include <zmq.hpp>

#include "axon/market_data/shm_feed.h"
#include "axon/market_data/shm_publisher.h"
#include "axon/market_data/zmq_publisher.h"

using namespace axon::market_data;
namespace shm = axon::market_data::shm;

namespace {

void pin(int core) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core % static_cast<int>(std::thread::hardware_concurrency()), &set);
    pthread_setaffinity_np(pthread_self(), sizeof set, &set);
}

MarketDataEvent book(std::int64_t seq) {
    OrderbookSnapshot s;
    s.symbol = "BTC_USDT_PERP";
    s.exchange = "binance_usdt_futures";
    for (int i = 0; i < 20; ++i) {
        s.bids.push_back({64000.0 - i * 0.1, 0.5 + i});
        s.asks.push_back({64000.1 + i * 0.1, 0.7 + i});
    }
    s.sequence = seq;
    MarketDataEvent e;
    e.data_type = DataType::Depth;
    e.event_type = "update";
    e.symbol = s.symbol;
    e.exchange = s.exchange;
    e.data = std::move(s);
    return e;
}

void report(const char* name, std::vector<double> us, std::size_t sent) {
    std::sort(us.begin(), us.end());
    auto at = [&](double p) { return us.empty() ? 0.0 : us[static_cast<std::size_t>(p * (us.size() - 1))]; };
    std::printf("%-4s  received %zu/%zu   p50 %8.2f us   p99 %8.2f us   p99.9 %8.2f us   max %8.2f us\n",
                name, us.size(), sent, at(0.5), at(0.99), at(0.999), us.empty() ? 0.0 : us.back());
}

void spin_until(std::int64_t t_ns) {
    while (shm::steady_ns() < t_ns) {
    }
}

void bench_shm(int events, int gap_us) {
    const std::string name = "axon_mds_bench_" + std::to_string(::getpid());
    ShmConfig cfg;
    cfg.name = name;
    cfg.capacity_mb = 16;
    ShmPublisher pub(cfg);
    shm::Reader reader(name);

    std::vector<double> latency;
    latency.reserve(static_cast<std::size_t>(events));
    std::atomic<bool> ready{false};
    std::atomic<bool> done{false};

    std::thread consumer([&] {
        pin(3);
        ready.store(true);
        double sink = 0;
        while (!done.load(std::memory_order_acquire) || reader.backlog() > 0) {
            reader.poll([&](const shm::RecordView& v) {
                sink += v.bids()[0].price + v.asks()[0].price;  // the prices, in hand
                latency.push_back(static_cast<double>(shm::steady_ns() - v.event().publish_ns) / 1e3);
            });
        }
        if (sink == 42) std::printf(" ");
    });
    while (!ready.load()) {
    }

    pin(2);
    std::vector<MarketDataEvent> evs;
    for (int i = 0; i < events; ++i) evs.push_back(book(i + 1));
    std::int64_t next = shm::steady_ns();
    for (const auto& e : evs) {
        next += gap_us * 1000;
        spin_until(next);
        pub.publish(e);
    }
    done.store(true, std::memory_order_release);
    consumer.join();
    shm::unlink(name);
    report("shm", std::move(latency), static_cast<std::size_t>(events));
}

void bench_zmq(int events, int gap_us) {
    const std::string endpoint = "tcp://127.0.0.1:5599";
    ZmqPublisher pub(endpoint);
    pub.start();

    // Send times by book sequence, written before each publish and read by the
    // consumer after it parses that sequence back out of the JSON.
    std::vector<std::atomic<std::int64_t>> sent_at(static_cast<std::size_t>(events) + 1);
    std::vector<double> latency;
    latency.reserve(static_cast<std::size_t>(events));
    std::atomic<bool> ready{false};
    std::atomic<bool> done{false};

    std::thread consumer([&] {
        pin(3);
        zmq::context_t ctx(1);
        zmq::socket_t sub(ctx, zmq::socket_type::sub);
        sub.set(zmq::sockopt::rcvhwm, 1'000'000);
        sub.connect(endpoint);
        sub.set(zmq::sockopt::subscribe, "");
        std::this_thread::sleep_for(std::chrono::milliseconds(300));  // the slow joiner
        ready.store(true);
        simdjson::ondemand::parser parser;
        double sink = 0;
        while (true) {
            zmq::message_t topic;
            if (!sub.recv(topic, zmq::recv_flags::dontwait)) {
                if (done.load(std::memory_order_acquire)) break;
                continue;
            }
            zmq::message_t body;
            (void)sub.recv(body, zmq::recv_flags::none);
            simdjson::padded_string json(static_cast<const char*>(body.data()), body.size());
            auto doc = parser.iterate(json);
            // The same work the shm consumer does: the prices in hand.
            double b = 0;
            double a = 0;
            for (auto level : doc["bids"].get_array()) { b = level["price"].get_double(); break; }
            for (auto level : doc["asks"].get_array()) { a = level["price"].get_double(); break; }
            const auto seq = doc["sequence"].get_int64().value();
            sink += a + b;
            latency.push_back(static_cast<double>(shm::steady_ns() - sent_at[static_cast<std::size_t>(seq)].load()) / 1e3);
        }
        if (sink == 42) std::printf(" ");
    });
    while (!ready.load()) {
    }

    pin(2);
    std::vector<MarketDataEvent> evs;
    for (int i = 0; i < events; ++i) evs.push_back(book(i + 1));
    std::int64_t next = shm::steady_ns();
    for (int i = 0; i < events; ++i) {
        next += gap_us * 1000;
        spin_until(next);
        sent_at[static_cast<std::size_t>(i + 1)].store(shm::steady_ns());
        pub.publish(evs[static_cast<std::size_t>(i)]);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    done.store(true, std::memory_order_release);
    consumer.join();
    pub.stop();
    report("zmq", std::move(latency), static_cast<std::size_t>(events));
}

}  // namespace

int main(int argc, char** argv) {
    const int events = argc > 1 ? std::atoi(argv[1]) : 20000;
    const int gap_us = argc > 2 ? std::atoi(argv[2]) : 20;
    std::printf("%d events, 20-level book, one every %d us, writer core 2 -> reader core 3\n",
                events, gap_us);
    bench_shm(events, gap_us);
    bench_zmq(events, gap_us);
}
