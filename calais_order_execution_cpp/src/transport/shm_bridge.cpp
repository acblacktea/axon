#include "calais/transport/shm_bridge.h"

#include <cstring>

#include "calais/util/logging.h"

namespace calais::transport {
namespace {

auto& log() {
  static auto logger = util::get_logger("transport.shm");
  return logger;
}

}  // namespace

void ShmBridge::start(const std::string& directory,
                      const std::vector<std::string>& strategies,
                      std::uint32_t slot_count) {
  stop();
  for (const auto& name : strategies) {
    Pair pair;
    // The engine CREATES both rings; strategies attach. Creating truncates, so
    // a restart never inherits cursors from a crashed run.
    pair.events = std::make_unique<core::ShmRing>(core::ShmRing::create(
        directory + "/calais." + name + ".events", kMaxHotMessageBytes, slot_count));
    pair.commands = std::make_unique<core::ShmRing>(core::ShmRing::create(
        directory + "/calais." + name + ".commands", kMaxHotMessageBytes,
        slot_count));
    // The engine owns the files; attaching strategies must not unlink them.
    pair.events->set_unlink_on_destroy(true);
    pair.commands->set_unlink_on_destroy(true);

    CALAIS_LOG_INFO(log(), "shm fast path for '{}': {} slots x {} bytes", name,
                    slot_count, kMaxHotMessageBytes);
    rings_.emplace(name, std::move(pair));
  }
}

void ShmBridge::stop() { rings_.clear(); }

template <typename Msg>
void ShmBridge::publish(const Msg& msg, std::string_view strategy,
                        HotMsgType type) {
  if (rings_.empty()) {
    return;
  }

  auto push_to = [&](Pair& pair) {
    void* slot = pair.events->try_acquire_write();
    if (slot == nullptr) {
      // The strategy has stopped reading. Dropping is deliberate: blocking
      // here would stall every venue feed on one wedged consumer.
      ++stats_.events_dropped;
      return;
    }
    auto* out = static_cast<Msg*>(slot);
    *out = msg;
    // Stamp AFTER the copy, so the timestamp is when it actually went into the
    // ring rather than when we started thinking about it.
    out->hdr.stamp(kStampEnqueued);
    pair.events->commit_write(sizeof(Msg));
    ++stats_.events_published;
  };

  if (strategy.empty()) {
    // No strategy id: nobody owns it, so everyone listening gets it.
    for (auto& [_, pair] : rings_) {
      push_to(pair);
    }
    return;
  }
  const auto it = rings_.find(std::string(strategy));
  if (it != rings_.end()) {
    push_to(it->second);
  }
  static_cast<void>(type);
}

void ShmBridge::publish_order(const OrderUpdateMsg& msg, std::string_view strategy) {
  publish(msg, strategy, HotMsgType::kOrderUpdate);
}

void ShmBridge::publish_fill(const FillMsg& msg, std::string_view strategy) {
  publish(msg, strategy, HotMsgType::kFill);
}

std::size_t ShmBridge::poll() {
  std::size_t handled = 0;

  for (auto& [name, pair] : rings_) {
    for (;;) {
      std::uint32_t len = 0;
      const void* raw = pair.commands->try_acquire_read(len);
      if (raw == nullptr) {
        break;
      }

      // The payoff of the fixed layout: consuming is a cast, not a parse.
      const auto* header = static_cast<const HotHeader*>(raw);
      const HotMsgType type = header->kind();

      switch (type) {
        case HotMsgType::kPlaceOrder:
          if (len == sizeof(PlaceOrderMsg) && on_place_) {
            PlaceOrderMsg msg;
            std::memcpy(&msg, raw, sizeof(msg));
            msg.hdr.stamp(kStampDequeued);
            on_place_(name, msg);
            msg.hdr.stamp(kStampHandled);
            // Fold the journey into the histograms. The stamps travelled WITH
            // the message, so this measures the real path end to end rather
            // than a benchmark standing in for it.
            latency_.record(msg.hdr.stamps, kHotStampSlots);
          }
          break;

        case HotMsgType::kCancelOrder:
          if (len == sizeof(CancelOrderMsg) && on_cancel_) {
            CancelOrderMsg msg;
            std::memcpy(&msg, raw, sizeof(msg));
            msg.hdr.stamp(kStampDequeued);
            on_cancel_(name, msg);
            msg.hdr.stamp(kStampHandled);
            latency_.record(msg.hdr.stamps, kHotStampSlots);
          }
          break;

        case HotMsgType::kHeartbeat:
          break;

        default:
          CALAIS_LOG_WARN(log(), "[{}] unexpected hot message type {}", name,
                          static_cast<int>(type));
          break;
      }

      pair.commands->commit_read();
      ++handled;
      ++stats_.commands_received;
    }
  }
  return handled;
}

}  // namespace calais::transport
