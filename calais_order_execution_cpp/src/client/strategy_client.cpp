#include "calais/client/strategy_client.h"

#include <zmq.hpp>

#include <cstring>

#include "calais/core/clock.h"
#include "calais/transport/wire.h"
#include "calais/util/logging.h"

namespace calais::client {
namespace {

auto& log() {
  static auto logger = util::get_logger("client");
  return logger;
}

}  // namespace

struct StrategyClient::Impl {
  zmq::context_t context{1};
  std::unique_ptr<zmq::socket_t> dealer;
  std::unique_ptr<zmq::socket_t> sub;

  // Fast path. Attached, not created -- the engine owns these files.
  std::unique_ptr<core::ShmRing> events;
  std::unique_ptr<core::ShmRing> commands;
  std::uint64_t sequence = 0;
};

StrategyClient::StrategyClient() : impl_(std::make_unique<Impl>()) {}
StrategyClient::~StrategyClient() { close(); }

void StrategyClient::connect(const StrategyClientConfig& config,
                             StrategyEvents events) {
  config_ = config;
  events_ = std::move(events);

  impl_->dealer =
      std::make_unique<zmq::socket_t>(impl_->context, zmq::socket_type::dealer);
  impl_->dealer->set(zmq::sockopt::linger, 0);
  impl_->dealer->set(zmq::sockopt::rcvtimeo, config_.request_timeout_ms);
  impl_->dealer->connect(config_.router_endpoint);

  impl_->sub = std::make_unique<zmq::socket_t>(impl_->context, zmq::socket_type::sub);
  impl_->sub->set(zmq::sockopt::linger, 0);
  // Subscribe to this strategy's topic only. The engine publishes with
  // strategy_id as the topic precisely so this filter happens server-side.
  impl_->sub->set(zmq::sockopt::subscribe, config_.strategy_id);
  impl_->sub->connect(config_.pub_endpoint);

  if (!config_.shm_directory.empty()) {
    try {
      // ATTACH, never create: the engine owns the rings and creating here
      // would truncate them out from under it.
      impl_->events = std::make_unique<core::ShmRing>(core::ShmRing::open(
          config_.shm_directory + "/calais." + config_.strategy_id + ".events"));
      impl_->commands = std::make_unique<core::ShmRing>(core::ShmRing::open(
          config_.shm_directory + "/calais." + config_.strategy_id + ".commands"));
      CALAIS_LOG_INFO(log(), "fast path attached for '{}'", config_.strategy_id);
    } catch (const std::exception& e) {
      // Falling back is correct but must be LOUD: a strategy silently running
      // 100x slower than it thinks is worse than one that fails to start.
      CALAIS_LOG_WARN(log(),
                      "fast path unavailable ({}); falling back to ZMQ, which is "
                      "roughly 100x slower per order",
                      e.what());
      impl_->events.reset();
      impl_->commands.reset();
    }
  }
}

void StrategyClient::close() {
  if (impl_) {
    impl_->events.reset();
    impl_->commands.reset();
    impl_->dealer.reset();
    impl_->sub.reset();
  }
}

bool StrategyClient::fast_path_available() const noexcept {
  return impl_ && impl_->events && impl_->commands;
}

// ---------------------------------------------------------------------------
bool StrategyClient::place_order_fast(const std::string& exchange,
                                      const models::OrderRequest& request) {
  if (!fast_path_available()) {
    return false;
  }
  transport::PlaceOrderMsg msg{};
  if (!transport::encode_place_order(request, exchange, ++impl_->sequence, msg)) {
    ++stats_.fast_rejected;
    return false;
  }
  // Stamp immediately before the push, so kStampOrigin -> kStampEnqueued
  // measures our own encode and nothing else.
  msg.hdr.stamp(transport::kStampEnqueued);
  if (!impl_->commands->try_push(&msg, sizeof(msg))) {
    // The engine is not draining. Backpressure, not an error -- the caller
    // decides whether to retry or fall back to the control plane.
    ++stats_.fast_rejected;
    return false;
  }
  ++stats_.fast_sent;
  return true;
}

bool StrategyClient::cancel_order_fast(const std::string& exchange,
                                       const std::string& instrument,
                                       const std::string& order_id) {
  if (!fast_path_available()) {
    return false;
  }
  transport::CancelOrderMsg msg{};
  if (!transport::encode_cancel_order(exchange, instrument, order_id, {},
                                      config_.strategy_id, ++impl_->sequence, msg)) {
    ++stats_.fast_rejected;
    return false;
  }
  msg.hdr.stamp(transport::kStampEnqueued);
  if (!impl_->commands->try_push(&msg, sizeof(msg))) {
    ++stats_.fast_rejected;
    return false;
  }
  ++stats_.fast_sent;
  return true;
}

// ---------------------------------------------------------------------------
std::size_t StrategyClient::poll() {
  std::size_t handled = 0;

  // Fast path first: it is the one with a latency budget.
  if (fast_path_available()) {
    for (;;) {
      std::uint32_t len = 0;
      const void* raw = impl_->events->try_acquire_read(len);
      if (raw == nullptr) {
        break;
      }
      const auto* header = static_cast<const transport::HotHeader*>(raw);
      switch (header->kind()) {
        case transport::HotMsgType::kOrderUpdate:
          if (len == sizeof(transport::OrderUpdateMsg)) {
            const auto* msg = static_cast<const transport::OrderUpdateMsg*>(raw);
            if (events_.on_order_fast) {
              events_.on_order_fast(*msg);
            }
            if (events_.on_order) {
              // Decoding allocates; only pay for it if the strategy asked for
              // the decoded form.
              events_.on_order(transport::decode_order_update(*msg));
            }
          }
          break;
        case transport::HotMsgType::kFill:
          if (len == sizeof(transport::FillMsg)) {
            const auto* msg = static_cast<const transport::FillMsg*>(raw);
            if (events_.on_fill_fast) {
              events_.on_fill_fast(*msg);
            }
            if (events_.on_fill) {
              events_.on_fill(transport::decode_fill(*msg));
            }
          }
          break;
        default:
          break;
      }
      impl_->events->commit_read();
      ++handled;
      ++stats_.fast_events;
    }
  }

  // Control-plane events. Same domain callbacks, so a strategy does not care
  // which transport a given update arrived on.
  if (impl_->sub) {
    for (;;) {
      zmq::message_t topic;
      const auto got = impl_->sub->recv(topic, zmq::recv_flags::dontwait);
      if (!got.has_value()) {
        break;
      }
      zmq::message_t body;
      if (!topic.more() ||
          !impl_->sub->recv(body, zmq::recv_flags::none).has_value()) {
        continue;
      }
      const auto event = transport::deserialize_event(
          std::string_view(static_cast<const char*>(body.data()), body.size()));
      if (!event.has_value()) {
        continue;
      }
      if (event->event_type == "order_update" && events_.on_order) {
        if (auto order = transport::order_from_json(event->data); order.has_value()) {
          events_.on_order(*order);
        }
      } else if (event->event_type == "fill_update" && events_.on_fill) {
        if (auto fill = transport::fill_from_json(event->data); fill.has_value()) {
          events_.on_fill(*fill);
        }
      }
      ++handled;
    }
  }
  return handled;
}

// ---------------------------------------------------------------------------
std::optional<transport::Json> StrategyClient::request(
    const std::string& command_type, const transport::Json& payload,
    std::string& error) {
  if (!impl_->dealer) {
    error = "not connected";
    return std::nullopt;
  }
  ++stats_.requests;

  transport::Command command;
  command.command_type = command_type;
  command.payload = payload;
  command.strategy_id = config_.strategy_id;
  const std::string bytes = transport::serialize_command(command);

  try {
    // DEALER prepends its own identity; the empty delimiter matches what the
    // Python client sends and what the ROUTER expects.
    impl_->dealer->send(zmq::message_t(), zmq::send_flags::sndmore);
    impl_->dealer->send(zmq::message_t(bytes.data(), bytes.size()),
                        zmq::send_flags::none);

    zmq::message_t frame;
    std::string last;
    // Read the whole multipart; the reply body is the final frame.
    for (;;) {
      if (!impl_->dealer->recv(frame, zmq::recv_flags::none).has_value()) {
        error = "request timed out";
        ++stats_.request_failures;
        return std::nullopt;
      }
      last.assign(static_cast<const char*>(frame.data()), frame.size());
      if (!frame.more()) {
        break;
      }
    }

    const auto response = transport::deserialize_response(last);
    if (!response.has_value()) {
      error = "malformed response";
      ++stats_.request_failures;
      return std::nullopt;
    }
    if (!response->success) {
      error = response->error.value_or("request failed");
      ++stats_.request_failures;
      return std::nullopt;
    }
    return response->data;
  } catch (const zmq::error_t& e) {
    error = e.what();
    ++stats_.request_failures;
    return std::nullopt;
  }
}

std::optional<models::Order> StrategyClient::place_order(
    const std::string& exchange, const models::OrderRequest& req,
    std::string& error) {
  transport::Json payload = transport::Json::object();
  payload["exchange"] = exchange;
  payload["request"] = transport::to_json(req);
  const auto data = request("place_order", payload, error);
  if (!data.has_value() || data->is_null()) {
    return std::nullopt;
  }
  return transport::order_from_json(*data);
}

bool StrategyClient::cancel_order(const std::string& exchange,
                                  const std::string& order_id, std::string& error) {
  transport::Json payload = transport::Json::object();
  payload["exchange"] = exchange;
  payload["order_id"] = order_id;
  return request("cancel_order", payload, error).has_value();
}

std::optional<models::Order> StrategyClient::modify_order(
    const std::string& exchange, const std::string& order_id,
    std::optional<core::Qty> amount, std::optional<core::Price> price,
    std::string& error) {
  transport::Json payload = transport::Json::object();
  payload["exchange"] = exchange;
  payload["order_id"] = order_id;
  if (amount.has_value()) {
    payload["amount"] = amount->to_double();
  }
  if (price.has_value()) {
    payload["price"] = price->to_double();
  }
  const auto data = request("modify_order", payload, error);
  if (!data.has_value() || data->is_null()) {
    return std::nullopt;
  }
  return transport::order_from_json(*data);
}

std::optional<models::Order> StrategyClient::get_order(const std::string& order_id,
                                                       std::string& error) {
  transport::Json payload = transport::Json::object();
  payload["order_id"] = order_id;
  const auto data = request("get_order", payload, error);
  if (!data.has_value() || data->is_null()) {
    return std::nullopt;
  }
  return transport::order_from_json(*data);
}

std::vector<models::Order> StrategyClient::get_active_orders(std::string& error) {
  std::vector<models::Order> out;
  const auto data = request("get_active_orders", transport::Json::object(), error);
  if (!data.has_value() || !data->is_array()) {
    return out;
  }
  for (const auto& item : *data) {
    if (auto order = transport::order_from_json(item); order.has_value()) {
      out.push_back(*order);
    }
  }
  return out;
}

std::optional<models::Ticker> StrategyClient::get_ticker(
    const std::string& exchange, const std::string& instrument, std::string& error) {
  transport::Json payload = transport::Json::object();
  payload["exchange"] = exchange;
  payload["instrument"] = instrument;
  const auto data = request("get_ticker", payload, error);
  if (!data.has_value() || data->is_null()) {
    return std::nullopt;
  }
  return transport::ticker_from_json(*data);
}

std::vector<models::Position> StrategyClient::get_positions(
    const std::string& exchange, std::string& error) {
  std::vector<models::Position> out;
  transport::Json payload = transport::Json::object();
  payload["exchange"] = exchange;
  const auto data = request("get_positions", payload, error);
  if (!data.has_value() || !data->is_array()) {
    return out;
  }
  for (const auto& item : *data) {
    if (auto p = transport::position_from_json(item); p.has_value()) {
      out.push_back(*p);
    }
  }
  return out;
}

std::optional<models::AccountSummary> StrategyClient::get_account_summary(
    const std::string& exchange, const std::string& currency, std::string& error) {
  transport::Json payload = transport::Json::object();
  payload["exchange"] = exchange;
  payload["currency"] = currency;
  const auto data = request("get_account_summary", payload, error);
  if (!data.has_value() || data->is_null()) {
    return std::nullopt;
  }
  return transport::account_summary_from_json(*data);
}

std::vector<models::Fill> StrategyClient::get_fills_by_order(
    const std::string& order_id, std::string& error) {
  std::vector<models::Fill> out;
  transport::Json payload = transport::Json::object();
  payload["order_id"] = order_id;
  const auto data = request("get_fills_by_order", payload, error);
  if (!data.has_value() || !data->is_array()) {
    return out;
  }
  for (const auto& item : *data) {
    if (auto fill = transport::fill_from_json(item); fill.has_value()) {
      out.push_back(*fill);
    }
  }
  return out;
}

}  // namespace calais::client
