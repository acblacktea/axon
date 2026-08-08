#include "calais/transport/zmq_server.h"

#include <zmq.hpp>

#include <cerrno>
#include <optional>
#include <stdexcept>
#include <vector>

#include "calais/transport/wire.h"
#include "calais/util/logging.h"

namespace calais::transport {
namespace {

auto& log() {
  static auto logger = util::get_logger("transport.zmq");
  return logger;
}

}  // namespace

struct ZmqServer::Impl {
  zmq::context_t context{1};
  std::unique_ptr<zmq::socket_t> router;
  std::unique_ptr<zmq::socket_t> pub;
};

ZmqServer::ZmqServer() : impl_(std::make_unique<Impl>()) {}

ZmqServer::~ZmqServer() { stop(); }

void ZmqServer::start(const ZmqConfig& config, CommandHandler handler) {
  handler_ = std::move(handler);

  impl_->router = std::make_unique<zmq::socket_t>(impl_->context, zmq::socket_type::router);
  impl_->pub = std::make_unique<zmq::socket_t>(impl_->context, zmq::socket_type::pub);

  // Never block on send. A strategy that has gone away must not be able to
  // stall the engine loop, which is exactly what an unbounded PUB queue would
  // do once the high-water mark is reached.
  impl_->router->set(zmq::sockopt::sndhwm, 10000);
  impl_->pub->set(zmq::sockopt::sndhwm, 100000);
  // Drop pending messages immediately on close rather than waiting.
  impl_->router->set(zmq::sockopt::linger, 0);
  impl_->pub->set(zmq::sockopt::linger, 0);

  try {
    impl_->router->bind(config.router_endpoint);
    impl_->pub->bind(config.pub_endpoint);
  } catch (const zmq::error_t& e) {
    throw std::runtime_error("could not bind ZMQ endpoints (" +
                             config.router_endpoint + ", " + config.pub_endpoint +
                             "): " + e.what());
  }

  running_ = true;
  CALAIS_LOG_INFO(log(), "ROUTER bound on {}, PUB bound on {}",
                  config.router_endpoint, config.pub_endpoint);
}

void ZmqServer::stop() {
  if (!running_) {
    return;
  }
  running_ = false;
  impl_->router.reset();
  impl_->pub.reset();
  CALAIS_LOG_INFO(log(), "ZMQ transport stopped");
}

std::size_t ZmqServer::poll() {
  if (!running_) {
    return 0;
  }

  std::size_t handled = 0;
  for (;;) {
    // ROUTER delivers [identity, empty, payload]. Read the whole multipart or
    // none of it -- a partially consumed message would desynchronise the
    // socket for every subsequent command.
    std::vector<zmq::message_t> frames;
    zmq::message_t frame;
    std::optional<std::size_t> first;
    try {
      first = impl_->router->recv(frame, zmq::recv_flags::dontwait);
    } catch (const zmq::error_t& e) {
      // EINTR: a signal arrived mid-syscall. That is how shutdown works --
      // SIGINT lands here -- and it is not an error. Letting it propagate made
      // Ctrl-C exit through the fatal path with "Interrupted system call".
      if (e.num() == EINTR) {
        break;
      }
      throw;
    }
    if (!first.has_value()) {
      break;  // nothing queued
    }
    frames.push_back(std::move(frame));
    while (frames.back().more()) {
      zmq::message_t next;
      if (!impl_->router->recv(next, zmq::recv_flags::none).has_value()) {
        break;
      }
      frames.push_back(std::move(next));
    }

    if (frames.size() < 3) {
      CALAIS_LOG_WARN(log(), "malformed ROUTER message: {} frames", frames.size());
      continue;
    }

    const std::string_view payload(static_cast<const char*>(frames[2].data()),
                                   frames[2].size());
    const auto command = deserialize_command(payload);
    if (!command.has_value()) {
      CALAIS_LOG_WARN(log(), "could not parse a command frame");
      continue;
    }

    std::string identity(static_cast<const char*>(frames[0].data()),
                         frames[0].size());
    ResponseSink sink(this, identity);
    try {
      handler_(*command, sink);
    } catch (const std::exception& e) {
      // An exception from a handler becomes an unsuccessful response, not a
      // dropped request: a strategy waiting on a reply that never comes is a
      // far worse failure than one that gets an error.
      send_response(identity, Response::fail(command->request_id, e.what()));
    }

    ++handled;
    ++commands_handled_;
  }
  return handled;
}

void ZmqServer::ResponseSink::operator()(const Response& response) {
  if (server_ == nullptr) {
    return;
  }
  if (sent_) {
    CALAIS_LOG_WARN(log(), "a command was answered twice; dropping the second");
    return;
  }
  sent_ = true;
  server_->send_response(identity_, response);
}

void ZmqServer::send_response(const std::string& identity,
                              const Response& response) {
  if (!running_) {
    return;
  }
  const std::string bytes = serialize_response(response);
  impl_->router->send(zmq::message_t(identity.data(), identity.size()),
                      zmq::send_flags::sndmore | zmq::send_flags::dontwait);
  impl_->router->send(zmq::message_t(),
                      zmq::send_flags::sndmore | zmq::send_flags::dontwait);
  impl_->router->send(zmq::message_t(bytes.data(), bytes.size()),
                      zmq::send_flags::dontwait);
}

void ZmqServer::publish(const Event& event) {
  if (!running_) {
    return;
  }
  const std::string bytes = serialize_event(event);
  try {
    // Topic first, so a SUB socket can filter on strategy_id without receiving
    // every other strategy's traffic.
    impl_->pub->send(zmq::message_t(event.strategy_id.data(), event.strategy_id.size()),
                     zmq::send_flags::sndmore | zmq::send_flags::dontwait);
    impl_->pub->send(zmq::message_t(bytes.data(), bytes.size()),
                     zmq::send_flags::dontwait);
    ++events_published_;
  } catch (const zmq::error_t& e) {
    // Dropping an event beats blocking the engine loop on a slow subscriber.
    CALAIS_LOG_WARN(log(), "could not publish event: {}", e.what());
  }
}

}  // namespace calais::transport
