// ZMQ protocol messages. Mirrors models/messages.py.
//
// DELIBERATE STRUCTURAL DEVIATION from the Python layout: Python keeps these
// in models/, we keep them in transport/. Two reasons:
//
//   1. Their payload is arbitrary JSON, so the definition has to depend on
//      nlohmann. Putting them in models/ would drag a JSON library into every
//      translation unit that merely wants an Order -- including hot-path code,
//      where nothing should be able to reach a DOM parser by accident.
//
//   2. They are protocol, not domain. An Order survives a change of transport;
//      a Command does not.
//
// `command_type` and `event_type` are strings on the wire, not enums, and are
// kept as strings here on purpose. A Python strategy running newer code may
// send a command this build has never heard of; that must produce a clean
// "unknown command" response, not a parse failure that kills the connection.

#pragma once

#include <optional>
#include <string>

#include <nlohmann/json.hpp>

#include "axon/models/enums.h"
#include "axon/models/ids.h"

namespace axon::transport {

// ordered_json, NOT json: nlohmann's default json sorts object keys, which
// would reorder every message relative to Python's insertion-ordered dicts and
// make byte-level comparison between the two implementations impossible.
using Json = nlohmann::ordered_json;

// Strategy -> engine (ZMQ DEALER -> ROUTER).
struct Command {
  std::string command_type;
  Json payload = Json::object();
  std::string request_id = models::generate_internal_id();
  std::string strategy_id;

  std::optional<models::CommandType> typed() const noexcept {
    return models::command_type_from_string(command_type);
  }
};

// Engine -> strategy (ROUTER -> DEALER).
struct Response {
  std::string request_id;
  bool success = false;
  Json data;  // null unless set; may be an object, array, or scalar
  std::optional<std::string> error;

  static Response ok(std::string request_id_, Json data_ = Json()) {
    Response r;
    r.request_id = std::move(request_id_);
    r.success = true;
    r.data = std::move(data_);
    return r;
  }

  static Response fail(std::string request_id_, std::string error_) {
    Response r;
    r.request_id = std::move(request_id_);
    r.success = false;
    r.error = std::move(error_);
    return r;
  }
};

// Engine -> strategies (ZMQ PUB -> SUB). `strategy_id` doubles as the PUB
// topic so a strategy only receives its own updates.
struct Event {
  std::string event_type;
  Json data = Json::object();
  std::string strategy_id;

  std::optional<models::EventType> typed() const noexcept {
    return models::event_type_from_string(event_type);
  }
};

}  // namespace axon::transport
