// JSON codec for the ZMQ control plane.
//
// COMPATIBILITY CONTRACT: for any object, this codec must produce the same
// keys, in the same order, with the same values as the Python
// implementation's transport/serialization.py. That is what lets a Python
// StrategyClient talk to this engine unchanged, which in turn is what makes an
// incremental port possible at all -- port the engine, keep the strategies,
// compare.
//
// Three things this depends on, all easy to break:
//   - key order matches Python dataclass field declaration order (hence
//     ordered_json and the fixed emit order in wire.cpp)
//   - enums emit their .value string
//   - datetimes emit datetime.isoformat(), including its rule of dropping the
//     fractional part when microseconds are zero
//
// NOT byte-identical, for two reasons that are both semantically irrelevant to
// JSON but would break a naive strcmp:
//   - Python's json.dumps defaults to ", " / ": " separators; nlohmann's
//     dump() emits compact. The cross-implementation test therefore compares
//     against json.dumps(..., separators=(',', ':')).
//   - Python's json.dumps defaults to ensure_ascii=True and escapes non-ASCII
//     as \uXXXX; nlohmann emits raw UTF-8. Every identifier we exchange is
//     ASCII, so this does not arise in practice -- but do not introduce a
//     non-ASCII label and expect the two to agree.
//
// NOT THE HOT PATH. This builds a DOM and allocates. Order entry uses
// transport/hot_messages.h. If you find yourself calling anything in this
// header from a busy-poll loop, that is the bug.

#pragma once

#include <optional>
#include <string>
#include <string_view>

#include "axon/models/fill.h"
#include "axon/models/order.h"
#include "axon/models/portfolio.h"
#include "axon/transport/messages.h"

namespace axon::transport {

// --- domain -> JSON --------------------------------------------------------
Json to_json(const models::Ticker& v);
Json to_json(const models::OrderRequest& v);
Json to_json(const models::Order& v);
Json to_json(const models::Fill& v);
Json to_json(const models::AccountSummary& v);
Json to_json(const models::Position& v);
Json to_json(const Command& v);
Json to_json(const Response& v);
Json to_json(const Event& v);

// --- JSON -> domain --------------------------------------------------------
//
// nullopt means "this is not a valid X": a required field was missing, null,
// or of the wrong type. Optional fields that are absent or null take their
// default. Unknown extra keys are ignored, so a newer Python peer adding a
// field does not break an older C++ engine.
std::optional<models::Ticker> ticker_from_json(const Json& j);
std::optional<models::OrderRequest> order_request_from_json(const Json& j);
std::optional<models::Order> order_from_json(const Json& j);
std::optional<models::Fill> fill_from_json(const Json& j);
std::optional<models::AccountSummary> account_summary_from_json(const Json& j);
std::optional<models::Position> position_from_json(const Json& j);
std::optional<Command> command_from_json(const Json& j);
std::optional<Response> response_from_json(const Json& j);
std::optional<Event> event_from_json(const Json& j);

// --- protocol messages as ZMQ frame bytes ----------------------------------
std::string serialize_command(const Command& v);
std::string serialize_response(const Response& v);
std::string serialize_event(const Event& v);

// Return nullopt on malformed JSON as well as on schema violations. A garbage
// frame from the network must never throw out of the receive loop.
std::optional<Command> deserialize_command(std::string_view bytes);
std::optional<Response> deserialize_response(std::string_view bytes);
std::optional<Event> deserialize_event(std::string_view bytes);

}  // namespace axon::transport
