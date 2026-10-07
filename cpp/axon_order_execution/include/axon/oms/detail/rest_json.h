// REST response parsing shared by the four VenueRest implementations.
//
// Internal to venue_rest.cpp; a header only so the parsing can be tested
// without a network. Nothing outside oms/ should include it.

#pragma once

#include <optional>
#include <string>
#include <vector>

#include "axon/net/http_client.h"
#include "axon/venue/json_view.h"

namespace axon::oms::detail {

inline venue::Document& doc() {
  static thread_local venue::Document d(1u << 20);
  return d;
}

inline std::string http_error(const net::HttpResponse& r) {
  if (!r.error.empty()) {
    return r.error;
  }
  return "HTTP " + std::to_string(r.status) + ": " + r.body;
}

// Walks a JSON array at `path` (a chain of object keys) and parses each
// element. An empty `path` means the whole body is the array -- Binance's
// shape for openOrders, positionRisk and userTrades.
template <typename Item, typename Parse, typename Callback>
void parse_array_at(const net::HttpResponse& r,
                    const std::vector<std::string>& path, Parse parse,
                    Callback callback) {
  if (!r.error.empty() || !r.ok()) {
    callback(std::vector<Item>{}, http_error(r));
    return;
  }

  std::optional<venue::Array> array;
  if (path.empty()) {
    // The whole document is the array. It must be wrapped BEFORE anything
    // tries to read it as an object: Document::parse_copy yields an object,
    // so a top-level array fails there -- which is how every Binance snapshot
    // used to come back as "malformed response".
    auto wrapped = doc().parse_copy("{\"_\":" + r.body + "}");
    if (!wrapped.has_value()) {
      callback(std::vector<Item>{}, "malformed array response");
      return;
    }
    array = (*wrapped)["_"].as_array();
  } else {
    auto root = doc().parse_copy(r.body);
    if (!root.has_value()) {
      callback(std::vector<Item>{}, "malformed response");
      return;
    }
    venue::Object current = *root;
    for (std::size_t i = 0; i + 1 < path.size(); ++i) {
      auto next = current[path[i]].as_object();
      if (!next.has_value()) {
        callback(std::vector<Item>{}, "response is missing '" + path[i] + "'");
        return;
      }
      current = *next;
    }
    array = current[path.back()].as_array();
  }

  if (!array.has_value()) {
    // An empty or absent list is not an error: no open orders is a normal
    // answer and must not look like a failed snapshot.
    callback(std::vector<Item>{}, {});
    return;
  }
  std::vector<Item> items;
  array->for_each_object([&](venue::Object& obj) {
    if (auto item = parse(obj); item.has_value()) {
      items.push_back(std::move(*item));
    }
  });
  callback(std::move(items), {});
}

}  // namespace axon::oms::detail
