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

// One object at `path` (empty: the whole body), parsed into a single item.
// For the single-order lookups. A venue error arrives as an HTTP error and is
// reported as one; a body that parses but is not an order is reported too,
// rather than read as "no such order".
template <typename Item, typename Parse, typename Callback>
void parse_object_at(const net::HttpResponse& r,
                     const std::vector<std::string>& path, Parse parse,
                     Callback callback) {
  if (!r.error.empty() || !r.ok()) {
    callback(std::optional<Item>{}, http_error(r));
    return;
  }
  auto root = doc().parse_copy(r.body);
  if (!root.has_value()) {
    callback(std::optional<Item>{}, "malformed response");
    return;
  }
  venue::Object current = *root;
  for (const auto& key : path) {
    auto next = current[key].as_object();
    if (!next.has_value()) {
      callback(std::optional<Item>{}, "response is missing '" + key + "'");
      return;
    }
    current = *next;
  }
  auto item = parse(current);
  if (!item.has_value()) {
    callback(std::optional<Item>{}, "response is not a recognisable item");
    return;
  }
  callback(std::move(item), {});
}

// The first element of the array at `path`. The venues that answer a
// single-order lookup with a one-element list (Bybit, OKX) go through here;
// an empty list means the venue does not know the order.
template <typename Item, typename Parse, typename Callback>
void first_of_array_at(const net::HttpResponse& r,
                       const std::vector<std::string>& path, Parse parse,
                       Callback callback) {
  parse_array_at<Item>(r, path, parse,
                       [&](std::vector<Item> items, const std::string& error) {
                         if (!error.empty()) {
                           callback(std::optional<Item>{}, error);
                         } else if (items.empty()) {
                           callback(std::optional<Item>{}, "not found");
                         } else {
                           callback(std::optional<Item>(std::move(items.front())), {});
                         }
                       });
}

}  // namespace axon::oms::detail
