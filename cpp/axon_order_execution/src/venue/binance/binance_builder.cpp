#include "axon/venue/binance/binance_builder.h"

#include <cstring>

#include "axon/net/crypto_lite.h"

namespace axon::venue::binance {

using axon::venue::detail::Writer;

// ---------------------------------------------------------------------------
// WebSocket order entry (ws-fapi).
//
// THE WHOLE POINT OF THE STRUCTURE BELOW: Binance signs the parameters sorted
// by name, and the signature must cover exactly the values that go in the JSON.
// Two separate emitters would drift the first time someone adds a field to one
// and not the other, and the symptom is a -1022 that says nothing about which
// field disagreed. So each parameter is declared ONCE, in `each_place_param`
// and friends, and two passes walk that one declaration.
namespace {

// Characters that survive a trip through JSON unescaped, so the signed text
// and the sent text are the same bytes. Everything a real client order id
// uses is in here.
bool is_signing_safe(std::string_view s) noexcept {
  for (char c : s) {
    const auto u = static_cast<unsigned char>(c);
    const bool safe = (u >= 'A' && u <= 'Z') || (u >= 'a' && u <= 'z') ||
                      (u >= '0' && u <= '9') || c == '-' || c == '_' || c == '.';
    if (!safe) {
      return false;
    }
  }
  return true;
}

// Pass 1: key=value&key=value... -- the bytes Binance HMACs.
struct QueryPass {
  Writer& w;
  bool first = true;
  template <typename WriteValue>
  void operator()(std::string_view key, WriteValue&& write_value) noexcept {
    if (!first) {
      w.raw("&");
    }
    first = false;
    w.raw(key);
    w.raw("=");
    write_value(w);
  }
};

// Pass 2: "key":"value","key":"value"... -- the bytes that go on the wire.
// Values are quoted rather than emitted bare so the text is identical to what
// pass 1 signed.
struct JsonPass {
  Writer& w;
  bool first = true;
  template <typename WriteValue>
  void operator()(std::string_view key, WriteValue&& write_value) noexcept {
    if (!first) {
      w.raw(",");
    }
    first = false;
    w.raw("\"");
    w.raw(key);
    w.raw("\":\"");
    write_value(w);
    w.raw("\"");
  }
};

// ALPHABETICAL ORDER IS LOAD-BEARING. Binance sorts parameters by name before
// signing, so this sequence is part of the contract, not a style choice.
template <typename Emit>
void each_place_param(const models::OrderRequest& req, std::string_view api_key,
                      std::int64_t timestamp_ms, int recv_window_ms,
                      Emit&& emit) noexcept {
  const bool limit = req.order_type == models::OrderType::kLimit;
  emit("apiKey", [&](Writer& w) { w.raw(api_key); });
  if (req.label.has_value() && !req.label->empty()) {
    emit("newClientOrderId", [&](Writer& w) { w.raw(*req.label); });
  }
  if (limit) {
    emit("price", [&](Writer& w) {
      if (req.price.has_value()) {
        w.decimal(*req.price);
      } else {
        w.raw("0");
      }
    });
  }
  emit("quantity", [&](Writer& w) { w.decimal(req.amount); });
  emit("recvWindow", [&](Writer& w) { w.integer(recv_window_ms); });
  emit("side", [&](Writer& w) {
    w.raw(req.side == models::OrderSide::kBuy ? "BUY" : "SELL");
  });
  emit("symbol", [&](Writer& w) { w.raw(req.instrument); });
  if (limit) {
    // GTX is Binance's post-only, matching place_order_query above.
    emit("timeInForce", [&](Writer& w) { w.raw(req.post_only ? "GTX" : "GTC"); });
  }
  emit("timestamp", [&](Writer& w) { w.integer(timestamp_ms); });
  emit("type", [&](Writer& w) { w.raw(limit ? "LIMIT" : "MARKET"); });
}

template <typename Emit>
void each_cancel_param(std::string_view symbol, std::string_view order_id,
                       std::string_view api_key, std::int64_t timestamp_ms,
                       int recv_window_ms, Emit&& emit) noexcept {
  emit("apiKey", [&](Writer& w) { w.raw(api_key); });
  emit("orderId", [&](Writer& w) { w.raw(order_id); });
  emit("recvWindow", [&](Writer& w) { w.integer(recv_window_ms); });
  emit("symbol", [&](Writer& w) { w.raw(symbol); });
  emit("timestamp", [&](Writer& w) { w.integer(timestamp_ms); });
}

template <typename Emit>
void each_modify_param(std::string_view symbol, std::string_view order_id,
                       models::OrderSide side, core::Qty amount,
                       core::Price price, std::string_view api_key,
                       std::int64_t timestamp_ms, int recv_window_ms,
                       Emit&& emit) noexcept {
  emit("apiKey", [&](Writer& w) { w.raw(api_key); });
  emit("orderId", [&](Writer& w) { w.raw(order_id); });
  emit("price", [&](Writer& w) { w.decimal(price); });
  emit("quantity", [&](Writer& w) { w.decimal(amount); });
  emit("recvWindow", [&](Writer& w) { w.integer(recv_window_ms); });
  emit("side", [&](Writer& w) {
    w.raw(side == models::OrderSide::kBuy ? "BUY" : "SELL");
  });
  emit("symbol", [&](Writer& w) { w.raw(symbol); });
  emit("timestamp", [&](Writer& w) { w.integer(timestamp_ms); });
}

// Runs both passes over one parameter declaration and assembles the request.
//
// The signing string is built into a scratch buffer, HMAC'd, and thrown away;
// only the JSON reaches the socket. One extra pass over ~200 bytes is the
// price of the two never disagreeing.
template <typename EachParam>
std::size_t build_ws_request(char* out, std::size_t cap, std::int64_t id,
                             std::string_view method,
                             std::string_view api_secret,
                             EachParam&& each_param) {
  char signing[kMaxRequestBytes];
  Writer sw(signing, sizeof(signing));
  QueryPass query{sw};
  each_param(query);
  if (!sw.ok) {
    return 0;
  }
  const std::string signature = net::to_hex(net::hmac_sha256(
      api_secret, std::string_view(signing, sw.written(signing))));

  Writer w(out, cap);
  // `id` is a string here. Binance accepts an integer too, but a string is
  // what its own examples use and it keeps the id opaque.
  w.raw(R"({"id":")");
  w.integer(id);
  w.raw(R"(","method":")");
  w.raw(method);
  w.raw(R"(","params":{)");
  JsonPass json{w};
  each_param(json);
  // The signature is appended last and is NOT part of what it covers.
  w.raw(R"(,"signature":")");
  w.raw(signature);
  w.raw(R"("}})");
  return w.written(out);
}

}  // namespace

std::size_t BinanceBuilder::ws_place_order(char* out, std::size_t cap,
                                           const models::OrderRequest& req,
                                           std::string_view api_key,
                                           std::string_view api_secret,
                                           std::int64_t timestamp_ms,
                                           std::int64_t& out_id,
                                           int recv_window_ms) {
  // A label needing JSON escaping cannot be signed raw and sent escaped.
  // Rejecting beats signing one thing and sending another.
  if (req.label.has_value() && !is_signing_safe(*req.label)) {
    return 0;
  }
  const std::int64_t id = next_id();
  out_id = id;
  return build_ws_request(
      out, cap, id, "order.place", api_secret, [&](auto& emit) {
        each_place_param(req, api_key, timestamp_ms, recv_window_ms, emit);
      });
}

std::size_t BinanceBuilder::ws_cancel_order(char* out, std::size_t cap,
                                            std::string_view symbol,
                                            std::string_view order_id,
                                            std::string_view api_key,
                                            std::string_view api_secret,
                                            std::int64_t timestamp_ms,
                                            std::int64_t& out_id,
                                            int recv_window_ms) {
  if (!is_signing_safe(order_id) || !is_signing_safe(symbol)) {
    return 0;
  }
  const std::int64_t id = next_id();
  out_id = id;
  return build_ws_request(
      out, cap, id, "order.cancel", api_secret, [&](auto& emit) {
        each_cancel_param(symbol, order_id, api_key, timestamp_ms,
                          recv_window_ms, emit);
      });
}

std::size_t BinanceBuilder::ws_modify_order(
    char* out, std::size_t cap, std::string_view symbol,
    std::string_view order_id, models::OrderSide side, core::Qty amount,
    core::Price price, std::string_view api_key, std::string_view api_secret,
    std::int64_t timestamp_ms, std::int64_t& out_id, int recv_window_ms) {
  if (!is_signing_safe(order_id) || !is_signing_safe(symbol)) {
    return 0;
  }
  const std::int64_t id = next_id();
  out_id = id;
  return build_ws_request(
      out, cap, id, "order.modify", api_secret, [&](auto& emit) {
        each_modify_param(symbol, order_id, side, amount, price, api_key,
                          timestamp_ms, recv_window_ms, emit);
      });
}

}  // namespace axon::venue::binance
