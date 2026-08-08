#include "calais/venue/bybit/bybit_builder.h"

#include <cstring>

#include "calais/net/crypto_lite.h"

namespace calais::venue::bybit {

using calais::venue::detail::Writer;

namespace {

std::string int_to_string(std::int64_t v) {
  char buf[24];
  std::size_t len = 0;
  bool negative = v < 0;
  std::uint64_t magnitude = negative ? static_cast<std::uint64_t>(-(v + 1)) + 1U
                                     : static_cast<std::uint64_t>(v);
  if (magnitude == 0) {
    buf[len++] = '0';
  }
  while (magnitude > 0) {
    buf[len++] = static_cast<char>('0' + (magnitude % 10));
    magnitude /= 10;
  }
  std::string out;
  if (negative) {
    out.push_back('-');
  }
  while (len > 0) {
    out.push_back(buf[--len]);
  }
  return out;
}

}  // namespace

// ---------------------------------------------------------------------------
std::string BybitBuilder::ws_signature(std::string_view api_secret,
                                       std::int64_t expires_ms) {
  // "GET/realtime" + expires, exactly as _authenticate builds it.
  const std::string pre_sign = "GET/realtime" + int_to_string(expires_ms);
  return net::to_hex(net::hmac_sha256(api_secret, pre_sign));
}

std::size_t BybitBuilder::ws_auth(char* out, std::size_t cap,
                                  std::string_view api_key,
                                  std::string_view api_secret,
                                  std::int64_t expires_ms,
                                  std::int64_t& out_id) {
  const std::int64_t id = next_id();
  out_id = id;
  const std::string signature = ws_signature(api_secret, expires_ms);

  Writer w(out, cap);
  // req_id is a STRING on Bybit, unlike everyone else's integer id.
  w.raw(R"({"req_id":")");
  w.raw(int_to_string(id));
  w.raw(R"(","op":"auth","args":[)");
  w.json_string(api_key);
  w.raw(",");
  w.integer(expires_ms);
  w.raw(",");
  w.json_string(signature);
  w.raw("]}");
  return w.written(out);
}

namespace {

std::size_t topic_request(char* out, std::size_t cap, std::string_view op,
                          std::int64_t id, const std::string_view* topics,
                          std::size_t count) noexcept {
  Writer w(out, cap);
  w.raw(R"({"req_id":")");
  w.raw(int_to_string(id));
  w.raw(R"(","op":")");
  w.raw(op);
  w.raw(R"(","args":[)");
  for (std::size_t i = 0; i < count; ++i) {
    if (i > 0) {
      w.raw(",");
    }
    w.json_string(topics[i]);
  }
  w.raw("]}");
  return w.written(out);
}

}  // namespace

std::size_t BybitBuilder::ws_subscribe(char* out, std::size_t cap,
                                       const std::string_view* topics,
                                       std::size_t count,
                                       std::int64_t& out_id) noexcept {
  out_id = next_id();
  return topic_request(out, cap, "subscribe", out_id, topics, count);
}

std::size_t BybitBuilder::ws_unsubscribe(char* out, std::size_t cap,
                                         const std::string_view* topics,
                                         std::size_t count,
                                         std::int64_t& out_id) noexcept {
  out_id = next_id();
  return topic_request(out, cap, "unsubscribe", out_id, topics, count);
}

std::size_t BybitBuilder::ws_ping(char* out, std::size_t cap) noexcept {
  Writer w(out, cap);
  w.raw(R"({"op":"ping"})");
  return w.written(out);
}

// ---------------------------------------------------------------------------
std::size_t BybitBuilder::place_order_body(
    char* out, std::size_t cap, const models::OrderRequest& req) noexcept {
  Writer w(out, cap);
  w.raw(R"({"category":")");
  w.raw(kCategory);
  w.raw(R"(","symbol":)");
  w.json_string(req.instrument);
  w.raw(R"(,"side":")");
  w.raw(req.side == models::OrderSide::kBuy ? "Buy" : "Sell");
  w.raw(R"(","orderType":")");
  w.raw(req.order_type == models::OrderType::kLimit ? "Limit" : "Market");

  // Bybit wants numbers as STRINGS. Emitting them bare is accepted on some
  // endpoints and rejected on others; quoting is what the Python does and
  // what the docs specify.
  w.raw(R"(","qty":")");
  w.decimal(req.amount);
  w.raw("\"");

  if (req.order_type == models::OrderType::kLimit) {
    w.raw(R"(,"price":")");
    if (req.price.has_value()) {
      w.decimal(*req.price);
    } else {
      w.raw("0");
    }
    w.raw("\"");
  }

  // From place_order: PostOnly when asked, otherwise GTC for a limit and IOC
  // for a market order.
  w.raw(R"(,"timeInForce":")");
  if (req.post_only) {
    w.raw("PostOnly");
  } else {
    w.raw(req.order_type == models::OrderType::kLimit ? "GTC" : "IOC");
  }
  w.raw("\"");

  if (req.label.has_value() && !req.label->empty()) {
    w.raw(R"(,"orderLinkId":)");
    w.json_string(*req.label);
  }

  w.raw("}");
  return w.written(out);
}

std::size_t BybitBuilder::cancel_order_body(char* out, std::size_t cap,
                                            std::string_view symbol,
                                            std::string_view order_id) noexcept {
  Writer w(out, cap);
  w.raw(R"({"category":")");
  w.raw(kCategory);
  w.raw(R"(","symbol":)");
  w.json_string(symbol);
  w.raw(R"(,"orderId":)");
  w.json_string(order_id);
  w.raw("}");
  return w.written(out);
}

std::size_t BybitBuilder::amend_order_body(
    char* out, std::size_t cap, std::string_view symbol,
    std::string_view order_id, std::optional<core::Qty> amount,
    std::optional<core::Price> price) noexcept {
  Writer w(out, cap);
  w.raw(R"({"category":")");
  w.raw(kCategory);
  w.raw(R"(","symbol":)");
  w.json_string(symbol);
  w.raw(R"(,"orderId":)");
  w.json_string(order_id);
  if (amount.has_value()) {
    w.raw(R"(,"qty":")");
    w.decimal(*amount);
    w.raw("\"");
  }
  if (price.has_value()) {
    w.raw(R"(,"price":")");
    w.decimal(*price);
    w.raw("\"");
  }
  w.raw("}");
  return w.written(out);
}

std::string BybitBuilder::rest_signature(std::string_view api_secret,
                                         std::int64_t timestamp_ms,
                                         std::string_view api_key,
                                         int recv_window_ms,
                                         std::string_view body) {
  std::string pre_sign;
  pre_sign.reserve(64 + body.size());
  pre_sign += int_to_string(timestamp_ms);
  pre_sign += api_key;
  pre_sign += int_to_string(recv_window_ms);
  pre_sign += body;
  return net::to_hex(net::hmac_sha256(api_secret, pre_sign));
}

}  // namespace calais::venue::bybit
