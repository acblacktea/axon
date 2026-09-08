#include "axon/venue/okx/okx_builder.h"

#include <cstring>

#include "axon/net/crypto_lite.h"

namespace axon::venue::okx {

using axon::venue::detail::Writer;

namespace {

std::string int_to_string(std::int64_t v) {
  char buf[24];
  std::size_t len = 0;
  const bool negative = v < 0;
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
std::string OkxBuilder::ws_signature(std::string_view api_secret,
                                     std::int64_t timestamp_seconds) {
  const std::string pre_sign =
      int_to_string(timestamp_seconds) + "GET/users/self/verify";
  const auto digest = net::hmac_sha256(api_secret, pre_sign);
  // BASE64, not hex -- OKX differs from Bybit and Binance here.
  return net::base64_encode(digest.data(), digest.size());
}

std::size_t OkxBuilder::ws_login(char* out, std::size_t cap,
                                 std::string_view api_key,
                                 std::string_view passphrase,
                                 std::string_view api_secret,
                                 std::int64_t timestamp_seconds) {
  const std::string timestamp = int_to_string(timestamp_seconds);
  const std::string signature = ws_signature(api_secret, timestamp_seconds);

  Writer w(out, cap);
  w.raw(R"({"op":"login","args":[{"apiKey":)");
  w.json_string(api_key);
  w.raw(R"(,"passphrase":)");
  w.json_string(passphrase);
  w.raw(R"(,"timestamp":)");
  w.json_string(timestamp);
  w.raw(R"(,"sign":)");
  w.json_string(signature);
  w.raw("}]}");
  return w.written(out);
}

namespace {

std::size_t channel_request(char* out, std::size_t cap, std::string_view op,
                            const std::string_view* channels, std::size_t count,
                            std::string_view inst_type) noexcept {
  Writer w(out, cap);
  w.raw(R"({"op":")");
  w.raw(op);
  w.raw(R"(","args":[)");
  for (std::size_t i = 0; i < count; ++i) {
    if (i > 0) {
      w.raw(",");
    }
    w.raw(R"({"channel":)");
    w.json_string(channels[i]);
    // The account channel is not scoped by instrument type; the others are.
    if (channels[i] != kAccountChannel) {
      w.raw(R"(,"instType":)");
      w.json_string(inst_type);
    }
    w.raw("}");
  }
  w.raw("]}");
  return w.written(out);
}

}  // namespace

std::size_t OkxBuilder::ws_subscribe(char* out, std::size_t cap,
                                     const std::string_view* channels,
                                     std::size_t count,
                                     std::string_view inst_type) noexcept {
  return channel_request(out, cap, "subscribe", channels, count, inst_type);
}

std::size_t OkxBuilder::ws_unsubscribe(char* out, std::size_t cap,
                                       const std::string_view* channels,
                                       std::size_t count,
                                       std::string_view inst_type) noexcept {
  return channel_request(out, cap, "unsubscribe", channels, count, inst_type);
}

// ---------------------------------------------------------------------------
std::size_t OkxBuilder::place_order_body(
    char* out, std::size_t cap, const models::OrderRequest& req) noexcept {
  Writer w(out, cap);
  w.raw(R"({"instId":)");
  w.json_string(req.instrument);
  w.raw(R"(,"tdMode":")");
  w.raw(kTradeMode);
  w.raw(R"(","side":")");
  w.raw(req.side == models::OrderSide::kBuy ? "buy" : "sell");

  // post_only is an ORDER TYPE on OKX, not a flag -- matching place_order in
  // the Python, which overwrites ordType rather than adding a field.
  w.raw(R"(","ordType":")");
  if (req.post_only) {
    w.raw("post_only");
  } else {
    w.raw(req.order_type == models::OrderType::kLimit ? "limit" : "market");
  }

  // OKX wants numbers as strings.
  w.raw(R"(","sz":")");
  w.decimal(req.amount);
  w.raw("\"");

  if (req.order_type == models::OrderType::kLimit && req.price.has_value()) {
    w.raw(R"(,"px":")");
    w.decimal(*req.price);
    w.raw("\"");
  }

  if (req.label.has_value() && !req.label->empty()) {
    w.raw(R"(,"clOrdId":)");
    w.json_string(*req.label);
  }

  w.raw("}");
  return w.written(out);
}

std::size_t OkxBuilder::cancel_order_body(char* out, std::size_t cap,
                                          std::string_view inst_id,
                                          std::string_view order_id) noexcept {
  Writer w(out, cap);
  w.raw(R"({"instId":)");
  w.json_string(inst_id);
  w.raw(R"(,"ordId":)");
  w.json_string(order_id);
  w.raw("}");
  return w.written(out);
}

std::size_t OkxBuilder::amend_order_body(
    char* out, std::size_t cap, std::string_view inst_id,
    std::string_view order_id, std::optional<core::Qty> amount,
    std::optional<core::Price> price) noexcept {
  Writer w(out, cap);
  w.raw(R"({"instId":)");
  w.json_string(inst_id);
  w.raw(R"(,"ordId":)");
  w.json_string(order_id);
  if (amount.has_value()) {
    w.raw(R"(,"newSz":")");
    w.decimal(*amount);
    w.raw("\"");
  }
  if (price.has_value()) {
    w.raw(R"(,"newPx":")");
    w.decimal(*price);
    w.raw("\"");
  }
  w.raw("}");
  return w.written(out);
}

// ---------------------------------------------------------------------------
// WebSocket order entry.
//
// The envelope is the only new bytes: `args` is the REST body verbatim, so
// these delegate rather than restating the field mapping. Writing it twice is
// how the WebSocket path and the REST path drift apart, and a drift here means
// two different orders depending on which transport was live.
namespace {

// Writes {"id":"<id>","op":"<op>","args":[ and returns false if it did not fit.
// The id is quoted because OKX's is a string, but the digits are written
// directly rather than through a std::string -- this is order entry.
bool open_ws_envelope(Writer& w, std::int64_t id, std::string_view op) noexcept {
  w.raw(R"({"id":")");
  w.integer(id);
  w.raw(R"(","op":")");
  w.raw(op);
  w.raw(R"(","args":[)");
  return w.ok;
}

// Appends the body a REST builder produces, straight into the writer's buffer.
template <typename BuildBody>
std::size_t close_ws_envelope(Writer& w, char* origin, BuildBody&& build) noexcept {
  const std::size_t n = build(w.p, static_cast<std::size_t>(w.end - w.p));
  if (n == 0) {
    return 0;
  }
  w.p += n;
  w.raw("]}");
  return w.written(origin);
}

}  // namespace

std::size_t OkxBuilder::ws_place_order(char* out, std::size_t cap,
                                       const models::OrderRequest& req,
                                       std::int64_t& out_id) noexcept {
  const std::int64_t id = next_id();
  out_id = id;
  Writer w(out, cap);
  if (!open_ws_envelope(w, id, "order")) {
    return 0;
  }
  return close_ws_envelope(w, out, [&](char* p, std::size_t n) {
    return place_order_body(p, n, req);
  });
}

std::size_t OkxBuilder::ws_cancel_order(char* out, std::size_t cap,
                                        std::string_view inst_id,
                                        std::string_view order_id,
                                        std::int64_t& out_id) noexcept {
  const std::int64_t id = next_id();
  out_id = id;
  Writer w(out, cap);
  if (!open_ws_envelope(w, id, "cancel-order")) {
    return 0;
  }
  return close_ws_envelope(w, out, [&](char* p, std::size_t n) {
    return cancel_order_body(p, n, inst_id, order_id);
  });
}

std::size_t OkxBuilder::ws_amend_order(char* out, std::size_t cap,
                                       std::string_view inst_id,
                                       std::string_view order_id,
                                       std::optional<core::Qty> amount,
                                       std::optional<core::Price> price,
                                       std::int64_t& out_id) noexcept {
  const std::int64_t id = next_id();
  out_id = id;
  Writer w(out, cap);
  if (!open_ws_envelope(w, id, "amend-order")) {
    return 0;
  }
  return close_ws_envelope(w, out, [&](char* p, std::size_t n) {
    return amend_order_body(p, n, inst_id, order_id, amount, price);
  });
}

}  // namespace axon::venue::okx
