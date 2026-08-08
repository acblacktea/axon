#include "calais/venue/okx/okx_builder.h"

#include <cstring>

#include "calais/net/crypto_lite.h"

namespace calais::venue::okx {

using calais::venue::detail::Writer;

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

std::string OkxBuilder::rest_signature(std::string_view api_secret,
                                       std::string_view timestamp_iso,
                                       std::string_view http_method,
                                       std::string_view request_path,
                                       std::string_view body) {
  std::string pre_sign;
  pre_sign.reserve(timestamp_iso.size() + http_method.size() +
                   request_path.size() + body.size());
  pre_sign += timestamp_iso;
  pre_sign += http_method;
  pre_sign += request_path;
  pre_sign += body;
  const auto digest = net::hmac_sha256(api_secret, pre_sign);
  return net::base64_encode(digest.data(), digest.size());
}

}  // namespace calais::venue::okx
