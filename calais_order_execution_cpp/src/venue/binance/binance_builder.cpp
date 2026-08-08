#include "calais/venue/binance/binance_builder.h"

#include <cstring>

#include "calais/net/crypto_lite.h"

namespace calais::venue::binance {

using calais::venue::detail::Writer;

namespace {

// Percent-encodes anything outside the unreserved set. Client order ids are
// caller-controlled, and an unencoded '&' would split one parameter into two --
// which in a signed query means the signature covers different fields from the
// ones the venue parses.
void query_value(Writer& w, std::string_view s) noexcept {
  static constexpr char kHex[] = "0123456789ABCDEF";
  for (char c : s) {
    const auto u = static_cast<unsigned char>(c);
    const bool unreserved = (u >= 'A' && u <= 'Z') || (u >= 'a' && u <= 'z') ||
                            (u >= '0' && u <= '9') || c == '-' || c == '.' ||
                            c == '_' || c == '~';
    if (unreserved) {
      w.raw(std::string_view(&c, 1));
    } else {
      char esc[3] = {'%', kHex[u >> 4], kHex[u & 0xF]};
      w.raw(std::string_view(esc, 3));
    }
  }
}

}  // namespace

std::size_t BinanceBuilder::place_order_query(char* out, std::size_t cap,
                                              const models::OrderRequest& req,
                                              std::int64_t timestamp_ms,
                                              int recv_window_ms) noexcept {
  Writer w(out, cap);

  w.raw("symbol=");
  query_value(w, req.instrument);

  w.raw("&side=");
  w.raw(req.side == models::OrderSide::kBuy ? "BUY" : "SELL");

  w.raw("&type=");
  w.raw(req.order_type == models::OrderType::kLimit ? "LIMIT" : "MARKET");

  w.raw("&quantity=");
  w.decimal(req.amount);

  if (req.order_type == models::OrderType::kLimit) {
    w.raw("&price=");
    if (req.price.has_value()) {
      w.decimal(*req.price);
    } else {
      w.raw("0");
    }
    // GTX is Binance's post-only. Matching place_order in the Python EMS.
    w.raw("&timeInForce=");
    w.raw(req.post_only ? "GTX" : "GTC");
  }

  if (req.label.has_value() && !req.label->empty()) {
    w.raw("&newClientOrderId=");
    query_value(w, *req.label);
  }

  w.raw("&recvWindow=");
  w.integer(recv_window_ms);
  w.raw("&timestamp=");
  w.integer(timestamp_ms);

  return w.written(out);
}

std::size_t BinanceBuilder::cancel_order_query(char* out, std::size_t cap,
                                               std::string_view symbol,
                                               std::string_view order_id,
                                               std::int64_t timestamp_ms,
                                               int recv_window_ms) noexcept {
  Writer w(out, cap);
  w.raw("symbol=");
  query_value(w, symbol);
  w.raw("&orderId=");
  query_value(w, order_id);
  w.raw("&recvWindow=");
  w.integer(recv_window_ms);
  w.raw("&timestamp=");
  w.integer(timestamp_ms);
  return w.written(out);
}

std::size_t BinanceBuilder::modify_order_query(
    char* out, std::size_t cap, std::string_view symbol,
    std::string_view order_id, models::OrderSide side, core::Qty amount,
    core::Price price, std::int64_t timestamp_ms, int recv_window_ms) noexcept {
  Writer w(out, cap);
  w.raw("symbol=");
  query_value(w, symbol);
  w.raw("&orderId=");
  query_value(w, order_id);
  w.raw("&side=");
  w.raw(side == models::OrderSide::kBuy ? "BUY" : "SELL");
  w.raw("&quantity=");
  w.decimal(amount);
  w.raw("&price=");
  w.decimal(price);
  w.raw("&recvWindow=");
  w.integer(recv_window_ms);
  w.raw("&timestamp=");
  w.integer(timestamp_ms);
  return w.written(out);
}

std::size_t BinanceBuilder::sign_query(char* buf, std::size_t len,
                                       std::size_t cap,
                                       std::string_view api_secret) {
  if (len == 0) {
    return 0;
  }
  const auto digest =
      net::hmac_sha256(api_secret, std::string_view(buf, len));
  const std::string hex = net::to_hex(digest);

  static constexpr std::string_view kPrefix = "&signature=";
  if (cap < len + kPrefix.size() + hex.size()) {
    return 0;
  }
  std::memcpy(buf + len, kPrefix.data(), kPrefix.size());
  std::memcpy(buf + len + kPrefix.size(), hex.data(), hex.size());
  return len + kPrefix.size() + hex.size();
}

}  // namespace calais::venue::binance
