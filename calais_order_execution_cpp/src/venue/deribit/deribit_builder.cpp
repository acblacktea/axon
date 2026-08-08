#include "calais/venue/deribit/deribit_builder.h"

#include <cstring>

namespace calais::venue::deribit {

using calais::venue::detail::Writer;

namespace {

// {"jsonrpc":"2.0","id":<id>,"method":"<method>","params":{
void open_envelope(Writer& w, std::int64_t id, std::string_view method) noexcept {
  w.raw(R"({"jsonrpc":"2.0","id":)");
  w.integer(id);
  w.raw(R"(,"method":")");
  w.raw(method);
  w.raw(R"(","params":{)");
}

void close_envelope(Writer& w) noexcept { w.raw("}}"); }

}  // namespace

// ---------------------------------------------------------------------------
std::size_t DeribitBuilder::place_order(char* out, std::size_t cap,
                                        const models::OrderRequest& req,
                                        std::int64_t& out_id) noexcept {
  const std::int64_t id = next_id();
  out_id = id;

  Writer w(out, cap);
  open_envelope(w, id, order_entry_method(req.side));

  w.raw(R"("instrument_name":)");
  w.json_string(req.instrument);

  w.raw(R"(,"amount":)");
  w.decimal(req.amount);

  w.raw(R"(,"type":")");
  w.raw(models::to_string(req.order_type));
  w.raw("\"");

  // Only limit orders carry a price, matching place_order in the Python EMS.
  if (req.order_type == models::OrderType::kLimit && req.price.has_value()) {
    w.raw(R"(,"price":)");
    w.decimal(*req.price);
  }

  if (req.label.has_value() && !req.label->empty()) {
    w.raw(R"(,"label":)");
    w.json_string(*req.label);
  }

  // DIVERGENCE from ems/deribit/deribit.py, deliberately.
  //
  // The Python sends the STRINGS "true" for post_only and reject_post_only:
  //     params["post_only"] = "true"
  // Deribit coerces them, so it works, but the documented type is boolean and
  // a venue that ever tightens validation would reject it. This emits real
  // JSON booleans.
  //
  // VERIFY THIS AGAINST TESTNET BEFORE TRUSTING IT IN PRODUCTION. It is the
  // only place in the venue layer that does something the running Python does
  // not, and it cannot be verified without a live connection.
  if (req.post_only) {
    w.raw(R"(,"post_only":true)");
    if (req.reject_post_only) {
      w.raw(R"(,"reject_post_only":true)");
    }
  }

  close_envelope(w);
  return w.written(out);
}

// ---------------------------------------------------------------------------
std::size_t DeribitBuilder::cancel_order(char* out, std::size_t cap,
                                         std::string_view order_id,
                                         std::int64_t& out_id) noexcept {
  const std::int64_t id = next_id();
  out_id = id;

  Writer w(out, cap);
  open_envelope(w, id, "private/cancel");
  w.raw(R"("order_id":)");
  w.json_string(order_id);
  close_envelope(w);
  return w.written(out);
}

// ---------------------------------------------------------------------------
std::size_t DeribitBuilder::modify_order(char* out, std::size_t cap,
                                         std::string_view order_id,
                                         std::optional<core::Qty> amount,
                                         std::optional<core::Price> price,
                                         std::int64_t& out_id) noexcept {
  const std::int64_t id = next_id();
  out_id = id;

  Writer w(out, cap);
  open_envelope(w, id, "private/edit");
  w.raw(R"("order_id":)");
  w.json_string(order_id);

  if (amount.has_value()) {
    w.raw(R"(,"amount":)");
    w.decimal(*amount);
  }
  if (price.has_value()) {
    w.raw(R"(,"price":)");
    w.decimal(*price);
  }

  close_envelope(w);
  return w.written(out);
}

// ---------------------------------------------------------------------------
std::size_t DeribitBuilder::authenticate(char* out, std::size_t cap,
                                         std::string_view client_id,
                                         std::string_view client_secret,
                                         std::int64_t& out_id) noexcept {
  const std::int64_t id = next_id();
  out_id = id;

  Writer w(out, cap);
  open_envelope(w, id, "public/auth");
  w.raw(R"("grant_type":"client_credentials","client_id":)");
  w.json_string(client_id);
  w.raw(R"(,"client_secret":)");
  w.json_string(client_secret);
  close_envelope(w);
  return w.written(out);
}

// ---------------------------------------------------------------------------
std::size_t DeribitBuilder::set_heartbeat(char* out, std::size_t cap,
                                          int interval_seconds,
                                          std::int64_t& out_id) noexcept {
  const std::int64_t id = next_id();
  out_id = id;

  Writer w(out, cap);
  open_envelope(w, id, "public/set_heartbeat");
  w.raw(R"("interval":)");
  w.integer(interval_seconds);
  close_envelope(w);
  return w.written(out);
}

// ---------------------------------------------------------------------------
namespace {

std::size_t build_channel_request(char* out, std::size_t cap,
                                  std::string_view method, std::int64_t id,
                                  const std::string_view* channels,
                                  std::size_t count) noexcept {
  Writer w(out, cap);
  open_envelope(w, id, method);
  w.raw(R"("channels":[)");
  for (std::size_t i = 0; i < count; ++i) {
    if (i > 0) {
      w.raw(",");
    }
    w.json_string(channels[i]);
  }
  w.raw("]");
  close_envelope(w);
  return w.written(out);
}

}  // namespace

std::size_t DeribitBuilder::subscribe(char* out, std::size_t cap,
                                      const std::string_view* channels,
                                      std::size_t count,
                                      std::int64_t& out_id) noexcept {
  out_id = next_id();
  return build_channel_request(out, cap, "private/subscribe", out_id, channels,
                               count);
}

std::size_t DeribitBuilder::unsubscribe(char* out, std::size_t cap,
                                        const std::string_view* channels,
                                        std::size_t count,
                                        std::int64_t& out_id) noexcept {
  out_id = next_id();
  return build_channel_request(out, cap, "private/unsubscribe", out_id, channels,
                               count);
}

// ---------------------------------------------------------------------------
std::size_t DeribitBuilder::test_response(char* out, std::size_t cap) noexcept {
  Writer w(out, cap);
  open_envelope(w, kTestResponseId, "public/test");
  close_envelope(w);
  return w.written(out);
}

std::size_t DeribitBuilder::heartbeat_probe(char* out, std::size_t cap) noexcept {
  Writer w(out, cap);
  open_envelope(w, kHeartbeatRequestId, "public/test");
  close_envelope(w);
  return w.written(out);
}

}  // namespace calais::venue::deribit
