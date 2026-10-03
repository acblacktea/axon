#include "axon/ems/okx/okx_ems.h"

#include <string>

#include "axon/util/metrics.h"
#include "axon/venue/json_view.h"

namespace axon::ems::okx {
namespace {

venue::okx::OkxBuilder& builder() {
  static thread_local venue::okx::OkxBuilder b;
  return b;
}

// OKX reports TWO outcomes per request: a top-level `code` for whether the
// request was processed, and an `sCode` PER ORDER for whether that order was
// accepted. The session checks the first; this checks the second.
//
// Missing this is how a rejected order reads as a successful one -- top-level
// code "0" with sCode "51008" (insufficient balance) is a perfectly normal
// shape.
std::optional<std::string> order_rejection(std::string_view payload) {
  static thread_local venue::Document doc(64 * 1024);
  auto root = doc.parse_copy(payload);
  if (!root.has_value()) {
    return std::nullopt;
  }
  auto data = (*root)["data"].as_array();
  if (!data.has_value()) {
    return std::nullopt;
  }
  std::optional<std::string> rejection;
  data->for_each_object([&](venue::Object& entry) {
    if (rejection.has_value()) {
      return;
    }
    const auto s_code = entry["sCode"].as_string().value_or("0");
    if (s_code == "0") {
      return;
    }
    const auto s_msg = entry["sMsg"].as_string().value_or("");
    rejection = "okx rejected the order (sCode " + std::string(s_code) + "): " +
                std::string(s_msg);
  });
  return rejection;
}

std::size_t build_place(char* buf, std::size_t cap,
                        const models::OrderRequest& request, const BuildContext&,
                        std::int64_t& id) {
  return builder().ws_place_order(buf, cap, request, id);
}

std::size_t build_cancel(char* buf, std::size_t cap, std::string_view symbol,
                         std::string_view order_id, const BuildContext&,
                         std::int64_t& id) {
  return builder().ws_cancel_order(buf, cap, std::string(symbol),
                                   std::string(order_id), id);
}

std::size_t build_modify(char* buf, std::size_t cap,
                         const models::Order& existing,
                         std::optional<core::Qty> amount,
                         std::optional<core::Price> price, const BuildContext&,
                         std::int64_t& id) {
  return builder().ws_amend_order(buf, cap, existing.instrument,
                                  existing.order_id, amount, price, id);
}

// Acceptance of the REQUEST is not acceptance of the ORDER, so a reply OKX
// called successful still has to be inspected.
OrderResult interpret_reply(std::string_view payload,
                            const models::OrderRequest&) {
  if (const auto rejection = order_rejection(payload); rejection.has_value()) {
    util::get_metrics().inc_order_rejected("okx", "venue");
    return OrderResult{false, std::nullopt, *rejection};
  }
  return OrderResult{true, std::nullopt, {}};
}

}  // namespace

const VenueOps& ops() {
  static const VenueOps kOps{
      "okx",
      /*own_trade_connection=*/false,  // orders ride the private stream
      /*needs_symbol=*/true,
      build_place, build_cancel, build_modify, interpret_reply,
  };
  return kOps;
}

}  // namespace axon::ems::okx
