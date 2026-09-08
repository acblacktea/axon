// Deribit outbound builder tests.
//
// Two things are checked for every message: that it is VALID JSON (parsed back
// with the same simdjson the inbound path uses), and that its fields match
// what ems/deribit/deribit.py sends. Checking only the byte string would pass
// on output that no JSON parser accepts.

#include "axon/venue/deribit/deribit_builder.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>
#include <vector>

#include "axon/venue/json_view.h"

using namespace axon;
using namespace axon::venue::deribit;
using axon::models::OrderSide;
using axon::models::OrderType;

namespace {

struct Built {
  std::string bytes;
  std::int64_t id = 0;
};

models::OrderRequest limit_buy() {
  models::OrderRequest r;
  r.instrument = "BTC-27JUN25-100000-C";
  r.side = OrderSide::kBuy;
  r.amount = *core::Qty::from_string("2.5");
  r.order_type = OrderType::kLimit;
  r.price = *core::Price::from_string("0.0345");
  r.internal_order_id = "0123456789abcdef0123456789abcdef";
  return r;
}

// Parse the built message back so the assertions are about structure, not
// about byte layout that could change harmlessly.
venue::Document& doc() {
  static venue::Document d(8192);
  return d;
}

}  // namespace

TEST(DeribitBuilder, PlaceLimitBuyMatchesPythonParameters) {
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = b.place_order(buf, sizeof(buf), limit_buy(), id);
  ASSERT_GT(n, 0u);

  const std::string_view msg(buf, n);
  auto root = doc().parse_copy(msg);
  ASSERT_TRUE(root.has_value()) << msg;

  EXPECT_EQ((*root)["jsonrpc"].as_string().value_or(""), "2.0");
  EXPECT_EQ((*root)["id"].as_int().value_or(-1), id);
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "private/buy");

  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ((*params)["instrument_name"].as_string().value_or(""),
            "BTC-27JUN25-100000-C");
  EXPECT_EQ((*params)["amount"].as_decimal().value_or(core::Price{}),
            *core::Qty::from_string("2.5"));
  EXPECT_EQ((*params)["type"].as_string().value_or(""), "limit");
  EXPECT_EQ((*params)["price"].as_decimal().value_or(core::Price{}),
            *core::Price::from_string("0.0345"));
}

TEST(DeribitBuilder, SellUsesTheSellEndpoint) {
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;
  auto req = limit_buy();
  req.side = OrderSide::kSell;
  const std::size_t n = b.place_order(buf, sizeof(buf), req, id);
  ASSERT_GT(n, 0u);

  auto root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "private/sell");
}

TEST(DeribitBuilder, MarketOrderOmitsPrice) {
  // place_order in the Python only sets price for limit orders.
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;
  auto req = limit_buy();
  req.order_type = OrderType::kMarket;
  const std::size_t n = b.place_order(buf, sizeof(buf), req, id);
  ASSERT_GT(n, 0u);

  const std::string_view msg(buf, n);
  EXPECT_EQ(msg.find("\"price\""), std::string_view::npos) << msg;

  auto root = doc().parse_copy(msg);
  ASSERT_TRUE(root.has_value());
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ((*params)["type"].as_string().value_or(""), "market");
}

TEST(DeribitBuilder, LabelIsIncludedOnlyWhenSet) {
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;

  auto req = limit_buy();
  std::size_t n = b.place_order(buf, sizeof(buf), req, id);
  EXPECT_EQ(std::string_view(buf, n).find("\"label\""), std::string_view::npos);

  req.label = "chase-maker";
  n = b.place_order(buf, sizeof(buf), req, id);
  auto root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ((*params)["label"].as_string().value_or(""), "chase-maker");

  // An empty label must be treated as absent, matching `if request.label:`.
  req.label = "";
  n = b.place_order(buf, sizeof(buf), req, id);
  EXPECT_EQ(std::string_view(buf, n).find("\"label\""), std::string_view::npos);
}

TEST(DeribitBuilder, PostOnlyEmitsJsonBooleansNotStrings) {
  // DIVERGENCE from the Python, which sends the strings "true". Deribit
  // coerces those, but boolean is the documented type. Asserted explicitly so
  // the divergence is visible rather than incidental.
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;

  auto req = limit_buy();
  req.post_only = true;
  std::size_t n = b.place_order(buf, sizeof(buf), req, id);
  std::string_view msg(buf, n);
  EXPECT_NE(msg.find(R"("post_only":true)"), std::string_view::npos) << msg;
  EXPECT_EQ(msg.find(R"("post_only":"true")"), std::string_view::npos) << msg;

  auto root = doc().parse_copy(msg);
  ASSERT_TRUE(root.has_value());
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_TRUE((*params)["post_only"].as_bool().value_or(false));

  // reject_post_only is nested inside post_only, exactly as in the Python.
  EXPECT_EQ(msg.find("reject_post_only"), std::string_view::npos);

  req.reject_post_only = true;
  n = b.place_order(buf, sizeof(buf), req, id);
  msg = std::string_view(buf, n);
  EXPECT_NE(msg.find(R"("reject_post_only":true)"), std::string_view::npos);

  // reject_post_only without post_only must not appear at all.
  req.post_only = false;
  n = b.place_order(buf, sizeof(buf), req, id);
  EXPECT_EQ(std::string_view(buf, n).find("post_only"), std::string_view::npos);
}

TEST(DeribitBuilder, PricesAreExactNotFloatFormatted) {
  // The reason the builder exists: a JSON library would route this through a
  // double and a shortest-round-trip formatter.
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;

  auto req = limit_buy();
  req.price = *core::Price::from_string("0.000000001");
  req.amount = *core::Qty::from_string("9999999.999999999");
  const std::size_t n = b.place_order(buf, sizeof(buf), req, id);
  const std::string_view msg(buf, n);

  EXPECT_NE(msg.find(R"("price":0.000000001)"), std::string_view::npos) << msg;
  EXPECT_NE(msg.find(R"("amount":9999999.999999999)"), std::string_view::npos)
      << msg;

  // And it must survive a round trip through the parser unchanged.
  auto root = doc().parse_copy(msg);
  ASSERT_TRUE(root.has_value());
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ((*params)["amount"].as_decimal()->raw(), 9999999999999999LL);
  EXPECT_EQ((*params)["price"].as_decimal()->raw(), 1);
}

TEST(DeribitBuilder, CancelOrder) {
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n = b.cancel_order(buf, sizeof(buf), "DERIBIT-12345", id);
  ASSERT_GT(n, 0u);

  auto root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "private/cancel");
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ((*params)["order_id"].as_string().value_or(""), "DERIBIT-12345");
}

TEST(DeribitBuilder, ModifyOrderIncludesOnlyTheFieldsGiven) {
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;

  std::size_t n = b.modify_order(buf, sizeof(buf), "ORD-1",
                                 core::Qty::from_string("5"), std::nullopt, id);
  std::string_view msg(buf, n);
  EXPECT_NE(msg.find(R"("amount":5)"), std::string_view::npos);
  EXPECT_EQ(msg.find("\"price\""), std::string_view::npos);

  n = b.modify_order(buf, sizeof(buf), "ORD-1", std::nullopt,
                     core::Price::from_string("0.05"), id);
  msg = std::string_view(buf, n);
  EXPECT_EQ(msg.find("\"amount\""), std::string_view::npos);
  EXPECT_NE(msg.find(R"("price":0.05)"), std::string_view::npos);

  n = b.modify_order(buf, sizeof(buf), "ORD-1", core::Qty::from_string("5"),
                     core::Price::from_string("0.05"), id);
  auto root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "private/edit");
}

TEST(DeribitBuilder, Authenticate) {
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;
  const std::size_t n =
      b.authenticate(buf, sizeof(buf), "my-client-id", "my-secret", id);
  ASSERT_GT(n, 0u);

  auto root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "public/auth");
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ((*params)["grant_type"].as_string().value_or(""),
            "client_credentials");
  EXPECT_EQ((*params)["client_id"].as_string().value_or(""), "my-client-id");
  EXPECT_EQ((*params)["client_secret"].as_string().value_or(""), "my-secret");
}

TEST(DeribitBuilder, SetHeartbeatAndSubscribe) {
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;

  std::size_t n = b.set_heartbeat(buf, sizeof(buf), 30, id);
  auto root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "public/set_heartbeat");
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ((*params)["interval"].as_int().value_or(0), 30);

  const std::string_view channels[] = {kAllOrdersChannel, kAllTradesChannel,
                                       "user.portfolio.BTC"};
  n = b.subscribe(buf, sizeof(buf), channels, 3, id);
  const std::string_view msg(buf, n);
  EXPECT_NE(msg.find("user.orders.any.any.raw"), std::string_view::npos);
  EXPECT_NE(msg.find("user.trades.any.any.raw"), std::string_view::npos);
  EXPECT_NE(msg.find("user.portfolio.BTC"), std::string_view::npos);

  root = doc().parse_copy(msg);
  ASSERT_TRUE(root.has_value()) << msg;
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "private/subscribe");

  n = b.unsubscribe(buf, sizeof(buf), channels, 1, id);
  root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "private/unsubscribe");
}

TEST(DeribitBuilder, HeartbeatResponsesUseTheReservedIds) {
  char buf[kMaxRequestBytes];

  std::size_t n = DeribitBuilder::test_response(buf, sizeof(buf));
  auto root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ((*root)["id"].as_int().value_or(0), kTestResponseId);
  EXPECT_EQ((*root)["method"].as_string().value_or(""), "public/test");

  n = DeribitBuilder::heartbeat_probe(buf, sizeof(buf));
  root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value());
  EXPECT_EQ((*root)["id"].as_int().value_or(0), kHeartbeatRequestId);
}

TEST(DeribitBuilder, RequestIdsAreUniqueAndSkipTheReservedOnes) {
  // 9998 and 9999 belong to the heartbeat exchange; reusing them would make a
  // reply ambiguous.
  DeribitBuilder b(kHeartbeatRequestId - 2);
  char buf[kMaxRequestBytes];
  std::vector<std::int64_t> ids;
  for (int i = 0; i < 10; ++i) {
    std::int64_t id = 0;
    b.cancel_order(buf, sizeof(buf), "X", id);
    ids.push_back(id);
  }

  for (std::int64_t id : ids) {
    EXPECT_NE(id, kHeartbeatRequestId);
    EXPECT_NE(id, kTestResponseId);
  }
  for (std::size_t i = 1; i < ids.size(); ++i) {
    EXPECT_NE(ids[i], ids[i - 1]);
  }
}

TEST(DeribitBuilder, EscapesJsonStrings) {
  // Labels are caller-controlled. An unescaped quote would produce a malformed
  // request -- or a well-formed one with fields the caller did not intend.
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;

  auto req = limit_buy();
  req.label = R"(evil","amount":999,"x":")";
  const std::size_t n = b.place_order(buf, sizeof(buf), req, id);
  ASSERT_GT(n, 0u);

  const std::string_view msg(buf, n);
  auto root = doc().parse_copy(msg);
  ASSERT_TRUE(root.has_value()) << msg;
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  // The amount must still be the real one, not the injected 999.
  EXPECT_EQ((*params)["amount"].as_decimal().value_or(core::Price{}),
            *core::Qty::from_string("2.5"));
  EXPECT_EQ((*params)["label"].as_string().value_or(""), req.label.value());
}

TEST(DeribitBuilder, EscapesControlCharacters) {
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;

  auto req = limit_buy();
  req.label = std::string("a\nb\tc\"d\\e\x01");
  const std::size_t n = b.place_order(buf, sizeof(buf), req, id);
  ASSERT_GT(n, 0u);

  auto root = doc().parse_copy(std::string_view(buf, n));
  ASSERT_TRUE(root.has_value()) << std::string_view(buf, n);
  auto params = (*root)["params"].as_object();
  ASSERT_TRUE(params.has_value());
  EXPECT_EQ((*params)["label"].as_string().value_or(""), req.label.value());
}

TEST(DeribitBuilder, RefusesToTruncate) {
  // A truncated order message is far worse than no message: it would be
  // rejected by the venue at best, and misparsed at worst.
  DeribitBuilder b;
  std::int64_t id = 0;
  for (std::size_t cap = 1; cap < 120; ++cap) {
    std::vector<char> small(cap);
    const std::size_t n = b.place_order(small.data(), cap, limit_buy(), id);
    EXPECT_EQ(n, 0u) << "cap=" << cap << " should not have produced a message";
  }
}

TEST(DeribitBuilder, EveryMessageIsValidJson) {
  // Belt and braces across the whole surface.
  DeribitBuilder b;
  char buf[kMaxRequestBytes];
  std::int64_t id = 0;
  const std::string_view channels[] = {kAllOrdersChannel};

  std::vector<std::size_t> lengths;
  lengths.push_back(b.place_order(buf, sizeof(buf), limit_buy(), id));
  std::vector<std::string> messages;
  messages.emplace_back(buf, lengths.back());

  lengths.push_back(b.cancel_order(buf, sizeof(buf), "ORD-1", id));
  messages.emplace_back(buf, lengths.back());

  lengths.push_back(b.modify_order(buf, sizeof(buf), "ORD-1",
                                   core::Qty::from_string("1"),
                                   core::Price::from_string("2"), id));
  messages.emplace_back(buf, lengths.back());

  lengths.push_back(b.authenticate(buf, sizeof(buf), "id", "secret", id));
  messages.emplace_back(buf, lengths.back());

  lengths.push_back(b.set_heartbeat(buf, sizeof(buf), 30, id));
  messages.emplace_back(buf, lengths.back());

  lengths.push_back(b.subscribe(buf, sizeof(buf), channels, 1, id));
  messages.emplace_back(buf, lengths.back());

  lengths.push_back(DeribitBuilder::test_response(buf, sizeof(buf)));
  messages.emplace_back(buf, lengths.back());

  for (const auto& m : messages) {
    EXPECT_GT(m.size(), 0u);
    EXPECT_TRUE(doc().parse_copy(m).has_value()) << m;
  }
}
