// REST snapshot parsing, without a network.
//
// Every reconciler reads its snapshot through parse_array_at. Binance returns
// a bare top-level array; the other venues wrap theirs in an object. Until
// this file existed nothing exercised the bare-array path, and it had never
// worked: every Binance openOrders / positionRisk / userTrades reply came back
// as "malformed response", so Binance reconciliation silently did nothing.
// Found on the live testnet; pinned here so it cannot come back unnoticed.

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "axon/oms/detail/rest_json.h"

namespace {

using axon::net::HttpResponse;
using axon::oms::detail::parse_array_at;

HttpResponse ok(std::string body) {
  HttpResponse r;
  r.status = 200;
  r.body = std::move(body);
  return r;
}

std::optional<std::int64_t> parse_id(axon::venue::Object& o) { return o["id"].as_int(); }

struct Result {
  std::vector<std::int64_t> items;
  std::string error;
  bool called = false;
};

Result run(const HttpResponse& r, const std::vector<std::string>& path) {
  Result out;
  parse_array_at<std::int64_t>(r, path, parse_id,
                               [&](std::vector<std::int64_t> items, const std::string& e) {
                                 out.items = std::move(items);
                                 out.error = e;
                                 out.called = true;
                               });
  return out;
}

// Binance: openOrders, positionRisk and userTrades are bare arrays.
TEST(RestJson, TopLevelArrayIsParsed) {
  auto r = run(ok(R"([{"id":1,"symbol":"BTCUSDT"},{"id":2,"symbol":"ETHUSDT"}])"), {});
  ASSERT_TRUE(r.called);
  EXPECT_EQ(r.error, "");
  EXPECT_EQ(r.items, (std::vector<std::int64_t>{1, 2}));
}

TEST(RestJson, EmptyTopLevelArrayIsNotAnError) {
  auto r = run(ok("[]"), {});
  EXPECT_EQ(r.error, "");
  EXPECT_TRUE(r.items.empty());
}

// OKX / Bybit / Deribit wrap the list in an object.
TEST(RestJson, ArrayAtAPathIsParsed) {
  auto r = run(ok(R"({"code":"0","data":[{"id":7}]})"), {"data"});
  EXPECT_EQ(r.error, "");
  EXPECT_EQ(r.items, (std::vector<std::int64_t>{7}));

  r = run(ok(R"({"retCode":0,"result":{"list":[{"id":8},{"id":9}]}})"), {"result", "list"});
  EXPECT_EQ(r.error, "");
  EXPECT_EQ(r.items, (std::vector<std::int64_t>{8, 9}));
}

TEST(RestJson, MissingIntermediateKeyIsAnError) {
  auto r = run(ok(R"({"retCode":10001})"), {"result", "list"});
  EXPECT_EQ(r.error, "response is missing 'result'");
}

TEST(RestJson, AbsentListIsAnEmptyAnswer) {
  auto r = run(ok(R"({"data":null})"), {"data"});
  EXPECT_EQ(r.error, "");
  EXPECT_TRUE(r.items.empty());
}

TEST(RestJson, HttpErrorCarriesStatusAndBody) {
  HttpResponse r;
  r.status = 400;
  r.body = R"({"code":-1022,"msg":"Signature for this request is not valid."})";
  auto out = run(r, {});
  EXPECT_EQ(out.error, "HTTP 400: " + r.body);
  EXPECT_TRUE(out.items.empty());
}

TEST(RestJson, TransportErrorWins) {
  HttpResponse r;
  r.error = "connection reset";
  EXPECT_EQ(run(r, {}).error, "connection reset");
}

TEST(RestJson, MalformedObjectBodyIsAnError) {
  EXPECT_EQ(run(ok("{not json"), {"data"}).error, "malformed response");
}

// simdjson On-Demand validates lazily, so a truncated array is not caught at
// parse time; it reads as an empty list. That is harmless for the reconcilers
// -- OrderStore::reconcile only applies orders a snapshot LISTS, so an empty
// one changes nothing -- but it is not reported as a failure either.
TEST(RestJson, TruncatedArrayYieldsNothing) {
  auto r = run(ok("[{"), {});
  EXPECT_TRUE(r.items.empty());
}

TEST(RestJson, ElementsThatDoNotParseAreSkipped) {
  auto r = run(ok(R"([{"id":1},{"symbol":"no id"},{"id":3}])"), {});
  EXPECT_EQ(r.items, (std::vector<std::int64_t>{1, 3}));
}

}  // namespace
