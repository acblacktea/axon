#pragma once

#include <string>
#include <string_view>
#include <unordered_map>

#include <boost/asio.hpp>

namespace axon_market_data {

namespace net = boost::asio;

using Headers = std::unordered_map<std::string, std::string>;

struct HttpResponse {
    int         status = 0;
    std::string body;
    Headers     headers;
};

// Async HTTPS GET / POST. One TCP+SSL connection per call (stateless).
// Suitable for infrequent REST calls (snapshots, instrument lists).

net::awaitable<HttpResponse> http_get(
    std::string_view host,
    std::string_view target,
    const Headers& extra_headers = {});

net::awaitable<HttpResponse> http_post(
    std::string_view host,
    std::string_view target,
    std::string_view content_type,
    std::string body,
    const Headers& extra_headers = {});

} // namespace axon_market_data
