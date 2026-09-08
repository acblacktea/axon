#include "axon_market_data/http_client.hpp"

#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <openssl/ssl.h>

namespace axon_market_data {

namespace beast = boost::beast;
namespace ssl   = net::ssl;
namespace http  = beast::http;
using tcp       = net::ip::tcp;

// ---------------------------------------------------------------------------
// Internal: perform a request and return the response
// ---------------------------------------------------------------------------

static net::awaitable<HttpResponse> do_request(
    std::string_view host,
    http::request<http::string_body> req) {

    auto executor = co_await net::this_coro::executor;

    // Resolve
    tcp::resolver resolver(executor);
    auto results = co_await resolver.async_resolve(
        std::string(host), "443", net::use_awaitable);

    // SSL context
    ssl::context ctx(ssl::context::tlsv12_client);
    ctx.set_default_verify_paths();

    beast::ssl_stream<beast::tcp_stream> stream(executor, ctx);
    SSL_set_tlsext_host_name(stream.native_handle(), std::string(host).c_str());

    // TCP connect
    auto& tcp_layer = beast::get_lowest_layer(stream);
    tcp_layer.expires_after(std::chrono::seconds(10));
    co_await tcp_layer.async_connect(results, net::use_awaitable);

    // SSL handshake
    tcp_layer.expires_after(std::chrono::seconds(10));
    co_await stream.async_handshake(ssl::stream_base::client, net::use_awaitable);

    // Send request
    tcp_layer.expires_after(std::chrono::seconds(10));
    co_await http::async_write(stream, req, net::use_awaitable);

    // Read response
    beast::flat_buffer buf;
    http::response<http::string_body> res;
    co_await http::async_read(stream, buf, res, net::use_awaitable);

    // Build result
    HttpResponse result;
    result.status = res.result_int();
    result.body   = std::move(res.body());
    for (auto& field : res) {
        result.headers[std::string(field.name_string())] =
            std::string(field.value());
    }

    // Graceful shutdown (best-effort)
    beast::error_code ec;
    stream.shutdown(ec); // ignore errors

    co_return result;
}

// ---------------------------------------------------------------------------
// GET
// ---------------------------------------------------------------------------

net::awaitable<HttpResponse> http_get(
    std::string_view host,
    std::string_view target,
    const Headers& extra_headers) {

    http::request<http::string_body> req(
        http::verb::get, std::string(target), 11);
    req.set(http::field::host, host);
    req.set(http::field::user_agent, "axon_market_data/0.1");

    for (auto& [k, v] : extra_headers)
        req.set(k, v);

    co_return co_await do_request(host, std::move(req));
}

// ---------------------------------------------------------------------------
// POST
// ---------------------------------------------------------------------------

net::awaitable<HttpResponse> http_post(
    std::string_view host,
    std::string_view target,
    std::string_view content_type,
    std::string body,
    const Headers& extra_headers) {

    http::request<http::string_body> req(
        http::verb::post, std::string(target), 11);
    req.set(http::field::host, host);
    req.set(http::field::user_agent, "axon_market_data/0.1");
    req.set(http::field::content_type, content_type);
    req.body() = std::move(body);
    req.prepare_payload();

    for (auto& [k, v] : extra_headers)
        req.set(k, v);

    co_return co_await do_request(host, std::move(req));
}

} // namespace axon_market_data
