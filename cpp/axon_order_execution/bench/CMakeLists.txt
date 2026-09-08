# Latency benchmarks.
#
# Hand-rolled rather than Google Benchmark: what matters here is the tail, and
# a harness that reports a mean with a confidence interval answers the wrong
# question. These measure every iteration into an HDR histogram and print
# percentiles.
#
# Numbers from a laptop are indicative only -- no core isolation, no pinning,
# a scheduler that will preempt anything. Trust them for relative comparisons
# and orders of magnitude; measure on the production host before believing an
# absolute figure.

add_executable(axon_bench bench_main.cpp)
target_link_libraries(axon_bench PRIVATE axon::transport axon::net axon::venue)

# ---------------------------------------------------------------------------
# JSON parser comparison. Opt-in, because it pulls two libraries the shipping
# code does not use.
#
#   cmake --preset default -DAXON_BENCH_JSON_LIBS=ON
# ---------------------------------------------------------------------------
if(AXON_BENCH_JSON_LIBS)
  include(AxonJsonBench)
  add_executable(axon_bench_json bench_json.cpp)
  target_link_libraries(axon_bench_json PRIVATE
    axon::core
    nlohmann_json::nlohmann_json
    axon_rapidjson
    axon_simdjson
  )
  # memmem is a GNU extension; needed by the hand-written scanner baseline.
  target_compile_definitions(axon_bench_json PRIVATE _GNU_SOURCE)
endif()

# ---------------------------------------------------------------------------
# Coroutine overhead. No external dependencies -- C++20 coroutines are in the
# language. Answers "can a coroutine go on the hot path" with numbers.
# ---------------------------------------------------------------------------
add_executable(axon_bench_coroutine bench_coroutine.cpp)
target_link_libraries(axon_bench_coroutine PRIVATE axon::core)
