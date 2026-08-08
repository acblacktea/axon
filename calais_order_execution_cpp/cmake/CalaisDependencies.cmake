# Dependency acquisition.
#
# Strategy: prefer a system/vcpkg-provided package if one is present, otherwise
# fetch a pinned version. Kept deliberately small -- the foundation layer must
# stay buildable on a bare machine.
#
# Note on nlohmann/json: we pull the single header rather than the full CMake
# project. Its packaged CMakeLists declares cmake_minimum_required(VERSION 3.1),
# which CMake >= 4.0 rejects outright. The single header sidesteps that and
# builds faster.

include(FetchContent)

set(CALAIS_NLOHMANN_VERSION "v3.11.3")
set(CALAIS_GTEST_VERSION    "v1.15.2")
set(CALAIS_SIMDJSON_VERSION "v3.10.1")

# ---------------------------------------------------------------------------
# simdjson -- the exchange feed parser.
#
# Chosen on measurement, not reputation: extracting the six fields an OMS needs
# from a 456-byte Deribit order update costs 125ns here against 1000ns for
# rapidjson DOM and 4127ns for nlohmann. The win is structural rather than a
# faster inner loop -- On-Demand never materialises the document, it walks
# lazily in a single SIMD pass and touches only the fields asked for. See
# bench/bench_json.cpp for the numbers and the full comparison.
#
# The single-file amalgamation is used deliberately: one translation unit, no
# CMake package to keep in sync, and it pins exactly.
# ---------------------------------------------------------------------------
set(_calais_simdjson_dir "${CMAKE_BINARY_DIR}/_deps/simdjson-single")
foreach(_f simdjson.h simdjson.cpp)
  if(NOT EXISTS "${_calais_simdjson_dir}/${_f}")
    message(STATUS "Fetching simdjson ${CALAIS_SIMDJSON_VERSION} (${_f})")
    file(DOWNLOAD
      "https://github.com/simdjson/simdjson/releases/download/${CALAIS_SIMDJSON_VERSION}/${_f}"
      "${_calais_simdjson_dir}/${_f}"
      TLS_VERIFY ON
      STATUS _calais_sj_status)
    list(GET _calais_sj_status 0 _calais_sj_rc)
    if(NOT _calais_sj_rc EQUAL 0)
      list(GET _calais_sj_status 1 _calais_sj_err)
      file(REMOVE "${_calais_simdjson_dir}/${_f}")
      message(FATAL_ERROR "Failed to download simdjson ${_f}: ${_calais_sj_err}")
    endif()
  endif()
endforeach()

add_library(calais_simdjson STATIC "${_calais_simdjson_dir}/simdjson.cpp")
# SYSTEM: simdjson is not built to our warning set, and it is not ours to fix.
target_include_directories(calais_simdjson SYSTEM PUBLIC "${_calais_simdjson_dir}")
target_compile_options(calais_simdjson PRIVATE -w)
set_target_properties(calais_simdjson PROPERTIES
  CXX_STANDARD 20
  POSITION_INDEPENDENT_CODE ON)

# ---------------------------------------------------------------------------
# nlohmann/json (control-plane JSON only -- never on the hot path)
# ---------------------------------------------------------------------------
find_package(nlohmann_json 3.11 QUIET)

if(NOT nlohmann_json_FOUND)
  set(_calais_json_dir "${CMAKE_BINARY_DIR}/_deps/nlohmann-single")
  set(_calais_json_hpp "${_calais_json_dir}/nlohmann/json.hpp")

  if(NOT EXISTS "${_calais_json_hpp}")
    message(STATUS "Fetching nlohmann/json ${CALAIS_NLOHMANN_VERSION} (single header)")
    file(DOWNLOAD
      "https://raw.githubusercontent.com/nlohmann/json/${CALAIS_NLOHMANN_VERSION}/single_include/nlohmann/json.hpp"
      "${_calais_json_hpp}"
      TLS_VERIFY ON
      STATUS _calais_json_status
      SHOW_PROGRESS)
    list(GET _calais_json_status 0 _calais_json_rc)
    if(NOT _calais_json_rc EQUAL 0)
      list(GET _calais_json_status 1 _calais_json_err)
      file(REMOVE "${_calais_json_hpp}")
      message(FATAL_ERROR "Failed to download nlohmann/json: ${_calais_json_err}")
    endif()
  endif()

  add_library(nlohmann_json INTERFACE)
  add_library(nlohmann_json::nlohmann_json ALIAS nlohmann_json)
  target_include_directories(nlohmann_json SYSTEM INTERFACE "${_calais_json_dir}")
endif()

# ---------------------------------------------------------------------------
# GoogleTest
# ---------------------------------------------------------------------------
if(CALAIS_BUILD_TESTS)
  find_package(GTest QUIET)

  if(NOT GTest_FOUND)
    message(STATUS "Fetching googletest ${CALAIS_GTEST_VERSION}")
    FetchContent_Declare(googletest
      URL "https://github.com/google/googletest/archive/refs/tags/${CALAIS_GTEST_VERSION}.tar.gz"
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)
  endif()
endif()
