# Optional JSON library comparison.
#
# Off by default. These libraries exist here only to answer "which parser
# should the exchange feed use", with numbers rather than folklore. Nothing in
# the shipping code depends on them.
#
# Enable with -DCALAIS_BENCH_JSON_LIBS=ON.

include(FetchContent)

set(CALAIS_RAPIDJSON_REF "master")
set(CALAIS_SIMDJSON_VERSION "v3.10.1")

# ---------------------------------------------------------------------------
# rapidjson -- header only.
#
# Fetched WITHOUT running its CMakeLists: it declares
# cmake_minimum_required(VERSION 2.8|3.0), which CMake >= 4.0 rejects outright.
# Pointing SOURCE_SUBDIR at a directory that does not exist makes
# FetchContent_MakeAvailable skip add_subdirectory and just unpack the sources.
# ---------------------------------------------------------------------------
FetchContent_Declare(rapidjson
  URL "https://github.com/Tencent/rapidjson/archive/refs/heads/${CALAIS_RAPIDJSON_REF}.tar.gz"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  SOURCE_SUBDIR does-not-exist
)
FetchContent_MakeAvailable(rapidjson)

add_library(calais_rapidjson INTERFACE)
# SYSTEM so its own warnings do not trip -Werror in our build.
target_include_directories(calais_rapidjson SYSTEM INTERFACE
  "${rapidjson_SOURCE_DIR}/include")
target_compile_definitions(calais_rapidjson INTERFACE
  # Without this rapidjson does unaligned loads that UBSan flags, and on some
  # targets they are genuinely unsafe.
  RAPIDJSON_HAS_STDSTRING=1
)

# ---------------------------------------------------------------------------
# simdjson -- the single-file amalgamation, which is all a benchmark needs.
# ---------------------------------------------------------------------------
set(_simdjson_dir "${CMAKE_BINARY_DIR}/_deps/simdjson-single")
foreach(_f simdjson.h simdjson.cpp)
  if(NOT EXISTS "${_simdjson_dir}/${_f}")
    message(STATUS "Fetching simdjson ${CALAIS_SIMDJSON_VERSION} (${_f})")
    file(DOWNLOAD
      "https://github.com/simdjson/simdjson/releases/download/${CALAIS_SIMDJSON_VERSION}/${_f}"
      "${_simdjson_dir}/${_f}"
      TLS_VERIFY ON
      STATUS _dl_status)
    list(GET _dl_status 0 _dl_rc)
    if(NOT _dl_rc EQUAL 0)
      list(GET _dl_status 1 _dl_err)
      file(REMOVE "${_simdjson_dir}/${_f}")
      message(FATAL_ERROR "Failed to download simdjson ${_f}: ${_dl_err}")
    endif()
  endif()
endforeach()

add_library(calais_simdjson STATIC "${_simdjson_dir}/simdjson.cpp")
target_include_directories(calais_simdjson SYSTEM PUBLIC "${_simdjson_dir}")
# simdjson is not built with our warning set; it is third-party measurement
# scaffolding, not code we maintain.
target_compile_options(calais_simdjson PRIVATE -w)
set_target_properties(calais_simdjson PROPERTIES CXX_STANDARD 20)
