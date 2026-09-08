include(GoogleTest)

add_executable(axon_tests
  test_decimal.cpp
  test_timestamp.cpp
  test_clock.cpp
  test_histogram.cpp
  test_spsc_ring.cpp
  test_shm_ring.cpp
  test_object_pool.cpp
  test_latency.cpp
  test_enums.cpp
  test_wire.cpp
  test_hot_messages.cpp
  test_byte_buffer.cpp
  test_ws_frame.cpp
  test_ws_message.cpp
  test_ws_handshake.cpp
  test_deribit_parser.cpp
  test_deribit_builder.cpp
  test_perp_venues.cpp
  test_ws_order_entry.cpp
  test_ws_connection.cpp
)

target_include_directories(axon_tests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(axon_tests PRIVATE
  axon::transport
  axon::net
  axon::venue
  GTest::gtest
  GTest::gtest_main
)

gtest_discover_tests(axon_tests
  DISCOVERY_TIMEOUT 60
  PROPERTIES TIMEOUT 120
)

# ---------------------------------------------------------------------------
# Cross-implementation wire compatibility.
#
# Emits canonical objects as JSON from C++, then has the real Python
# implementation parse them with transport/serialization.py, rebuild the
# dataclasses, and re-serialise. Any divergence in key order, enum spelling,
# datetime format, or numeric formatting fails here.
#
# This is the single most valuable test in the repo: it is what guarantees an
# existing Python strategy can talk to the C++ engine without modification.
# ---------------------------------------------------------------------------
add_executable(axon_wire_fixtures wire_fixture_tool.cpp)
target_link_libraries(axon_wire_fixtures PRIVATE axon::transport)

# Each service lives at <language>/<service>/ in one repo. This file is
# cpp/axon_order_execution/tests/, so the matching Python project root is
# ../../../python/axon_order_execution. Allow an override for anyone
# building the C++ tree standalone.
set(AXON_PYTHON_PROJECT "${CMAKE_CURRENT_SOURCE_DIR}/../../../python/axon_order_execution"
    CACHE PATH "Root of the Python implementation, used by python_wire_compat")

# Probe for the module this test actually imports rather than for the directory.
# A directory check would still pass if the layout moved, and the test would
# then silently skip -- which is the worst outcome for the one test that
# guarantees Python strategies keep working.
set(_axon_serialization
    "${AXON_PYTHON_PROJECT}/axon_order_execution/transport/serialization.py")

find_program(AXON_PYTHON
  NAMES python3 python
  HINTS "${AXON_PYTHON_PROJECT}/.venv/bin"
  NO_CMAKE_SYSTEM_PATH
)
if(NOT AXON_PYTHON)
  find_program(AXON_PYTHON NAMES python3 python)
endif()

if(AXON_PYTHON AND EXISTS "${_axon_serialization}")
  add_test(NAME python_wire_compat
    COMMAND "${AXON_PYTHON}"
            "${CMAKE_CURRENT_SOURCE_DIR}/python_wire_compat.py"
            --fixture-tool "$<TARGET_FILE:axon_wire_fixtures>"
            --python-project "${AXON_PYTHON_PROJECT}"
  )
  set_tests_properties(python_wire_compat PROPERTIES TIMEOUT 120)
  message(STATUS "  python wire compat . ${AXON_PYTHON}")
elseif(NOT EXISTS "${_axon_serialization}")
  message(WARNING
    "python_wire_compat is DISABLED: could not find "
    "${_axon_serialization}. Set -DAXON_PYTHON_PROJECT=<path> to the root "
    "of the Python implementation. Without this test, nothing verifies that "
    "existing Python strategies can still talk to this engine.")
else()
  message(WARNING "python_wire_compat is DISABLED: no Python interpreter found.")
endif()
