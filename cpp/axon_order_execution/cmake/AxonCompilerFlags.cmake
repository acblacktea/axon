# Central compiler configuration, exported as an INTERFACE target so every
# axon_* library and test picks up an identical flag set.

find_package(Threads REQUIRED)

add_library(axon_compiler_flags INTERFACE)

target_compile_features(axon_compiler_flags INTERFACE cxx_std_20)

if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang|AppleClang")
  target_compile_options(axon_compiler_flags INTERFACE
    -Wall
    -Wextra
    -Wpedantic
    -Wshadow
    -Wconversion
    -Wsign-conversion
    -Wcast-qual
    -Wold-style-cast
    -Wnon-virtual-dtor
    -Woverloaded-virtual
    -Wdouble-promotion
    -Wformat=2
    -Wundef
  )

  if(AXON_WARNINGS_AS_ERRORS)
    target_compile_options(axon_compiler_flags INTERFACE -Werror)
  endif()

  # Release: optimise hard, but keep frame pointers so `perf` can unwind the
  # hot path. Losing 1 register is worth being able to profile production.
  target_compile_options(axon_compiler_flags INTERFACE
    $<$<CONFIG:Release>:-O3>
    $<$<CONFIG:Release>:-fno-omit-frame-pointer>
    $<$<CONFIG:RelWithDebInfo>:-O2>
    $<$<CONFIG:RelWithDebInfo>:-fno-omit-frame-pointer>
  )

  if(AXON_NATIVE_ARCH)
    include(CheckCXXCompilerFlag)
    check_cxx_compiler_flag(-march=native AXON_HAS_MARCH_NATIVE)
    if(AXON_HAS_MARCH_NATIVE)
      target_compile_options(axon_compiler_flags INTERFACE -march=native)
    else()
      # Apple Silicon clang wants -mcpu=native instead.
      check_cxx_compiler_flag(-mcpu=native AXON_HAS_MCPU_NATIVE)
      if(AXON_HAS_MCPU_NATIVE)
        target_compile_options(axon_compiler_flags INTERFACE -mcpu=native)
      endif()
    endif()
  endif()

  if(AXON_ASAN)
    target_compile_options(axon_compiler_flags INTERFACE
      -fsanitize=address,undefined -fno-sanitize-recover=all -g)
    target_link_options(axon_compiler_flags INTERFACE
      -fsanitize=address,undefined)
  endif()

  if(AXON_TSAN)
    target_compile_options(axon_compiler_flags INTERFACE -fsanitize=thread -g)
    target_link_options(axon_compiler_flags INTERFACE -fsanitize=thread)
  endif()
endif()

if(UNIX AND NOT APPLE)
  # shm_open / clock_gettime live in librt on older glibc; harmless otherwise.
  target_link_libraries(axon_compiler_flags INTERFACE rt)
endif()
