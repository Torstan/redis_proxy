cmake_minimum_required(VERSION 3.16)
project(redis_proxy LANGUAGES C CXX ASM)

set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(REDIS_PROXY_USE_JEMALLOC "Link redis_proxy with bundled jemalloc" ON)
option(REDIS_PROXY_BUILD_TESTS "Build redis_proxy tests" ON)
option(REDIS_PROXY_BUILD_BENCH "Build redis_proxy benchmarks" ON)

include(cmake/Jemalloc.cmake)

add_subdirectory(thirdparty/libco EXCLUDE_FROM_ALL)

set(CPP_UTIL_CONN_UTIL_DIR ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/cpp_util/conn_util)
add_library(cpp_util_conn_util STATIC
  ${CPP_UTIL_CONN_UTIL_DIR}/src/endpoint.cpp
  ${CPP_UTIL_CONN_UTIL_DIR}/src/fd_notifier.cpp
  ${CPP_UTIL_CONN_UTIL_DIR}/src/socket_utils.cpp)
target_include_directories(cpp_util_conn_util PUBLIC
  ${CPP_UTIL_CONN_UTIL_DIR}/include)
target_compile_features(cpp_util_conn_util PUBLIC cxx_std_17)

if(REDIS_PROXY_BUILD_TESTS)
  set(REDIS_PROXY_LIBCO_TEST_TARGETS
    test_co_routine
    test_co_async
    test_co_poll
    test_public_api)
  foreach(libco_test ${REDIS_PROXY_LIBCO_TEST_TARGETS})
    if(TARGET ${libco_test})
      list(APPEND REDIS_PROXY_PRESENT_LIBCO_TEST_TARGETS ${libco_test})
    endif()
  endforeach()
  if(REDIS_PROXY_PRESENT_LIBCO_TEST_TARGETS)
    add_custom_target(redis_proxy_libco_tests ALL
      DEPENDS ${REDIS_PROXY_PRESENT_LIBCO_TEST_TARGETS})
  endif()
endif()

add_library(redis_resp INTERFACE)
target_include_directories(redis_resp INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/cpp_util/redis/include)

file(GLOB REDIS_PROXY_CORE_SRCS CONFIGURE_DEPENDS src/*.cpp)
list(FILTER REDIS_PROXY_CORE_SRCS EXCLUDE REGEX ".*/main\\.cpp$")
add_library(redis_proxy_core ${REDIS_PROXY_CORE_SRCS})
target_include_directories(redis_proxy_core PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/libco)
target_link_libraries(redis_proxy_core PUBLIC cpp_util_conn_util colib_static redis_resp pthread)
if(NOT APPLE)
  target_link_libraries(redis_proxy_core PUBLIC dl)
endif()
if(REDIS_PROXY_USE_JEMALLOC)
  target_link_libraries(redis_proxy_core PUBLIC redis_proxy_jemalloc)
endif()

if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/src/main.cpp)
  add_executable(redis_proxy src/main.cpp)
  target_link_libraries(redis_proxy PRIVATE redis_proxy_core)
endif()

enable_testing()

if(REDIS_PROXY_BUILD_TESTS)
  file(GLOB REDIS_PROXY_TEST_SRCS CONFIGURE_DEPENDS tests/*_test.cpp)
  foreach(test_src ${REDIS_PROXY_TEST_SRCS})
    get_filename_component(test_name ${test_src} NAME_WE)
    add_executable(${test_name} ${test_src})
    target_link_libraries(${test_name} PRIVATE redis_proxy_core)
    add_test(NAME ${test_name} COMMAND ${test_name})
  endforeach()
endif()

if(REDIS_PROXY_BUILD_BENCH AND EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/bench/proxy_bench.cpp)
  add_executable(proxy_bench bench/proxy_bench.cpp)
  target_link_libraries(proxy_bench PRIVATE redis_proxy_core)
endif()
