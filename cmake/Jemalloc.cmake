set(REDIS_PROXY_JEMALLOC_PREFIX ${CMAKE_BINARY_DIR}/jemalloc)
set(REDIS_PROXY_JEMALLOC_LIB ${REDIS_PROXY_JEMALLOC_PREFIX}/lib/libjemalloc.a)

add_custom_command(
  OUTPUT ${REDIS_PROXY_JEMALLOC_LIB}
  COMMAND ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build_jemalloc.sh ${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_BINARY_DIR}
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/scripts/build_jemalloc.sh
  COMMENT "Building bundled jemalloc")

add_custom_target(redis_proxy_jemalloc_build DEPENDS ${REDIS_PROXY_JEMALLOC_LIB})

add_library(redis_proxy_jemalloc STATIC IMPORTED GLOBAL)
set_target_properties(redis_proxy_jemalloc PROPERTIES
  IMPORTED_LOCATION ${REDIS_PROXY_JEMALLOC_LIB}
  INTERFACE_INCLUDE_DIRECTORIES ${REDIS_PROXY_JEMALLOC_PREFIX}/include)
add_dependencies(redis_proxy_jemalloc redis_proxy_jemalloc_build)
