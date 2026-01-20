cmake_minimum_required(VERSION 3.13)
cmake_policy(SET CMP0077 NEW)
project(zvec-example-c++)
set(CMAKE_CXX_STANDARD 17)

# Enable compile commands generation
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(ZVEC_INCLUDE_DIR ${CMAKE_BINARY_DIR}/../../../src/include)
set(ZVEC_LIB_DIR ${CMAKE_BINARY_DIR}/../../../build/lib)
set(ZVEC_DEPENDENCY_LIB_DIR ${CMAKE_BINARY_DIR}/../../../build/external/usr/local/lib)

include_directories(${ZVEC_INCLUDE_DIR})
link_directories(${ZVEC_LIB_DIR} ${ZVEC_DEPENDENCY_LIB_DIR})

add_executable(example main.cc)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(GLOG_LIB glogd)
    set(GFLAGS_LIB gflags_nothreads_debug)
    set(PROTOBUF_LIB protobufd)
else()
    set(GLOG_LIB glog)
    set(GFLAGS_LIB gflags_nothreads)
    set(PROTOBUF_LIB protobuf)
endif()

set(zvec_dependencies
    roaring
    rocksdb
    arrow
    arrow_acero
    arrow_bundled_dependencies
    arrow_compute
    arrow_dataset
    parquet
    antlr4-runtime
    ${GLOG_LIB}
    ${GFLAGS_LIB}
    ${PROTOBUF_LIB}
    lz4
)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    target_link_libraries(example PRIVATE
        # zvec components
        zvec_db
        zvec_ailego
        # whole-archive for libzvec_core.a
        -Wl,--whole-archive
        ${ZVEC_LIB_DIR}/libzvec_core.a
        -Wl,--no-whole-archive
        # dependencies
        ${zvec_dependencies}
    )
elseif(APPLE)
    target_link_libraries(example PRIVATE
        # zvec components
        zvec_db
        zvec_ailego
        # force_load for macOS
        "-Wl,-force_load,${ZVEC_LIB_DIR}/libzvec_core.a"
        # dependencies
        ${zvec_dependencies}
    )
else()
    message(FATAL_ERROR "Unsupported platform: ${CMAKE_SYSTEM_NAME}")
endif()