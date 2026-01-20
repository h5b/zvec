include(${PROJECT_ROOT_DIR}/cmake/bazel.cmake)
include(${PROJECT_ROOT_DIR}/cmake/option.cmake)

cc_proto_library(
  NAME zvec_proto STATIC
  SRCS proto/*.proto
  PROTOROOT ./
)

cc_directory(common)
cc_directory(index)
cc_directory(sqlengine)

message(STATUS "proto include ${zvec_proto_SRCS} ${zvec_proto_HDR_DIR} ${CMAKE_CURRENT_BINARY_DIR}")

cc_library(
  NAME zvec_db STATIC STRICT
  SRCS 
    # collection.cc
    *.cc 
    # common
    common/*.cc
    # index
    index/*.cc 
    index/segment/*.cc 
    index/column/vector_column/*.cc 
    index/column/inverted_column/*.cc 
    index/storage/*.cc 
    index/storage/wal/*.cc 
    index/common/*.cc
    # sqlengine
    sqlengine/*.cc
    sqlengine/common/*.cc
    sqlengine/antlr/gen/*.cc
    sqlengine/parser/*.cc
    sqlengine/analyzer/*.cc
    sqlengine/planner/*.cc
    sqlengine/planner/ops/*.cc
    sqlengine/planner/physical_rules/*.cc
    # proto
    ${CMAKE_CURRENT_BINARY_DIR}/proto/*.cc
  INCS . ${CMAKE_CURRENT_BINARY_DIR}
  LIBS 
    zvec_ailego
    zvec_core
    glog
    roaring
    rocksdb
    antlr4
    libprotobuf
    Arrow::arrow_static
    Arrow::arrow_compute
    Arrow::arrow_dataset
    Arrow::arrow_acero
  DEPS zvec_proto
  VERSION "${PROXIMA_ZVEC_VERSION}"
)