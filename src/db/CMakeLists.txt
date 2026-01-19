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

cc_library(
  NAME zvec_db SHARED STATIC STRICT
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
  INCS .
  LIBS 
    zvec_ailego
    zvec_proto
    core_interface
    glog
    roaring
    rocksdb
    antlr4
    Arrow::arrow_static
    Arrow::arrow_compute
    Arrow::arrow_dataset
    Arrow::arrow_acero
  VERSION "${PROXIMA_ZVEC_VERSION}"
)