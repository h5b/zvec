##
##  Copyright (C) The Software Authors. All rights reserved.
##
##  \file     CMakeLists.txt
##  \author   Hechong.xyf
##  \date     Oct 2019
##  \version  1.0
##  \brief    Detail cmake build script
##

include(${CMAKE_SOURCE_DIR}/cmake/bazel.cmake)

file(GLOB_RECURSE ALL_SRCS *.cc *.c)

git_version(GIT_SRCS_VER ${CMAKE_CURRENT_SOURCE_DIR})
cc_library(
    NAME core_knn_diskann STATIC SHARED STRICT ALWAYS_LINK
    SRCS *.cc
    LIBS zvec_ailego core_framework core_knn_cluster aio
    INCS . ${PROJECT_ROOT_DIR}/src/core ${PROJECT_ROOT_DIR}/src/core/algorithm
    VERSION "${PROXIMA_ZVEC_VERSION}"
  )