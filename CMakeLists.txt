cmake_minimum_required(VERSION 3.5)

set(TARGET_NAME server)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

set(LPTS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/lpts")
include_directories(src/include ${LPTS_DIR}/src/include)

set(EXTENSION_SOURCES
    src/server_extension.cpp
    src/common/common.cpp
    src/server/run_server.cpp
    src/flush/flush_function.cpp
    src/flush/update_metrics.cpp
    src/parser/parser.cpp
    src/parser/parser_helper.cpp
    src/parser/parser_helpers.cpp
    src/compiler/compiler_utils.cpp
    src/compiler/generate_python_script.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
