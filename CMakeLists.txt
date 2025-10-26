cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME otlp)

# Detect WASM build
set(BUILD_WASM OFF)
if(EMSCRIPTEN OR CMAKE_SYSTEM_NAME STREQUAL "Emscripten")
  set(BUILD_WASM ON)
  message(
    STATUS
      "Building OTLP extension for WebAssembly (JSON-only, no protobuf support)"
  )
endif()

# Find dependencies (skip protobuf for WASM builds)
if(NOT BUILD_WASM)
  find_package(Protobuf CONFIG)
endif()

# Require C++17 for std::optional support
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage
  ${CMAKE_CURRENT_SOURCE_DIR}/src/schema
  ${CMAKE_CURRENT_SOURCE_DIR}/src/receiver
  ${CMAKE_CURRENT_SOURCE_DIR}/src/parsers
  ${CMAKE_CURRENT_SOURCE_DIR}/src/function)

# Common sources (used by both native and WASM builds)
set(COMMON_SOURCES
    src/storage/otlp_extension.cpp src/parsers/json_parser.cpp
    src/function/read_otlp.cpp src/receiver/row_builders_traces_logs.cpp
    src/receiver/row_builders.cpp src/receiver/row_builders_metrics.cpp)

# Conditional sources based on build target
if(BUILD_WASM)
  # WASM build: use stubs instead of protobuf
  set(EXTENSION_SOURCES ${COMMON_SOURCES} src/wasm/protobuf_parser_stub.cpp
                        src/wasm/format_detector_stub.cpp)
else()
  # Native build: use full protobuf support
  set(PROTO_SOURCES
      src/generated/opentelemetry/proto/common/v1/common.pb.cc
      src/generated/opentelemetry/proto/resource/v1/resource.pb.cc
      src/generated/opentelemetry/proto/trace/v1/trace.pb.cc
      src/generated/opentelemetry/proto/metrics/v1/metrics.pb.cc
      src/generated/opentelemetry/proto/logs/v1/logs.pb.cc)

  set(EXTENSION_SOURCES ${COMMON_SOURCES} src/parsers/protobuf_parser.cpp
                        src/parsers/format_detector.cpp ${PROTO_SOURCES})
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
set_target_properties(${EXTENSION_NAME} PROPERTIES CXX_STANDARD 17
                                                   CXX_STANDARD_REQUIRED ON)

build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
set_target_properties(${LOADABLE_EXTENSION_NAME}
                      PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)

# Add protobuf include directories (only for native builds)
if(NOT BUILD_WASM)
  target_include_directories(${EXTENSION_NAME}
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/generated)
  target_include_directories(${LOADABLE_EXTENSION_NAME}
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/generated)

  if(NOT Protobuf_FOUND)
    message(FATAL_ERROR "Protobuf library is required to build otlp extension")
  endif()

  target_link_libraries(${EXTENSION_NAME} protobuf::libprotobuf)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} protobuf::libprotobuf)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
