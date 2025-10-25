cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME otlp)

# Find dependencies
find_package(Protobuf)

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

# Generated protobuf sources (Phase 5.1)
set(PROTO_SOURCES
    src/generated/opentelemetry/proto/common/v1/common.pb.cc
    src/generated/opentelemetry/proto/resource/v1/resource.pb.cc
    src/generated/opentelemetry/proto/trace/v1/trace.pb.cc
    src/generated/opentelemetry/proto/metrics/v1/metrics.pb.cc
    src/generated/opentelemetry/proto/logs/v1/logs.pb.cc)

set(EXTENSION_SOURCES
    src/storage/otlp_extension.cpp
    src/parsers/json_parser.cpp
    src/parsers/protobuf_parser.cpp
    src/parsers/format_detector.cpp
    src/function/read_otlp.cpp
    src/receiver/row_builders_traces_logs.cpp
    src/receiver/row_builders.cpp
    src/receiver/row_builders_metrics.cpp
    ${PROTO_SOURCES})

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Add protobuf/gRPC include directories
target_include_directories(${EXTENSION_NAME}
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/generated)
target_include_directories(${LOADABLE_EXTENSION_NAME}
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/generated)

if(NOT Protobuf_FOUND)
  message(FATAL_ERROR "Protobuf library is required to build otlp extension")
endif()

target_link_libraries(${EXTENSION_NAME} protobuf::libprotobuf)
target_link_libraries(${LOADABLE_EXTENSION_NAME} protobuf::libprotobuf)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
