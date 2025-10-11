cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME duckspan)

# Find dependencies (vcpkg will provide these)
find_package(gRPC CONFIG)
find_package(Protobuf)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

# Generated protobuf sources (Phase 5.1)
set(PROTO_SOURCES
    src/generated/opentelemetry/proto/common/v1/common.pb.cc
    src/generated/opentelemetry/proto/resource/v1/resource.pb.cc
    src/generated/opentelemetry/proto/trace/v1/trace.pb.cc
    src/generated/opentelemetry/proto/metrics/v1/metrics.pb.cc
    src/generated/opentelemetry/proto/logs/v1/logs.pb.cc
    src/generated/opentelemetry/proto/collector/trace/v1/trace_service.pb.cc
    src/generated/opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.cc
    src/generated/opentelemetry/proto/collector/metrics/v1/metrics_service.pb.cc
    src/generated/opentelemetry/proto/collector/metrics/v1/metrics_service.grpc.pb.cc
    src/generated/opentelemetry/proto/collector/logs/v1/logs_service.pb.cc
    src/generated/opentelemetry/proto/collector/logs/v1/logs_service.grpc.pb.cc
)

set(EXTENSION_SOURCES
    src/duckspan_extension.cpp
    src/otlp_schema.cpp
    src/json_parser.cpp
    src/protobuf_parser.cpp
    src/format_detector.cpp
    src/read_otlp.cpp
    src/ring_buffer.cpp
    src/otlp_catalog.cpp
    src/otlp_schema_entry.cpp
    src/otlp_table_entry.cpp
    src/otlp_scan.cpp
    src/otlp_storage_extension.cpp
    src/otlp_receiver.cpp
    src/otlp_transaction.cpp
    ${PROTO_SOURCES}
)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Add protobuf/gRPC include directories
target_include_directories(${EXTENSION_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/generated)
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/src/proto
    ${CMAKE_CURRENT_SOURCE_DIR}/src/generated)

# Link Protobuf (found from Homebrew)
if(Protobuf_FOUND)
    target_link_libraries(${EXTENSION_NAME} protobuf::libprotobuf)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} protobuf::libprotobuf)
endif()

# Link gRPC if available (for Phase 5 - gRPC receiver)
if(gRPC_FOUND)
    target_link_libraries(${EXTENSION_NAME} gRPC::grpc++)
    target_link_libraries(${LOADABLE_EXTENSION_NAME} gRPC::grpc++)
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
