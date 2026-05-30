cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME otlp)

# Require C++17 for std::optional support
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

project(${TARGET_NAME})

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_SOURCE_DIR}/src/storage
  ${CMAKE_CURRENT_SOURCE_DIR}/src/function
  ${CMAKE_CURRENT_SOURCE_DIR}/src/include
  ${CMAKE_CURRENT_SOURCE_DIR}/duckdb/third_party/httplib)

# Extension sources (Rust backend only)
set(EXTENSION_SOURCES
    src/storage/otlp_extension.cpp
    src/function/read_otlp.cpp
    src/otlp_arrow.cpp
    src/otlp_disk_buffer.cpp
    src/otlp_disk_codec.cpp
    src/otlp_request.cpp
    src/otlp_uri.cpp
    src/otlp_server.cpp
    src/otlp_storage.cpp
    src/otlp_start_stop.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
set_target_properties(${EXTENSION_NAME} PROPERTIES CXX_STANDARD 17
                                                   CXX_STANDARD_REQUIRED ON)

build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
set_target_properties(${LOADABLE_EXTENSION_NAME}
                      PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)

# Rust backend integration (required)
message(STATUS "Enabling Rust backend via otlp2records")

# Find/build otlp2records
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(FindOtlp2Records)

# Link the Rust library
target_link_libraries(${EXTENSION_NAME} otlp2records::otlp2records)
target_link_libraries(${LOADABLE_EXTENSION_NAME} otlp2records::otlp2records)

if(NOT EMSCRIPTEN)
  find_package(ZLIB REQUIRED)
  target_compile_definitions(${EXTENSION_NAME} PRIVATE CPPHTTPLIB_ZLIB_SUPPORT)
  target_compile_definitions(${LOADABLE_EXTENSION_NAME}
                             PRIVATE CPPHTTPLIB_ZLIB_SUPPORT)
  target_link_libraries(${EXTENSION_NAME} ZLIB::ZLIB)
  target_link_libraries(${LOADABLE_EXTENSION_NAME} ZLIB::ZLIB)
endif()

# Define preprocessor macro for conditional compilation
target_compile_definitions(${EXTENSION_NAME} PRIVATE OTLP_RUST_BACKEND)
target_compile_definitions(${LOADABLE_EXTENSION_NAME} PRIVATE OTLP_RUST_BACKEND)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
