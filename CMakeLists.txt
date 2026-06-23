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
    src/otlp_uri.cpp
    src/otlp_column_promote.cpp
    src/otlp_server.cpp
    src/otlp_server_http.cpp
    src/otlp_server_grpc.cpp
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

  add_executable(duckdb_otlp_server src/server/main.cpp
                                    src/server/server_config.cpp)
  target_include_directories(
    duckdb_otlp_server
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/server
            ${CMAKE_CURRENT_SOURCE_DIR}/src
            ${CMAKE_CURRENT_SOURCE_DIR}/src/storage
            ${CMAKE_CURRENT_SOURCE_DIR}/src/include)
  set_target_properties(duckdb_otlp_server PROPERTIES CXX_STANDARD 17
                                                      CXX_STANDARD_REQUIRED ON)
  # Link the zlib-enabled OTLP archive before duckdb_static. Both archives
  # contain weak inline cpp-httplib symbols; ordering DuckDB first selects its
  # non-zlib request decompressor and makes the daemon reject gzip OTLP bodies
  # with 415. This depends on linker weak-symbol resolution order, so it can
  # regress silently if DuckDB's httplib vendoring or the linker changes; the
  # gzip-encoded POST in scripts/smoke_test.py (the docker-smoke CI gate) is the
  # canary that catches a regression.
  target_link_libraries(duckdb_otlp_server ${EXTENSION_NAME} duckdb_static
                        ${DUCKDB_EXTRA_LINK_FLAGS})
  link_threads(duckdb_otlp_server "")
  link_extension_libraries(duckdb_otlp_server "")

  # Direct C++ ingest/seal harness (test/cpp/test_seal_harness.cpp). It needs
  # the test-only OtlpServer seam (DUCKDB_OTLP_ENABLE_TEST_SEAM), so it links a
  # SEPARATE static library built from the same sources with that macro defined.
  # The production archives (${EXTENSION_NAME} / ${LOADABLE_EXTENSION_NAME}) and
  # the daemon are built WITHOUT the macro, so production stays byte-for-byte
  # identical. Gated on BUILD_UNITTESTS (the normal release build sets it TRUE;
  # WASM/EMSCRIPTEN is excluded by the enclosing NOT EMSCRIPTEN block) so it is
  # built only where wanted.
  if(BUILD_UNITTESTS
     AND EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test/cpp/test_seal_harness.cpp")
    add_library(otlp_test_seam STATIC ${EXTENSION_SOURCES})
    target_compile_definitions(
      otlp_test_seam PUBLIC DUCKDB_OTLP_ENABLE_TEST_SEAM
                            CPPHTTPLIB_ZLIB_SUPPORT)
    set_target_properties(otlp_test_seam PROPERTIES CXX_STANDARD 17
                                                    CXX_STANDARD_REQUIRED ON)
    target_link_libraries(otlp_test_seam duckdb_static
                          otlp2records::otlp2records ZLIB::ZLIB)

    add_executable(otlp_seal_harness test/cpp/test_seal_harness.cpp)
    target_compile_definitions(otlp_seal_harness
                               PRIVATE DUCKDB_OTLP_ENABLE_TEST_SEAM)
    target_include_directories(
      otlp_seal_harness
      PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
              ${CMAKE_CURRENT_SOURCE_DIR}/src/include
              ${DUCKDB_MODULE_BASE_DIR}/third_party/catch)
    set_target_properties(otlp_seal_harness PROPERTIES CXX_STANDARD 17
                                                       CXX_STANDARD_REQUIRED ON)
    target_link_libraries(otlp_seal_harness otlp_test_seam duckdb_static
                          ${DUCKDB_EXTRA_LINK_FLAGS})
    link_threads(otlp_seal_harness "")
    link_extension_libraries(otlp_seal_harness "")

    # Direct C++ harness for the Arrow list-conversion guard
    # (test/cpp/test_arrow_harness.cpp). It reuses the same otlp_test_seam
    # archive (which contains otlp_arrow.cpp / CopyArrowToDuckDB) but is a
    # SEPARATE Catch2 executable (its own main) because the list-child offset!=0
    # branch is unreachable from SQL — the Rust producer always emits child
    # offset 0.
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/test/cpp/test_arrow_harness.cpp")
      add_executable(otlp_arrow_harness test/cpp/test_arrow_harness.cpp)
      target_compile_definitions(otlp_arrow_harness
                                 PRIVATE DUCKDB_OTLP_ENABLE_TEST_SEAM)
      target_include_directories(
        otlp_arrow_harness
        PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src
                ${CMAKE_CURRENT_SOURCE_DIR}/src/include
                ${DUCKDB_MODULE_BASE_DIR}/third_party/catch)
      set_target_properties(otlp_arrow_harness
                            PROPERTIES CXX_STANDARD 17 CXX_STANDARD_REQUIRED ON)
      target_link_libraries(otlp_arrow_harness otlp_test_seam duckdb_static
                            ${DUCKDB_EXTRA_LINK_FLAGS})
      link_threads(otlp_arrow_harness "")
      link_extension_libraries(otlp_arrow_harness "")
    endif()
  endif()
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
