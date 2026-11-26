cmake_minimum_required(VERSION 3.10)

# Set extension name here
set(TARGET_NAME otlp)

project(${TARGET_NAME})

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src/include)

set(EXTENSION_SOURCES
    src/otlp_extension.cpp
    src/otlp_read.cpp
    src/otlp_arrow.cpp
    src/otlp_function_docs.cpp
    src/otlp_uri.cpp)
if(NOT EMSCRIPTEN)
  list(APPEND EXTENSION_SOURCES
       src/otlp_column_promote.cpp
       src/otlp_server.cpp
       src/otlp_server_http.cpp
       src/otlp_server_grpc.cpp
       src/otlp_registry.cpp
       src/otlp_control.cpp)
endif()

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

if(MSVC)
  # cpp-httplib is a ~12k-line header compiled into several of the TUs above,
  # which pushes their object files past MSVC's default 2^16 section limit
  # (C1128). DuckDB applies /bigobj to its own sources but not to out-of-tree
  # extension targets.
  target_compile_options(${EXTENSION_NAME} PRIVATE /bigobj)
  target_compile_options(${LOADABLE_EXTENSION_NAME} PRIVATE /bigobj)
endif()

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

# Link the extensions DuckDB statically links (parquet, json, core_functions,
# ...) into an executable that embeds DuckDB. Since DuckDB 2.0 a linked
# extension only registers itself when the program roots it, and
# link_extension_libraries reads the set DuckDB resolves *after* its extension
# loop -- which is where this file runs. Calling it here would link nothing,
# leaving the daemon without the parquet COPY function, so the call is deferred
# to the end of the top-level configure, once the set is known. A deferred call
# expands its arguments when it runs, after ${target} has gone out of scope, so
# the target name is baked in with EVAL CODE.
function(otlp_link_duckdb_extensions target)
  set(deferred_link "link_extension_libraries(${target} \"\")")
  cmake_language(
    EVAL CODE "cmake_language(DEFER DIRECTORY [[${CMAKE_SOURCE_DIR}]]
                 CALL cmake_language EVAL CODE [[${deferred_link}]])")
endfunction()

# The native daemon and the C++ test harnesses below are built on native
# non-Windows targets only. `make release` builds the default ALL target, so
# without the NOT WIN32 guard the Windows extension build would compile and link
# them too — and the daemon is only ever shipped as the Linux distroless
# container image (docker/duckdb-otlp-server), never as a Windows binary.
# Windows therefore builds the extension (static + loadable) and nothing else.
if(NOT EMSCRIPTEN AND NOT WIN32)
  # Compile the mode presets (src/server/modes/*.sql) into the daemon as a
  # {name, sql} table. Written only when it changes, and re-run whenever a
  # preset is added, removed or edited.
  file(GLOB OTLP_MODE_FILES CONFIGURE_DEPENDS
       ${CMAKE_CURRENT_SOURCE_DIR}/src/server/modes/*.sql)
  set_property(
    DIRECTORY
    APPEND
    PROPERTY CMAKE_CONFIGURE_DEPENDS ${OTLP_MODE_FILES})
  set(OTLP_MODES_SRC "#include \"server_config.hpp\"\n")
  string(APPEND OTLP_MODES_SRC "namespace duckdb_otlp_server {\n")
  string(APPEND OTLP_MODES_SRC "const ModePreset MODE_PRESETS[] = {\n")
  foreach(mode_file ${OTLP_MODE_FILES})
    get_filename_component(mode_name ${mode_file} NAME_WE)
    file(READ ${mode_file} mode_sql)
    string(APPEND OTLP_MODES_SRC
           "{\"${mode_name}\", R\"MODE(${mode_sql})MODE\"},\n")
  endforeach()
  string(APPEND OTLP_MODES_SRC "{nullptr, nullptr}};\n}\n")
  file(WRITE ${CMAKE_CURRENT_BINARY_DIR}/otlp_mode_presets.cpp.in
       "${OTLP_MODES_SRC}")
  configure_file(${CMAKE_CURRENT_BINARY_DIR}/otlp_mode_presets.cpp.in
                 ${CMAKE_CURRENT_BINARY_DIR}/otlp_mode_presets.cpp COPYONLY)

  add_executable(
    duckdb_otlp_server
    src/server/main.cpp src/server/server_config.cpp src/server/cli.cpp
    src/server/server_util.cpp src/server/env_source.cpp
    ${CMAKE_CURRENT_BINARY_DIR}/otlp_mode_presets.cpp)
  # The target keeps its historical name (scripts and CI build --target
  # duckdb_otlp_server) but ships as `duckdb-otlp`: it is a general CLI now, not
  # only a daemon. The container installs a `duckdb-otlp-server` alias alongside
  # it.
  set_target_properties(duckdb_otlp_server PROPERTIES OUTPUT_NAME duckdb-otlp)
  target_include_directories(duckdb_otlp_server
                             PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/server)
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
  otlp_link_duckdb_extensions(duckdb_otlp_server)

  # Direct C++ ingest/seal harness (test/cpp/test_seal_harness.cpp). It needs
  # the test-only OtlpServer seam (DUCKDB_OTLP_ENABLE_TEST_SEAM), so it links a
  # SEPARATE static library built from the same sources with that macro defined.
  # The production archives (${EXTENSION_NAME} / ${LOADABLE_EXTENSION_NAME}) and
  # the daemon are built WITHOUT the macro, so production stays byte-for-byte
  # identical. Gated on BUILD_UNITTESTS (the normal release build sets it TRUE;
  # WASM/EMSCRIPTEN is excluded by the enclosing NOT EMSCRIPTEN block) so it is
  # built only where wanted.
  if(BUILD_UNITTESTS)
    add_library(otlp_test_seam STATIC ${EXTENSION_SOURCES})
    target_compile_definitions(
      otlp_test_seam PUBLIC DUCKDB_OTLP_ENABLE_TEST_SEAM
                            CPPHTTPLIB_ZLIB_SUPPORT)
    target_link_libraries(otlp_test_seam duckdb_static
                          otlp2records::otlp2records ZLIB::ZLIB)

    add_executable(otlp_seal_harness test/cpp/test_seal_harness.cpp)
    target_compile_definitions(otlp_seal_harness
                               PRIVATE DUCKDB_OTLP_ENABLE_TEST_SEAM)
    target_include_directories(
      otlp_seal_harness PRIVATE ${DUCKDB_MODULE_BASE_DIR}/third_party/catch)
    target_link_libraries(otlp_seal_harness otlp_test_seam duckdb_static
                          ${DUCKDB_EXTRA_LINK_FLAGS})
    link_threads(otlp_seal_harness "")
    otlp_link_duckdb_extensions(otlp_seal_harness)

  endif()
endif()

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
