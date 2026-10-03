# This file is included by DuckDB's build system. It specifies which extension
# to load

# For WASM builds, link the Rust library into the extension
if(EMSCRIPTEN)
  set(OTLP2RECORDS_WASM_LIB
      "external/otlp2records/target/wasm32-unknown-emscripten/release")
  set(OTLP_LINKED_LIBS
      "${CMAKE_CURRENT_LIST_DIR}/${OTLP2RECORDS_WASM_LIB}/libotlp2records.a")
  duckdb_extension_load(otlp SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR} LOAD_TESTS
                        LINKED_LIBS ${OTLP_LINKED_LIBS})
else()
  duckdb_extension_load(otlp SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR} LOAD_TESTS)
endif()
# DuckDB 2.0 links an extension into its own binaries (the shell, unittest) only
# when a config asks for it; loading it no longer implies linking it.
duckdb_extension_statically_link(otlp)

# Runtime extensions compiled into the binary instead of INSTALLed when it
# starts, e.g. "ducklake;quack" (set by `make server-release`). Each one is
# taken from DuckDB's own pinned config -- the commit and patches DuckDB builds
# and tests against this source -- so its ABI matches the daemon by
# construction, which no published binary can promise for a development build.
# The daemon notices at runtime that an extension is linked and stops emitting
# INSTALL/LOAD for it.
set(DUCKDB_OTLP_BUILT_IN_EXTENSIONS
    ""
    CACHE STRING "Extensions to link into DuckDB from DuckDB's pinned configs")
foreach(built_in IN LISTS DUCKDB_OTLP_BUILT_IN_EXTENSIONS)
  include(
    "${CMAKE_CURRENT_LIST_DIR}/duckdb/.github/config/extensions/${built_in}.cmake"
  )
  duckdb_extension_statically_link(${built_in})
endforeach()

# Any extra extensions that should be built e.g.: duckdb_extension_load(json)
