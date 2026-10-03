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

# Any extra extensions that should be built e.g.: duckdb_extension_load(json)
