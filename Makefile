PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=otlp
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# FFI functions that need to be exported for WASM Rust backend
OTLP_FFI_EXPORTS := _otlp_parser_create,_otlp_parser_destroy,_otlp_parser_push,_otlp_parser_drain,_otlp_parser_last_error,_otlp_get_schema

# Link WASM extension with Rust FFI exports
# Uses -O1 to skip wasm-opt post-processing (needed for Emscripten 3.1.x compatibility)
wasm_link:
	@if [ -f build/wasm_eh/extension/otlp/libotlp_extension.a ]; then \
		echo "Linking WASM extension with FFI exports..."; \
		emcc build/wasm_eh/extension/otlp/libotlp_extension.a \
			-o build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm \
			-O1 -sSIDE_MODULE=2 \
			-sEXPORTED_FUNCTIONS="_otlp_duckdb_cpp_init,$(OTLP_FFI_EXPORTS)" \
			external/otlp2records/target/wasm32-unknown-emscripten/release/libotlp2records.a; \
		echo "Appending DuckDB metadata..."; \
		cmake -DABI_TYPE=CPP \
			-DEXTENSION=build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm \
			-DPLATFORM_FILE=build/wasm_eh/duckdb_platform_out \
			-DVERSION_FIELD="v1.4.3" \
			-DEXTENSION_VERSION="v0.1.0" \
			-DNULL_FILE=duckdb/scripts/null.txt \
			-P duckdb/scripts/append_metadata.cmake; \
		echo "WASM linked successfully with metadata"; \
	else \
		echo "Error: Run 'make wasm_build' first to build the static library"; \
		exit 1; \
	fi

# Build the static library (without triggering problematic wasm-opt)
wasm_build:
	emmake make -Cbuild/wasm_eh otlp_extension

# Build WASM with Rust backend support (full workflow)
# Requires: Emscripten 3.1.71-3.1.73 for duckdb-wasm compatibility
wasm_rust:
	@echo "Building WASM extension with Rust backend..."
	@echo "Note: Use Emscripten 3.1.71-3.1.73 for duckdb-wasm 1.32.0 compatibility"
	$(MAKE) wasm_eh || $(MAKE) wasm_build
	$(MAKE) wasm_link

# Legacy alias
wasm_relink: wasm_link
