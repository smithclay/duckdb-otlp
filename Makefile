PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=otlp
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

DOCKER_IMAGE ?= duckdb-otlp:local
DOCKER_PLATFORMS ?= linux/amd64,linux/arm64
DOCKER_LOCAL_PLATFORM ?= linux/arm64
DOCKER_OCI_OUTPUT ?= output/docker/duckdb-otlp-server-multiarch.oci.tar

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# FFI functions that need to be exported for WASM Rust backend
OTLP_FFI_EXPORTS := _otlp_get_schema,_otlp_transform,_otlp_transform_metrics_all,_otlp_status_message

# =============================================================================
# WASM Build Overrides
# =============================================================================
# These targets override DuckDB's default WASM build to support Rust FFI exports.
# CI calls 'make wasm_eh' directly, so we override it to run our custom workflow.

# Build Rust library for WASM target
wasm_rust_lib:
	@echo "Building Rust otlp2records library for WASM..."
	cd external/otlp2records && cargo build --target wasm32-unknown-emscripten --release --features ffi

# Override wasm_eh to use custom workflow with Rust FFI
# This ensures CI builds produce correct output with Rust exports
# We build only the static library target to avoid DuckDB's wasm-opt post-build
# step (which fails with Emscripten 3.1.x), then run our custom wasm_link
wasm_eh: wasm_rust_lib
	@echo "Building WASM extension with Rust backend (wasm_eh)..."
	@echo "Note: Using Emscripten 3.1.71+ for duckdb-wasm compatibility"
	mkdir -p build/wasm_eh
	emcmake cmake $(GENERATOR) $(EXTENSION_CONFIG_FLAG) $(VCPKG_MANIFEST_FLAGS) $(WASM_COMPILE_TIME_COMMON_FLAGS) $(BUILD_FLAGS) \
		-Bbuild/wasm_eh \
		-S $(DUCKDB_SRCDIR) \
		-DCMAKE_CXX_FLAGS="$(WASM_CXX_EH_FLAGS)" \
		-DDUCKDB_EXPLICIT_PLATFORM=wasm_eh \
		-DDUCKDB_CUSTOM_PLATFORM=wasm_eh
	emmake make -j8 -Cbuild/wasm_eh otlp_extension duckdb_platform
	$(MAKE) wasm_link

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
			-DVERSION_FIELD="v1.5.3" \
			-DEXTENSION_VERSION="v0.1.0" \
			-DNULL_FILE=duckdb/scripts/null.txt \
			-P duckdb/scripts/append_metadata.cmake; \
		echo "WASM linked successfully with metadata"; \
	else \
		echo "Error: Static library not found. Build may have failed."; \
		exit 1; \
	fi

# Build the static library only (without full cmake reconfigure)
wasm_build:
	emmake make -Cbuild/wasm_eh otlp_extension

# Legacy alias for manual workflow
wasm_rust: wasm_eh

# Legacy alias
wasm_relink: wasm_link

.PHONY: server-release docker-image docker-image-local docker-image-multiarch

server-release: ${EXTENSION_CONFIG_STEP}
	mkdir -p build/release
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/release
	cmake --build build/release --config Release --target duckdb_otlp_server

docker-image: docker-image-local

docker-image-local:
	docker buildx build \
		--platform $(DOCKER_LOCAL_PLATFORM) \
		--target runtime-source \
		--load \
		-t $(DOCKER_IMAGE) \
		-f docker/duckdb-otlp-server/Dockerfile \
		.

docker-image-multiarch:
	mkdir -p $(dir $(DOCKER_OCI_OUTPUT))
	docker buildx build \
		--platform $(DOCKER_PLATFORMS) \
		--target runtime-source \
		--output type=oci,dest=$(DOCKER_OCI_OUTPUT) \
		-t $(DOCKER_IMAGE) \
		-f docker/duckdb-otlp-server/Dockerfile \
		.
