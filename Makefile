PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=otlp
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

DOCKER_IMAGE ?= duckdb-otlp:local
DOCKER_PLATFORMS ?= linux/amd64,linux/arm64
DOCKER_LOCAL_PLATFORM ?= linux/arm64
DOCKER_OCI_OUTPUT ?= output/docker/duckdb-otlp-server-multiarch.oci.tar

# The CLI advertises JSON/NDJSON output (`convert --format ndjson` is an example in the docs),
# which needs DuckDB's json COPY function. The daemon is a standalone binary with extension
# autoloading off, so json is linked in rather than installed at runtime: `convert` has to work
# on a machine with nothing set up, and the distroless image has no network. Set before the
# include below, which reads CORE_EXTENSIONS at parse time.
CORE_EXTENSIONS=json

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
			-DVERSION_FIELD="v1.5.6" \
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

# Runtime extensions the daemon links instead of INSTALLing at startup (see
# extension_config.cmake). ducklake is the default mode's catalog. quack is not
# here: on DuckDB 2.0 it draws its tokens from the writable crypto module httpfs
# (OpenSSL) provides, so building it in means building httpfs in too.
DAEMON_BUILT_IN_EXTENSIONS ?= ducklake

# ducklake needs CRoaring. The extension builds take it from vcpkg, but the daemon
# builds (Docker, macOS CI) run without vcpkg, so build it into a private prefix,
# for the same architecture as the daemon. v4.7.2 rather than vcpkg's 4.4.2: the
# 4.4.2 package config declares cmake_minimum_required(2.8), which CMake 4 rejects.
CROARING_VERSION := v4.7.2
CROARING_PREFIX := $(PROJ_DIR)build/deps/croaring
CROARING_ARCH_FLAG := $(if $(OSX_BUILD_ARCH),-DCMAKE_OSX_ARCHITECTURES=$(OSX_BUILD_ARCH),)

$(CROARING_PREFIX)/.built-$(CROARING_VERSION):
	rm -rf build/deps/croaring-src $(CROARING_PREFIX)
	git clone --quiet --depth 1 --branch $(CROARING_VERSION) https://github.com/RoaringBitmap/CRoaring build/deps/croaring-src
	cmake $(GENERATOR) -S build/deps/croaring-src -B build/deps/croaring-build -DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_INSTALL_PREFIX=$(CROARING_PREFIX) -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DBUILD_SHARED_LIBS=OFF \
		-DENABLE_ROARING_TESTS=OFF -DROARING_USE_CPM=OFF $(CROARING_ARCH_FLAG)
	cmake --build build/deps/croaring-build --target install
	touch $@

server-release: ${EXTENSION_CONFIG_STEP} $(CROARING_PREFIX)/.built-$(CROARING_VERSION)
	mkdir -p build/release
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release \
		-DDUCKDB_OTLP_BUILT_IN_EXTENSIONS="$(DAEMON_BUILT_IN_EXTENSIONS)" -DCMAKE_PREFIX_PATH=$(CROARING_PREFIX) \
		-S $(DUCKDB_SRCDIR) -B build/release
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
