PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=otlp
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

DOCKER_IMAGE ?= duckdb-otlp:local
DOCKER_LOCAL_PLATFORM ?= linux/arm64

# Attribute promotion extracts keys with json_extract_string, so the daemon needs DuckDB's json
# extension. It is a standalone binary with extension autoloading off, and the distroless image
# has no network, so json is linked in rather than installed at runtime. Set before the include
# below, which reads CORE_EXTENSIONS at parse time.
CORE_EXTENSIONS=json

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Run the separate C++ seal harness alongside the SQL tests.
test_release: seal_harness_release
test_debug: seal_harness_debug
seal_harness_%:
	@bin=build/$*/extension/otlp/otlp_seal_harness; \
	if [ "$(SKIP_TESTS)" = "1" ] || [ ! -x "$$bin" ]; then echo "seal harness skipped"; else "$$bin"; fi

.PHONY: server-release docker-image-local

server-release: ${EXTENSION_CONFIG_STEP}
	mkdir -p build/release
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/release
	cmake --build build/release --config Release --target duckdb_otlp_server

docker-image-local:
	docker buildx build \
		--platform $(DOCKER_LOCAL_PLATFORM) \
		--target runtime-source \
		--load \
		-t $(DOCKER_IMAGE) \
		-f docker/duckdb-otlp-server/Dockerfile \
		.
