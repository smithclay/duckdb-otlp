PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckspan
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Run end-to-end OTLP export integration test
.PHONY: test-otlp-export
test-otlp-export:
	@echo "Running OTLP export integration test..."
	cd test/python && uvx --with grpcio --with duckdb==1.4.0 --with opentelemetry-api --with opentelemetry-sdk --with opentelemetry-exporter-otlp-proto-grpc python test_otlp_export.py

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Override format-check to exclude protobuf generated files
format-check:
	@echo "Checking C++ format (excluding proto directory)..."
	@bash -c 'source .venv/bin/activate && \
		find src -type f \( -name "*.cpp" -o -name "*.hpp" \) | grep -v "/proto/" | \
		xargs clang-format --dry-run -Werror'
	@echo "Checking Python format..."
	@bash -c 'source .venv/bin/activate && find test -type f -name "*.py" | xargs black --check' || true
	@echo "Format check complete (proto directory excluded)"

format-fix:
	@echo "Formatting C++ files (excluding proto directory)..."
	@bash -c 'source .venv/bin/activate && \
		find src -type f \( -name "*.cpp" -o -name "*.hpp" \) | grep -v "/proto/" | \
		xargs clang-format -i'
	@echo "Formatting Python files..."
	@bash -c 'source .venv/bin/activate && find test -type f -name "*.py" | xargs black' || true
	@echo "Format fix complete (proto directory excluded)"

# Override tidy-check to exclude protobuf generated files
tidy-check:
	mkdir -p ./build/tidy
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) -DDISABLE_UNITY=1 -DCLANG_TIDY=1 -S $(DUCKDB_SRCDIR) -B build/tidy
	cp .clang-tidy build/tidy/.clang-tidy
	cd build/tidy && python3 ../../duckdb/scripts/run-clang-tidy.py '$(PROJ_DIR)src/(?!proto).*\.(cpp|hpp)$$' -header-filter '$(PROJ_DIR)src/include/.*' -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS}