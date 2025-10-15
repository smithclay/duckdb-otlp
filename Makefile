PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=duckspan
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Run end-to-end OTLP export integration test
.PHONY: test-otlp-export
test-otlp-export:
	@echo "Running OTLP export integration test..."
	uv run test/python/test_otlp_export.py

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Override format-check to exclude generated files
format-check:
	@echo "Checking C++ format (excluding generated directory)..."
	@find src -type f \( -name "*.cpp" -o -name "*.hpp" \) | grep -v "/generated/" | \
		xargs clang-format --dry-run -Werror
	@echo "Checking Python format..."
	@uvx black --check test/python || true
	@echo "Format check complete"

format-fix:
	@echo "Formatting C++ files (excluding generated directory)..."
	@find src -type f \( -name "*.cpp" -o -name "*.hpp" \) | grep -v "/generated/" | \
		xargs clang-format -i
	@echo "Formatting Python files..."
	@uvx black test/python || true
	@echo "Format fix complete"

# Override tidy-check to exclude generated files
# Note: tidy-check requires vcpkg dependencies - run locally with VCPKG_TOOLCHAIN_PATH set
tidy-check:
	mkdir -p ./build/tidy
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_DEBUG_FLAGS) -DDISABLE_UNITY=1 -DCLANG_TIDY=1 -S $(DUCKDB_SRCDIR) -B build/tidy
	cp .clang-tidy build/tidy/.clang-tidy
	cd build/tidy && python3 ../../duckdb/scripts/run-clang-tidy.py '$(PROJ_DIR)src/(?!generated).*\.(cpp|hpp)$$' -header-filter '$(PROJ_DIR)src/.*' -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER} ${TIDY_PERFORM_CHECKS}