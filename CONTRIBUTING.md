# Contributing to DuckDB OTLP Extension

Thanks for your interest in contributing! This guide covers building, testing, and developing the extension.

## Development Setup

### Prerequisites

- **vcpkg** for dependency management
- **Ninja** (recommended) or Make
- **DuckDB** 0.10+
- **C++ compiler** with C++17 support
- **Python 3.8+** with `uv` for tooling

### 1. Clone and set up vcpkg

```bash
# Clone the repository
git clone https://github.com/smithclay/duckdb-otlp.git
cd duckdb-otlp

# Set up vcpkg for dependency management
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh

# Set the toolchain path (add to your shell profile)
export VCPKG_TOOLCHAIN_PATH=$(pwd)/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### 2. Build the extension

Use Ninja for faster builds:

```bash
GEN=ninja make
```

This produces:
- `./build/release/duckdb` - DuckDB shell with extension loaded
- `./build/release/test/unittest` - Test runner
- `./build/release/extension/otlp/otlp.duckdb_extension` - Loadable extension

**Build debug version:**
```bash
GEN=ninja make debug
```

## Testing

### Run all tests

```bash
# SQL logic tests
make test

# Debug build tests
make test_debug
```

### Run specific test files

```bash
# Run a single test file
./build/release/test/unittest "test/sql/read_otlp_protobuf.test"

# Run all tests matching a pattern
./build/release/test/unittest "test/sql/read_otlp_*.test"
```

### Test structure

Tests are written in DuckDB's SQLLogicTest format under `test/sql/`:

- `test/sql/otlp.test` - Basic extension functionality
- `test/sql/read_otlp_json.test` - JSON parsing
- `test/sql/read_otlp_protobuf.test` - Protobuf parsing
- `test/sql/read_otlp_errors.test` - Error handling
- `test/sql/read_otlp_on_error.test` - `on_error` parameter
- `test/sql/read_otlp_metrics_helpers.test` - Metrics helpers
- `test/sql/schema_bridge.test` - Schema compatibility

## Code Quality

### Formatting

Check formatting before committing:

```bash
# Check C++ and Python formatting
make format-check

# Auto-fix formatting issues
make format-fix
```

### Linting

Run clang-tidy (requires vcpkg dependencies):

```bash
GEN=ninja make tidy-check
```

### Pre-commit hooks

Install hooks with `uv`:

```bash
# Install pre-commit hooks
uvx --from pre-commit pre-commit install

# Run manually
uvx --from pre-commit pre-commit run --all-files
```

## Project Structure

```
duckdb-otlp/
├── src/
│   ├── include/          # Public headers
│   ├── storage/          # Extension entry point
│   ├── function/         # Table function implementations
│   ├── parsers/          # JSON/protobuf parsers, format detector
│   ├── receiver/         # Row builders (convert OTLP → DuckDB)
│   ├── schema/           # Column layout definitions
│   └── generated/        # Protobuf stubs (DO NOT EDIT)
├── test/
│   ├── sql/              # SQLLogicTests
│   └── data/             # Test fixtures (OTLP JSON/protobuf)
├── docs/                 # User documentation
└── extension-ci-tools/   # Build system utilities
```

## Architecture

### Data Flow

```
OTLP File → Format Detector → Parser → Row Builders → DuckDB Vectors
```

1. **Format Detector** (`parsers/format_detector.cpp`) - Sniffs first bytes to choose JSON vs protobuf parser
2. **Parsers** (`parsers/json_parser.cpp`, `parsers/protobuf_parser.cpp`) - Stream data from files
3. **Row Builders** (`receiver/row_builders*.cpp`) - Convert OTLP structs to DuckDB column vectors
4. **Schema** (`schema/*.hpp`) - Define ClickHouse-compatible column layouts

### Key Files

- `src/storage/otlp_extension.cpp` - Extension registration
- `src/function/read_otlp.cpp` - Table function implementations
- `src/parsers/format_detector.cpp` - Auto-detect JSON vs protobuf
- `src/receiver/row_builders.cpp` - Shared row builder logic
- `src/schema/otlp_metrics_schemas.hpp` - Metrics schema definitions

### Generated Code

`src/generated/` contains protobuf message stubs auto-generated from OpenTelemetry `.proto` files:

- `opentelemetry/proto/common/v1/common.pb.{h,cc}`
- `opentelemetry/proto/resource/v1/resource.pb.{h,cc}`
- `opentelemetry/proto/trace/v1/trace.pb.{h,cc}`
- `opentelemetry/proto/logs/v1/logs.pb.{h,cc}`
- `opentelemetry/proto/metrics/v1/metrics.pb.{h,cc}`

**Do not edit these files manually.** They are excluded from formatting and linting.

## Adding Features

### Adding a new table function

1. Define the schema in `src/schema/`
2. Implement row builders in `src/receiver/`
3. Register the function in `src/storage/otlp_extension.cpp`
4. Add table function wrapper in `src/function/read_otlp.cpp`
5. Add tests in `test/sql/`

### Modifying parsers

- **JSON parser**: `src/parsers/json_parser.cpp`
- **Protobuf parser**: `src/parsers/protobuf_parser.cpp`
- **Format detection**: `src/parsers/format_detector.cpp`

All parsers call shared row builders in `src/receiver/`.

## Updating DuckDB Version

When a new DuckDB release is available:

1. **Update submodules**:
   ```bash
   cd duckdb
   git checkout <new-version-tag>
   cd ../extension-ci-tools
   git checkout <matching-version-branch>
   ```

2. **Update workflows**:
   - `.github/workflows/MainDistributionPipeline.yml`:
     - `duckdb_version` in `duckdb-stable-build` job
     - `duckdb_version` in `duckdb-stable-deploy` job
     - Reusable workflow version

3. **Test for API changes**:
   DuckDB's C++ API is not stable. Check:
   - [DuckDB Release Notes](https://github.com/duckdb/duckdb/releases)
   - [Core extension patches](https://github.com/duckdb/duckdb/commits/main/.github/patches/extensions)
   - Git history of relevant headers

4. **Rebuild and test**:
   ```bash
   rm -rf build/
   GEN=ninja make
   make test
   ```

## Submitting Pull Requests

1. **Fork the repository** and create a feature branch
2. **Make your changes** with clear, descriptive commits
3. **Add tests** for new functionality
4. **Run formatting and linting**:
   ```bash
   make format-fix
   GEN=ninja make tidy-check
   make test
   ```
5. **Update documentation** if adding features
6. **Submit PR** with:
   - Clear description of changes
   - Link to related issues
   - Before/after examples if relevant

## Questions?

- **Development questions**: Open a [Discussion](https://github.com/smithclay/duckdb-otlp/discussions)
- **Bug reports**: File an [Issue](https://github.com/smithclay/duckdb-otlp/issues)
- **Documentation**: See [docs/](docs/)

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
