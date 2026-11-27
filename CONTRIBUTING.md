# Contributing to duckdb-otlp

## Build from source

Install Git, a C++17 compiler, CMake 3.19 or later, GNU Make, Ninja, Python 3, and Rust with Cargo. The pinned `otlp2records` crate declares Rust 1.90 as its minimum. Install `uv` for Python tools and tests.

Windows builds the extension with MSVC. The daemon and C++ seal harness build on Linux and macOS.

```sh
git clone --recurse-submodules https://github.com/smithclay/duckdb-otlp.git
cd duckdb-otlp
GEN=ninja make
```

For an existing checkout, initialize or refresh the pinned submodules with `git submodule update --init --recursive`.

The release build puts the DuckDB shell with the extension linked at `build/release/duckdb` and the loadable extension at `build/release/extension/otlp/otlp.duckdb_extension`. Use `GEN=ninja make debug` for a debug build.

On Linux or macOS, build just the receiver with `GEN=ninja make server-release`. The binary is `build/release/extension/otlp/duckdb-otlp`.

For a browser build, activate Emscripten and add the Rust target. Use the Make generator for this target because it invokes `emmake make`.

```sh
rustup target add wasm32-unknown-emscripten
make wasm_eh
```

The browser artifact is `build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm`.

## Test and format

After a release build, run `GEN=ninja make test` for the SQL logic tests and the C++ seal harness on supported native platforms. See the [test guide](test/README.md) for all four suites and their CI jobs.

```sh
GEN=ninja make format-check
GEN=ninja make format-fix
GEN=ninja make tidy-check
```

Install `clang-tidy` for the last target. To install the repository's pre-commit hooks, run `uvx --from pre-commit pre-commit install`.

For the source map and implementation details, see [AGENTS.md](AGENTS.md). For the documentation build, see the [site README](site/README.md).

## Update the DuckDB pin

The current build targets DuckDB `v2.0.0`. When moving to a new release, update these pins together.

1. Check out the release tag in the `duckdb` submodule and record the new gitlink.
2. Update `duckdb_version` and `DAEMON_DUCKDB_VERSION` in `MainDistributionPipeline.yml`. Update the daemon test's `duckdb==…` package pin there and in the [test guide](test/README.md).
3. Update `duckdb_version` and `DUCKDB_VERSION` in `pages.yml` and both `DUCKDB_VERSION` defaults in the server Dockerfile.
4. Match the `extension-ci-tools` submodule, its branch in `.gitmodules`, the reusable workflow refs, and the `ci_tools_version` inputs to the community build tools. They currently use `v2.0-cyanoptera`. Keep the DuckDB submodule branch hint aligned with the release line.
5. Rebuild from a fresh build directory, run the test suites, and check that the daemon can load the extensions its modes install. The reported DuckDB version must match the version used to prime the image's extension cache.

## Releases

Check that the release commit passed the main distribution pipeline before tagging it. A `v*` tag runs the pipeline and attaches the Linux and macOS receiver archives and `SHA256SUMS` after Docker smoke tests pass. Confirm the assets are available with `gh release view <tag>`.

Coordinate the community catalog update through `repo.ref` in `description.yml`. Its `variegata` pin records the last build compatible with DuckDB v1.5.6.

## Pull requests

Keep the change focused, include regression coverage for behavior changes, and update user documentation when the public interface changes. Describe the change, link the related issue if there is one, and list the checks you ran.

Contributions are licensed under the [MIT License](LICENSE).
