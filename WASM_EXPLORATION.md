# WASM Exploration: Rust Backend in DuckDB-WASM Extensions

This document captures the investigation and **working solution** for enabling the Rust `otlp2records` backend in WASM builds of the DuckDB OTLP extension.

## Background

The OTLP extension uses a Rust library (`otlp2records`) via FFI for parsing OTLP telemetry data. Native builds work correctly, but WASM builds previously used JSON-only stubs because integrating Rust with Emscripten's dynamic linking model is non-trivial.

## Status: WORKING

The Rust backend now works in WASM builds. The solution required solving three separate issues:

| Issue | Solution |
|-------|----------|
| FFI symbols stripped by dead code elimination | Post-link with explicit `EXPORTED_FUNCTIONS` |
| Emscripten version mismatch (wasm-opt flags) | Use `-O1` to skip wasm-opt post-processing |
| Missing DuckDB extension metadata | Call `append_metadata.cmake` after linking |

## Quick Start

```bash
# Prerequisites: Emscripten 3.1.71-3.1.73 (for duckdb-wasm 1.32.0 compatibility)
# Build Rust library for WASM
cd external/otlp2records
cargo build --target wasm32-unknown-emscripten --features ffi --release

# Build WASM extension with Rust backend
cd ../..
make wasm_rust

# Copy to demo directory
cp build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm demo/

# Serve demo
cd demo && python3 -m http.server 8080
```

## Technical Details

### 1. Rust Compiles for Emscripten

The `otlp2records` library successfully compiles for `wasm32-unknown-emscripten`:

```bash
cargo build --target wasm32-unknown-emscripten --features ffi --release
# Produces: target/wasm32-unknown-emscripten/release/libotlp2records.a
```

All dependencies (Arrow, serde, prost, etc.) compile without issue.

### 2. DuckDB WASM Extension Architecture

DuckDB-WASM extensions are built as **side modules** (`-sSIDE_MODULE=2`) that are dynamically loaded via `dlopen`. The build process:

1. C++ sources compiled to `.o` files via `emcc`
2. Objects archived into `libotlp_extension.a` static library
3. Final `emcc` link produces `.wasm` side module with explicit exports
4. DuckDB metadata appended via `append_metadata.cmake`

### 3. Problem: Dead Code Elimination

Emscripten's `-sSIDE_MODULE=2` aggressively eliminates symbols not explicitly exported. DuckDB's build only exports `_otlp_duckdb_cpp_init`, so FFI functions are stripped even though C++ code calls them.

The C++ code references FFI functions:
```cpp
// src/function/read_otlp.cpp
OtlpStatus status = otlp_parser_create(bind_data.signal_type, bind_data.format, &result->parser);
```

But the linker doesn't see these as "live" exports for a side module.

**Solution**: Post-link step that re-runs `emcc` with all FFI functions in `EXPORTED_FUNCTIONS`.

### 4. Problem: Emscripten Version Mismatch

| Component | Emscripten Version |
|-----------|-------------------|
| duckdb-wasm 1.32.0 | 3.1.71 |
| DuckDB v1.4.3 with `-O3` | Requires 4.x (for wasm-opt flags) |

Building with Emscripten 3.1.x and `-O3` fails:
```
Unknown option '--enable-bulk-memory-opt'
```

This flag is passed to `wasm-opt` by `-O3`, but only exists in Emscripten 4.x.

**Solution**: Use `-O1` which skips wasm-opt post-processing entirely.

### 5. Problem: Missing DuckDB Metadata

DuckDB extensions require a `duckdb_signature` custom section with 512 bytes of metadata:
- ABI type (CPP, C_STRUCT, etc.)
- Platform (wasm_eh, wasm_mvp, etc.)
- DuckDB version
- Extension version

Our manual `emcc` link bypassed DuckDB's POST_BUILD step that adds this metadata, causing:
```
Unknown ABI type of value '109'
```

**Solution**: Call `append_metadata.cmake` after linking.

## Implementation

The complete solution is implemented in the Makefile:

```makefile
# FFI functions that need to be exported for WASM Rust backend
OTLP_FFI_EXPORTS := _otlp_parser_create,_otlp_parser_destroy,_otlp_parser_push,_otlp_parser_drain,_otlp_parser_last_error,_otlp_get_schema

# Link WASM extension with Rust FFI exports
# Uses -O1 to skip wasm-opt post-processing (needed for Emscripten 3.1.x compatibility)
wasm_link:
	emcc build/wasm_eh/extension/otlp/libotlp_extension.a \
		-o build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm \
		-O1 -sSIDE_MODULE=2 \
		-sEXPORTED_FUNCTIONS="_otlp_duckdb_cpp_init,$(OTLP_FFI_EXPORTS)" \
		external/otlp2records/target/wasm32-unknown-emscripten/release/libotlp2records.a
	cmake -DABI_TYPE=CPP \
		-DEXTENSION=build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm \
		-DPLATFORM_FILE=build/wasm_eh/duckdb_platform_out \
		-DVERSION_FIELD="v1.4.3" \
		-DEXTENSION_VERSION="v0.1.0" \
		-DNULL_FILE=duckdb/scripts/null.txt \
		-P duckdb/scripts/append_metadata.cmake

# Build the static library (without triggering problematic wasm-opt)
wasm_build:
	emmake make -Cbuild/wasm_eh otlp_extension

# Build WASM with Rust backend support (full workflow)
wasm_rust:
	$(MAKE) wasm_eh || $(MAKE) wasm_build
	$(MAKE) wasm_link
```

### Build Workflow

1. `make wasm_eh` - Runs DuckDB's WASM build, creates `libotlp_extension.a`
2. `make wasm_link` - Re-links with FFI exports and appends metadata

Or simply: `make wasm_rust`

## Alternatives Tested (Did Not Work)

| Approach | Result |
|----------|--------|
| `--whole-archive` | Does not preserve exports in SIDE_MODULE mode |
| Multiple `-sEXPORTED_FUNCTIONS` | Second call overrides first (no merge) |
| `-sEXPORTED_FUNCTIONS+=...` | Not supported with SIDE_MODULE |
| Emscripten 4.x with duckdb-wasm 1.32.0 | ABI mismatch: `imported function does not match the expected type` |
| Emscripten 3.1.71 with `-O3` | `Unknown option '--enable-bulk-memory-opt'` |

## Demo Setup

The `demo/` directory contains a browser-based demo:

```javascript
// demo/app.js - Uses duckdb-wasm 1.32.0 for Emscripten 3.1.x compatibility
const bundle = await duckdb.selectBundle({
    eh: {
        mainModule: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-eh.wasm',
        mainWorker: 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.32.0/dist/duckdb-browser-eh.worker.js',
    },
});
```

The extension loads from the same origin:
```javascript
const extensionUrl = `${window.location.origin}${window.location.pathname}otlp.duckdb_extension.wasm`;
await conn.query(`LOAD "${extensionUrl}";`);
```

## Environment Requirements

- **Emscripten**: 3.1.71-3.1.73 (must match duckdb-wasm version)
- **Rust target**: `wasm32-unknown-emscripten`
- **DuckDB**: v1.4.3
- **duckdb-wasm**: 1.32.0

## References

- [Emscripten Dynamic Linking](https://emscripten.org/docs/compiling/Dynamic-Linking.html)
- [DuckDB WASM Extensions](https://duckdb.org/2023/12/18/duckdb-extensions-in-wasm)
- [wasm32-unknown-emscripten Target](https://doc.rust-lang.org/rustc/platform-support/wasm32-unknown-emscripten.html)
- [DuckDB Extension Distribution](https://duckdb.org/docs/stable/extensions/extension_distribution)
