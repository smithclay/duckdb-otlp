# Rust Backend Migration Design

**Date:** 2026-01-12
**Status:** Approved
**Author:** Claude (with user validation)

## Summary

Replace C++ OTLP parsing implementation with Rust-based otlp2records crate. This removes the protobuf C++ dependency, simplifies the build, and provides a unified parsing backend.

## Goals

- Remove protobuf C++ dependency and vcpkg requirement
- Simplify codebase by removing ~50k LOC of C++ parsing code
- Use Rust otlp2records for all OTLP parsing (JSON and protobuf)
- Maintain feature parity for logs, traces, and gauge/sum metrics
- Provide clear error messages for unsupported metric types

## Non-Goals

- WASM support (deferred - requires Rust WASM compilation)
- Histogram, exponential histogram, and summary metrics (future work)
- Backward-compatible column names (accepting breaking change)

## Function Surface

### Functions to Keep (6 total)

| Function | Source | Notes |
|----------|--------|-------|
| `read_otlp_logs(path)` | Rust | Full support |
| `read_otlp_traces(path)` | Rust | Full support |
| `read_otlp_metrics(path)` | Rust | Union of gauge + sum |
| `read_otlp_metrics_gauge(path)` | Rust | Gauge only |
| `read_otlp_metrics_sum(path)` | Rust | Sum/counter only |

### Functions That Throw "Not Supported"

| Function | Error Message |
|----------|---------------|
| `read_otlp_metrics_histogram(path)` | "Histogram metrics not yet supported" |
| `read_otlp_metrics_exp_histogram(path)` | "Exponential histogram metrics not yet supported" |
| `read_otlp_metrics_summary(path)` | "Summary metrics not yet supported" |

### Functions to Remove

- `read_otlp_stats()` - Dropped
- `read_otlp_options()` - Dropped

## Schema Changes

All column names change to lowercase. MAP types become VARCHAR containing JSON.

### Logs Schema (15 columns)

```
timestamp, observed_timestamp, trace_id, span_id, service_name,
service_namespace, service_instance_id, severity_number, severity_text,
body, resource_attributes, scope_name, scope_version, scope_attributes,
log_attributes
```

### Traces Schema (25 columns)

```
timestamp, end_timestamp, duration, trace_id, span_id, parent_span_id,
trace_state, service_name, service_namespace, service_instance_id,
span_name, span_kind, status_code, status_message, resource_attributes,
scope_name, scope_version, scope_attributes, span_attributes,
events_json, links_json, dropped_attributes_count, dropped_events_count,
dropped_links_count, flags
```

### Gauge Metrics Schema (16 columns)

```
timestamp, start_timestamp, metric_name, metric_description, metric_unit,
value, service_name, service_namespace, service_instance_id,
resource_attributes, scope_name, scope_version, scope_attributes,
metric_attributes, flags, exemplars_json
```

### Sum Metrics Schema (18 columns)

Same as gauge, plus: `aggregation_temporality`, `is_monotonic`

### Union Metrics Schema (19 columns)

All sum columns plus `metric_type` VARCHAR ('gauge' or 'sum')

## Migration Phases

### Phase 1: Rust Backend Enhancements

1. Add `read_otlp_metrics` union function combining gauge + sum
2. Add stub functions for histogram/exp_histogram/summary that throw errors

### Phase 2: Function Replacement

3. Rename functions (remove `_rust` suffix)
4. Remove old C++ implementations
5. Rename `read_otlp_rust.cpp` → `read_otlp.cpp`
6. Update `read_otlp.hpp`

### Phase 3: Dependency Removal

7. Delete `src/parsers/json_parser.cpp/hpp` (~35k LOC)
8. Delete `src/parsers/protobuf_parser.cpp/hpp` (~14k LOC)
9. Delete `src/parsers/format_detector.cpp/hpp` (~3k LOC)
10. Delete `src/generated/` (protobuf stubs)
11. Delete `src/receiver/row_builders*.cpp`
12. Delete `src/schema/*.hpp`
13. Delete `src/wasm/*.cpp` (WASM stubs)
14. Delete `vcpkg.json` and remove `vcpkg/` submodule

### Phase 4: Build System Update

15. Update CMakeLists.txt:
    - Remove `OTLP_USE_RUST` option
    - Remove protobuf find_package and linking
    - Remove WASM build configuration
    - Keep otlp2records as required

### Phase 5: Test Updates

16. Update SQLLogicTests for lowercase column names
17. Remove tests for dropped functions
18. Add tests for "not supported" errors
19. Verify all tests pass

## Build Changes

### Before

```bash
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
GEN=ninja make
```

### After

```bash
GEN=ninja make  # Rust builds automatically
```

## Breaking Changes

1. All column names are now lowercase
2. MAP types become VARCHAR containing JSON strings
3. `read_otlp_stats()` and `read_otlp_options()` removed
4. Histogram, exp_histogram, summary metrics throw errors
5. WASM builds not supported (deferred)

## Future Work

1. Add histogram/exp_histogram/summary support to otlp2records
2. Implement WASM builds with Rust WASM target
3. Consider adding stats/options utility functions to Rust
