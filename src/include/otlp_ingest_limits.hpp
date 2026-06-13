#pragma once

#include <cstdint>

// Centralized ingest/read byte and cadence defaults.
//
// These values were previously duplicated as raw literals across the extension
// (OtlpServerConfig in otlp_server.hpp, the read_otlp_* scan in read_otlp.cpp) and
// the native daemon (ServerConfig in server_config.hpp). A drifting copy is a silent
// surface, so each default has exactly one definition here and every site references it.
//
// The constants are plain integral constexpr values (no DuckDB types) so this header is
// dependency-free and can be included from the daemon's lightweight TUs as well as the
// extension's DuckDB-typed structs (which assign them into idx_t / uint64_t / int64_t fields).
namespace otlp_limits {

// --- live ingest server (otlp_serve) defaults ---

//! Per-request OTLP/HTTP body cap. Bounds a single ingest POST's encoded request body;
//! a larger body is rejected. This is the *wire* size of one request, distinct from the
//! whole-file read cap below (which bounds an entire file materialized at once).
constexpr uint64_t DEFAULT_MAX_BODY_BYTES = 16ULL * 1024ULL * 1024ULL; // 16 MiB

//! Backpressure admission cap (over it -> 503): cumulative admitted request-body bytes,
//! not decoded buffer heap. An admission/throughput proxy, not a precise memory bound.
constexpr uint64_t DEFAULT_MAX_BUFFERED_BYTES = 512ULL * 1024ULL * 1024ULL; // 512 MiB

//! Seal (group-commit) size trigger: seal when admitted *input* bytes reach this.
constexpr uint64_t DEFAULT_SEAL_TARGET_BYTES = 128ULL * 1024ULL * 1024ULL; // 128 MiB

//! Seal age trigger: seal when the oldest buffered row is at least this old.
constexpr int64_t DEFAULT_SEAL_MAX_AGE_MS = 5000;

//! Tier-1 compaction OUTPUT Parquet file size the post-seal CHECKPOINT merge bin-packs
//! toward. Distinct from DEFAULT_SEAL_TARGET_BYTES, which bounds admitted *input* bytes.
constexpr uint64_t DEFAULT_TARGET_FILE_SIZE = 256ULL * 1024ULL * 1024ULL; // 256 MiB

//! How old snapshots/files must be before CHECKPOINT expires and deletes them.
constexpr int64_t DEFAULT_MAINTENANCE_RETENTION_MS = 15LL * 60LL * 1000LL; // 15 minutes

// --- file-reading table functions (read_otlp_*) ---

//! Whole-file materialization cap for the read_otlp_* table functions. The entire file is
//! read into memory and transformed into a single Arrow batch at once, so this bounds that
//! one in-memory copy. It is a *different* concept from DEFAULT_MAX_BODY_BYTES: that caps one
//! streamed ingest request body on the live server; this caps a complete file the prototype
//! reader materializes whole. They are intentionally separate numbers (a file on disk can be
//! larger than a single acceptable HTTP request body), so they are NOT unified.
constexpr uint64_t MAX_READ_FILE_BYTES = 100ULL * 1024ULL * 1024ULL; // 100 MB

} // namespace otlp_limits
