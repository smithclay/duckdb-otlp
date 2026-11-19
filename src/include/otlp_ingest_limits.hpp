#pragma once

#include <cstdint>

// Ingest/read defaults shared by the extension and the daemon: one definition each. Plain
// integral constants (no DuckDB types) so the daemon's lightweight TUs can include this.
namespace otlp_limits {

//! Checked by the daemon (to name the env var) and again by otlp_serve() for direct SQL callers.
constexpr uint64_t MIN_TOKEN_LENGTH = 16;

//! Per-request body cap (encoded wire size), for HTTP and gRPC alike.
constexpr uint64_t DEFAULT_MAX_BODY_BYTES = 16ULL * 1024ULL * 1024ULL; // 16 MiB

//! Backpressure cap (over it -> 503) on admitted request-body bytes, not decoded buffer heap.
constexpr uint64_t DEFAULT_MAX_BUFFERED_BYTES = 512ULL * 1024ULL * 1024ULL; // 512 MiB

//! Seal (group-commit) size trigger: seal when admitted *input* bytes reach this.
constexpr uint64_t DEFAULT_SEAL_TARGET_BYTES = 128ULL * 1024ULL * 1024ULL; // 128 MiB

//! Seal age trigger: seal when the oldest buffered row is at least this old.
constexpr int64_t DEFAULT_SEAL_MAX_AGE_MS = 5000;

//! Output Parquet file size CHECKPOINT compaction bin-packs toward (not an input-byte bound).
constexpr uint64_t DEFAULT_TARGET_FILE_SIZE = 256ULL * 1024ULL * 1024ULL; // 256 MiB

//! How old snapshots/files must be before CHECKPOINT expires and deletes them.
constexpr int64_t DEFAULT_MAINTENANCE_RETENTION_MS = 15LL * 60LL * 1000LL; // 15 minutes

//! The most often catalog maintenance (CHECKPOINT) runs.
constexpr int64_t DEFAULT_MAINTENANCE_INTERVAL_MS = 5LL * 60LL * 1000LL; // 5 minutes

//! read_ot*_* reads a whole file into memory at once. Deliberately separate from
//! DEFAULT_MAX_BODY_BYTES: a file may be larger than one acceptable request body.
constexpr uint64_t MAX_READ_FILE_BYTES = 100ULL * 1024ULL * 1024ULL; // 100 MB

} // namespace otlp_limits
