# OTLP compaction investigation

A standalone, **read-only research harness** that measures what the pinned (2026) DuckDB
1.5.3 + DuckLake `CHECKPOINT` / maintenance path can and cannot do for **compacting the
daemon's many-small-seal-files**, and what it costs the single seal writer. It never
touches the daemon or its catalogs; it builds throwaway local DuckLake catalogs under
`output/<run-id>/` (gitignored).

This is **pure research/measurement**, not production code. It exists to answer one
question honestly before building: *operationalizing compaction in a single-writer,
no-WAL daemon — is it a background loop, or does it change the write path / concurrency
model?*

## Why compaction is the load-bearing query-perf work

The daemon seals each group-commit as **one Parquet file per signal**, so a high seal
cadence produces many small files. The companion S3 GET-count campaign found that **S3
GET *count* is driven by file and row-group count** — not by blooms — and that a
`trace_id` bloom cuts *bytes* (~8×) and latency (~3×) but leaves *count* flat. So:

- **Compaction** (fewer, bigger files / row groups) is the lever for the GET-**count**
  axis — the load-bearing small-file fix.
- **Clustering** by `(service, time)` is the lever for service-scoped pruning (the ~3.2×
  single-service win in `../otlp-query-probe`).
- **`trace_id` blooms** are the lever for the **bytes/latency** axis on point lookups.

This probe checks which of those three the native `CHECKPOINT`/merge path delivers for
free, which need custom work, and what each costs.

## What the 2026 stack provides (measured, DuckDB 1.5.3 / DuckLake `e6a3bd0a`)

| Capability | Native? | Mechanism | Cost / notes |
|---|---|---|---|
| Merge many small files | **Yes** | `CHECKPOINT <cat>` or `CALL <cat>.merge_adjacent_files()` | 80 small files → 2 in ~150–250 ms |
| Bound output size | **Yes** | `CALL <cat>.set_option('target_file_size', '128MB')` | bin-packs to target **and leaves at-target files alone next cycle** → **bounded, O(new)/cycle** |
| Concurrent compactor | **No** | file catalog is single-connection | a 2nd connection **can't attach** → compaction must run **in the seal-writer connection** |
| Cluster by `(service,time)` | **No** | merge only concatenates; `SET SORTED BY` is **not** honored by merge | needs an explicit sort-rewrite (`INSERT … ORDER BY` / `COPY`) |
| `trace_id` bloom on rewrite | **No** | dictionary-encoding gate holds through merge | needs out-of-band `COPY … DICTIONARY_SIZE_LIMIT` + `ducklake_add_data_files` |
| Reclaim orphaned files | **Yes** | `ducklake_expire_snapshots` + `ducklake_cleanup_old_files` | merge leaves old files as snapshot-held orphans until expiry; reclaim drops 104 → 3 |
| `rewrite_data_files` | n/a | `delete_threshold` only | delete-compaction, **not** sorting/clustering |

Two findings carry the design:

1. **`target_file_size` makes compaction bounded.** Without it, `merge_adjacent_files()`
   rewrites toward one ever-growing file — re-merging already-compacted data every cycle
   (O(total) write amplification). With it set, merge bin-packs to ~target and **skips
   files already at target**, so a re-merge after new seals touches only the new small
   files (O(new)/cycle). This is the difference between a viable loop and a runaway one.
2. **The file catalog is single-connection.** A second connection cannot attach the
   `.ducklake` metadata file, so there is **no concurrent compactor** — compaction must be
   issued on the *same* connection as the seal writer. It therefore **serializes with
   seals** (one writer thread), at ~150 ms per bounded merge.

## Findings → design: a two-tier compaction model

**Tier 1 — native bin-packing (cheap, in the seal thread).** Periodically call
`merge_adjacent_files()` with `target_file_size` set, scoped to recent un-compacted files,
plus `expire_snapshots` + `cleanup_old_files` on a slower cadence. This fixes the
small-file / GET-count problem — the load-bearing win — and is **bounded** and conflict-free.
It runs **serialized in the seal thread** (it must: single-connection catalog), triggered
by file-count/age, sized so the ~150 ms stall stays small relative to the seal interval.

**Tier 2 — custom sort + bloom rewrite (expensive, out-of-band).** Clustering by
`(service, time)` and `trace_id` blooms are **not** native to merge. They require a
rewrite of sealed partitions via `COPY … ORDER BY service_name, time_unix_nano …
DICTIONARY_SIZE_LIMIT <n>` (the only way to get a `trace_id` bloom — DuckLake's writer has
no option for it) registered with `ducklake_add_data_files`. The bloom carries an ~18×
encode tax on the high-cardinality columns, which is acceptable **only off the ingest hot
path** — i.e. exactly here, on cold/sealed partitions, run rarely.

### Architectural consequences

- **No-WAL / single-writer is *not* an obstacle to compaction.** Compaction is just more
  writes through the same serialized writer; because it *is* the writer, it cannot hit the
  optimistic-concurrency conflict that a concurrent compactor would. Tier 1 needs **no
  concurrency-model change**.
- **But compaction is not free background work.** Single-connection catalog ⇒ it competes
  with the seal writer for the one thread. Keep merges bounded (`target_file_size` + scope
  to recent files) and run them between seals. A *truly* concurrent compactor would require
  moving the catalog to a server (Postgres) — a larger architectural change the daemon does
  not currently make (same control/data-plane coupling as the Quack-locked file catalog).
- **The real build is Tier 2.** "Operationalize compaction" = a cheap native tier (a
  bounded loop) **plus** a genuine custom rewrite path for clustering + blooms.

### Verdict on "compaction is just a background loop"

**Half true.** Tier 1 (the small-file fix — the part that moves the GET-count cost axis) is
close to a background loop: `target_file_size` makes it bounded and the single writer makes
it conflict-free; it just has to live in the seal thread and pay ~150 ms stalls. Tier 2
(clustering + blooms — the bytes/latency layer) is **real write-path feature work**, a
custom sort+bloom rewrite + register, not a loop.

## Run

```bash
uv run benchmark/otlp-compaction-probe/probe.py --run-id myrun --seals 80 --per 50000 --target 128MB
```

Outputs `output/<run-id>/summary.md`. Knobs: `--seals` (small files to simulate), `--per`
(rows/seal), `--target` (DuckLake `target_file_size`).

## Caveats / not measured

- **Local disk only** — measures compaction *mechanics* and *writer-stall* cost, not S3
  GET impact (that is `../otlp-query-probe` + the GET-count campaign).
- **Throwaway single-process catalog**, not the daemon's live seal loop (`src/otlp_storage.cpp`
  single seal thread, `src/otlp_server.cpp` `SealOnce`/append path). The stall numbers are a
  directional lower bound, not the in-daemon figure.
- The sized table-function args (`min_file_size`/`max_file_size` on
  `ducklake_merge_adjacent_files`) were finicky; the **`target_file_size` catalog option**
  is the reliable bound used here.
