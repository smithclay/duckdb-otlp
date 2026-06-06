# OTLP logs query-performance probe

A standalone, **read-only** research harness that measures DuckLake/OTLP **query**
performance for realistic, time-bounded observability log queries — and quantifies
three layout mitigations. It never touches the daemon or its catalogs; it builds
throwaway local DuckLake catalogs under `output/<run-id>/` (gitignored) and queries
them.

This is **pure research/measurement**, not production code. It informs whether (and
how) the daemon should add physical layout (partitioning / sort / compaction) to the
`otlp_logs` table — which today is created bare (no partitioning, no sort, no bloom).

## What it measures (three experiments)

1. **trace_id point lookup** — does a time/service sort *scatter* trace_ids so a lookup
   becomes a full-window scan, and does clustering by trace_id recover file pruning?
   (DuckDB 1.5.3 cannot *write* Parquet bloom filters, so `sorted_trace_id` is the
   on-1.5.3 stand-in for the bloom filter a newer DuckDB would write.)
2. **body substring search** — with no inverted index, `body ILIKE '%phrase%'` is a
   brute-force scan; how does cost scale with the time window, and does time
   partitioning prune it?
3. **service layout** — observability on-call is overwhelmingly **single-service-scoped**
   (you own a service; alerts route per-service). So nearly every query carries
   `service_name = X`. Does organizing by service (sort or partition) prune those
   queries, and what does it cost in file count / size? Compares baseline vs
   `sorted_service_time` vs `partition_service` vs `partition_hour_service`.

## Decisions baked in

- **Controlled synthetic data** with *known* selectivities (the benchmark producer is
  unusable for query realism: null trace_ids, random-noise bodies, single severity/
  service). The generator emits the exact 18-column `otlp_logs` schema, deterministic
  from `--seed`: a trace_id needle of ~20 rows, a rare error phrase in ~0.01% of bodies,
  realistic severity/service/`http.status` distributions, and 16 services.
- **DuckDB pinned 1.5.3** (matches the daemon). 1.5.3 reads/probes bloom filters but
  **cannot write** them — so the bloom variant is out of scope; `sorted_trace_id` is the
  stand-in. Layout APIs used (verified on 1.5.3): `SET PARTITIONED BY`,
  `CALL <cat>.merge_adjacent_files()`, and the metadata oracle via
  `ATTACH '<cat>.ducklake' AS m (READ_ONLY)` → `m.ducklake_data_file` /
  `m.ducklake_file_column_stats` / `m.ducklake_column`.
- **Local disk only.** The real DuckLake-on-S3 cost is *request count* (DuckDB does
  synchronous per-thread S3 I/O), which local disk cannot measure — that is a noted
  follow-up (point `DATA_PATH` at MinIO/S3). **Local disk is not S3**: treat the file
  pruning + latency here as directional for the mitigations, not as S3 cost.

## Run

```bash
# end-to-end (generate -> verify -> run) at the default 20M rows
uv run benchmark/otlp-query-probe/probe.py all --run-id myrun

# quick smoke
uv run benchmark/otlp-query-probe/probe.py all --run-id smoke --rows 1000000 --rows-per-file 250000 --repeats 3

# phases individually
uv run benchmark/otlp-query-probe/probe.py generate --run-id myrun
uv run benchmark/otlp-query-probe/probe.py verify   --run-id myrun
uv run benchmark/otlp-query-probe/probe.py run      --run-id myrun
uv run benchmark/otlp-query-probe/probe.py clean    --run-id myrun
```

Key knobs: `--rows` (default 20M), `--span-hours` (24), `--rows-per-file` (default 2M ≈
128 MB+ files), `--seed`, `--target-trace-needle-rows`, `--repeats`, `--hazard-variants`.

## Variants

Four **primary** variants (built by default), each mapping to a decision: `baseline`
(time-ordered), `sorted_service_time` (cluster by service, time-sorted within),
`sorted_trace_id` (cluster by trace_id — the on-1.5.3 stand-in for a bloom filter), and
`promoted` (hot JSON attrs lifted to typed columns). Three **hazard/secondary** variants
are opt-in via `--hazard-variants` (`partition_hour` ≈ baseline; `partition_service` ≈
`sorted_service_time`; `partition_hour_service` exists to demonstrate the over-partition
file blow-up). Outputs: `run_meta.json`, `results.json`, `summary.md`.

## Metrics & caveats

- **PRIMARY metric = files (and MB) scanned** after file-level pruning (`scan_cost`): a
  file is scanned iff its `time_unix_nano` min/max overlaps the window **and** (no
  equality predicate, or the service/trace min/max could contain the value). Deterministic
  and layout-sensitive; the **MB** figure is the S3-relevant unit (bytes ≈ what you fetch).
- **`warm_ms` is a noisy SECONDARY** — warm-cache local-disk wall-clock, dominated by
  fixed overhead at this scale. Do not treat it as the headline (an earlier draft did, and
  it produced a wrong "time beats service" conclusion that the scan-cost metric reverses).
- **Two things this does NOT measure** (so it is a *probe*, not a benchmark): (1) row-group
  pruning *within* a file — DuckDB 1.5.3 exposes no reliable rows-scanned, so `scan_cost`
  is file-level and slightly over-counts where row-groups would prune further; (2) the
  essential S3 cost — **GET request count + bytes** — which is the real decider and is a
  follow-up.

## Follow-ups (out of scope)

- Bloom-on-trace_id once the pinned DuckDB can write them (informed by experiment i).
- MinIO/S3 `DATA_PATH` pass to capture per-query GET count + bytes scanned.
- Real-corpus (Loghub / OTel-demo) realism cross-check of body content.
