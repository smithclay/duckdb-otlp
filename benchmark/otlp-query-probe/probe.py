#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.3"]
# ///
"""Logs-only query-performance feasibility probe for the DuckLake/OTLP store.

Pure research / measurement. Builds throwaway *local* DuckLake catalogs populated
with a controlled synthetic OTLP-logs dataset (known selectivities), materializes
several physical layouts, and times realistic time-bounded observability queries
against each -- to quantify three mitigations:

  (i)   trace_id point lookup: time-sort scatters trace_ids -> full-window scan;
        sort-by-trace_id clusters them so file min/max pruning works (the on-1.5.3
        stand-in for the Parquet bloom filter we cannot write on 1.5.3).
  (ii)  body substring search: no inverted index -> brute-force scan; time-bounding
        (+ time partitioning) is the only lever.
  (iii) service layout: partition-by-service speeds service-scoped queries but
        explodes file count (the ingest-time hazard); compaction-sort-by-(service,
        time) gets the locality without the per-seal fan-out.

It never touches the daemon or its catalogs. DuckDB pinned 1.5.3 (1.5.3 cannot WRITE
Parquet bloom filters, so the bloom variant is out of scope -- see plan). Local disk
only (S3 request-count cost is a noted follow-up).
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import platform
import shutil
import statistics
import subprocess
import time
from pathlib import Path
from typing import Any

import duckdb

ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = ROOT / "output"

# ---- generator vocabulary (controlled, realistic-ish) ----
SERVICES = [
    "checkout",
    "cart",
    "payment",
    "frontend",
    "catalogue",
    "shipping",
    "inventory",
    "recommendation",
    "auth",
    "email",
    "currency",
    "ad",
    "quote",
    "accounting",
    "fraud",
    "loadgenerator",
]
ROUTES = [
    "/api/cart",
    "/api/checkout",
    "/api/products",
    "/api/orders",
    "/api/payment",
    "/api/shipping",
    "/api/users",
    "/api/recommendations",
    "/api/ads",
    "/api/currency",
    "/api/health",
    "/api/search",
]
NAMESPACES = ["payments", "frontend", "platform", "data"]
EVENTS = ["http.server.request", "db.query", "cache.lookup", "rpc.call", "startup"]

# A trace_id needle that cannot collide with the md5(int) pool ids.
NEEDLE_HEX = hashlib.md5(b"__otlp_probe_needle__").hexdigest()
RARE_PHRASE = "connection reset by peer"
COMMON_TOKEN = "latency"

# 18-column otlp_logs schema, matching the daemon's CreateOrValidateTable exactly.
SCHEMA = (
    "time_unix_nano TIMESTAMP_NS, observed_time_unix_nano TIMESTAMP_NS, "
    "trace_id VARCHAR, span_id VARCHAR, service_name VARCHAR, service_namespace VARCHAR, "
    "service_instance_id VARCHAR, severity_number INTEGER, severity_text VARCHAR, "
    "event_name VARCHAR, body VARCHAR, resource_attributes VARCHAR, scope_name VARCHAR, "
    "scope_version VARCHAR, scope_attributes VARCHAR, log_attributes VARCHAR, "
    "dropped_attributes_count UINTEGER, flags UINTEGER"
)
COLUMNS = [c.split()[0] for c in SCHEMA.split(", ")]


def sql_list(values: list[str]) -> str:
    inner = ", ".join("'" + v.replace("'", "''") + "'" for v in values)
    return f"[{inner}]"


def gen_select(lo: int, hi: int, p: dict[str, Any]) -> str:
    """SELECT producing the 18 columns deterministically for row indices [lo, hi)."""
    seed = p["seed"]
    step_us = p["step_us"]
    start_epoch_us = p["start_epoch_us"]
    needle_period = p["needle_period"]
    trace_pool = p["trace_pool"]

    def h(k: int) -> str:  # independent uniform hash stream for column k (offset precomputed as BIGINT)
        return f"hash(i + {seed + k * 1000000007})"

    services, routes, nss, events = sql_list(SERVICES), sql_list(ROUTES), sql_list(NAMESPACES), sql_list(EVENTS)
    return f"""
WITH d AS (
  SELECT
    i,
    ({start_epoch_us} + i * {step_us} + ({h(1)} % 2000000))::BIGINT AS t_us,
    ({h(15)} % 50000)::BIGINT AS obs_lag_us,
    {services}[({h(2)} % {len(SERVICES)})::INTEGER + 1] AS service_name,
    {routes}[({h(3)} % {len(ROUTES)})::INTEGER + 1] AS route,
    {nss}[({h(4)} % {len(NAMESPACES)})::INTEGER + 1] AS ns,
    CASE WHEN ({h(5)} % 100) < 92 THEN 200 WHEN ({h(5)} % 100) < 97 THEN 404 ELSE 500 END AS status,
    CASE WHEN ({h(6)} % 1000) < 600 THEN 9 WHEN ({h(6)} % 1000) < 850 THEN 5
         WHEN ({h(6)} % 1000) < 950 THEN 13 WHEN ({h(6)} % 1000) < 990 THEN 17 ELSE 21 END AS sev,
    (i % {needle_period}) = {needle_period // 2} AS is_needle,
    ({h(7)} % 10) >= 3 AS has_trace,
    {h(8)} % {trace_pool} AS trace_slot,
    ({h(9)} % 2000) AS took_ms,
    ({h(10)} % 10000) < 500 AS has_common,
    ({h(11)} % 1000000) < 100 AS has_rare,
    {h(12)} % 6 AS inst,
    {events}[({h(13)} % {len(EVENTS)})::INTEGER + 1] AS event_name,
    {h(14)} % 5 AS scope_minor
  FROM range({lo}, {hi}) t(i)
), e AS (
  SELECT *,
    CASE WHEN is_needle THEN '{NEEDLE_HEX}'
         WHEN NOT has_trace THEN NULL
         ELSE md5((trace_slot)::VARCHAR) END AS trace_id
  FROM d
)
SELECT
  make_timestamp(t_us)::TIMESTAMP_NS AS time_unix_nano,
  make_timestamp(t_us + obs_lag_us)::TIMESTAMP_NS AS observed_time_unix_nano,
  trace_id,
  CASE WHEN trace_id IS NULL THEN NULL ELSE substr(md5(i::VARCHAR), 1, 16) END AS span_id,
  service_name,
  'otel-demo' AS service_namespace,
  service_name || '-' || inst::VARCHAR AS service_instance_id,
  sev AS severity_number,
  CASE sev WHEN 9 THEN 'INFO' WHEN 5 THEN 'DEBUG' WHEN 13 THEN 'WARN'
           WHEN 17 THEN 'ERROR' ELSE 'FATAL' END AS severity_text,
  event_name,
  'GET ' || route || ' status=' || status::VARCHAR || ' took=' || took_ms::VARCHAR || 'ms'
    || CASE WHEN has_common THEN ' {COMMON_TOKEN}=high' ELSE '' END
    || CASE WHEN has_rare THEN ' error: {RARE_PHRASE}' ELSE '' END AS body,
  '{{"k8s.namespace.name":"' || ns || '","cloud.region":"us-west-2"}}' AS resource_attributes,
  'io.opentelemetry.' || service_name AS scope_name,
  '1.' || scope_minor::VARCHAR || '.0' AS scope_version,
  '{{}}' AS scope_attributes,
  '{{"http.response.status_code":' || status::VARCHAR || ',"http.route":"' || route
    || '","k8s.namespace.name":"' || ns || '"}}' AS log_attributes,
  0::UINTEGER AS dropped_attributes_count,
  CASE WHEN trace_id IS NULL THEN 0 ELSE 1 END::UINTEGER AS flags
FROM e
""".strip()


def connect(catalog_dir: Path, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    catalog_dir.mkdir(parents=True, exist_ok=True)
    (catalog_dir / "data").mkdir(exist_ok=True)
    con = duckdb.connect(":memory:")
    con.execute("INSTALL ducklake; LOAD ducklake;")
    ro = ", READ_ONLY" if read_only else ""
    con.execute(f"ATTACH 'ducklake:{catalog_dir}/meta.ducklake' AS lake " f"(DATA_PATH '{catalog_dir}/data/'{ro});")
    con.execute("USE lake.main;")
    return con


def attach_meta(con: duckdb.DuckDBPyConnection, catalog_dir: Path, alias: str) -> None:
    con.execute(f"ATTACH '{catalog_dir}/meta.ducklake' AS {alias} (READ_ONLY);")


# ---------------------------------------------------------------------------
# generate
# ---------------------------------------------------------------------------
def gen_params(args: argparse.Namespace) -> dict[str, Any]:
    span_us = args.span_hours * 3600 * 1_000_000
    start_epoch_us = int(dt.datetime.fromisoformat(args.start_ts).replace(tzinfo=dt.UTC).timestamp() * 1_000_000)
    needle_period = max(1, args.rows // args.target_trace_needle_rows)
    # pool sized so the non-needle non-null traces are a realistic needle haystack
    trace_pool = max(1000, args.rows // 20)
    return {
        "seed": args.seed,
        "start_ts": args.start_ts,
        "start_epoch_us": start_epoch_us,
        "step_us": max(1, span_us // args.rows),
        "needle_period": needle_period,
        "trace_pool": trace_pool,
    }


def out_dir(run_id: str) -> Path:
    return OUTPUT_ROOT / run_id


def variant_dir(run_id: str, name: str) -> Path:
    return out_dir(run_id) / name


def insert_batched(con: duckdb.DuckDBPyConnection, args: argparse.Namespace, p: dict[str, Any]) -> None:
    """Baseline: time-ordered files, one INSERT (= one file) per row-index slice."""
    n, step = args.rows, args.rows_per_file
    b = 0
    while b * step < n:
        lo, hi = b * step, min((b + 1) * step, n)
        con.execute(f"INSERT INTO lake.main.otlp_logs {gen_select(lo, hi, p)}")
        b += 1


def generate(args: argparse.Namespace) -> None:
    p = gen_params(args)
    run = out_dir(args.run_id)
    run.mkdir(parents=True, exist_ok=True)
    # 1) baseline (the source every derived variant reads from)
    base = variant_dir(args.run_id, "baseline")
    print(f"[generate] baseline -> {base} ({args.rows:,} rows)")
    con = connect(base)
    con.execute(f"CREATE TABLE lake.main.otlp_logs ({SCHEMA});")
    insert_batched(con, args, p)
    (n,) = con.execute("SELECT count(*) FROM lake.main.otlp_logs").fetchone()
    con.close()
    print(f"[generate] baseline rows = {n:,}")

    # 2) the four primary variants -- each maps to a decision. Derived from baseline.
    derive(args, "sorted_service_time", order_by="service_name, time_unix_nano", batch_col="service_name")
    derive(args, "sorted_trace_id", order_by="trace_id", batch_col="trace_id")
    derive(args, "promoted", promote=True)
    # 3) hazard / secondary variants (opt-in): partition_service is ~redundant with sorted_service_time,
    #    partition_hour ~= baseline, and partition_hour_service exists to show the over-partition blow-up.
    if args.hazard_variants:
        derive(args, "partition_hour", partition_by="hour(time_unix_nano)")
        derive(args, "partition_service", partition_by="service_name")
        derive(args, "partition_hour_service", partition_by="hour(time_unix_nano), service_name")

    meta = {
        "run_id": args.run_id,
        "rows": args.rows,
        "span_hours": args.span_hours,
        "seed": args.seed,
        "needle_hex": NEEDLE_HEX,
        "rare_phrase": RARE_PHRASE,
        "common_token": COMMON_TOKEN,
        "duckdb_version": duckdb.__version__,
        "host": platform.platform(),
        "git_commit": git_commit(),
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "params": p,
    }
    (run / "run_meta.json").write_text(json.dumps(meta, indent=2, default=str))


PROMOTE_COLS = (
    ", CAST(json_extract_string(log_attributes, '$.\"http.response.status_code\"') AS INTEGER) AS http_status_code"
    ", json_extract_string(log_attributes, '$.\"http.route\"') AS http_route"
    ", json_extract_string(resource_attributes, '$.\"k8s.namespace.name\"') AS k8s_namespace"
)


def derive(
    args: argparse.Namespace,
    name: str,
    *,
    order_by: str | None = None,
    batch_col: str | None = None,
    partition_by: str | None = None,
    promote: bool = False,
) -> None:
    base = variant_dir(args.run_id, "baseline")
    vdir = variant_dir(args.run_id, name)
    if vdir.exists():
        shutil.rmtree(vdir)
    print(f"[generate] {name} -> {vdir}")
    con = connect(vdir)
    attach_meta_lake(con, base, "base")  # attach baseline as a second writable-read ducklake
    select_cols = "*" + (PROMOTE_COLS if promote else "")
    schema = SCHEMA + (", http_status_code INTEGER, http_route VARCHAR, k8s_namespace VARCHAR" if promote else "")
    con.execute(f"CREATE TABLE lake.main.otlp_logs ({schema});")
    if partition_by:
        con.execute(f"ALTER TABLE lake.main.otlp_logs SET PARTITIONED BY ({partition_by});")
        ob = f" ORDER BY {order_by}" if order_by else ""
        con.execute(f"INSERT INTO lake.main.otlp_logs SELECT {select_cols} FROM base.main.otlp_logs{ob}")
    elif batch_col == "service_name":
        for svc in SERVICES:  # one file per service -> service-clustered, time-ordered within
            con.execute(
                f"INSERT INTO lake.main.otlp_logs SELECT {select_cols} FROM base.main.otlp_logs "
                f"WHERE service_name = '{svc}' ORDER BY time_unix_nano"
            )
    elif batch_col == "trace_id":
        # bucket the hex space so each file holds a contiguous trace_id range (NULLs in their own file)
        edges = [format(i * (16**7), "08x") for i in range(16)] + ["g"]  # 'g' > any hex char
        con.execute(
            f"INSERT INTO lake.main.otlp_logs SELECT {select_cols} FROM base.main.otlp_logs " f"WHERE trace_id IS NULL"
        )
        for lo, hi in zip(edges[:-1], edges[1:]):
            con.execute(
                f"INSERT INTO lake.main.otlp_logs SELECT {select_cols} FROM base.main.otlp_logs "
                f"WHERE trace_id >= '{lo}' AND trace_id < '{hi}' ORDER BY trace_id"
            )
    else:  # promoted (baseline layout, batched by row-index via the time column slices)
        con.execute(f"INSERT INTO lake.main.otlp_logs SELECT {select_cols} FROM base.main.otlp_logs")
    con.close()


def attach_meta_lake(con: duckdb.DuckDBPyConnection, catalog_dir: Path, alias: str) -> None:
    con.execute(
        f"ATTACH 'ducklake:{catalog_dir}/meta.ducklake' AS {alias} " f"(DATA_PATH '{catalog_dir}/data/', READ_ONLY);"
    )


def git_commit() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"], cwd=ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# verify
# ---------------------------------------------------------------------------
def verify(args: argparse.Namespace) -> None:
    base = variant_dir(args.run_id, "baseline")
    con = connect(base, read_only=True)
    checks: list[tuple[str, bool, str]] = []

    def check(label: str, ok: bool, detail: str) -> None:
        checks.append((label, ok, detail))
        print(f"  [{'ok' if ok else 'FAIL'}] {label}: {detail}")

    (n,) = con.execute("SELECT count(*) FROM otlp_logs").fetchone()
    check("row count", n == args.rows, f"{n:,} (expected {args.rows:,})")
    (needle,) = con.execute("SELECT count(*) FROM otlp_logs WHERE trace_id = ?", [NEEDLE_HEX]).fetchone()
    check(
        "trace needle",
        1 <= needle <= args.target_trace_needle_rows * 2,
        f"{needle} rows (target ~{args.target_trace_needle_rows})",
    )
    (null_frac,) = con.execute("SELECT avg(CASE WHEN trace_id IS NULL THEN 1 ELSE 0 END) FROM otlp_logs").fetchone()
    check("trace null frac", 0.25 <= null_frac <= 0.35, f"{null_frac:.3f} (~0.30)")
    (badhex,) = con.execute(
        "SELECT count(*) FROM otlp_logs WHERE trace_id IS NOT NULL AND NOT regexp_matches(trace_id,'^[0-9a-f]{32}$')"
    ).fetchone()
    check("trace_id hex", badhex == 0, f"{badhex} malformed")
    (rare,) = con.execute("SELECT count(*) FROM otlp_logs WHERE body ILIKE ?", [f"%{RARE_PHRASE}%"]).fetchone()
    check("rare phrase ~0.01%", 0.00005 <= rare / n <= 0.0002, f"{rare} rows ({rare/n*100:.4f}%)")
    (err,) = con.execute("SELECT avg(CASE WHEN severity_number>=17 THEN 1 ELSE 0 END) FROM otlp_logs").fetchone()
    check("errors ~5%", 0.04 <= err <= 0.06, f"{err*100:.2f}%")
    (badjson,) = con.execute("SELECT count(*) FROM otlp_logs WHERE NOT json_valid(log_attributes)").fetchone()
    check("log_attributes valid json", badjson == 0, f"{badjson} invalid")
    con.close()

    # file size band + pruning sanity from the metadata oracle
    mcon = duckdb.connect(":memory:")
    attach_meta(mcon, base, "m")
    files, avg_mb, rgmax = mcon.execute(
        "SELECT count(*), round(avg(file_size_bytes)/1e6,1), max(record_count) FROM m.ducklake_data_file"
    ).fetchone()
    check("file size band (informational)", True, f"{files} files, avg {avg_mb} MB, max rows/file {rgmax:,}")
    mcon.close()

    if any(not ok for _, ok, _ in checks):
        raise SystemExit("verify FAILED")
    print("verify OK")


# ---------------------------------------------------------------------------
# run (query matrix + measurement)
# ---------------------------------------------------------------------------
def windows(start_ts: str, span_hours: int) -> dict[str, tuple[str, str]]:
    """15m/1h anchored mid-span (recent-ish, fully inside the data); 24h = the whole span."""
    start = dt.datetime.fromisoformat(start_ts)
    mid = start + dt.timedelta(hours=span_hours // 2)

    def w(lo: dt.datetime, minutes: int) -> tuple[str, str]:
        return (lo.isoformat(sep=" "), (lo + dt.timedelta(minutes=minutes)).isoformat(sep=" "))

    return {"15m": w(mid, 15), "1h": w(mid, 60), "24h": w(start, span_hours * 60)}


# Service-organized layouts, exercised by the single-service-scoped queries (the common
# on-call case: you own one service and scope to `service = X`). See experiment (iii).
SVC_VARIANTS = ["baseline", "sorted_service_time", "partition_service", "partition_hour_service"]
SVC = "checkout"  # the "service I own / was paged about"


def query_specs(win: dict[str, tuple[str, str]]) -> list[dict[str, Any]]:
    w15, w1h, w24 = win["15m"], win["1h"], win["24h"]
    tw = lambda w: f"time_unix_nano >= TIMESTAMP '{w[0]}' AND time_unix_nano < TIMESTAMP '{w[1]}'"
    specs = [
        # --- single-service-scoped on-call queries (the common case) ---
        {
            "id": "L1_tail_svc",
            "variants": SVC_VARIANTS,
            "sql": f"SELECT time_unix_nano, service_name, severity_text, body FROM otlp_logs "
            f"WHERE {tw(w15)} AND service_name = '{SVC}' ORDER BY time_unix_nano DESC LIMIT 200",
            "window": "15m",
        },
        {
            "id": "L3_errcount_svc",
            "variants": SVC_VARIANTS,
            "sql": f"SELECT time_bucket(INTERVAL 5 MINUTE, time_unix_nano) b, count(*) FROM otlp_logs "
            f"WHERE service_name = '{SVC}' AND severity_number >= 17 AND {tw(w1h)} GROUP BY b ORDER BY b",
            "window": "1h",
        },
        {
            "id": "L6_body_svc",
            "variants": SVC_VARIANTS,
            "sql": f"SELECT count(*) FROM otlp_logs WHERE service_name = '{SVC}' "
            f"AND body ILIKE '%{RARE_PHRASE}%' AND {tw(w1h)}",
            "window": "1h",
        },
        {
            "id": "L8_histogram_svc",
            "variants": SVC_VARIANTS,
            "sql": f"SELECT time_bucket(INTERVAL 1 MINUTE, time_unix_nano) b, count(*) FROM otlp_logs "
            f"WHERE service_name = '{SVC}' AND {tw(w1h)} GROUP BY b ORDER BY b",
            "window": "1h",
        },
        # --- trace_id needle (experiment i) ---
        {
            "id": "L2_trace",
            "variants": ["baseline", "sorted_service_time", "sorted_trace_id"],
            "sql": f"SELECT time_unix_nano, service_name, span_id, body FROM otlp_logs "
            f"WHERE trace_id = '{NEEDLE_HEX}' AND {tw(w1h)}",
            "window": "1h",
            "prune_col": "trace_id",
        },
        # --- body-search window sweep (experiment ii) ---
        *[
            {
                "id": f"L6_body_{label}",
                "variants": ["baseline", "partition_hour"],
                "sql": f"SELECT count(*) FROM otlp_logs WHERE body ILIKE '%{RARE_PHRASE}%' AND {tw(win[label])}",
                "window": label,
            }
            for label in ("15m", "1h", "24h")
        ],
        # --- cross-service overview / alert-storm exception ---
        {
            "id": "L4_topn_service",
            "variants": SVC_VARIANTS,
            "sql": f"SELECT service_name, count(*) n FROM otlp_logs WHERE severity_number >= 17 AND {tw(w1h)} "
            f"GROUP BY service_name ORDER BY n DESC LIMIT 10",
            "window": "1h",
        },
        # --- attribute filter: JSON-extract vs promoted column ---
        {
            "id": "L5_attr_json",
            "variants": ["baseline"],
            "sql": f"SELECT count(*) FROM otlp_logs WHERE "
            f"CAST(json_extract_string(log_attributes,'$.\"http.response.status_code\"') AS INTEGER) = 500 "
            f"AND {tw(w1h)}",
            "window": "1h",
        },
        {
            "id": "L5_attr_promoted",
            "variants": ["promoted"],
            "sql": f"SELECT count(*) FROM otlp_logs WHERE http_status_code = 500 AND {tw(w1h)}",
            "window": "1h",
        },
    ]
    for s in specs:
        if s["id"].endswith("_svc"):
            s["eq_col"], s["eq_val"] = "service_name", SVC
        elif s["id"] == "L2_trace":
            s["eq_col"], s["eq_val"] = "trace_id", NEEDLE_HEX
    return specs


def layout_footprint(catalog_dir: Path) -> dict[str, Any]:
    mcon = duckdb.connect(":memory:")
    attach_meta(mcon, catalog_dir, "m")
    files, total_mb, med_mb = mcon.execute(
        "SELECT count(*), round(sum(file_size_bytes)/1e6,1), round(median(file_size_bytes)/1e6,2) "
        "FROM m.ducklake_data_file"
    ).fetchone()
    mcon.close()
    return {"files": files, "total_mb": total_mb, "median_file_mb": med_mb}


def scan_cost(catalog_dir: Path, win: tuple[str, str], eq_col: str | None, eq_val: str | None) -> dict[str, Any]:
    """PRIMARY cost metric. Files (and MB) a query must read after ALL file-level pruning: a file is
    scanned iff its time_unix_nano min/max overlaps the window AND (no equality predicate, or the eq
    column's min/max could contain the value). Deterministic, layout-sensitive, and the MB figure is
    the S3-relevant unit (bytes fetched ~ files x size). NOTE: file-level only -- row-group pruning
    *within* a file is not captured (DuckDB 1.5.3 exposes no reliable rows-scanned), so this is a
    lower-bound-ish proxy, not the final S3 request-count cost."""
    mcon = duckdb.connect(":memory:")
    attach_meta(mcon, catalog_dir, "m")
    ft, mt, fs, ms = mcon.execute(
        """
        WITH stats AS (
          SELECT f.data_file_id, f.file_size_bytes,
                 max(s.min_value) FILTER (WHERE c.column_name='time_unix_nano') AS tmin,
                 max(s.max_value) FILTER (WHERE c.column_name='time_unix_nano') AS tmax,
                 max(s.min_value) FILTER (WHERE c.column_name=?) AS emin,
                 max(s.max_value) FILTER (WHERE c.column_name=?) AS emax
          FROM m.ducklake_data_file f
          JOIN m.ducklake_file_column_stats s ON s.data_file_id=f.data_file_id
          JOIN m.ducklake_column c ON c.column_id=s.column_id
          GROUP BY 1, 2
        ), scanned AS (
          SELECT file_size_bytes,
                 (NOT (tmax::TIMESTAMP_NS < ?::TIMESTAMP_NS OR tmin::TIMESTAMP_NS >= ?::TIMESTAMP_NS))
                 AND (? IS NULL OR (emin <= ? AND emax >= ?)) AS hit
          FROM stats
        )
        SELECT count(*), round(sum(file_size_bytes) / 1e6, 1),
               count(*) FILTER (WHERE hit), round(coalesce(sum(file_size_bytes) FILTER (WHERE hit), 0) / 1e6, 1)
        FROM scanned
        """,
        [eq_col, eq_col, win[0], win[1], eq_val, eq_val, eq_val],
    ).fetchone()
    mcon.close()
    return {"files_total": ft, "mb_total": mt, "files_scanned": fs, "mb_scanned": ms}


def measure_warm(catalog_dir: Path, sql: str, repeats: int) -> dict[str, Any]:
    """SECONDARY, noisy: warm-cache local-disk wall-clock. One connection per (variant,query) so the
    INSTALL/LOAD/ATTACH overhead is outside the timed window. At this scale latency is dominated by
    fixed overhead -- treat as a sanity check, not the headline (see scan_cost)."""
    con = connect(catalog_dir, read_only=True)
    samples, result_rows = [], None
    try:
        for _ in range(repeats):
            t0 = time.perf_counter()
            rows = con.execute(sql).fetchall()
            samples.append((time.perf_counter() - t0) * 1000)
            result_rows = len(rows)
    finally:
        con.close()
    return {
        "warm_ms_median": round(statistics.median(samples), 2),
        "warm_ms_p90": round(sorted(samples)[min(len(samples) - 1, int(0.9 * len(samples)))], 2),
        "result_rows": result_rows,
    }


def run(args: argparse.Namespace) -> None:
    run = out_dir(args.run_id)
    meta = json.loads((run / "run_meta.json").read_text())
    win = windows(meta["params"]["start_ts"], meta["span_hours"])
    results = []
    print(f"  {'query':18s} {'variant':22s} {'files↓':>9s} {'MB↓':>8s} {'(warm ms)':>10s}")
    for spec in query_specs(win):
        for variant in spec["variants"]:
            vdir = variant_dir(args.run_id, variant)
            if not vdir.exists():
                continue
            cost = scan_cost(vdir, win[spec["window"]], spec.get("eq_col"), spec.get("eq_val"))
            m = measure_warm(vdir, spec["sql"], args.repeats)
            results.append(
                {
                    "query": spec["id"],
                    "variant": variant,
                    "window": spec["window"],
                    **cost,
                    **layout_footprint(vdir),
                    **m,
                }
            )
            print(
                f"  {spec['id']:18s} {variant:22s} "
                f"{str(cost['files_scanned'])+'/'+str(cost['files_total']):>9s} "
                f"{cost['mb_scanned']:>8} {m['warm_ms_median']:>10.1f}"
            )
    (run / "results.json").write_text(json.dumps({"meta": meta, "results": results}, indent=2, default=str))
    write_summary(run, meta, results)
    print(f"\nwrote {run/'results.json'} and {run/'summary.md'}")


def write_summary(run: Path, meta: dict[str, Any], results: list[dict[str, Any]]) -> None:
    def rows_for(pred) -> list[dict[str, Any]]:
        return [r for r in results if pred(r)]

    def cost(r: dict[str, Any]) -> str:
        return f"{r['files_scanned']}/{r['files_total']} ({r['mb_scanned']} MB)"

    lines = [
        f"# Query-probe summary: {meta['run_id']}",
        "",
        f"- {meta['rows']:,} rows over {meta['span_hours']}h | DuckDB {meta['duckdb_version']} | "
        f"commit {meta['git_commit']}",
        "",
        "> **This is a probe, not a benchmark.** PRIMARY metric = **files (and MB) scanned** after",
        "> file-level pruning (deterministic, layout-sensitive, MB ~ the S3-relevant unit). It is",
        "> file-level only — row-group pruning *within* a file is not captured, and the essential S3",
        "> cost (GET **request count** + bytes) is **NOT measured here** (local disk; see README follow-ups).",
        "> `warm ms` is warm-cache local wall-clock — a noisy sanity check at this scale, not the headline.",
        "",
        "## Layout file footprint",
        "",
        "| variant | files | total MB | median file MB |",
        "|---|---|---|---|",
    ]
    seen = set()
    for r in results:
        if r["variant"] in seen:
            continue
        seen.add(r["variant"])
        lines.append(f"| {r['variant']} | {r['files']} | {r['total_mb']} | {r['median_file_mb']} |")

    lines += [
        "",
        "## Experiment (i): trace_id lookup (L2, 1h window)",
        "",
        "| variant | files scanned (MB) | warm ms |",
        "|---|---|---|",
    ]
    for r in rows_for(lambda r: r["query"] == "L2_trace"):
        lines.append(f"| {r['variant']} | {cost(r)} | {r['warm_ms_median']} |")

    lines += [
        "",
        "## Experiment (ii): body search vs window (baseline)",
        "",
        "No index → brute-force scan; the time window is the only lever.",
        "",
        "| window | files scanned (MB) | warm ms | matches |",
        "|---|---|---|---|",
    ]
    sweep = ("L6_body_15m", "L6_body_1h", "L6_body_24h")
    for r in sorted(rows_for(lambda r: r["query"] in sweep), key=lambda r: (r["window"], r["variant"])):
        lines.append(f"| {r['window']} ({r['variant']}) | {cost(r)} | {r['warm_ms_median']} | {r['result_rows']} |")

    lines += [
        "",
        "## Experiment (iii): service layout (single-service on-call queries vs file cost)",
        "",
        "files scanned = files surviving the query's combined time+service predicate.",
        "",
        "| query | variant | files scanned (MB) | median file MB | warm ms |",
        "|---|---|---|---|---|",
    ]
    svc_queries = ("L1_tail_svc", "L3_errcount_svc", "L6_body_svc", "L8_histogram_svc", "L4_topn_service")
    for qid in svc_queries:
        for r in rows_for(lambda r: r["query"] == qid):
            lines.append(
                f"| {r['query']} | {r['variant']} | {cost(r)} | {r['median_file_mb']} | {r['warm_ms_median']} |"
            )

    lines += [
        "",
        "## L5 attribute filter: JSON-extract vs promoted column",
        "",
        "| variant | files scanned (MB) | warm ms |",
        "|---|---|---|",
    ]
    for r in rows_for(lambda r: r["query"].startswith("L5_attr")):
        lines.append(f"| {r['query']} ({r['variant']}) | {cost(r)} | {r['warm_ms_median']} |")
    lines.append("")
    (run / "summary.md").write_text("\n".join(lines))


def clean(args: argparse.Namespace) -> None:
    d = out_dir(args.run_id)
    if d.exists():
        shutil.rmtree(d)
        print(f"removed {d}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("command", choices=["generate", "verify", "run", "all", "clean"])
    ap.add_argument("--run-id", default="probe-" + dt.datetime.now().strftime("%Y%m%d-%H%M%S"))
    ap.add_argument("--rows", type=int, default=20_000_000)
    ap.add_argument("--span-hours", type=int, default=24)
    ap.add_argument("--seed", type=int, default=1337)
    ap.add_argument("--start-ts", default="2026-06-04 00:00:00")
    ap.add_argument("--target-trace-needle-rows", type=int, default=20)
    ap.add_argument("--rows-per-file", type=int, default=2_000_000, help="bigger = fewer/larger files (~128MB+)")
    ap.add_argument("--repeats", type=int, default=7)
    ap.add_argument(
        "--hazard-variants",
        action="store_true",
        help="also build partition_hour / partition_service / partition_hour_service "
        "(redundant/negative-result variants kept for the over-partition finding)",
    )
    args = ap.parse_args()
    print(f"run-id: {args.run_id}")
    if args.command in ("generate", "all"):
        generate(args)
    if args.command in ("verify", "all"):
        verify(args)
    if args.command in ("run", "all"):
        run(args)
    if args.command == "clean":
        clean(args)


if __name__ == "__main__":
    main()
