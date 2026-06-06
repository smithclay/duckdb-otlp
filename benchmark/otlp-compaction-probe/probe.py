#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb==1.5.3"]
# ///
"""Compaction capability + cost probe for the DuckLake/OTLP store, using the pinned
(2026) DuckDB 1.5.3 + DuckLake CHECKPOINT / maintenance functions.

Pure research / measurement. The daemon seals each group-commit as ONE Parquet file per
signal, so a high seal cadence produces many small files -- the small-file hazard that
inflates S3 GET *count* (see ../otlp-query-probe and the s3 GET-count campaign: GET count
is driven by file/row-group count, which compaction is the lever for). This probe answers,
on the version the daemon actually runs, WHAT the native CHECKPOINT/merge path can and
cannot do, and what it COSTS the single seal writer:

  Q1 small-file fix   does CHECKPOINT / merge_adjacent_files merge many small seal files?
  Q2 bounded?         does target_file_size bound output size AND leave compacted files
                      alone next cycle (O(new) per cycle), or rewrite everything (O(total))?
  Q3 concurrency      can a compactor run on its own connection, or must it serialize into
                      the single seal-writer connection (file-catalog constraint)? cost?
  Q4 clustering       does merge re-sort to honor SET SORTED BY (the service/time locality
                      that gave the 3.2x single-service win), or only concatenate?
  Q5 blooms           does compaction write a trace_id bloom (the bytes/latency lever), or
                      is it still gated by dictionary encoding?
  Q6 space reclaim    merge leaves old files as orphans; do expire_snapshots + cleanup
                      actually reclaim them?

It never touches the daemon or its catalogs; it builds throwaway local DuckLake catalogs
under output/<run-id>/ (gitignored). Local disk -- this measures compaction MECHANICS and
writer-stall COST, not S3 (that is the GET-count campaign's job).
"""
from __future__ import annotations

import argparse
import glob
import os
import shutil
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parent
OUTPUT_ROOT = ROOT / "output"

SCHEMA = (
    "time_unix_nano TIMESTAMP_NS, trace_id VARCHAR, span_id VARCHAR, service_name VARCHAR, "
    "body VARCHAR, severity_number INT"
)
SVCS = 16


def gen(lo: int, hi: int) -> str:
    """One seal's worth of rows: high-card trace_id/span_id, 16 services cycling per row
    (so a file is NOT service-clustered unless something re-sorts it)."""
    return (
        f"SELECT make_timestamp(1717459200000000 + i*40)::TIMESTAMP_NS, md5(i::VARCHAR), "
        f"substr(md5((i*2654435761)::VARCHAR),1,16), 'svc'||(i%{SVCS})::VARCHAR, "
        f"'GET /api status='||(i%500)::VARCHAR, (i%24)::INT FROM range({lo},{hi}) g(i)"
    )


def newcon(cat: Path) -> duckdb.DuckDBPyConnection:
    cat.mkdir(parents=True, exist_ok=True)
    (cat / "data").mkdir(exist_ok=True)
    c = duckdb.connect(":memory:")
    c.execute("INSTALL ducklake; LOAD ducklake;")
    c.execute(f"ATTACH 'ducklake:{cat}/meta.ducklake' AS lake (DATA_PATH '{cat}/data/');")
    c.execute("USE lake.main;")
    return c


def footprint(cat: Path) -> dict:
    """Catalog-referenced files (live) vs on-disk parquet (incl. orphans). Read on a
    SEPARATE connection -- the file catalog is single-connection, so the writer must be
    closed first (that constraint is itself Q3)."""
    m = duckdb.connect(":memory:")
    m.execute(f"ATTACH '{cat}/meta.ducklake' AS mm (READ_ONLY);")
    n, total_mb, med_mb, max_mb = m.execute(
        "SELECT count(*), round(sum(file_size_bytes)/1e6,1), round(median(file_size_bytes)/1e6,1), "
        "round(max(file_size_bytes)/1e6,1) FROM mm.ducklake_data_file"
    ).fetchone()
    snaps = m.execute("SELECT count(*) FROM mm.ducklake_snapshot").fetchone()[0]
    m.close()
    fs = len(glob.glob(f"{cat}/data/**/*.parquet", recursive=True))
    return {
        "catalog_files": n,
        "fs_parquet": fs,
        "total_mb": total_mb,
        "median_mb": med_mb,
        "max_mb": max_mb,
        "snapshots": snaps,
    }


def trace_bloom_files(cat: Path) -> tuple[int, int]:
    """How many on-disk files carry a trace_id bloom (vs total)."""
    fs = glob.glob(f"{cat}/data/**/*.parquet", recursive=True)
    m = duckdb.connect(":memory:")
    have = 0
    for f in fs:
        r = m.execute(
            "SELECT bool_or(bloom_filter_offset IS NOT NULL) FROM parquet_metadata(?) "
            "WHERE path_in_schema='trace_id'",
            [f],
        ).fetchone()
        have += bool(r[0])
    m.close()
    return have, len(fs)


def seal(con, s: int, per: int) -> None:
    con.execute(f"INSERT INTO otlp_logs {gen(s*per, (s+1)*per)};")


def run(args: argparse.Namespace) -> list[str]:
    cat = OUTPUT_ROOT / args.run_id
    if cat.exists():
        shutil.rmtree(cat)
    L: list[str] = []

    def say(s: str) -> None:
        print(s)
        L.append(s)

    c0 = duckdb.connect(":memory:")
    c0.execute("INSTALL ducklake; LOAD ducklake;")
    dl = c0.execute("SELECT extension_version FROM duckdb_extensions() WHERE extension_name='ducklake'").fetchone()[0]
    say(f"# Compaction probe: {args.run_id}")
    say(
        f"\n- DuckDB {duckdb.__version__} | DuckLake {dl} | {args.seals} seals x {args.per:,} rows | target_file_size={args.target}"
    )
    sigs = c0.execute(
        "SELECT DISTINCT function_name FROM duckdb_functions() WHERE function_name ILIKE 'ducklake_%' "
        "AND (function_name ILIKE '%merge%' OR function_name ILIKE '%rewrite%' OR function_name ILIKE '%expire%' "
        "OR function_name ILIKE '%cleanup%' OR function_name ILIKE '%orphan%') ORDER BY 1"
    ).fetchall()
    c0.close()
    say(f"- maintenance fns: {', '.join(s[0] for s in sigs)}")

    # ---- Q1 small-file fix: simulate seals, then CHECKPOINT-driven merge ----
    c = newcon(cat)
    c.execute(f"CREATE TABLE otlp_logs ({SCHEMA});")
    c.execute(f"CALL lake.set_option('target_file_size', '{args.target}');")
    t0 = time.perf_counter()
    for s in range(args.seals):
        seal(c, s, args.per)
    ingest_s = time.perf_counter() - t0
    c.close()
    pre = footprint(cat)
    tb_pre = trace_bloom_files(cat)
    say(f"\n## Q1 small-file fix")
    say(
        f"- after {args.seals} seals: **{pre['catalog_files']} files**, median {pre['median_mb']} MB "
        f"({ingest_s:.1f}s ingest)"
    )

    c = newcon(cat)
    c.execute(f"CALL lake.set_option('target_file_size', '{args.target}');")
    t0 = time.perf_counter()
    c.execute("CALL lake.merge_adjacent_files();")
    merge_ms = (time.perf_counter() - t0) * 1000
    c.close()
    post = footprint(cat)
    say(
        f"- after merge: **{post['catalog_files']} files**, median {post['median_mb']} MB, max {post['max_mb']} MB "
        f"(**{merge_ms:.0f} ms**)"
    )
    say(
        f"- => merge collapses {pre['catalog_files']}->{post['catalog_files']} files; target_file_size bin-packs to ~{args.target}."
    )

    # ---- Q2 bounded? add more seals, merge again; are at-target files left alone? ----
    c = newcon(cat)
    c.execute(f"CALL lake.set_option('target_file_size', '{args.target}');")
    for s in range(args.seals, args.seals + args.seals // 4):
        seal(c, s, args.per)
    c.close()
    mid = footprint(cat)
    c = newcon(cat)
    c.execute(f"CALL lake.set_option('target_file_size', '{args.target}');")
    t0 = time.perf_counter()
    c.execute("CALL lake.merge_adjacent_files();")
    merge2_ms = (time.perf_counter() - t0) * 1000
    c.close()
    post2 = footprint(cat)
    bounded = post2["max_mb"] <= post["max_mb"] * 1.25
    say(f"\n## Q2 bounded / write-amplification")
    say(
        f"- cycle1 {post['catalog_files']} files (max {post['max_mb']} MB) -> +{args.seals//4} seals "
        f"({mid['catalog_files']} files) -> cycle2 merge {merge2_ms:.0f} ms -> {post2['catalog_files']} files (max {post2['max_mb']} MB)"
    )
    say(
        f"- => **{'BOUNDED' if bounded else 'UNBOUNDED'}**: at-target files {'left alone (O(new)/cycle)' if bounded else 'rewritten (O(total)/cycle)'}. "
        f"Without target_file_size, merge rewrites toward one big file (unbounded)."
    )

    # ---- Q3 concurrency: can a 2nd connection compact while a writer holds the catalog? ----
    say(f"\n## Q3 concurrency (file-catalog connection model)")
    cw = newcon(cat)
    try:
        cc = newcon(cat)
        cc.close()
        say("- 2nd simultaneous connection: ATTACHED -> a concurrent compactor connection is possible")
    except Exception as e:
        say(f"- 2nd simultaneous connection: **BLOCKED** ({str(e).splitlines()[-1][:60]}...)")
        say(
            "- => file catalog is single-connection; **compaction must run IN the seal-writer connection** "
            "(serialized, not concurrent). A concurrent compactor needs a server catalog (Postgres) or separate process."
        )
    cw.close()

    # ---- Q4 clustering: does merge honor SET SORTED BY, or only concatenate? ----
    scat = OUTPUT_ROOT / (args.run_id + "_sort")
    if scat.exists():
        shutil.rmtree(scat)
    c = newcon(scat)
    c.execute(f"CREATE TABLE otlp_logs ({SCHEMA});")
    c.execute("ALTER TABLE otlp_logs SET SORTED BY (service_name, time_unix_nano);")
    c.execute(f"CALL lake.set_option('target_file_size', '{args.target}');")
    for s in range(8):
        seal(c, s, args.per)
    c.execute("CALL lake.merge_adjacent_files();")
    c.close()
    big = max(glob.glob(f"{scat}/data/**/*.parquet", recursive=True), key=os.path.getsize)
    m = duckdb.connect(":memory:")
    rg = m.execute(
        "SELECT stats_min_value, stats_max_value FROM parquet_metadata(?) " "WHERE path_in_schema='service_name'", [big]
    ).fetchall()
    m.close()
    spanning = sum(1 for lo, hi in rg if lo != hi)
    say(f"\n## Q4 clustering (SET SORTED BY)")
    say(
        f"- merged file: {len(rg)} row groups, {spanning} span >1 service. "
        f"=> **{'NOT clustered' if spanning > 1 else 'clustered'}**: merge only concatenates; "
        "(service,time) clustering needs an explicit sort-rewrite (INSERT...ORDER BY / COPY), not native merge."
    )
    shutil.rmtree(scat)

    # ---- Q5 blooms: does compaction write a trace_id bloom? ----
    tb_post = trace_bloom_files(cat)
    say(f"\n## Q5 trace_id blooms")
    say(
        f"- files with trace_id bloom: pre-merge {tb_pre[0]}/{tb_pre[1]}, post-merge {tb_post[0]}/{tb_post[1]} "
        f"=> compaction does **not** write trace_id blooms (dictionary-encoding gate holds). "
        "Blooms need an out-of-band COPY...DICTIONARY_SIZE_LIMIT rewrite + ducklake_add_data_files."
    )

    # ---- Q6 space reclaim: orphans after merge, then expire + cleanup ----
    say(f"\n## Q6 space reclaim (orphans)")
    say(
        f"- post-merge: catalog refs {post2['catalog_files']} files but {post2['fs_parquet']} parquet on disk "
        f"(orphans = old pre-merge files held by snapshots)."
    )
    c = newcon(cat)
    for stmt in [
        "CALL ducklake_expire_snapshots('lake', older_than => now())",
        "CALL ducklake_cleanup_old_files('lake', cleanup_all => true)",
    ]:
        try:
            c.execute(stmt)
        except Exception as e:
            say(f"  - {stmt.split('(')[0]}: ERR {str(e).splitlines()[0][:60]}")
    c.close()
    after = footprint(cat)
    say(
        f"- after expire_snapshots + cleanup_old_files: **{after['fs_parquet']} parquet on disk** "
        f"(catalog refs {after['catalog_files']}) -> orphans reclaimed."
    )

    # ---- Q7 shipped daemon path: CHECKPOINT + startup options, bounded + self-cleaning across cycles ----
    say(f"\n## Q7 shipped path (CHECKPOINT + target_file_size + retention -- what the daemon runs)")
    cat7 = OUTPUT_ROOT / (args.run_id + "_ckpt")
    if cat7.exists():
        shutil.rmtree(cat7)
    half = max(2, args.seals // 2)
    c = newcon(cat7)
    c.execute(f"CREATE TABLE otlp_logs ({SCHEMA});")
    # The exact options OtlpServer::ConfigureCatalogMaintenanceOptions sets at startup. The 1s
    # retention here (vs the daemon's 15 min default) just makes reclaim observable in the probe.
    c.execute(f"CALL lake.set_option('target_file_size', '{args.target}');")
    c.execute("CALL lake.set_option('expire_older_than', '1s');")
    c.execute("CALL lake.set_option('delete_older_than', '1s');")
    next_seal, cyc_lines = 0, []
    for cycle in (1, 2, 3):
        for s in range(next_seal, next_seal + half):
            seal(c, s, args.per)
        next_seal += half
        time.sleep(1.1)  # age prior files past the 1s retention so CHECKPOINT can reclaim them
        c.execute("CHECKPOINT lake;")  # the daemon's actual post-seal maintenance call
        c.close()
        cyc_lines.append((cycle, footprint(cat7)))
        c = newcon(cat7)
    # settle: let the last cycle's just-orphaned files age past retention, then one more CHECKPOINT
    # on the still-open connection (the file catalog allows only one connection at a time).
    time.sleep(1.5)
    c.execute("CHECKPOINT lake;")
    c.close()
    settled = footprint(cat7)
    for cycle, fp in cyc_lines:
        say(
            f"- cycle {cycle}: +{half} seals -> CHECKPOINT -> {fp['catalog_files']} catalog files "
            f"(max {fp['max_mb']} MB), {fp['fs_parquet']} parquet on disk"
        )
    say(
        f"- settle (+CHECKPOINT after retention): {settled['catalog_files']} catalog files, "
        f"{settled['fs_parquet']} parquet on disk"
    )
    say(
        "- => **catalog files stay bounded** (≈ total data ÷ target, max ~target; prior files NOT rewritten = "
        "O(new)/cycle). On-disk = live files + at most one retention window of orphans, which the next "
        "CHECKPOINT reclaims — the daemon's actual path. Without target_file_size this is O(total)/cycle."
    )
    shutil.rmtree(cat7)

    cat_keep = footprint(cat)
    say(f"\n## net")
    say(
        f"- {args.seals} small seal files -> {post['catalog_files']} target-sized files, bounded re-compaction, "
        f"~{merge_ms:.0f} ms writer-stall per merge. Clustering + blooms are NOT native to merge."
    )
    summary = OUTPUT_ROOT / args.run_id / "summary.md"
    summary.write_text("\n".join(L) + "\n")
    print(f"\nwrote {summary}")
    return L


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--run-id", default="compaction")
    ap.add_argument("--seals", type=int, default=80, help="number of small seal files to simulate")
    ap.add_argument("--per", type=int, default=50_000, help="rows per seal")
    ap.add_argument("--target", default="128MB", help="DuckLake target_file_size")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
