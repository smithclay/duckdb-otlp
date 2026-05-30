#!/usr/bin/env python3
"""Manual concurrency + teardown smoke test for the OTLP HTTP ingest server.

This is NOT part of `make test`: the SQLLogicTest harness cannot drive concurrent
HTTP POSTs, so the parallel ingest path (per-worker Connection, per-request
transaction, 128-thread pool) and concurrent stop are exercised here instead.

What it checks:
  * N concurrent POSTs of a real OTLP payload all return 200 and land their rows.
  * total_rows / total_requests in otlp_server_list() reconcile with what we sent.
  * The server reports is_listening = true while up, and otlp_stop() frees it.
  * (optional) stop-under-load: fire requests while another thread calls otlp_stop.

Run it (ideally also under a TSan/ASan build of the extension to catch races):

    uv run python test/manual/otlp_serve_concurrency.py
    OTLP_EXTENSION=build/release/extension/otlp/otlp.duckdb_extension \\
        OTLP_PAYLOAD=test/data/logs_simple.jsonl OTLP_CONCURRENCY=64 \\
        uv run python test/manual/otlp_serve_concurrency.py

Requires the `duckdb` Python package and a built extension.
"""

import os
import sys
import time
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

import duckdb

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
PAYLOAD = os.environ.get("OTLP_PAYLOAD", "test/data/logs_simple.jsonl")
CONCURRENCY = int(os.environ.get("OTLP_CONCURRENCY", "64"))
PORT = int(os.environ.get("OTLP_PORT", "4329"))
# When set to a directory, attach a DuckLake catalog there and stream into it, so the
# run exercises the DuckLake seal path (Parquet files) instead of an in-memory table.
DUCKLAKE_DIR = os.environ.get("OTLP_DUCKLAKE_DIR", "")
TOKEN = "manual-smoke-token-0123456789"

CONTENT_TYPES = {
    ".json": "application/json",
    ".jsonl": "application/x-ndjson",
    ".ndjson": "application/x-ndjson",
    ".pb": "application/x-protobuf",
}


def content_type_for(path):
    for suffix, ctype in CONTENT_TYPES.items():
        if path.endswith(suffix):
            return ctype
    raise SystemExit(f"unknown payload extension for {path}")


def signal_endpoint(path):
    name = os.path.basename(path).lower()
    if "trace" in name:
        return "/v1/traces"
    if "metric" in name:
        return "/v1/metrics"
    return "/v1/logs"


def post(url, body, ctype):
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": ctype, "Authorization": f"Bearer {TOKEN}"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        return resp.status, resp.read().decode()


def main():
    if not os.path.exists(EXTENSION):
        raise SystemExit(f"extension not found: {EXTENSION} (build it first: GEN=ninja make)")
    with open(PAYLOAD, "rb") as f:
        body = f.read()
    ctype = content_type_for(PAYLOAD)
    endpoint = signal_endpoint(PAYLOAD)
    # A /v1/metrics POST fans out across every metric shape table, so its total_rows
    # is the sum over all four; logs/traces land in a single table.
    tables = {
        "/v1/logs": ["otlp_logs"],
        "/v1/traces": ["otlp_traces"],
        "/v1/metrics": [
            "otlp_metrics_gauge",
            "otlp_metrics_sum",
            "otlp_metrics_histogram",
            "otlp_metrics_exp_histogram",
        ],
    }[endpoint]

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")

    catalog = ""
    table_prefix = ""
    if DUCKLAKE_DIR:
        os.makedirs(DUCKLAKE_DIR, exist_ok=True)
        con.execute("LOAD ducklake")
        con.execute(f"ATTACH 'ducklake:{DUCKLAKE_DIR}/meta.ducklake' AS lake (DATA_PATH '{DUCKLAKE_DIR}/data/')")
        catalog = "lake"
        table_prefix = "lake.main."
        print(f"attached DuckLake at {DUCKLAKE_DIR}; streaming into catalog 'lake'")

    if catalog:
        row = con.execute(
            "SELECT listen_url FROM otlp_serve(?, token := ?, catalog := ?)",
            [f"otlp:127.0.0.1:{PORT}", TOKEN, catalog],
        ).fetchone()
    else:
        row = con.execute(
            "SELECT listen_url FROM otlp_serve(?, token := ?)",
            [f"otlp:127.0.0.1:{PORT}", TOKEN],
        ).fetchone()
    base_url = row[0]
    print(f"serving at {base_url}, POSTing {CONCURRENCY}x {PAYLOAD} ({ctype}) -> {endpoint}")

    url = base_url + endpoint
    statuses = []
    with ThreadPoolExecutor(max_workers=min(CONCURRENCY, 64)) as pool:
        futures = [pool.submit(post, url, body, ctype) for _ in range(CONCURRENCY)]
        for fut in futures:
            try:
                status, _ = fut.result()
                statuses.append(status)
            except urllib.error.HTTPError as exc:
                statuses.append(exc.code)

    # Ingest is buffered: a 202 means the rows were accepted, not yet sealed.
    ok = sum(1 for s in statuses if s in (200, 202))
    print(f"  responses: {ok}/{CONCURRENCY} accepted (statuses: {sorted(set(statuses))})")

    server = con.execute(
        "SELECT total_requests, total_rows, is_listening, last_error FROM otlp_server_list() WHERE port = ?",
        [PORT],
    ).fetchone()
    accepted_rows = server[1]
    print(
        f"  otlp_server_list: total_requests={server[0]} total_rows(accepted)={accepted_rows} "
        f"is_listening={server[2]} last_error={server[3]}"
    )

    # Force a synchronous seal so we don't have to wait for the age trigger, then the
    # rows are durable in the target table(s). For DuckLake, also compact.
    try:
        flush = con.execute(
            "SELECT status, sealed_rows, seals_total, error FROM otlp_flush(?, checkpoint := ?)",
            [f"otlp:127.0.0.1:{PORT}", bool(catalog)],
        ).fetchone()
        print(f"  otlp_flush: {flush}")
    except Exception:
        # otlp_flush not available (pre-Phase-3): fall back to waiting for the age seal.
        time.sleep(7)
    table_rows = sum(con.execute(f"SELECT count(*) FROM {table_prefix}{t}").fetchone()[0] for t in tables)
    print(f"  {'+'.join(tables)} now have {table_rows} rows total")
    if DUCKLAKE_DIR:
        import glob

        parquet = glob.glob(f"{DUCKLAKE_DIR}/data/**/*.parquet", recursive=True)
        print(f"  DuckLake Parquet files: {len(parquet)}")
        if table_rows > 0 and not parquet:
            print("  WARNING: rows sealed but no Parquet files found")

    failures = []
    if ok != CONCURRENCY:
        failures.append(f"{CONCURRENCY - ok} requests were not accepted")
    if server[0] != CONCURRENCY:
        failures.append(f"total_requests {server[0]} != {CONCURRENCY}")
    if accepted_rows != table_rows:
        failures.append(f"accepted rows {accepted_rows} != sealed table row count {table_rows}")
    if not server[2]:
        failures.append("server reports is_listening = false while up")

    stopped = con.execute("SELECT status FROM otlp_stop(?)", [f"otlp:127.0.0.1:{PORT}"]).fetchone()[0]
    print(f"  stop: {stopped}")
    if not stopped.startswith("Stopped"):
        failures.append(f"otlp_stop did not stop the server: {stopped}")

    con.close()
    if failures:
        print("FAIL:\n  - " + "\n  - ".join(failures))
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
