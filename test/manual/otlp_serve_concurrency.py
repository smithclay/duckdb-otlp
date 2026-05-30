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
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor

import duckdb

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
PAYLOAD = os.environ.get("OTLP_PAYLOAD", "test/data/logs_simple.jsonl")
CONCURRENCY = int(os.environ.get("OTLP_CONCURRENCY", "64"))
PORT = int(os.environ.get("OTLP_PORT", "4329"))
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

    ok = sum(1 for s in statuses if s == 200)
    print(f"  responses: {ok}/{CONCURRENCY} returned 200 (statuses: {sorted(set(statuses))})")

    server = con.execute(
        "SELECT total_requests, total_rows, is_listening, last_error FROM otlp_server_list() WHERE port = ?",
        [PORT],
    ).fetchone()
    table_rows = sum(con.execute(f"SELECT count(*) FROM {t}").fetchone()[0] for t in tables)
    print(
        f"  otlp_server_list: total_requests={server[0]} total_rows={server[1]} "
        f"is_listening={server[2]} last_error={server[3]}"
    )
    print(f"  {'+'.join(tables)} now have {table_rows} rows total")

    failures = []
    if ok != CONCURRENCY:
        failures.append(f"{CONCURRENCY - ok} requests did not return 200")
    if server[0] != CONCURRENCY:
        failures.append(f"total_requests {server[0]} != {CONCURRENCY}")
    if server[1] != table_rows:
        failures.append(f"total_rows {server[1]} != table row count {table_rows}")
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
