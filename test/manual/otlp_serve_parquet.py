#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "duckdb==1.5.3",
# ]
# ///
"""Manual coverage for `otlp_serve(parquet_export_path := ...)` (plain Parquet export).

Outside `make test`: SQLLogicTest cannot drive HTTP POSTs or seal failures. This
harness exercises the parquet-export seal path and its durability contract:

  * happy path: rows land in <root>/<table>/year=/month=/day=/seal_*.parquet
  * no local copy: the destination object is a VIEW over the Parquet files, not a
    data-bearing base table (regression guard for the old double-write)
  * the inspection view and a direct read_parquet() agree with accepted rows
  * parquet_export_path is mutually exclusive with a catalog target
  * at-least-once: a seal whose COPY fails re-buffers its rows (no loss) and a
    later flush exports them once the export path is writable again

A deterministic *cross-signal no-duplication* assertion would need in-code fault
injection; this harness verifies the no-loss/recovery half of the contract.

Run:

    uv run --script test/manual/otlp_serve_parquet.py
    OTLP_EXTENSION=build/release/extension/otlp/otlp.duckdb_extension \\
        uv run --script test/manual/otlp_serve_parquet.py
"""

import json
import os
import stat
import sys
import tempfile
import urllib.error
import urllib.request

import duckdb

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
LOG_PAYLOAD = os.environ.get("OTLP_PAYLOAD", "test/data/logs_simple.jsonl")
BASE_PORT = int(os.environ.get("OTLP_PORT", "4351"))
TOKEN = "manual-parquet-token-0123456789"


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    return con


def start_server(con, port, **options):
    sql = "SELECT listen_url FROM otlp_serve(?, token := ?"
    params = [f"otlp:127.0.0.1:{port}", TOKEN]
    for name, value in options.items():
        sql += f", {name} := ?"
        params.append(value)
    sql += ")"
    return con.execute(sql, params).fetchone()[0]


def flush_server(con, port):
    # otlp_flush reports a seal failure in its `error` column (it does not raise).
    return con.execute(
        "SELECT status, sealed_rows, seals_total, error FROM otlp_flush(?)",
        [f"otlp:127.0.0.1:{port}"],
    ).fetchone()


def server_stat(con, port, column):
    return con.execute(
        f"SELECT {column} FROM otlp_server_list() WHERE listen_url LIKE ?",
        [f"%:{port}"],
    ).fetchone()[0]


def stop_server(con, port):
    return con.execute("SELECT status FROM otlp_stop(?)", [f"otlp:127.0.0.1:{port}"]).fetchone()[0]


def post(url, body, auth=TOKEN, timeout=10):
    headers = {"Content-Type": "application/x-ndjson"}
    if auth:
        headers["Authorization"] = f"Bearer {auth}"
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode(errors="replace")


def accepted(status, text):
    return int(json.loads(text)["rows"]) if status == 202 else 0


def require(failures, condition, message):
    # `message` describes the failure case, so only print it when the check fails.
    if condition:
        print("  ok")
    else:
        failures.append(message)
        print(f"  FAIL: {message}")


def parquet_files(root):
    found = []
    for dirpath, _dirs, files in os.walk(root):
        found += [os.path.join(dirpath, f) for f in files if f.endswith(".parquet")]
    return found


def scenario_happy_path(failures, port, workdir):
    root = os.path.join(workdir, "export")
    con = connect()
    body = open(LOG_PAYLOAD, "rb").read()
    base_url = start_server(con, port, parquet_export_path=root)
    print(f"[happy] {base_url} -> {root}")

    rows = 0
    for _ in range(3):
        status, text = post(base_url + "/v1/logs", body)
        rows += accepted(status, text)
    require(failures, rows > 0, f"expected accepted rows > 0, got {rows}")

    status, sealed, seals, err = flush_server(con, port)
    require(failures, status == "sealed", f"flush failed: {status} ({err})")
    require(failures, server_stat(con, port, "seal_failures_total") == 0, "unexpected seal failures on happy path")

    files = parquet_files(root)
    require(failures, len(files) >= 1, f"expected parquet files under {root}, found {len(files)}")
    layout_ok = any(
        ("/otlp_logs/year=" in f) and ("/month=" in f) and ("/day=" in f) and os.path.basename(f).startswith("seal_")
        for f in files
    )
    require(failures, layout_ok, f"parquet files do not match the date-partition layout: {files[:3]}")

    pq_count = con.execute(
        "SELECT count(*) FROM read_parquet(?, hive_partitioning := false, union_by_name := true)",
        [os.path.join(root, "otlp_logs", "**", "*.parquet")],
    ).fetchone()[0]
    require(failures, pq_count == rows, f"parquet rows {pq_count} != accepted {rows}")

    view_count = con.execute("SELECT count(*) FROM main.otlp_logs").fetchone()[0]
    require(failures, view_count == rows, f"inspection view rows {view_count} != accepted {rows}")

    kind = con.execute("SELECT table_type FROM information_schema.tables WHERE table_name = 'otlp_logs'").fetchall()
    require(
        failures,
        kind == [("VIEW",)],
        f"otlp_logs should be a single VIEW (no local data copy), got {kind}",
    )

    require(failures, stop_server(con, port).startswith("Stopped"), "stop failed")
    con.close()


def scenario_mutual_exclusion(failures, port, workdir):
    con = connect()
    con.execute("ATTACH ':memory:' AS lake")
    try:
        start_server(con, port, catalog="lake", parquet_export_path=os.path.join(workdir, "x"))
        require(failures, False, "expected otlp_serve(catalog + parquet_export_path) to fail")
        stop_server(con, port)
    except duckdb.Error as exc:
        require(
            failures,
            "cannot be combined with a catalog" in str(exc),
            f"unexpected error for catalog+parquet_export_path: {exc}",
        )
    con.close()


def scenario_failure_recovery(failures, port, workdir):
    # Block writes so the seal's COPY fails, then confirm the rows are re-buffered
    # (not lost) and exported once the path becomes writable again.
    blocked = os.path.join(workdir, "blocked")
    os.makedirs(blocked, exist_ok=True)
    root = os.path.join(blocked, "export")
    con = connect()
    body = open(LOG_PAYLOAD, "rb").read()
    base_url = start_server(con, port, parquet_export_path=root)
    print(f"[recovery] {base_url} -> {root}")

    rows = 0
    for _ in range(3):
        status, text = post(base_url + "/v1/logs", body)
        rows += accepted(status, text)

    os.chmod(blocked, 0)  # deny writes -> COPY fails
    try:
        status, sealed, seals, err = flush_server(con, port)
        require(failures, status == "error", f"expected flush error while path unwritable, got {status}")
        require(failures, server_stat(con, port, "seal_failures_total") >= 1, "expected a seal failure")
        still_buffered = server_stat(con, port, "buffered_rows")
        require(
            failures, still_buffered == rows, f"rows should stay buffered after failure: {still_buffered} != {rows}"
        )
    finally:
        os.chmod(blocked, stat.S_IRWXU)  # restore writes

    flush_server(con, port)
    pq_count = con.execute(
        "SELECT count(*) FROM read_parquet(?, hive_partitioning := false, union_by_name := true)",
        [os.path.join(root, "otlp_logs", "**", "*.parquet")],
    ).fetchone()[0]
    require(failures, pq_count >= rows, f"after recovery parquet rows {pq_count} < accepted {rows}")
    drained = server_stat(con, port, "buffered_rows")
    require(failures, drained == 0, f"rows should be drained after recovery, still buffered: {drained}")

    require(failures, stop_server(con, port).startswith("Stopped"), "stop failed")
    con.close()


def main():
    if not os.path.exists(EXTENSION):
        raise SystemExit(f"extension not found: {EXTENSION} (set OTLP_EXTENSION)")
    failures = []
    with tempfile.TemporaryDirectory(prefix="otlp-parquet-") as workdir:
        scenario_happy_path(failures, BASE_PORT, os.path.join(workdir, "happy"))
        scenario_mutual_exclusion(failures, BASE_PORT + 1, workdir)
        scenario_failure_recovery(failures, BASE_PORT + 2, workdir)
    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nall parquet-export checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
