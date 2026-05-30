#!/usr/bin/env python3
"""Manual HTTP hot-path coverage for the OTLP ingest server.

This is intentionally outside `make test`: SQLLogicTest cannot drive concurrent
HTTP POSTs or race `otlp_flush` with `otlp_stop`. The harness is deterministic
enough for CI opt-in and covers:

  * Bearer and x-api-key auth success
  * missing/bad auth -> 401
  * unsupported content type -> 415
  * unsupported content encoding -> 415 (and gzip/deflate when OTLP_EXPECT_ZLIB=0)
  * max_body_bytes -> 413
  * low max_buffered_bytes under concurrency -> some 503s
  * /v1/metrics fanout across all four metric tables
  * stop-under-load final drain
  * concurrent otlp_flush plus otlp_stop

Run:

    uv run python test/manual/otlp_serve_concurrency.py
    OTLP_EXTENSION=build/release/extension/otlp/otlp.duckdb_extension \\
        OTLP_CONCURRENCY=64 uv run python test/manual/otlp_serve_concurrency.py

Requires the `duckdb` Python package and a built extension.
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import duckdb

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
LOG_PAYLOAD = os.environ.get("OTLP_PAYLOAD", "test/data/logs_simple.jsonl")
METRICS_PAYLOAD = os.environ.get("OTLP_METRICS_PAYLOAD", "test/data/metrics_all_types.jsonl")
CONCURRENCY = int(os.environ.get("OTLP_CONCURRENCY", "32"))
BASE_PORT = int(os.environ.get("OTLP_PORT", "4329"))
DUCKLAKE_DIR = os.environ.get("OTLP_DUCKLAKE_DIR", "")
EXPECT_ZLIB = os.environ.get("OTLP_EXPECT_ZLIB", "1") != "0"
TOKEN = "manual-smoke-token-0123456789"
DISK_ROOT = os.environ.get("OTLP_DISK_BUFFER_ROOT", "")

CONTENT_TYPES = {
    ".json": "application/json",
    ".jsonl": "application/x-ndjson",
    ".ndjson": "application/x-ndjson",
    ".pb": "application/x-protobuf",
}

METRIC_READ_FUNCTIONS = {
    "otlp_metrics_gauge": "read_otlp_metrics_gauge",
    "otlp_metrics_sum": "read_otlp_metrics_sum",
    "otlp_metrics_histogram": "read_otlp_metrics_histogram",
    "otlp_metrics_exp_histogram": "read_otlp_metrics_exp_histogram",
}


def quote_ident(value):
    return '"' + value.replace('"', '""') + '"'


def content_type_for(path):
    for suffix, ctype in CONTENT_TYPES.items():
        if path.endswith(suffix):
            return ctype
    raise SystemExit(f"unknown payload extension for {path}")


def load_payload(path):
    with open(path, "rb") as f:
        return f.read()


def repeated_jsonl(body, repeats):
    if not body.endswith(b"\n"):
        body += b"\n"
    return body * repeats


def connect(scenario):
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    catalog = ""
    table_prefix = ""
    if DUCKLAKE_DIR:
        catalog = "lake_" + "".join(c if c.isalnum() else "_" for c in scenario)
        scenario_dir = os.path.join(DUCKLAKE_DIR, scenario)
        os.makedirs(scenario_dir, exist_ok=True)
        con.execute("LOAD ducklake")
        con.execute(
            f"ATTACH 'ducklake:{scenario_dir}/meta.ducklake' AS {quote_ident(catalog)} "
            f"(DATA_PATH '{scenario_dir}/data/')"
        )
        table_prefix = f"{quote_ident(catalog)}.main."
    return con, catalog, table_prefix


def start_server(con, port, catalog="", **options):
    sql = "SELECT listen_url FROM otlp_serve(?, token := ?"
    params = [f"otlp:127.0.0.1:{port}", TOKEN]
    if catalog:
        sql += ", catalog := ?"
        params.append(catalog)
    for name, value in options.items():
        sql += f", {name} := ?"
        params.append(value)
    sql += ")"
    return con.execute(sql, params).fetchone()[0]


def stop_server(con, port):
    return con.execute("SELECT status FROM otlp_stop(?)", [f"otlp:127.0.0.1:{port}"]).fetchone()[0]


def flush_server(con, port, checkpoint=False):
    return con.execute(
        "SELECT status, sealed_rows, seals_total, error FROM otlp_flush(?, checkpoint := ?)",
        [f"otlp:127.0.0.1:{port}", checkpoint],
    ).fetchone()


def post(url, body, ctype, auth="bearer", encoding="", timeout=10):
    headers = {"Content-Type": ctype}
    if auth == "bearer":
        headers["Authorization"] = f"Bearer {TOKEN}"
    elif auth == "api-key":
        headers["x-api-key"] = TOKEN
    elif auth == "bad":
        headers["Authorization"] = "Bearer wrong-token"
    if encoding:
        headers["Content-Encoding"] = encoding
    req = urllib.request.Request(url, data=body, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        try:
            text = exc.read().decode()
        except Exception:
            text = ""
        return exc.code, text
    except Exception as exc:
        return 0, str(exc)


def rows_from_response(status, text):
    if status != 202:
        return 0
    try:
        return int(json.loads(text)["rows"])
    except Exception:
        return 0


def table_count(con, table_prefix, table_name):
    return con.execute(f"SELECT count(*) FROM {table_prefix}{table_name}").fetchone()[0]


def require(failures, condition, message):
    if not condition:
        failures.append(message)


def scenario_auth_and_validation(failures, port):
    con, catalog, table_prefix = connect("auth_validation")
    body = load_payload(LOG_PAYLOAD)
    ctype = content_type_for(LOG_PAYLOAD)
    base_url = start_server(con, port, catalog=catalog, seal_max_age_ms=60000)
    url = base_url + "/v1/logs"
    print(f"[auth] {base_url}")

    accepted_rows = 0
    for label, auth in (("bearer", "bearer"), ("x-api-key", "api-key")):
        status, text = post(url, body, ctype, auth=auth)
        print(f"  {label}: {status}")
        require(failures, status == 202, f"{label} auth expected 202, got {status}: {text}")
        accepted_rows += rows_from_response(status, text)

    for label, auth in (("missing auth", "none"), ("bad auth", "bad")):
        status, text = post(url, body, ctype, auth=auth)
        print(f"  {label}: {status}")
        require(failures, status == 401, f"{label} expected 401, got {status}: {text}")

    status, text = post(url, body, "text/plain", auth="bearer")
    print(f"  unsupported content-type: {status}")
    require(
        failures,
        status == 415,
        f"unsupported content-type expected 415, got {status}: {text}",
    )

    status, text = post(url, body, ctype, auth="bearer", encoding="br")
    print(f"  unsupported content-encoding br: {status}")
    require(
        failures,
        status == 415,
        f"unsupported content-encoding expected 415, got {status}: {text}",
    )

    if not EXPECT_ZLIB:
        for encoding in ("gzip", "deflate"):
            status, text = post(url, body, ctype, auth="bearer", encoding=encoding)
            print(f"  unsupported content-encoding {encoding}: {status}")
            require(
                failures,
                status == 415,
                f"{encoding} expected 415 without zlib, got {status}: {text}",
            )

    flush_server(con, port, checkpoint=bool(catalog))
    rows = table_count(con, table_prefix, "otlp_logs")
    require(
        failures,
        rows == accepted_rows,
        f"auth scenario table rows {rows} != accepted rows {accepted_rows}",
    )
    stopped = stop_server(con, port)
    require(failures, stopped.startswith("Stopped"), f"auth scenario stop failed: {stopped}")
    con.close()


def scenario_max_body(failures, port):
    con, catalog, _ = connect("max_body")
    body = load_payload(LOG_PAYLOAD)
    ctype = content_type_for(LOG_PAYLOAD)
    base_url = start_server(con, port, catalog=catalog, max_body_bytes=32)
    status, text = post(base_url + "/v1/logs", body, ctype, auth="bearer")
    print(f"[max-body] {status}")
    require(failures, status == 413, f"max_body_bytes expected 413, got {status}: {text}")
    stopped = stop_server(con, port)
    require(failures, stopped.startswith("Stopped"), f"max body stop failed: {stopped}")
    con.close()


def scenario_backpressure(failures, port):
    con, catalog, table_prefix = connect("backpressure")
    body = repeated_jsonl(load_payload(LOG_PAYLOAD), 64)
    ctype = content_type_for(LOG_PAYLOAD)
    reservation = max(len(body), 1024)
    base_url = start_server(
        con,
        port,
        catalog=catalog,
        max_body_bytes=len(body) + 1024,
        max_buffered_bytes=reservation,
        seal_target_bytes=reservation,
        seal_max_age_ms=60000,
    )
    url = base_url + "/v1/logs"
    print(f"[backpressure] {base_url} reservation={reservation} concurrency={CONCURRENCY}")
    statuses = []
    accepted_rows = 0
    with ThreadPoolExecutor(max_workers=min(CONCURRENCY, 64)) as pool:
        futures = [pool.submit(post, url, body, ctype, "bearer", "", 20) for _ in range(CONCURRENCY)]
        for fut in as_completed(futures):
            status, text = fut.result()
            statuses.append(status)
            accepted_rows += rows_from_response(status, text)

    accepted = sum(1 for s in statuses if s == 202)
    rejected = sum(1 for s in statuses if s == 503)
    print(f"  statuses={sorted(set(statuses))} accepted={accepted} rejected_503={rejected}")
    require(failures, accepted > 0, "backpressure accepted no requests")
    require(
        failures,
        rejected > 0,
        f"backpressure expected some 503s, got statuses {statuses}",
    )

    flush_server(con, port, checkpoint=bool(catalog))
    rows = table_count(con, table_prefix, "otlp_logs")
    require(
        failures,
        rows == accepted_rows,
        f"backpressure table rows {rows} != accepted rows {accepted_rows}",
    )
    stopped = stop_server(con, port)
    require(failures, stopped.startswith("Stopped"), f"backpressure stop failed: {stopped}")
    con.close()


def scenario_metrics_fanout(failures, port):
    con, catalog, table_prefix = connect("metrics_fanout")
    body = load_payload(METRICS_PAYLOAD)
    ctype = content_type_for(METRICS_PAYLOAD)
    expected = {
        table: con.execute(f"SELECT count(*) FROM {func}(?)", [METRICS_PAYLOAD]).fetchone()[0]
        for table, func in METRIC_READ_FUNCTIONS.items()
    }
    base_url = start_server(con, port, catalog=catalog, seal_max_age_ms=60000)
    status, text = post(base_url + "/v1/metrics", body, ctype, auth="bearer")
    print(f"[metrics] {status} expected={expected}")
    require(failures, status == 202, f"metrics fanout expected 202, got {status}: {text}")
    require(
        failures,
        rows_from_response(status, text) == sum(expected.values()),
        f"metrics response rows {rows_from_response(status, text)} != expected {sum(expected.values())}",
    )
    flush_server(con, port, checkpoint=bool(catalog))
    actual = {table: table_count(con, table_prefix, table) for table in expected}
    require(
        failures,
        actual == expected,
        f"metrics table counts {actual} != expected {expected}",
    )
    require(
        failures,
        all(v > 0 for v in actual.values()),
        f"metrics payload did not cover all shapes: {actual}",
    )
    stopped = stop_server(con, port)
    require(failures, stopped.startswith("Stopped"), f"metrics stop failed: {stopped}")
    con.close()


def scenario_stop_under_load(failures, port):
    con, catalog, table_prefix = connect("stop_under_load")
    body = repeated_jsonl(load_payload(LOG_PAYLOAD), 32)
    ctype = content_type_for(LOG_PAYLOAD)
    base_url = start_server(
        con,
        port,
        catalog=catalog,
        max_body_bytes=len(body) + 1024,
        max_buffered_bytes=max(len(body), 1024) * max(CONCURRENCY, 4),
        seal_target_bytes=max(len(body), 1024) * max(CONCURRENCY, 4),
        seal_max_age_ms=60000,
    )
    url = base_url + "/v1/logs"
    print(f"[stop-load] {base_url}")
    warm_status, warm_text = post(url, body, ctype, auth="bearer", timeout=20)
    accepted_rows = rows_from_response(warm_status, warm_text)
    require(
        failures,
        warm_status == 202,
        f"warm request before stop expected 202, got {warm_status}: {warm_text}",
    )

    with ThreadPoolExecutor(max_workers=min(CONCURRENCY, 64)) as pool:
        futures = [pool.submit(post, url, body, ctype, "bearer", "", 20) for _ in range(CONCURRENCY)]
        time.sleep(0.02)
        stopped = stop_server(con, port)
        require(
            failures,
            stopped.startswith("Stopped"),
            f"stop-under-load stop failed: {stopped}",
        )
        for fut in as_completed(futures):
            status, text = fut.result()
            accepted_rows += rows_from_response(status, text)

    rows = table_count(con, table_prefix, "otlp_logs")
    print(f"  accepted_rows={accepted_rows} table_rows={rows}")
    require(
        failures,
        rows == accepted_rows,
        f"stop-under-load table rows {rows} != accepted rows {accepted_rows}",
    )
    con.close()


def scenario_flush_stop_race(failures, port):
    con, catalog, table_prefix = connect("flush_stop_race")
    body = repeated_jsonl(load_payload(LOG_PAYLOAD), 16)
    ctype = content_type_for(LOG_PAYLOAD)
    base_url = start_server(
        con,
        port,
        catalog=catalog,
        max_body_bytes=len(body) + 1024,
        seal_max_age_ms=60000,
    )
    url = base_url + "/v1/logs"
    accepted_rows = 0
    for _ in range(4):
        status, text = post(url, body, ctype, auth="bearer", timeout=20)
        require(
            failures,
            status == 202,
            f"flush/stop setup POST expected 202, got {status}: {text}",
        )
        accepted_rows += rows_from_response(status, text)

    results = {}

    def run_flush():
        try:
            cur = con.cursor()
            results["flush"] = flush_server(cur, port, checkpoint=bool(catalog))
        except Exception as exc:
            results["flush_error"] = str(exc)

    def run_stop():
        try:
            cur = con.cursor()
            results["stop"] = stop_server(cur, port)
        except Exception as exc:
            results["stop_error"] = str(exc)

    print(f"[flush-stop] {base_url}")
    flush_thread = threading.Thread(target=run_flush, daemon=True)
    stop_thread = threading.Thread(target=run_stop, daemon=True)
    flush_thread.start()
    time.sleep(0.01)
    stop_thread.start()
    flush_thread.join(20)
    stop_thread.join(20)
    require(failures, not flush_thread.is_alive(), "otlp_flush thread did not finish")
    require(failures, not stop_thread.is_alive(), "otlp_stop thread did not finish")
    require(
        failures,
        "flush_error" not in results,
        f"otlp_flush raised: {results.get('flush_error')}",
    )
    require(
        failures,
        "stop_error" not in results,
        f"otlp_stop raised: {results.get('stop_error')}",
    )
    require(
        failures,
        "stop" in results and str(results["stop"]).startswith("Stopped"),
        f"concurrent stop did not stop server: {results}",
    )

    rows = table_count(con, table_prefix, "otlp_logs")
    print(f"  results={results} accepted_rows={accepted_rows} table_rows={rows}")
    require(
        failures,
        rows == accepted_rows,
        f"flush/stop table rows {rows} != accepted rows {accepted_rows}",
    )
    con.close()


def scenario_disk_clean_flush(failures, port):
    disk_root = tempfile.mkdtemp(prefix="otlp-disk-buffer-", dir=DISK_ROOT or None)
    try:
        con, catalog, table_prefix = connect("disk_clean_flush")
        body = load_payload(LOG_PAYLOAD)
        ctype = content_type_for(LOG_PAYLOAD)
        base_url = start_server(
            con,
            port,
            catalog=catalog,
            buffer="disk",
            disk_buffer_dir=disk_root,
            seal_max_age_ms=60000,
        )
        status, text = post(base_url + "/v1/logs", body, ctype, auth="bearer", timeout=20)
        accepted_rows = rows_from_response(status, text)
        print(f"[disk-clean] {status} dir={disk_root}")
        require(failures, status == 202, f"disk clean POST expected 202, got {status}: {text}")
        snap = con.execute(
            "SELECT buffer_mode, disk_pending_records, disk_buffered_bytes, disk_healthy "
            "FROM otlp_server_list() WHERE listen_uri = ?",
            [f"otlp:127.0.0.1:{port}"],
        ).fetchone()
        require(failures, snap is not None and snap[0] == "disk", f"disk clean wrong buffer snapshot: {snap}")
        require(failures, snap is not None and snap[1] >= 1, f"disk clean expected pending journal record: {snap}")
        require(failures, snap is not None and snap[2] > 0, f"disk clean expected pending bytes: {snap}")
        require(failures, snap is not None and snap[3], f"disk clean expected healthy disk buffer: {snap}")

        flush_server(con, port, checkpoint=bool(catalog))
        rows = table_count(con, table_prefix, "otlp_logs")
        require(failures, rows == accepted_rows, f"disk clean table rows {rows} != accepted rows {accepted_rows}")
        snap = con.execute(
            "SELECT disk_pending_records, disk_buffered_bytes, oldest_unsealed_seq "
            "FROM otlp_server_list() WHERE listen_uri = ?",
            [f"otlp:127.0.0.1:{port}"],
        ).fetchone()
        require(failures, snap is not None and snap[0] == 0, f"disk clean expected no pending records: {snap}")
        require(failures, snap is not None and snap[1] == 0, f"disk clean expected no pending bytes: {snap}")
        require(failures, snap is not None and snap[2] is None, f"disk clean expected no oldest seq: {snap}")
        stopped = stop_server(con, port)
        require(failures, stopped.startswith("Stopped"), f"disk clean stop failed: {stopped}")
        con.close()
    finally:
        shutil.rmtree(disk_root, ignore_errors=True)


def scenario_disk_full(failures, port):
    disk_root = tempfile.mkdtemp(prefix="otlp-disk-full-", dir=DISK_ROOT or None)
    try:
        con, catalog, table_prefix = connect("disk_full")
        body = load_payload(LOG_PAYLOAD)
        ctype = content_type_for(LOG_PAYLOAD)
        base_url = start_server(
            con,
            port,
            catalog=catalog,
            buffer="disk",
            disk_buffer_dir=disk_root,
            max_body_bytes=len(body) + 1024,
            disk_segment_bytes=4096,
            disk_max_bytes=4096,
            seal_target_bytes=1 << 30,
            seal_max_age_ms=60000,
        )
        url = base_url + "/v1/logs"
        statuses = []
        accepted_rows = 0
        for _ in range(16):
            status, text = post(url, body, ctype, auth="bearer", timeout=20)
            statuses.append(status)
            accepted_rows += rows_from_response(status, text)
        print(f"[disk-full] statuses={sorted(set(statuses))} dir={disk_root}")
        require(failures, 202 in statuses, f"disk full expected at least one 202, got {statuses}")
        require(failures, 507 in statuses or 503 in statuses, f"disk full expected 507/503, got {statuses}")
        flush_server(con, port, checkpoint=bool(catalog))
        rows = table_count(con, table_prefix, "otlp_logs")
        require(failures, rows == accepted_rows, f"disk full table rows {rows} != accepted rows {accepted_rows}")
        stopped = stop_server(con, port)
        require(failures, stopped.startswith("Stopped"), f"disk full stop failed: {stopped}")
        con.close()
    finally:
        shutil.rmtree(disk_root, ignore_errors=True)


# ---------------------------------------------------------------------------
# Crash-recovery helpers
#
# The scenarios above run the OTLP HTTP server in-process (the extension spins
# the listener up on a background thread inside this Python process). To test
# durability across a *hard* crash we instead launch the server in a separate
# OS process that we can SIGKILL. The child process:
#   * loads the extension + ducklake,
#   * ATTACHes a persistent DuckLake catalog living in a fixed directory,
#   * calls otlp_serve(buffer:='disk', disk_buffer_dir:=<fixed dir>, ...),
#   * idles until killed (no graceful otlp_stop on SIGKILL).
# On restart we point a fresh child at the SAME disk_buffer_dir + SAME catalog;
# the server replays un-checkpointed journal records from disk on startup.
# ---------------------------------------------------------------------------

# Source of the child server process. Reads its configuration from OTLP_CHILD_*
# environment variables, starts the disk-buffered server, then idles so the
# parent can POST to it. Control plane via OS signals (the parent owns no SQL
# connection to the child's server, so otlp_flush/otlp_stop must run in-child):
#   * SIGKILL  -> hard crash (no graceful shutdown; what we want to test).
#   * SIGTERM  -> graceful commit: otlp_flush(checkpoint:=true) + otlp_stop,
#                 then exit 0. Used on the *restarted* child to durably commit
#                 replayed records before the parent counts catalog rows.
#   * SIGUSR1  -> in-flight control flush: otlp_flush(checkpoint:=false) -- a
#                 seal+COMMIT to the catalog that does NOT reclaim the journal.
#                 The child keeps serving. Completion is signalled by touching
#                 the file named in OTLP_CHILD_FLUSH_SENTINEL (the parent polls
#                 for it, since the child's stdout is drained in the background).
# stdout line "READY <url>" marks the listener as accepting connections.
_CHILD_SERVER_SOURCE = r"""
import os, sys, json, time, signal, threading
import duckdb

ext = os.environ["OTLP_CHILD_EXTENSION"]
catalog = os.environ["OTLP_CHILD_CATALOG"]
catalog_dir = os.environ["OTLP_CHILD_CATALOG_DIR"]
disk_dir = os.environ["OTLP_CHILD_DISK_DIR"]
port = int(os.environ["OTLP_CHILD_PORT"])
token = os.environ["OTLP_CHILD_TOKEN"]
options = json.loads(os.environ.get("OTLP_CHILD_OPTIONS", "{}"))
flush_sentinel = os.environ.get("OTLP_CHILD_FLUSH_SENTINEL", "")
listen_uri = f"otlp:127.0.0.1:{port}"


def qident(value):
    return '"' + value.replace('"', '""') + '"'


con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
con.execute(f"LOAD '{ext}'")
con.execute("LOAD ducklake")
os.makedirs(catalog_dir, exist_ok=True)
con.execute(
    f"ATTACH 'ducklake:{catalog_dir}/meta.ducklake' AS {qident(catalog)} "
    f"(DATA_PATH '{catalog_dir}/data/')"
)

sql = "SELECT listen_url FROM otlp_serve(?, token := ?, catalog := ?, "
sql += "buffer := 'disk', disk_buffer_dir := ?"
params = [listen_uri, token, catalog, disk_dir]
for name, value in options.items():
    sql += f", {name} := ?"
    params.append(value)
sql += ")"
listen_url = con.execute(sql, params).fetchone()[0]

sys.stdout.write("READY " + listen_url + "\n")
sys.stdout.flush()

_done = threading.Event()


def _graceful(signum, frame):
    try:
        con.execute("SELECT status FROM otlp_flush(?, checkpoint := ?)", [listen_uri, True]).fetchone()
        con.execute("SELECT status FROM otlp_stop(?)", [listen_uri]).fetchone()
        sys.stdout.write("COMMITTED\n")
        sys.stdout.flush()
    except Exception as exc:  # pragma: no cover - surfaced to parent via stderr
        sys.stderr.write("COMMIT_ERROR " + str(exc) + "\n")
        sys.stderr.flush()
    finally:
        _done.set()


def _control_flush(signum, frame):
    # Commit buffered rows to the catalog WITHOUT a checkpoint (journal stays
    # un-reclaimed), then keep serving. Touch the sentinel so the parent knows.
    try:
        con.execute("SELECT status FROM otlp_flush(?, checkpoint := ?)", [listen_uri, False]).fetchone()
        ok = True
    except Exception as exc:  # pragma: no cover - surfaced via stderr
        sys.stderr.write("CONTROL_FLUSH_ERROR " + str(exc) + "\n")
        sys.stderr.flush()
        ok = False
    if flush_sentinel:
        try:
            with open(flush_sentinel, "w") as fh:
                fh.write("ok" if ok else "err")
        except Exception:
            pass


signal.signal(signal.SIGTERM, _graceful)
try:
    signal.signal(signal.SIGUSR1, _control_flush)
except (AttributeError, ValueError):  # SIGUSR1 absent (e.g. Windows)
    pass

# Idle until SIGTERM asks for a graceful commit (or SIGKILL ends us abruptly).
while not _done.wait(timeout=3600):
    pass
"""


def spawn_server_process(port, catalog, catalog_dir, disk_dir, options=None, ready_timeout=30, flush_sentinel=""):
    """Launch the disk-buffered OTLP server in a separately killable OS process.

    Returns (proc, listen_url). The caller MUST eventually kill/terminate proc.
    Pass flush_sentinel (a writable path) to enable the SIGUSR1 control-flush
    handshake (the child touches that file when the commit-without-checkpoint
    completes).
    """
    env = dict(os.environ)
    env["OTLP_CHILD_EXTENSION"] = EXTENSION
    env["OTLP_CHILD_CATALOG"] = catalog
    env["OTLP_CHILD_CATALOG_DIR"] = catalog_dir
    env["OTLP_CHILD_DISK_DIR"] = disk_dir
    env["OTLP_CHILD_PORT"] = str(port)
    env["OTLP_CHILD_TOKEN"] = TOKEN
    env["OTLP_CHILD_OPTIONS"] = json.dumps(options or {})
    env["OTLP_CHILD_FLUSH_SENTINEL"] = flush_sentinel
    proc = subprocess.Popen(
        [sys.executable, "-c", _CHILD_SERVER_SOURCE],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    listen_url = None
    deadline = time.time() + ready_timeout
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                break
            continue
        line = line.strip()
        if line.startswith("READY "):
            listen_url = line.split(" ", 1)[1]
            break
    if listen_url is None:
        rest = ""
        try:
            rest = proc.stdout.read()
        except Exception:
            pass
        try:
            proc.kill()
        except Exception:
            pass
        raise SystemExit(f"child server failed to start on port {port}: rc={proc.poll()} output={rest}")
    # Drain remaining stdout in the background so the child never blocks on a
    # full pipe while it idles waiting to be killed.
    threading.Thread(target=lambda: proc.stdout.read(), daemon=True).start()
    return proc, listen_url


def sigkill_process(proc):
    """Hard-kill the child (SIGKILL / kill -9). No graceful shutdown."""
    if proc.poll() is not None:
        return
    try:
        proc.send_signal(signal.SIGKILL)
    except Exception:
        try:
            os.kill(proc.pid, signal.SIGKILL)
        except Exception:
            pass
    try:
        proc.wait(timeout=10)
    except Exception:
        pass


def control_flush_child(proc, sentinel_path, settle=0.5, timeout=60):
    """SIGUSR1 the running child to commit buffered rows WITHOUT a checkpoint.

    The child touches sentinel_path on completion. Returns True if the flush
    completed successfully (sentinel content "ok"). `settle` adds a small wait
    afterwards so the COMMIT is observably durable before the caller crashes.
    """
    if not hasattr(signal, "SIGUSR1"):
        return False
    if proc.poll() is not None:
        return False
    try:
        if os.path.exists(sentinel_path):
            os.remove(sentinel_path)
    except OSError:
        pass
    try:
        proc.send_signal(signal.SIGUSR1)
    except Exception:
        return False
    deadline = time.time() + timeout
    while time.time() < deadline:
        if proc.poll() is not None:
            return False
        if os.path.exists(sentinel_path):
            try:
                with open(sentinel_path) as fh:
                    ok = fh.read().strip() == "ok"
            except OSError:
                ok = False
            if settle:
                time.sleep(settle)
            return ok
        time.sleep(0.02)
    return False


def graceful_stop_child(proc, timeout=60):
    """SIGTERM the child so it runs otlp_flush(checkpoint) + otlp_stop in-process.

    Returns True if the child exited cleanly (durably committed replayed records).
    """
    if proc.poll() is not None:
        return proc.returncode == 0
    try:
        proc.send_signal(signal.SIGTERM)
    except Exception:
        return False
    try:
        return proc.wait(timeout=timeout) == 0
    except Exception:
        return False


def open_catalog(catalog, catalog_dir):
    """Re-open an existing persistent DuckLake catalog in-process for counting."""
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    con.execute("LOAD ducklake")
    con.execute(
        f"ATTACH 'ducklake:{catalog_dir}/meta.ducklake' AS {quote_ident(catalog)} " f"(DATA_PATH '{catalog_dir}/data/')"
    )
    return con, f"{quote_ident(catalog)}.main."


def catalog_table_count(catalog, catalog_dir, table_name):
    """Row count for a table in a persistent catalog (0 if the table is absent)."""
    con, table_prefix = open_catalog(catalog, catalog_dir)
    try:
        try:
            return con.execute(f"SELECT count(*) FROM {table_prefix}{table_name}").fetchone()[0]
        except duckdb.Error:
            # Table never created (e.g. nothing ever committed) -> zero rows.
            return 0
    finally:
        con.close()


# scenario_disk_crash_before_seal: core at-least-once durability. Records are
# fsynced to the disk journal before the 202, but a huge seal window means
# nothing is ever committed to the catalog before the crash. After SIGKILL +
# restart on the same dir/catalog, replay + flush MUST land exactly N rows
# (no loss, no duplicates -- nothing was committed pre-crash).
def scenario_disk_crash_before_seal(failures, port):
    disk_dir = tempfile.mkdtemp(prefix="otlp-crash-preseal-disk-", dir=DISK_ROOT or None)
    catalog_dir = tempfile.mkdtemp(prefix="otlp-crash-preseal-lake-", dir=DISK_ROOT or None)
    catalog = "lake_crash_preseal"
    body = load_payload(LOG_PAYLOAD)
    ctype = content_type_for(LOG_PAYLOAD)
    n = 24
    # Big seal window + huge target so nothing commits to the catalog pre-crash.
    options = {"seal_max_age_ms": 600000, "seal_target_bytes": 1 << 34}
    proc = proc2 = None
    try:
        proc, listen_url = spawn_server_process(port, catalog, catalog_dir, disk_dir, options)
        url = listen_url + "/v1/logs"
        print(f"[disk-crash-preseal] {listen_url} dir={disk_dir} catalog={catalog_dir}")

        accepted = 0
        accepted_rows = 0
        for _ in range(n):
            status, text = post(url, body, ctype, auth="bearer", timeout=20)
            require(failures, status == 202, f"preseal POST expected 202 (durable on disk), got {status}: {text}")
            if status == 202:
                accepted += 1
                accepted_rows += rows_from_response(status, text)
        print(f"  posted accepted={accepted}/{n} accepted_rows={accepted_rows}")

        # Hard crash: no graceful otlp_stop. Nothing has sealed to the catalog,
        # but every 202 is fsynced to the disk journal.
        sigkill_process(proc)
        print("  SIGKILL delivered (pre-seal)")

        # Restart on the SAME disk dir + catalog -> server replays the journal.
        proc2, listen_url2 = spawn_server_process(port, catalog, catalog_dir, disk_dir, options)
        # Graceful SIGTERM -> in-child otlp_flush(checkpoint) + otlp_stop commits
        # the replayed records durably to the catalog.
        committed = graceful_stop_child(proc2)
        require(failures, committed, "crash-before-seal: restarted child did not commit cleanly on SIGTERM")
        print("  restart replayed + committed")

        rows = catalog_table_count(catalog, catalog_dir, "otlp_logs")
        print(f"  post-recovery table_rows={rows} expected={accepted_rows}")
        require(
            failures,
            rows == accepted_rows,
            f"crash-before-seal lost rows: catalog has {rows}, expected {accepted_rows} (at-least-once)",
        )
    finally:
        if proc is not None:
            sigkill_process(proc)
        if proc2 is not None:
            sigkill_process(proc2)
        shutil.rmtree(disk_dir, ignore_errors=True)
        shutil.rmtree(catalog_dir, ignore_errors=True)


# scenario_disk_crash_after_commit: at-least-once duplicate window. We force a
# seal+COMMIT of the buffered records to the catalog, then SIGKILL before the
# journal checkpoint (which marks those records as reclaimable) is guaranteed
# durable. On restart the server cannot tell those records were already
# committed and replays them again. Contract: NO LOSS (row count >= N);
# duplicates are EXPECTED and acceptable here, so we must NOT fail on them.
def scenario_disk_crash_after_commit(failures, port):
    disk_dir = tempfile.mkdtemp(prefix="otlp-crash-postcommit-disk-", dir=DISK_ROOT or None)
    catalog_dir = tempfile.mkdtemp(prefix="otlp-crash-postcommit-lake-", dir=DISK_ROOT or None)
    catalog = "lake_crash_postcommit"
    body = load_payload(LOG_PAYLOAD)
    ctype = content_type_for(LOG_PAYLOAD)
    n = 24
    # Big seal window so seals happen only when we explicitly ask (via the
    # control flush below), keeping the commit/checkpoint ordering observable.
    options = {"seal_max_age_ms": 600000, "seal_target_bytes": 1 << 34}
    sentinel = os.path.join(catalog_dir, "control_flush.sentinel")
    proc = proc2 = None
    try:
        proc, listen_url = spawn_server_process(port, catalog, catalog_dir, disk_dir, options, flush_sentinel=sentinel)
        url = listen_url + "/v1/logs"
        print(f"[disk-crash-postcommit] {listen_url} dir={disk_dir} catalog={catalog_dir}")

        accepted = 0
        accepted_rows = 0
        for _ in range(n):
            status, text = post(url, body, ctype, auth="bearer", timeout=20)
            require(failures, status == 202, f"postcommit POST expected 202, got {status}: {text}")
            if status == 202:
                accepted += 1
                accepted_rows += rows_from_response(status, text)
        print(f"  posted accepted={accepted}/{n} accepted_rows={accepted_rows}")

        # Drive a control flush *inside* the running child: seal+COMMIT WITHOUT a
        # checkpoint so rows land in the catalog but the journal stays
        # un-reclaimed. The child handles SIGUSR1 as a no-checkpoint flush.
        committed_to_catalog = control_flush_child(proc, sentinel, settle=0.5)
        require(
            failures,
            committed_to_catalog,
            "postcommit: in-child control flush (commit-without-checkpoint) did not succeed",
        )

        # Crash before the checkpoint fsync is guaranteed durable.
        sigkill_process(proc)
        print("  SIGKILL delivered (post-commit, pre-checkpoint)")

        # Restart -> server replays the journal records again (they were
        # committed but not marked reclaimed), then we commit them.
        proc2, listen_url2 = spawn_server_process(port, catalog, catalog_dir, disk_dir, options)
        committed = graceful_stop_child(proc2)
        require(failures, committed, "crash-after-commit: restarted child did not commit cleanly on SIGTERM")
        print("  restart replayed + committed")

        rows = catalog_table_count(catalog, catalog_dir, "otlp_logs")
        print(f"  post-recovery table_rows={rows} accepted_rows={accepted_rows} (duplicates OK)")
        require(
            failures,
            rows >= accepted_rows,
            f"crash-after-commit lost rows: catalog has {rows}, expected >= {accepted_rows} (at-least-once)",
        )
    finally:
        if proc is not None:
            sigkill_process(proc)
        if proc2 is not None:
            sigkill_process(proc2)
        shutil.rmtree(disk_dir, ignore_errors=True)
        shutil.rmtree(catalog_dir, ignore_errors=True)


# scenario_disk_concurrent_same_kind_crash: watermark-bug regression. Many
# threads hammer the SAME signal kind (all logs) so buffer appends interleave
# out of order while seals fire mid-ingest (small seal_target_bytes). A SIGKILL
# at a short random interval crashes the server mid-flight. On restart, every
# acknowledged (202) record must be durable -- catalog row count == accepted
# count. A watermark that reclaimed records by max-sequence rather than the
# exact sealed set would silently drop un-sealed-but-lower-sequence records;
# this asserts that does not happen.
def scenario_disk_concurrent_same_kind_crash(failures, port):
    disk_dir = tempfile.mkdtemp(prefix="otlp-crash-concurrent-disk-", dir=DISK_ROOT or None)
    catalog_dir = tempfile.mkdtemp(prefix="otlp-crash-concurrent-lake-", dir=DISK_ROOT or None)
    catalog = "lake_crash_concurrent"
    body = load_payload(LOG_PAYLOAD)
    ctype = content_type_for(LOG_PAYLOAD)
    threads = max(CONCURRENCY, 8)
    total_requests = max(threads * 32, 256)
    # Small seal trigger so seals interleave with concurrent ingest.
    seal_bytes = max(len(body) * 4, 4096)
    options = {"seal_max_age_ms": 50, "seal_target_bytes": seal_bytes}
    proc = proc2 = None
    try:
        proc, listen_url = spawn_server_process(port, catalog, catalog_dir, disk_dir, options)
        url = listen_url + "/v1/logs"
        print(
            f"[disk-crash-concurrent] {listen_url} threads={threads} "
            f"requests={total_requests} seal_bytes={seal_bytes}"
        )

        accepted = 0
        accepted_rows = 0
        statuses = []
        stop_flag = threading.Event()
        lock = threading.Lock()

        def worker(count):
            nonlocal accepted, accepted_rows
            for _ in range(count):
                if stop_flag.is_set():
                    return
                status, text = post(url, body, ctype, auth="bearer", timeout=20)
                with lock:
                    statuses.append(status)
                    if status == 202:
                        accepted += 1
                        accepted_rows += rows_from_response(status, text)

        # Crash at a short random-ish interval mid-ingest.
        crash_delay = 0.05 + (port % 7) * 0.01

        per_thread = (total_requests + threads - 1) // threads

        def crasher():
            time.sleep(crash_delay)
            stop_flag.set()
            sigkill_process(proc)

        crash_thread = threading.Thread(target=crasher, daemon=True)
        with ThreadPoolExecutor(max_workers=threads) as pool:
            crash_thread.start()
            futures = [pool.submit(worker, per_thread) for _ in range(threads)]
            for fut in as_completed(futures):
                fut.result()
        crash_thread.join(10)

        with lock:
            accepted_snapshot = accepted
            accepted_rows_snapshot = accepted_rows
            status_set = sorted(set(statuses))
        print(
            f"  SIGKILL after {crash_delay:.2f}s statuses={status_set} "
            f"accepted={accepted_snapshot} accepted_rows={accepted_rows_snapshot}"
        )
        require(
            failures,
            accepted_snapshot > 0,
            "concurrent-crash accepted no requests before the crash (tune timing/payload)",
        )

        # Restart on the same dir/catalog, replay, and commit durably.
        proc2, listen_url2 = spawn_server_process(port, catalog, catalog_dir, disk_dir, options)
        committed = graceful_stop_child(proc2)
        require(failures, committed, "concurrent-crash: restarted child did not commit cleanly on SIGTERM")

        rows = catalog_table_count(catalog, catalog_dir, "otlp_logs")
        print(f"  post-recovery table_rows={rows} accepted_rows={accepted_rows_snapshot}")
        require(
            failures,
            rows == accepted_rows_snapshot,
            f"concurrent-crash dropped acknowledged rows: catalog has {rows}, "
            f"expected {accepted_rows_snapshot} (every 202 must be durable; watermark bug?)",
        )
    finally:
        if proc is not None:
            sigkill_process(proc)
        if proc2 is not None:
            sigkill_process(proc2)
        shutil.rmtree(disk_dir, ignore_errors=True)
        shutil.rmtree(catalog_dir, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--disk", action="store_true", help="also run durable disk-buffer scenarios")
    args = parser.parse_args()

    if not os.path.exists(EXTENSION):
        raise SystemExit(f"extension not found: {EXTENSION} (build it first: GEN=ninja make)")
    for path in (LOG_PAYLOAD, METRICS_PAYLOAD):
        if not os.path.exists(path):
            raise SystemExit(f"payload not found: {path}")

    failures = []
    scenarios = [
        scenario_auth_and_validation,
        scenario_max_body,
        scenario_backpressure,
        scenario_metrics_fanout,
        scenario_stop_under_load,
        scenario_flush_stop_race,
    ]
    if args.disk:
        scenarios.extend(
            [
                scenario_disk_clean_flush,
                scenario_disk_full,
                scenario_disk_crash_before_seal,
                scenario_disk_crash_after_commit,
                scenario_disk_concurrent_same_kind_crash,
            ]
        )
    for offset, scenario in enumerate(scenarios):
        scenario(failures, BASE_PORT + offset)

    if failures:
        print("FAIL:\n  - " + "\n  - ".join(failures))
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
