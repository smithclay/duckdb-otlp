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

import json
import os
import sys
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


def flush_server(con, port):
    return con.execute(
        "SELECT status, sealed_rows, seals_total, error FROM otlp_flush(?)",
        [f"otlp:127.0.0.1:{port}"],
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
    base_url = start_server(con, port, catalog=catalog)
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

    flush_server(con, port)
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

    flush_server(con, port)
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
    base_url = start_server(con, port, catalog=catalog)
    status, text = post(base_url + "/v1/metrics", body, ctype, auth="bearer")
    print(f"[metrics] {status} expected={expected}")
    require(failures, status == 202, f"metrics fanout expected 202, got {status}: {text}")
    require(
        failures,
        rows_from_response(status, text) == sum(expected.values()),
        f"metrics response rows {rows_from_response(status, text)} != expected {sum(expected.values())}",
    )
    flush_server(con, port)
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
            results["flush"] = flush_server(cur, port)
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


def main():
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
    for offset, scenario in enumerate(scenarios):
        scenario(failures, BASE_PORT + offset)

    if failures:
        print("FAIL:\n  - " + "\n  - ".join(failures))
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
