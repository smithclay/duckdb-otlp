#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "duckdb==1.5.4",
# ]
# ///
"""Manual HTTP hot-path coverage for attribute promotion.

SQLLogicTest cannot drive HTTP ingest, so the promotion path (extract operator-named
resource/scope attribute keys out of the JSON blob into first-class columns at seal)
is verified here against the default in-memory catalog and, optionally, DuckLake:

  * promote_resource_attributes / promote_scope_attributes create attr columns at startup
  * those columns are populated from the blob at ingest
  * COALESCE(attr_x, json_extract_string(blob,'$."x"')) reconstructs (blob kept)
  * otlp_server_list().promoted_columns_total reflects the count

Run:
    uv run --script test/manual/otlp_serve_promote.py
    OTLP_DUCKLAKE_DIR=/tmp/otlp_promote_lake uv run --script test/manual/otlp_serve_promote.py
"""

import json
import os
import sys
import urllib.error
import urllib.request

import duckdb

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
BASE_PORT = int(os.environ.get("OTLP_PORT", "4347"))
DUCKLAKE_DIR = os.environ.get("OTLP_DUCKLAKE_DIR", "")
TOKEN = "manual-promote-token-0123456789"

# One log record carrying a resource attribute (host.name) and a scope attribute (scope.team).
PAYLOAD = json.dumps(
    {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "svc"}},
                        {"key": "host.name", "value": {"stringValue": "host-1"}},
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {
                            "name": "s",
                            "attributes": [{"key": "scope.team", "value": {"stringValue": "observability"}}],
                        },
                        "logRecords": [
                            {
                                "timeUnixNano": "1700000000000000000",
                                "severityText": "INFO",
                                "body": {"stringValue": "hello"},
                                "attributes": [{"key": "http.route", "value": {"stringValue": "/x"}}],
                            }
                        ],
                    }
                ],
            }
        ]
    }
).encode()


def quote_ident(name):
    return '"' + name.replace('"', '""') + '"'


def connect():
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    catalog = ""
    table_prefix = ""
    if DUCKLAKE_DIR:
        catalog = "lake_promote"
        os.makedirs(DUCKLAKE_DIR, exist_ok=True)
        con.execute("LOAD ducklake")
        con.execute(
            f"ATTACH 'ducklake:{DUCKLAKE_DIR}/meta.ducklake' AS {quote_ident(catalog)} "
            f"(DATA_PATH '{DUCKLAKE_DIR}/data/')"
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


def post(url, body):
    req = urllib.request.Request(
        url,
        data=body,
        method="POST",
        headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode() if exc.fp else ""
    except Exception as exc:
        return 0, str(exc)


def column_names(con, table_prefix, table):
    return [r[0] for r in con.execute(f"SELECT * FROM {table_prefix}{table} LIMIT 0").description]


def require(failures, condition, message):
    if not condition:
        failures.append(message)
    print(("  ok " if condition else "  FAIL ") + message)


def run(failures):
    port = BASE_PORT
    con, catalog, table_prefix = connect()
    base_url = start_server(
        con,
        port,
        catalog=catalog,
        promote_resource_attributes="host.name",
        promote_scope_attributes="scope.team",
    )
    print(f"[promote] {base_url} catalog={catalog or '<default>'}")

    # Columns exist before any data (added at server start).
    cols = column_names(con, table_prefix, "otlp_logs")
    require(failures, "resource_attr_host_name" in cols, f"resource_attr_host_name present at startup (cols={cols})")
    require(failures, "scope_attr_scope_team" in cols, "scope_attr_scope_team present at startup")

    accepted = 0
    for _ in range(3):
        status, text = post(base_url + "/v1/logs", PAYLOAD)
        require(failures, status == 202, f"ingest expected 202, got {status}: {text}")
        if status == 202:
            accepted += int(json.loads(text)["rows"])
    con.execute("SELECT status FROM otlp_flush(?)", [f"otlp:127.0.0.1:{port}"]).fetchone()

    total = con.execute(f"SELECT count(*) FROM {table_prefix}otlp_logs").fetchone()[0]
    require(failures, total == accepted, f"otlp_logs rows {total} == accepted {accepted}")

    res_ok = con.execute(
        f"SELECT count(*) FROM {table_prefix}otlp_logs WHERE resource_attr_host_name = 'host-1'"
    ).fetchone()[0]
    require(failures, res_ok == total, f"resource attr promoted+populated ({res_ok}/{total})")

    scope_ok = con.execute(
        f"SELECT count(*) FROM {table_prefix}otlp_logs WHERE scope_attr_scope_team = 'observability'"
    ).fetchone()[0]
    require(failures, scope_ok == total, f"scope attr promoted+populated ({scope_ok}/{total})")

    mismatches = con.execute(
        f"SELECT count(*) FROM {table_prefix}otlp_logs "
        f"WHERE resource_attr_host_name IS DISTINCT FROM "
        f"json_extract_string(resource_attributes, '$.\"host.name\"')"
    ).fetchone()[0]
    require(failures, mismatches == 0, f"promoted column reconstructs from blob (mismatches={mismatches})")

    promoted_total = con.execute(
        "SELECT promoted_columns_total FROM otlp_server_list() WHERE listen_uri = ?",
        [f"otlp:127.0.0.1:{port}"],
    ).fetchone()[0]
    require(failures, promoted_total == 2, f"otlp_server_list promoted_columns_total={promoted_total} (expected 2)")

    stopped = con.execute("SELECT status FROM otlp_stop(?)", [f"otlp:127.0.0.1:{port}"]).fetchone()[0]
    require(failures, stopped.startswith("Stopped"), f"stop: {stopped}")
    con.close()


def main():
    failures = []
    try:
        run(failures)
    except Exception as exc:  # noqa: BLE001
        failures.append(f"unhandled exception: {exc!r}")
    print()
    if failures:
        print(f"FAILED ({len(failures)}):")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print("PASS")


if __name__ == "__main__":
    main()
