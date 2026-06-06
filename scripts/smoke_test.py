#!/usr/bin/env python3
"""Minimal end-to-end smoke for the published duckdb-otlp daemon image.

Boots the distroless runtime image in local-ducklake mode, POSTs a fixed
OTLP/JSON log batch over HTTP, flushes via Quack, and asserts the committed
row count matches what was sent. This exercises the real published artifact
end to end: distroless boot -> OTLP/HTTP ingest -> buffered seal -> DuckLake
commit -> Quack read.

The image is distroless (no shell/duckdb inside), so the Quack queries run
from a host `duckdb` CLI against the published Quack port. Requires `docker`
and `duckdb` on PATH.
"""

from __future__ import annotations

import argparse
import http.client
import json
import platform
import secrets
import socket
import subprocess
import sys
import time

LOG_RECORDS = 5
CATALOG = "lake"
SCHEMA = "otlp"
SERVE_URI = "otlp:0.0.0.0:4318"


def eprint(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def host_platform() -> str:
    machine = platform.machine().lower()
    if machine in {"arm64", "aarch64"}:
        return "linux/arm64"
    if machine in {"x86_64", "amd64"}:
        return "linux/amd64"
    raise SystemExit(f"unsupported host architecture: {machine}")


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def otlp_logs_payload(count: int) -> bytes:
    base_ts = 1_700_000_000_000_000_000
    records = [
        {
            "timeUnixNano": str(base_ts + i),
            "observedTimeUnixNano": str(base_ts + i),
            "severityNumber": 9,
            "severityText": "INFO",
            "body": {"stringValue": f"smoke record {i}"},
            "attributes": [{"key": "smoke.sequence", "value": {"intValue": str(i)}}],
        }
        for i in range(count)
    ]
    payload = {
        "resourceLogs": [
            {
                "resource": {"attributes": [{"key": "service.name", "value": {"stringValue": "duckdb-otlp-smoke"}}]},
                "scopeLogs": [{"scope": {"name": "duckdb-otlp-smoke"}, "logRecords": records}],
            }
        ]
    }
    return json.dumps(payload).encode()


def run(args: list[str], *, check: bool = True, timeout: float | None = None) -> subprocess.CompletedProcess:
    proc = subprocess.run(args, text=True, capture_output=True, timeout=timeout)
    if check and proc.returncode != 0:
        raise SystemExit(
            f"command failed ({proc.returncode}): {' '.join(args)}\n" f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
        )
    return proc


def wait_for_health(port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            conn.request("GET", "/healthz")
            resp = conn.getresponse()
            body = resp.read()
            conn.close()
            if resp.status == 200 and b"ok" in body:
                return
        except OSError as exc:
            last_error = str(exc)
        time.sleep(1)
    raise SystemExit(f"server did not become healthy on port {port}: {last_error}")


def post_logs(port: int, token: str, body: bytes) -> tuple[int, str]:
    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
    conn.request(
        "POST",
        "/v1/logs",
        body=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    resp = conn.getresponse()
    text = resp.read().decode(errors="replace")
    conn.close()
    return resp.status, text


def quack_rows(quack_port: int, token: str, sql: str) -> list[dict]:
    """Run a single SQL statement on the daemon's connection over Quack from a host duckdb."""
    remote = sql_quote(f"quack:127.0.0.1:{quack_port}")
    control = (
        "INSTALL quack;\nLOAD quack;\n"
        f"SELECT * FROM quack_query({remote}, {sql_quote(sql)}, token = {sql_quote(token)});\n"
    )
    proc = subprocess.run(
        ["duckdb", "-unsigned", "-json", ":memory:"],
        input=control,
        text=True,
        capture_output=True,
        timeout=60,
    )
    if proc.returncode != 0:
        raise SystemExit(f"quack query failed: {sql}\nstdout:\n{proc.stdout}\nstderr:\n{proc.stderr}")
    raw = proc.stdout.strip()
    if not raw:
        return []
    payload = json.loads(raw)
    return payload if isinstance(payload, list) else [payload]


def container_logs(container: str) -> str:
    proc = subprocess.run(["docker", "logs", "--tail", "200", container], text=True, capture_output=True)
    return (proc.stdout + proc.stderr).strip()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--platform", default=host_platform())
    parser.add_argument("--startup-timeout", type=float, default=120)
    parser.add_argument("--keep", action="store_true", help="Leave the container running for debugging.")
    args = parser.parse_args()

    for program in ("docker", "duckdb"):
        if subprocess.run(["which", program], capture_output=True).returncode != 0:
            raise SystemExit(f"required executable not found on PATH: {program}")

    token = "smoke-" + secrets.token_hex(8)
    quack_token = token + "-quack"
    container = "duckdb-otlp-smoke-" + secrets.token_hex(4)
    otlp_port = free_port()
    quack_port = free_port()
    env = {
        "DUCKDB_MODE": "local-ducklake",
        "DUCKLAKE_NAME": CATALOG,
        "DUCKLAKE_CATALOG_PATH": "/tmp/duckdb-otlp-smoke.ducklake",
        "DUCKLAKE_DATA_PATH": "/tmp/duckdb-otlp-smoke-files/",
        "DUCKDB_CATALOG": CATALOG,
        "DUCKDB_SCHEMA": SCHEMA,
        "DUCKDB_DATABASE": "/tmp/duckdb-otlp-smoke-control.duckdb",
        "DUCKDB_OTLP_TOKEN": token,
        "DUCKDB_QUACK_ENABLED": "1",
        "DUCKDB_QUACK_TOKEN": quack_token,
        "OTEL_HTTP_ADDR": "0.0.0.0:4318",
    }

    docker_args = [
        "docker",
        "run",
        "-d",
        "--name",
        container,
        "--platform",
        args.platform,
        "-p",
        f"127.0.0.1:{otlp_port}:4318",
        "-p",
        f"127.0.0.1:{quack_port}:9494",
    ]
    for key, value in sorted(env.items()):
        docker_args += ["-e", f"{key}={value}"]
    docker_args.append(args.image)

    subprocess.run(["docker", "rm", "-f", container], capture_output=True)
    eprint(f"[smoke] starting {args.image} ({args.platform})")
    run(docker_args)
    try:
        wait_for_health(otlp_port, args.startup_timeout)
        eprint(f"[smoke] posting {LOG_RECORDS} OTLP/JSON log records")
        status, body = post_logs(otlp_port, token, otlp_logs_payload(LOG_RECORDS))
        if status != 202:
            raise SystemExit(f"ingest returned {status}, expected 202: {body}")
        accepted = int(json.loads(body).get("rows") or 0)
        if accepted != LOG_RECORDS:
            raise SystemExit(f"ingest buffered {accepted} rows, expected {LOG_RECORDS}: {body}")

        eprint("[smoke] flushing (forces a synchronous seal)")
        flush = quack_rows(quack_port, quack_token, f"SELECT * FROM otlp_flush('{SERVE_URI}')")
        if any(row.get("status") == "error" or row.get("error") for row in flush):
            raise SystemExit(f"flush failed: {json.dumps(flush)}")

        eprint("[smoke] verifying committed row count")
        rows = quack_rows(quack_port, quack_token, f"SELECT count(*) AS n FROM {CATALOG}.{SCHEMA}.otlp_logs")
        committed = int(rows[0]["n"]) if rows else 0
        if committed != LOG_RECORDS:
            raise SystemExit(f"committed otlp_logs rows ({committed}) did not match sent records ({LOG_RECORDS})")
        eprint(f"[smoke] PASS: {committed} rows ingested and durably committed")
        return 0
    except BaseException as exc:
        eprint(f"[smoke] FAILED: {exc}")
        logs = container_logs(container)
        if logs:
            eprint("--- container logs (tail) ---")
            eprint(logs)
            eprint("--- end container logs ---")
        return 1
    finally:
        if not args.keep:
            subprocess.run(["docker", "rm", "-f", container], capture_output=True, timeout=90)


if __name__ == "__main__":
    raise SystemExit(main())
