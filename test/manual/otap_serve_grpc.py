#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "duckdb==1.5.3",
#   "grpcio>=1.60",
#   "opentelemetry-proto>=1.20",
# ]
# ///
"""Manual OTLP/gRPC ingest coverage for otap_serve (Phase 1: logs unary).

SQLLogicTest cannot drive a gRPC client, so this lives outside `make test`
(it mirrors test/manual/otlp_serve_concurrency.py, which covers the HTTP
server). The server runs in-process via the duckdb Python module; the gRPC
client talks to it over loopback. It checks:

  * OTLP/gRPC LogsService.Export with a valid Bearer token -> rows land
  * the rows match what was sent (count, service_name, severity)
  * a bad token is rejected with UNAUTHENTICATED
  * an unauthenticated Export buffers nothing
  * a clean otlp_stop drains in-flight rows (dropped_rows == 0)

Run (requires a built loadable extension; `uv` resolves the deps):

    uv run --script test/manual/otap_serve_grpc.py
    OTLP_EXTENSION=build/release/extension/otlp/otlp.duckdb_extension \\
        OTLP_PORT=4327 uv run --script test/manual/otap_serve_grpc.py
"""

import os
import sys

import duckdb
import grpc
from opentelemetry.proto.collector.logs.v1 import (
    logs_service_pb2 as logs_service,
    logs_service_pb2_grpc as logs_service_grpc,
)
from opentelemetry.proto.common.v1 import common_pb2 as common
from opentelemetry.proto.logs.v1 import logs_pb2 as logs
from opentelemetry.proto.resource.v1 import resource_pb2 as resource

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
PORT = int(os.environ.get("OTLP_PORT", "4327"))
TOKEN = "manual-grpc-token-0123456789"
URI = f"otap:localhost:{PORT}"
TARGET = f"localhost:{PORT}"
NUM_RECORDS = 3


def build_request(n):
    records = [
        logs.LogRecord(
            time_unix_nano=1_700_000_000_000_000_000 + i,
            severity_number=9,
            severity_text="INFO",
            body=common.AnyValue(string_value=f"hello grpc {i}"),
        )
        for i in range(n)
    ]
    return logs_service.ExportLogsServiceRequest(
        resource_logs=[
            logs.ResourceLogs(
                resource=resource.Resource(
                    attributes=[
                        common.KeyValue(
                            key="service.name",
                            value=common.AnyValue(string_value="grpc-demo"),
                        )
                    ]
                ),
                scope_logs=[
                    logs.ScopeLogs(
                        scope=common.InstrumentationScope(name="manual-test", version="1.0.0"),
                        log_records=records,
                    )
                ],
            )
        ]
    )


def main():
    failures = []
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    con.execute(f"SELECT * FROM otap_serve('{URI}', token := '{TOKEN}')")
    try:
        channel = grpc.insecure_channel(TARGET)
        # otap_serve binds synchronously so the socket is already listening, but wait
        # for readiness to avoid racing the serve loop spinning up on the runtime.
        grpc.channel_ready_future(channel).result(timeout=10)
        stub = logs_service_grpc.LogsServiceStub(channel)

        # 1) Valid token -> OK, rows buffered.
        stub.Export(
            build_request(NUM_RECORDS),
            metadata=[("authorization", f"Bearer {TOKEN}")],
            timeout=10,
        )

        # 2) Bad token -> UNAUTHENTICATED, nothing buffered.
        try:
            stub.Export(
                build_request(5),
                metadata=[("authorization", "Bearer wrong-token")],
                timeout=10,
            )
            failures.append("bad token was NOT rejected")
        except grpc.RpcError as exc:
            if exc.code() != grpc.StatusCode.UNAUTHENTICATED:
                failures.append(f"bad token -> {exc.code()} (expected UNAUTHENTICATED)")

        # Force a synchronous seal so the buffered rows are queryable.
        con.execute(f"SELECT * FROM otlp_flush('{URI}')")

        count = con.execute("SELECT count(*) FROM otlp_logs").fetchone()[0]
        if count != NUM_RECORDS:
            failures.append(f"otlp_logs has {count} rows, expected {NUM_RECORDS} (rejected request must add 0)")

        services = [r[0] for r in con.execute("SELECT DISTINCT service_name FROM otlp_logs").fetchall()]
        if services != ["grpc-demo"]:
            failures.append(f"service_name = {services}, expected ['grpc-demo']")

        info = con.execute("SELECT count(*) FROM otlp_logs WHERE severity_text = 'INFO'").fetchone()[0]
        if info != NUM_RECORDS:
            failures.append(f"{info} INFO rows, expected {NUM_RECORDS}")
    finally:
        stop = con.execute(f"SELECT * FROM otlp_stop('{URI}')").fetchone()
        dropped = stop[1]
        if dropped != 0:
            failures.append(f"otlp_stop dropped {dropped} rows (expected clean drain)")

    if failures:
        print("FAIL")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print(f"OK: {NUM_RECORDS} OTLP/gRPC log records ingested, bad token rejected, clean stop")


if __name__ == "__main__":
    main()
