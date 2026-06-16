#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "duckdb==1.5.3",
#   "grpcio>=1.60",
#   "opentelemetry-proto>=1.20",
# ]
# ///
"""Manual OTLP/gRPC unary ingest coverage for otap_serve.

SQLLogicTest cannot drive a gRPC client, so this lives outside `make test`
(it mirrors test/manual/otlp_serve_concurrency.py, which covers the HTTP
server). The server runs in-process via the duckdb Python module; the gRPC
client talks to it over loopback. It checks the standard OTLP/gRPC unary
Export RPCs for all three signals:

  * LogsService / TraceService / MetricsService Export with a valid Bearer token
    -> rows land in otlp_logs / otlp_traces / otlp_metrics_{gauge,sum}
  * a bad token is rejected with UNAUTHENTICATED (and buffers nothing)
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
    logs_service_pb2_grpc as logs_grpc,
)
from opentelemetry.proto.collector.metrics.v1 import (
    metrics_service_pb2 as metrics_service,
    metrics_service_pb2_grpc as metrics_grpc,
)
from opentelemetry.proto.collector.trace.v1 import (
    trace_service_pb2 as trace_service,
    trace_service_pb2_grpc as trace_grpc,
)
from opentelemetry.proto.common.v1 import common_pb2 as common
from opentelemetry.proto.logs.v1 import logs_pb2 as logs
from opentelemetry.proto.metrics.v1 import metrics_pb2 as metrics
from opentelemetry.proto.resource.v1 import resource_pb2 as resource
from opentelemetry.proto.trace.v1 import trace_pb2 as trace

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
PORT = int(os.environ.get("OTLP_PORT", "4327"))
TOKEN = "manual-grpc-token-0123456789"
URI = f"otap:localhost:{PORT}"
TARGET = f"localhost:{PORT}"
TS = 1_700_000_000_000_000_000


def service_resource():
    return resource.Resource(
        attributes=[common.KeyValue(key="service.name", value=common.AnyValue(string_value="grpc-demo"))]
    )


def logs_request(n):
    records = [
        logs.LogRecord(
            time_unix_nano=TS + i,
            severity_number=9,
            severity_text="INFO",
            body=common.AnyValue(string_value=f"hello grpc {i}"),
        )
        for i in range(n)
    ]
    return logs_service.ExportLogsServiceRequest(
        resource_logs=[
            logs.ResourceLogs(
                resource=service_resource(),
                scope_logs=[logs.ScopeLogs(scope=common.InstrumentationScope(name="t"), log_records=records)],
            )
        ]
    )


def traces_request():
    span = trace.Span(
        trace_id=bytes(range(16)),
        span_id=bytes(range(8)),
        name="GET /demo",
        kind=trace.Span.SPAN_KIND_SERVER,
        start_time_unix_nano=TS,
        end_time_unix_nano=TS + 1_000_000,
    )
    return trace_service.ExportTraceServiceRequest(
        resource_spans=[
            trace.ResourceSpans(
                resource=service_resource(),
                scope_spans=[trace.ScopeSpans(scope=common.InstrumentationScope(name="t"), spans=[span])],
            )
        ]
    )


def metrics_request():
    gauge = metrics.Metric(
        name="temperature",
        gauge=metrics.Gauge(data_points=[metrics.NumberDataPoint(time_unix_nano=TS, as_double=21.5)]),
    )
    counter = metrics.Metric(
        name="requests",
        sum=metrics.Sum(
            aggregation_temporality=metrics.AGGREGATION_TEMPORALITY_CUMULATIVE,
            is_monotonic=True,
            data_points=[metrics.NumberDataPoint(time_unix_nano=TS, as_double=7)],
        ),
    )
    return metrics_service.ExportMetricsServiceRequest(
        resource_metrics=[
            metrics.ResourceMetrics(
                resource=service_resource(),
                scope_metrics=[
                    metrics.ScopeMetrics(scope=common.InstrumentationScope(name="t"), metrics=[gauge, counter])
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
        grpc.channel_ready_future(channel).result(timeout=10)
        auth = [("authorization", f"Bearer {TOKEN}")]
        logs_stub = logs_grpc.LogsServiceStub(channel)
        trace_stub = trace_grpc.TraceServiceStub(channel)
        metrics_stub = metrics_grpc.MetricsServiceStub(channel)

        # Valid token -> each signal's rows buffered.
        logs_stub.Export(logs_request(3), metadata=auth, timeout=10)
        trace_stub.Export(traces_request(), metadata=auth, timeout=10)
        metrics_stub.Export(metrics_request(), metadata=auth, timeout=10)

        # Bad token -> UNAUTHENTICATED, nothing buffered.
        try:
            logs_stub.Export(logs_request(5), metadata=[("authorization", "Bearer wrong")], timeout=10)
            failures.append("bad token was NOT rejected")
        except grpc.RpcError as exc:
            if exc.code() != grpc.StatusCode.UNAUTHENTICATED:
                failures.append(f"bad token -> {exc.code()} (expected UNAUTHENTICATED)")

        con.execute(f"SELECT * FROM otlp_flush('{URI}')")

        expectations = {
            "otlp_logs": 3,
            "otlp_traces": 1,
            "otlp_metrics_gauge": 1,
            "otlp_metrics_sum": 1,
        }
        for table, want in expectations.items():
            got = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            if got != want:
                failures.append(f"{table} has {got} rows, expected {want}")

        services = [r[0] for r in con.execute("SELECT DISTINCT service_name FROM otlp_logs").fetchall()]
        if services != ["grpc-demo"]:
            failures.append(f"service_name = {services}, expected ['grpc-demo']")
    finally:
        stop = con.execute(f"SELECT * FROM otlp_stop('{URI}')").fetchone()
        if stop[1] != 0:
            failures.append(f"otlp_stop dropped {stop[1]} rows (expected clean drain)")

    if failures:
        print("FAIL")
        for f in failures:
            print(f"  - {f}")
        sys.exit(1)
    print(
        "OK: OTLP/gRPC unary Export for logs (3), traces (1), and metrics (gauge+sum) "
        "ingested; bad token rejected; clean stop"
    )


if __name__ == "__main__":
    main()
