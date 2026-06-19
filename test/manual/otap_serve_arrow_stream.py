#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "duckdb==1.5.4",
#   "grpcio>=1.60",
# ]
# ///
"""Manual OTAP/Arrow bidirectional-streaming coverage for otap_serve.

The canonical OpenTelemetry Arrow Protocol ships each signal over a gRPC bidi
stream (Arrow{Logs,Traces,Metrics}Service: stream BatchArrowRecords -> stream
BatchStatus), where later messages reuse Arrow dictionaries established by
earlier ones. This test drives all three signals with a raw-bytes gRPC client
(no generated stubs): it sends the canonical .bar fixtures verbatim as stream
messages. SQLLogicTest cannot do this, so it lives outside `make test`.

It checks:
  * a logs stream of two messages (initial + a dictionary-reuse follow-up)
    returns one OK BatchStatus per message -- proving cross-message reuse over
    the wire (the reuse message has no schema of its own)
  * traces and metrics streams ingest their fixtures (metrics -> all four shapes)
  * every signal's rows land in its table after otlp_flush
  * a bad token is rejected with UNAUTHENTICATED
  * a clean otlp_stop drains in-flight rows (dropped_rows == 0)

Run (requires a built loadable extension; `uv` resolves the deps):

    uv run --script test/manual/otap_serve_arrow_stream.py
    OTLP_EXTENSION=build/release/extension/otlp/otlp.duckdb_extension \\
        OTLP_PORT=4347 uv run --script test/manual/otap_serve_arrow_stream.py
"""

import os
import sys

import duckdb
import grpc

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
PORT = int(os.environ.get("OTLP_PORT", "4347"))
TOKEN = "manual-otap-token-0123456789"
URI = f"otap:localhost:{PORT}"
TARGET = f"localhost:{PORT}"

ARROW = "/opentelemetry.proto.experimental.arrow.v1"
ARROW_LOGS = f"{ARROW}.ArrowLogsService/ArrowLogs"
ARROW_TRACES = f"{ARROW}.ArrowTracesService/ArrowTraces"
ARROW_METRICS = f"{ARROW}.ArrowMetricsService/ArrowMetrics"

LOGS_INITIAL = "test/data/otap/logs-initial.bar"
LOGS_REUSE = "test/data/otap/logs-reuse.bar"
TRACES = "test/data/otap/traces-initial.bar"
METRICS = "test/data/otap/metrics-initial.bar"
METRIC_TABLES = [
    "otlp_metrics_gauge",
    "otlp_metrics_sum",
    "otlp_metrics_histogram",
    "otlp_metrics_exp_histogram",
]


def read_varint(buf, i):
    shift = 0
    result = 0
    while True:
        b = buf[i]
        i += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, i
        shift += 7


def batch_status_code(raw):
    """Extract BatchStatus.status_code (proto field #2); 0 (OK) if absent."""
    i = 0
    code = 0
    while i < len(raw):
        key, i = read_varint(raw, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            val, i = read_varint(raw, i)
            if field == 2:
                code = val
        elif wire == 2:
            ln, i = read_varint(raw, i)
            i += ln
        elif wire == 1:
            i += 8
        elif wire == 5:
            i += 4
        else:
            break
    return code


def arrow_method(channel, path):
    return channel.stream_stream(
        path,
        request_serializer=lambda b: b,  # raw BatchArrowRecords bytes
        response_deserializer=lambda b: b,  # raw BatchStatus bytes
    )


def read_bytes(path):
    with open(path, "rb") as fh:
        return fh.read()


def main():
    failures = []
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")

    # Ground truth: per-signal row counts the fixtures decode to.
    want_logs = con.execute(f"SELECT count(*) FROM read_otap_logs('{LOGS_INITIAL}')").fetchone()[0]
    want_traces = con.execute(f"SELECT count(*) FROM read_otap_traces('{TRACES}')").fetchone()[0]
    want_metrics = {
        t: con.execute(f"SELECT count(*) FROM read_otap_{t[5:]}('{METRICS}')").fetchone()[0] for t in METRIC_TABLES
    }

    con.execute(f"SELECT * FROM otap_serve('{URI}', token := '{TOKEN}')")
    try:
        channel = grpc.insecure_channel(TARGET)
        grpc.channel_ready_future(channel).result(timeout=10)
        auth = [("authorization", f"Bearer {TOKEN}")]

        def stream(path, messages, label):
            responses = list(arrow_method(channel, path)(iter(messages), metadata=auth, timeout=20))
            if len(responses) != len(messages):
                failures.append(f"{label}: expected {len(messages)} BatchStatus, got {len(responses)}")
            bad = [batch_status_code(r) for r in responses if batch_status_code(r) != 0]
            if bad:
                failures.append(f"{label}: non-OK BatchStatus codes {bad}")

        # Logs: initial + a dictionary-reuse follow-up. The reuse message carries no
        # schema, so it decodes only because the server kept the per-stream decoder
        # state -- proving cross-message reuse over the wire.
        stream(ARROW_LOGS, [read_bytes(LOGS_INITIAL), read_bytes(LOGS_REUSE)], "logs")
        stream(ARROW_TRACES, [read_bytes(TRACES)], "traces")
        stream(ARROW_METRICS, [read_bytes(METRICS)], "metrics")

        # Bad token -> the stream is rejected with UNAUTHENTICATED.
        try:
            list(
                arrow_method(channel, ARROW_LOGS)(
                    iter([read_bytes(LOGS_INITIAL)]), metadata=[("authorization", "Bearer nope")], timeout=20
                )
            )
            failures.append("bad token was NOT rejected")
        except grpc.RpcError as exc:
            if exc.code() != grpc.StatusCode.UNAUTHENTICATED:
                failures.append(f"bad token -> {exc.code()} (expected UNAUTHENTICATED)")

        con.execute(f"SELECT * FROM otlp_flush('{URI}')")

        logs_count = con.execute("SELECT count(*) FROM otlp_logs").fetchone()[0]
        if logs_count <= want_logs:
            failures.append(f"otlp_logs has {logs_count}; reuse added none on top of {want_logs}")
        traces_count = con.execute("SELECT count(*) FROM otlp_traces").fetchone()[0]
        if traces_count != want_traces:
            failures.append(f"otlp_traces has {traces_count}, expected {want_traces}")
        for table, want in want_metrics.items():
            got = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
            if got != want:
                failures.append(f"{table} has {got}, expected {want}")
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
        "OK: OTAP/Arrow streams for logs (incl. dictionary reuse), traces, and all four "
        "metric shapes ingested; bad token rejected; clean stop"
    )


if __name__ == "__main__":
    main()
