#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "duckdb==1.5.3",
#   "grpcio>=1.60",
# ]
# ///
"""Manual OTAP/Arrow bidirectional-streaming coverage for otap_serve (Phase 2).

The canonical OpenTelemetry Arrow Protocol ships logs over a gRPC bidi stream
(ArrowLogsService.ArrowLogs: stream BatchArrowRecords -> stream BatchStatus),
where later messages reuse Arrow dictionaries established by earlier ones. This
test drives that path with a raw-bytes gRPC client (no generated stubs needed):
it sends the canonical .bar fixtures verbatim as stream messages. SQLLogicTest
cannot do this, so it lives outside `make test` (alongside otap_serve_grpc.py).

It checks:
  * an ArrowLogs stream of two messages (initial + a dictionary-reuse follow-up)
    returns one BatchStatus per message, all OK
  * the reuse message decodes ONLY because the per-stream decoder kept the
    initial message's dictionaries -- i.e. cross-message reuse works over the wire
  * the decoded rows land in otlp_logs after otlp_flush
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
ARROW_LOGS = "/opentelemetry.proto.experimental.arrow.v1.ArrowLogsService/ArrowLogs"
INITIAL = "test/data/otap/logs-initial.bar"
REUSE = "test/data/otap/logs-reuse.bar"


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


def arrow_logs_method(channel):
    return channel.stream_stream(
        ARROW_LOGS,
        request_serializer=lambda b: b,  # raw BatchArrowRecords bytes
        response_deserializer=lambda b: b,  # raw BatchStatus bytes
    )


def main():
    with open(INITIAL, "rb") as fh:
        initial = fh.read()
    with open(REUSE, "rb") as fh:
        reuse = fh.read()

    failures = []
    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    # Ground truth: how many rows the initial fixture alone yields.
    initial_rows = con.execute(f"SELECT count(*) FROM read_otap_logs('{INITIAL}')").fetchone()[0]
    con.execute(f"SELECT * FROM otap_serve('{URI}', token := '{TOKEN}')")
    try:
        channel = grpc.insecure_channel(TARGET)
        grpc.channel_ready_future(channel).result(timeout=10)
        call = arrow_logs_method(channel)

        # 1) Valid stream: initial then a dictionary-reuse follow-up. The reuse
        # message carries no schema, so it decodes only because the server kept the
        # per-stream decoder state -- proving cross-message reuse over the wire.
        responses = list(call(iter([initial, reuse]), metadata=[("authorization", f"Bearer {TOKEN}")], timeout=20))
        if len(responses) != 2:
            failures.append(f"expected 2 BatchStatus, got {len(responses)}")
        bad = [batch_status_code(r) for r in responses if batch_status_code(r) != 0]
        if bad:
            failures.append(f"non-OK BatchStatus codes: {bad}")

        # 2) Bad token -> the stream is rejected with UNAUTHENTICATED.
        try:
            list(call(iter([initial]), metadata=[("authorization", "Bearer nope")], timeout=20))
            failures.append("bad token was NOT rejected")
        except grpc.RpcError as exc:
            if exc.code() != grpc.StatusCode.UNAUTHENTICATED:
                failures.append(f"bad token -> {exc.code()} (expected UNAUTHENTICATED)")

        con.execute(f"SELECT * FROM otlp_flush('{URI}')")

        count = con.execute("SELECT count(*) FROM otlp_logs").fetchone()[0]
        # initial + reuse must both have landed; the reuse rows are on top of initial.
        if count <= initial_rows:
            failures.append(f"otlp_logs has {count} rows; reuse message added none on top of {initial_rows}")

        services = [r[0] for r in con.execute("SELECT DISTINCT service_name FROM otlp_logs").fetchall()]
        if services != ["fixture-service"]:
            failures.append(f"service_name = {services}, expected ['fixture-service']")
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
        f"OK: OTAP/Arrow stream ingested ({count} rows incl. dictionary-reuse follow-up), "
        "bad token rejected, clean stop"
    )


if __name__ == "__main__":
    main()
