"""Real daemon ingest edge cases: HTTP request handling, backpressure, metric fan-out and OTAP/Arrow
streaming. The tests read what the daemon wrote during its SIGTERM drain.

Run with uv run --with pytest --with grpcio --with opentelemetry-proto \
    --with duckdb==2.0.0 pytest test/server
"""

import gzip
import urllib.error
import urllib.request
import zlib
from pathlib import Path

import duckdb
import grpc
import pytest
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2_grpc

from test_server_config import pytestmark  # noqa: F401
from test_server_transports import TOKEN, configuration, daemon, grpc_addr, http_addr, trace_request, wait_ready

DATA = Path(__file__).resolve().parents[1] / 'data' / 'otap'
METRIC_TABLES = ['otlp_metrics_gauge', 'otlp_metrics_sum', 'otlp_metrics_histogram', 'otlp_metrics_exp_histogram']


def post(env, path, body, content_type='application/x-protobuf', headers=None):
    """POST and return the status code, including error statuses."""
    request = urllib.request.Request(
        f"http://{http_addr(env)}{path}",
        data=body,
        headers={'Content-Type': content_type, **(headers or {})},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status
    except urllib.error.HTTPError as error:
        return error.code


def bearer(token=TOKEN):
    return {'Authorization': f'Bearer {token}'}


def table_rows(tmp_path, table):
    files = [str(p) for p in (tmp_path / 'parquet' / table).rglob('*.parquet')]
    if not files:
        return 0
    with duckdb.connect() as connection:
        return connection.execute('SELECT count(*) FROM read_parquet(?)', [files]).fetchone()[0]


def run_daemon(tmp_path, env, body):
    """Start the daemon, run `body(env)`, stop it with SIGTERM, and require a clean drain."""
    with daemon(tmp_path, env) as (process, log_path):
        wait_ready(process, env, log_path)
        body(env)
    assert process.returncode == 0, log_path.read_text()


def test_http_auth_content_type_and_encoding(tmp_path):
    logs = (DATA / 'logs-initial.otlp').read_bytes()  # 3 log records

    def body(env):
        assert post(env, '/v1/logs', logs, headers=bearer()) == 202
        assert post(env, '/v1/logs', logs, headers={'x-api-key': TOKEN}) == 202
        assert post(env, '/v1/logs', logs) == 401
        assert post(env, '/v1/logs', logs, headers={'x-api-key': 'wrong-token'}) == 401
        assert post(env, '/v1/logs', logs, content_type='text/plain', headers=bearer()) == 415
        assert post(env, '/v1/logs', logs, headers={**bearer(), 'Content-Encoding': 'br'}) == 415
        assert post(env, '/v1/logs', b'\xff\xff', headers=bearer()) == 400
        assert post(env, '/v1/logs', b'{not json', content_type='application/json', headers=bearer()) == 400
        for encoding, compressed in (('gzip', gzip.compress(logs)), ('deflate', zlib.compress(logs))):
            assert post(env, '/v1/logs', compressed, headers={**bearer(), 'Content-Encoding': encoding}) == 202

    run_daemon(tmp_path, configuration(tmp_path, 'http'), body)
    # bearer + x-api-key + gzip + deflate; every rejected request buffered nothing.
    assert table_rows(tmp_path, 'otlp_logs') == 4 * 3


def test_oversized_body_is_413_and_a_full_buffer_is_503(tmp_path):
    logs = (DATA / 'logs-initial.otlp').read_bytes()
    env = configuration(tmp_path, 'http')
    # Room for exactly one request: admission reserves max(body, 1024) bytes until the rows seal,
    # and SEAL_MAX_AGE_MS keeps them buffered, so the second request must be turned away.
    env['DUCKDB_OTLP_MAX_BUFFERED_BYTES'] = str(max(len(logs), 1024) + 512)
    env['DUCKDB_OTLP_MAX_BODY_BYTES'] = str(len(logs) * 2)

    def body(env):
        assert post(env, '/v1/logs', logs * 3, headers=bearer()) == 413
        assert post(env, '/v1/logs', logs, headers=bearer()) == 202
        assert post(env, '/v1/logs', logs, headers=bearer()) == 503

    run_daemon(tmp_path, env, body)
    assert table_rows(tmp_path, 'otlp_logs') == 3


def test_http_and_grpc_share_one_admission_budget(tmp_path):
    logs = (DATA / 'logs-initial.otlp').read_bytes()
    env = configuration(tmp_path, 'http,grpc')
    # Room for one HTTP request; what is left is under the 1024-byte minimum a gRPC batch reserves.
    env['DUCKDB_OTLP_MAX_BUFFERED_BYTES'] = str(max(len(logs), 1024) + 512)

    def body(env):
        assert post(env, '/v1/logs', logs, headers=bearer()) == 202
        with grpc.insecure_channel(grpc_addr(env)) as channel:
            stub = trace_service_pb2_grpc.TraceServiceStub(channel)
            with pytest.raises(grpc.RpcError) as refused:
                stub.Export(trace_request(1), metadata=[('authorization', f'Bearer {TOKEN}')], timeout=5)
            assert refused.value.code() == grpc.StatusCode.RESOURCE_EXHAUSTED

    run_daemon(tmp_path, env, body)
    assert table_rows(tmp_path, 'otlp_logs') == 3
    assert table_rows(tmp_path, 'otlp_traces') == 0


def test_one_metrics_request_fans_out_to_every_shape_table(tmp_path):
    metrics = (DATA / 'metrics-initial.otlp').read_bytes()  # one data point of each shape

    def body(env):
        assert post(env, '/v1/metrics', metrics, headers=bearer()) == 202

    run_daemon(tmp_path, configuration(tmp_path, 'http'), body)
    assert {table: table_rows(tmp_path, table) for table in METRIC_TABLES} == dict.fromkeys(METRIC_TABLES, 1)


ARROW = '/opentelemetry.proto.experimental.arrow.v1'


def read_varint(buf, i):
    shift = result = 0
    while True:
        byte = buf[i]
        i += 1
        result |= (byte & 0x7F) << shift
        if not byte & 0x80:
            return result, i
        shift += 7


def batch_status_code(raw):
    """BatchStatus.status_code (field 2), 0 (OK) when absent. Decoded by hand: there are no
    published Python stubs for the OTAP protos."""
    i = code = 0
    while i < len(raw):
        key, i = read_varint(raw, i)
        field, wire = key >> 3, key & 7
        if wire == 0:
            value, i = read_varint(raw, i)
            code = value if field == 2 else code
        elif wire == 2:
            length, i = read_varint(raw, i)
            i += length
        elif wire in (1, 5):
            i += 8 if wire == 1 else 4
        else:
            break
    return code


def stream_arrow(channel, service, files, token=TOKEN):
    method = channel.stream_stream(f'{ARROW}.Arrow{service}Service/Arrow{service}')
    messages = [(DATA / name).read_bytes() for name in files]
    return list(method(iter(messages), metadata=[('authorization', f'Bearer {token}')], timeout=20))


def test_otlp_and_otap_share_a_grpc_listener_and_trace_table(tmp_path):
    database = tmp_path / 'telemetry.duckdb'
    env = configuration(tmp_path, 'grpc')
    env.update(DUCKDB_MODE='none', DUCKDB_DATABASE=str(database))

    def body(env):
        with grpc.insecure_channel(grpc_addr(env)) as channel:
            stub = trace_service_pb2_grpc.TraceServiceStub(channel)
            stub.Export(trace_request(1), metadata=[('authorization', f'Bearer {TOKEN}')], timeout=5)
            assert [batch_status_code(r) for r in stream_arrow(channel, 'Traces', ['traces-initial.bar'])] == [0]

    run_daemon(tmp_path, env, body)
    with duckdb.connect(str(database), read_only=True) as connection:
        assert connection.execute('SELECT name FROM otlp_traces ORDER BY name').fetchall() == [
            ('first-span',),
            ('transport-1',),
        ]


def test_otap_streams_ingest_every_signal_with_dictionary_reuse(tmp_path):
    def body(env):
        with grpc.insecure_channel(grpc_addr(env)) as channel:
            # The reuse message carries no schema of its own: it decodes only because the server
            # keeps one decoder per stream, so its rows prove cross-message dictionary reuse.
            responses = stream_arrow(channel, 'Logs', ['logs-initial.bar', 'logs-reuse.bar'])
            assert [batch_status_code(r) for r in responses] == [0, 0]
            assert [batch_status_code(r) for r in stream_arrow(channel, 'Traces', ['traces-initial.bar'])] == [0]
            assert [batch_status_code(r) for r in stream_arrow(channel, 'Metrics', ['metrics-initial.bar'])] == [0]
            with pytest.raises(grpc.RpcError) as refused:
                stream_arrow(channel, 'Logs', ['logs-initial.bar'], token='wrong-token')
            assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED

    run_daemon(tmp_path, configuration(tmp_path, 'grpc'), body)
    assert table_rows(tmp_path, 'otlp_logs') > 3, 'the dictionary-reuse message added no rows'
    assert table_rows(tmp_path, 'otlp_traces') == 1
    assert {table: table_rows(tmp_path, table) for table in METRIC_TABLES} == dict.fromkeys(METRIC_TABLES, 1)
