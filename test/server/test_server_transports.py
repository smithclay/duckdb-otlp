"""Real daemon transport/auth/shutdown tests, using disposable local Parquet storage.

Run with uv run --with pytest --with grpcio --with opentelemetry-proto \
    --with duckdb==1.5.5 pytest test/server
"""

import contextlib
import os
import socket
import subprocess
import time
import urllib.error
import urllib.request

import duckdb
import grpc
import pytest
from opentelemetry.proto.collector.trace.v1 import trace_service_pb2, trace_service_pb2_grpc
from opentelemetry.proto.trace.v1 import trace_pb2

from test_server_config import SERVER_BIN, pytestmark

TOKEN = 'transport-test-token-0123456789'


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def configuration(tmp_path, transports):
    http_port, grpc_port = free_port(), free_port()
    while grpc_port == http_port:
        grpc_port = free_port()
    return {
        'PATH': os.environ.get('PATH', ''),
        'DUCKDB_MODE': 'parquet',
        'DUCKDB_DATABASE': ':memory:',
        'DUCKDB_OTLP_DATA_DIR': str(tmp_path),
        'PARQUET_EXPORT_PATH': str(tmp_path / 'parquet'),
        'DUCKDB_OTLP_TRANSPORTS': transports,
        'OTEL_HTTP_ADDR': f'127.0.0.1:{http_port}',
        'OTEL_GRPC_ADDR': f'127.0.0.1:{grpc_port}',
        'DUCKDB_OTLP_TOKEN': TOKEN,
        # Ensure these small batches require the SIGTERM drain to become durable.
        'DUCKDB_OTLP_SEAL_MAX_AGE_MS': '600000',
        'DUCKDB_OTLP_STARTUP_TIMEOUT': '5',
    }


@contextlib.contextmanager
def daemon(tmp_path, env):
    log_path = tmp_path / 'daemon.log'
    with log_path.open('w') as log:
        process = subprocess.Popen([str(SERVER_BIN)], env=env, stdout=log, stderr=log)
        try:
            yield process, log_path
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
                pytest.fail(f'daemon did not shut down:\n{log_path.read_text()}')


def wait_ready(process, env, log_path):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        assert process.poll() is None, log_path.read_text()
        ready = subprocess.run([str(SERVER_BIN), 'healthcheck'], env=env, capture_output=True, timeout=10)
        if ready.returncode == 0:
            return
        time.sleep(0.05)
    pytest.fail(f'listeners never became ready:\n{log_path.read_text()}')


def trace_request(identifier):
    span = trace_pb2.Span(
        trace_id=identifier.to_bytes(16, 'big'),
        span_id=identifier.to_bytes(8, 'big'),
        name=f'transport-{identifier}',
        start_time_unix_nano=1700000000000000000,
        end_time_unix_nano=1700000000001000000,
    )
    return trace_service_pb2.ExportTraceServiceRequest(
        resource_spans=[trace_pb2.ResourceSpans(scope_spans=[trace_pb2.ScopeSpans(spans=[span])])]
    )


def post_http(env, message, token):
    request = urllib.request.Request(
        f"http://{env['OTEL_HTTP_ADDR']}/v1/traces",
        data=message.SerializeToString(),
        headers={'Content-Type': 'application/x-protobuf', 'Authorization': f'Bearer {token}'},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return response.status


@pytest.mark.parametrize('transports', ['http', 'grpc', 'http,grpc'])
def test_ingest_auth_and_sigterm_drain_for_selected_transports(tmp_path, transports):
    env = configuration(tmp_path, transports)
    expected = []
    with daemon(tmp_path, env) as (process, log_path):
        wait_ready(process, env, log_path)
        if 'http' in transports:
            with pytest.raises(urllib.error.HTTPError) as refused:
                post_http(env, trace_request(9), 'wrong-token')
            assert refused.value.code in (401, 403)
            assert post_http(env, trace_request(1), TOKEN) == 202
            expected.append('transport-1')
        if 'grpc' in transports:
            with grpc.insecure_channel(env['OTEL_GRPC_ADDR']) as channel:
                stub = trace_service_pb2_grpc.TraceServiceStub(channel)
                with pytest.raises(grpc.RpcError) as refused:
                    stub.Export(trace_request(9), metadata=[('authorization', 'Bearer wrong-token')], timeout=5)
                assert refused.value.code() == grpc.StatusCode.UNAUTHENTICATED
                stub.Export(trace_request(2), metadata=[('authorization', f'Bearer {TOKEN}')], timeout=5)
                expected.append('transport-2')
        assert not list((tmp_path / 'parquet').rglob('*.parquet')), 'batch should still be buffered'
    assert process.returncode == 0, log_path.read_text()
    files = list((tmp_path / 'parquet').rglob('*.parquet'))
    assert files, log_path.read_text()
    with duckdb.connect() as connection:
        actual = connection.execute(
            'SELECT name FROM read_parquet(?) ORDER BY name', [[str(p) for p in files]]
        ).fetchall()
    assert actual == [(name,) for name in sorted(expected)]
    # Both transports feed one server: one buffer set and one final seal, so the SIGTERM drain
    # writes a single traces file rather than one per listener.
    assert len(files) == 1, files
    log = log_path.read_text()
    listeners = [f"otlp:{env['OTEL_HTTP_ADDR']}"] if 'http' in transports else []
    listeners += [f"otlp:{env['OTEL_GRPC_ADDR']}"] if 'grpc' in transports else []
    assert log.count('Stopped listening on') == 1, log
    assert all(listener in log for listener in listeners), log
    assert TOKEN not in log


def test_second_listener_bind_failure_cleans_up_first_listener(tmp_path):
    env = configuration(tmp_path, 'http,grpc')
    with socket.socket() as occupied:
        occupied.bind(('127.0.0.1', 0))
        occupied.listen(16)
        env['OTEL_GRPC_ADDR'] = f'127.0.0.1:{occupied.getsockname()[1]}'
        with daemon(tmp_path, env) as (process, log_path):
            assert process.wait(timeout=15) == 1
        log = log_path.read_text()
        # The failed otlp_serve call closes the HTTP listener it had already bound and registers
        # nothing, so the shutdown stop finds no server.
        assert 'Failed to start OTLP/gRPC server' in log, log
        assert 'No server found listening on' in log, log
        with socket.socket() as probe:
            assert probe.connect_ex(('127.0.0.1', int(env['OTEL_HTTP_ADDR'].split(':')[1]))) != 0
