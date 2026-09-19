"""Config-generation tests for the duckdb-otlp-server daemon.

These exercise the pure ``env -> ServerConfig -> boot SQL`` path via ``DRY_RUN=1``
(no DB is opened, no listener starts), so they are fast and need only the daemon
binary — not Docker. The highest-value invariants here:

  * mode validation fails early with an actionable message;
  * the auth token is referenced via ``getvariable(...)`` and never appears as a literal;
  * backend secrets are referenced via ``getenv(...)`` and never appear as literals
    (DRY_RUN prints the generated SQL, and the engine can echo it in errors).

Point the tests at a binary with DUCKDB_OTLP_SERVER_BIN, or rely on the default
``build/release`` path. If the binary is missing the whole module is skipped.
"""

from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_BIN = REPO_ROOT / "build" / "release" / "extension" / "otlp" / "duckdb-otlp"
SERVER_BIN = Path(os.environ.get("DUCKDB_OTLP_SERVER_BIN", str(DEFAULT_BIN)))

pytestmark = pytest.mark.skipif(
    not SERVER_BIN.exists(),
    reason=f"daemon binary not found at {SERVER_BIN}; build with `make server-release`",
)

# A sentinel secret value: if any of these ever appear in DRY_RUN output, a secret is
# being interpolated as a literal instead of referenced via getenv()/getvariable().
SECRET = "PLAINTEXT-SECRET-SHOULD-NEVER-APPEAR"


def run(env: dict, data_dir: Path) -> subprocess.CompletedProcess:
    """Run the daemon in DRY_RUN mode with a temp data dir and return the result."""
    full_env = {
        # Keep PATH so the dynamic loader works; drop the rest for a clean slate.
        "PATH": os.environ.get("PATH", ""),
        "DRY_RUN": "1",
        "DUCKDB_OTLP_DATA_DIR": str(data_dir),
        "DUCKDB_DATABASE": str(data_dir / "control.duckdb"),
    }
    full_env.update(env)
    return subprocess.run(
        [str(SERVER_BIN)],
        env=full_env,
        capture_output=True,
        text=True,
        timeout=60,
    )


def test_missing_mode_defaults_to_local_ducklake(tmp_path):
    """DUCKDB_MODE is optional: a bare invocation must start a working local lakehouse.

    Every other mode still has to be named, so this default cannot redirect an intended
    remote target -- and the banner always prints the mode in effect.
    """
    result = run({}, tmp_path)  # no DUCKDB_MODE
    assert result.returncode == 0, result.stderr
    assert "Mode: local-ducklake" in result.stdout


def test_unsupported_mode_lists_supported(tmp_path):
    result = run({"DUCKDB_MODE": "not-a-mode"}, tmp_path)
    assert result.returncode == 1
    assert "Unsupported DUCKDB_MODE" in result.stderr
    assert "local-ducklake" in result.stderr  # the message enumerates valid modes


def test_local_ducklake_boot_sql(tmp_path):
    result = run(
        {"DUCKDB_MODE": "local-ducklake", "DUCKDB_OTLP_TOKEN": "a-private-token-123456"},
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "ATTACH 'ducklake:" in out
    assert "FROM otlp_serve(" in out
    # Token is read from a session variable, never interpolated.
    assert "getvariable('duckdb_otlp_effective_token')" in out
    assert "a-private-token-123456" not in out


def test_otap_listen_uri_routes_to_otap_serve(tmp_path):
    # An otap: listen URI must dispatch to otap_serve (OTAP/Arrow), never otlp_serve;
    # the two serve functions are bound to their own scheme and reject the other's.
    result = run(
        {
            "DUCKDB_MODE": "local-ducklake",
            "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
            "DUCKDB_OTLP_LISTEN_URI": "otap:127.0.0.1:4317",
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "FROM otap_serve(" in out
    assert "FROM otlp_serve(" not in out


def test_aws_ducklake_uses_instance_role_and_local_catalog(tmp_path):
    catalog = tmp_path / "ducklake" / "catalog.duckdb"
    result = run(
        {
            "DUCKDB_MODE": "aws-ducklake",
            "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
            "DUCKLAKE_CATALOG_PATH": str(catalog),
            "DUCKLAKE_DATA_PATH": "s3://benchmark-bucket/run-123",
            "AWS_REGION": "us-west-2",
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "PROVIDER credential_chain" in out
    assert "CHAIN instance" in out
    assert f"ATTACH 'ducklake:{catalog}'" in out
    assert "'s3://benchmark-bucket/run-123'" in out
    assert "KEY_ID" not in out
    assert "SECRET '" not in out


def test_otlp_limits_are_configurable(tmp_path):
    result = run(
        {
            "DUCKDB_MODE": "local-ducklake",
            "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
            "DUCKDB_OTLP_HTTP_THREADS": "4",
            "DUCKDB_OTLP_MAX_BODY_BYTES": "2097152",
            "DUCKDB_OTLP_MAX_BUFFERED_BYTES": "2147483648",
            "DUCKDB_OTLP_SEAL_TARGET_BYTES": "134217728",
            "DUCKDB_OTLP_SEAL_MAX_AGE_MS": "3000",
            "DUCKDB_OTLP_TARGET_FILE_SIZE": "268435456",
            "DUCKDB_OTLP_MAINTENANCE_RETENTION_MS": "600000",
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "http_threads := 4" in result.stdout
    assert "max_body_bytes := 2097152" in result.stdout
    assert "max_buffered_bytes := 2147483648" in result.stdout
    assert "seal_target_bytes := 134217728" in result.stdout
    assert "seal_max_age_ms := 3000" in result.stdout
    assert "target_file_size := 268435456" in result.stdout
    assert "maintenance_retention_ms := 600000" in result.stdout


def test_promotion_params_emitted_when_set(tmp_path):
    result = run(
        {
            "DUCKDB_MODE": "local-ducklake",
            "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
            "DUCKDB_OTLP_PROMOTE_RESOURCE_ATTRIBUTES": "host.name,deployment.environment",
            "DUCKDB_OTLP_PROMOTE_SCOPE_ATTRIBUTES": "scope.team",
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "promote_resource_attributes := 'host.name,deployment.environment'" in result.stdout
    assert "promote_scope_attributes := 'scope.team'" in result.stdout


def test_promotion_off_by_default(tmp_path):
    result = run(
        {"DUCKDB_MODE": "local-ducklake", "DUCKDB_OTLP_TOKEN": "a-private-token-123456"},
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "promote_resource_attributes" not in result.stdout
    assert "promote_scope_attributes" not in result.stdout


def test_no_token_on_loopback_disables_auth(tmp_path):
    """There is no built-in default token any more.

    A token published in this repository authenticates nothing, so falling back to one gave
    servers only the appearance of protection. With no token configured, an unauthenticated
    server is allowed exactly where it cannot be reached from off the machine.
    """
    result = run({"DUCKDB_MODE": "local-ducklake"}, tmp_path)  # no token set
    assert result.returncode == 0, result.stderr
    assert "Authentication is DISABLED" in result.stdout
    assert "disable_auth := true" in result.stdout
    assert "dev-otlp-token" not in result.stdout


def test_no_token_on_a_public_bind_is_rejected(tmp_path):
    result = run({"DUCKDB_MODE": "local-ducklake", "DUCKDB_OTLP_HOST": "0.0.0.0"}, tmp_path)
    assert result.returncode == 1
    assert "bearer token is required" in result.stderr


def test_explicit_token_is_used(tmp_path):
    result = run(
        {"DUCKDB_MODE": "local-ducklake", "DUCKDB_OTLP_TOKEN": "a-private-token-123456"},
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "token := getvariable('duckdb_otlp_effective_token')" in result.stdout
    assert "disable_auth" not in result.stdout


def test_parquet_local_path(tmp_path):
    export = tmp_path / "pq"
    result = run(
        {
            "DUCKDB_MODE": "parquet",
            "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
            "PARQUET_EXPORT_PATH": str(export),
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "parquet_export_path :=" in out
    assert str(export) in out


def test_r2_data_catalog_secrets_use_getvariable(tmp_path):
    """The R2 catalog mode wires several secrets — none may appear as literals, and they
    must be read via getvariable() (getenv() is a CLI-only function the daemon lacks)."""
    env = {
        "DUCKDB_MODE": "r2-data-catalog",
        "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
        "CLOUDFLARE_ACCOUNT_ID": "acct123",
        "CLOUDFLARE_R2_BUCKET": "mybucket",
        "CLOUDFLARE_CATALOG_URI": "https://catalog.example/uri",
        "CLOUDFLARE_CATALOG_TOKEN": SECRET,
        "CLOUDFLARE_ACCESS_KEY_ID": SECRET,
        "CLOUDFLARE_SECRET_ACCESS_KEY": SECRET,
    }
    result = run(env, tmp_path)
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert SECRET not in out, "a secret value leaked into generated SQL"
    assert "getenv(" not in out, "getenv() is not available in the embedded daemon; use getvariable()"
    assert "getvariable('env_CLOUDFLARE_CATALOG_TOKEN')" in out
    assert "getvariable('env_CLOUDFLARE_ACCESS_KEY_ID')" in out
    assert "getvariable('env_CLOUDFLARE_SECRET_ACCESS_KEY')" in out


def test_r2_neon_ducklake_secrets_use_getvariable(tmp_path):
    env = {
        "DUCKDB_MODE": "r2-neon-ducklake",
        "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
        "CLOUDFLARE_R2_BUCKET": "mybucket",
        "CLOUDFLARE_ACCESS_KEY_ID": SECRET,
        "CLOUDFLARE_SECRET_ACCESS_KEY": SECRET,
        "CLOUDFLARE_R2_ENDPOINT": "https://acct.r2.cloudflarestorage.com",
        "NEON_PGHOST": "db.example",
        "NEON_PGDATABASE": "lake",
        "NEON_PGUSER": "lakeuser",
        "NEON_PGPASSWORD": SECRET,
    }
    result = run(env, tmp_path)
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert SECRET not in out, "a secret value leaked into generated SQL"
    assert "getenv(" not in out, "getenv() is not available in the embedded daemon; use getvariable()"
    assert "getvariable('env_NEON_PGPASSWORD')" in out
    assert "getvariable('env_CLOUDFLARE_ACCESS_KEY_ID')" in out


def gcp_env():
    return {
        "DUCKDB_MODE": "gcp-ducklake",
        "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
        "DUCKLAKE_DATA_PATH": "gcss://trace-bucket/ducklake/",
        "PGHOST": "/cloudsql/project:region:instance",
        "PGDATABASE": "tracelake",
        "PGUSER": "trace_writer",
        "PGPASSWORD": SECRET,
    }


def test_gcp_ducklake_uses_adc_and_remote_postgres(tmp_path):
    result = run(gcp_env(), tmp_path)
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "INSTALL gcs FROM community;" in out
    assert "LOAD gcs;" in out
    assert "TYPE gcp" in out
    assert "PROVIDER credential_chain" in out
    assert "DATA_PATH 'gcss://trace-bucket/ducklake/'" in out
    assert "ATTACH 'ducklake:ducklake_secret' AS" in out
    assert "'TYPE': 'postgres'" in out
    assert "PORT '5432'" in out
    assert "SSLMODE 'require'" in out
    for name in ("PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"):
        assert f"getvariable('env_{name}')" in out
    assert SECRET not in result.stdout + result.stderr
    assert "KEY_ID" not in out
    assert "getenv(" not in out


def test_gcp_ducklake_proxy_and_catalog_overrides(tmp_path):
    env = gcp_env()
    env.update({"PGPORT": "5433", "PGSSLMODE": "disable", "DUCKLAKE_NAME": "trace-lake", "DUCKDB_SCHEMA": "traces"})
    result = run(env, tmp_path)
    assert result.returncode == 0, result.stderr
    assert "PORT getvariable('env_PGPORT')" in result.stdout
    assert "SSLMODE getvariable('env_PGSSLMODE')" in result.stdout
    assert 'AS "trace-lake"' in result.stdout
    assert "schema := 'traces'" in result.stdout


def test_ducklake_inlining_default_leaves_attach_untouched(tmp_path):
    result = run(gcp_env(), tmp_path)
    assert result.returncode == 0, result.stderr
    assert "DATA_INLINING_ROW_LIMIT" not in result.stdout


def test_ducklake_inlining_can_be_disabled_on_secret_attach(tmp_path):
    env = gcp_env()
    env["DUCKLAKE_DATA_INLINING_ROW_LIMIT"] = "0"
    result = run(env, tmp_path)
    assert result.returncode == 0, result.stderr
    assert "ATTACH 'ducklake:ducklake_secret' AS \"lake\" (DATA_INLINING_ROW_LIMIT 0);" in result.stdout


def test_ducklake_inlining_limit_on_path_attach(tmp_path):
    result = run({"DUCKDB_MODE": "local-ducklake", "DUCKLAKE_DATA_INLINING_ROW_LIMIT": "50"}, tmp_path)
    assert result.returncode == 0, result.stderr
    assert ",\n  DATA_INLINING_ROW_LIMIT 50\n);" in result.stdout


@pytest.mark.parametrize("value", ["-1", "abc", "1.5"])
def test_ducklake_inlining_limit_rejects_invalid(tmp_path, value):
    result = run({"DUCKDB_MODE": "local-ducklake", "DUCKLAKE_DATA_INLINING_ROW_LIMIT": value}, tmp_path)
    assert result.returncode == 1
    assert "DUCKLAKE_DATA_INLINING_ROW_LIMIT" in result.stderr


@pytest.mark.parametrize("missing", ["DUCKLAKE_DATA_PATH", "PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"])
def test_gcp_ducklake_requires_catalog_and_storage_config(tmp_path, missing):
    env = gcp_env()
    del env[missing]
    result = run(env, tmp_path)
    assert result.returncode == 1
    assert missing in result.stderr
    assert SECRET not in result.stdout + result.stderr


@pytest.mark.parametrize(
    "path", ["gs://bucket/lake", "gcs://bucket/lake", "s3://bucket/lake", "/data/lake", "gcss://", "gcss:///lake"]
)
def test_gcp_ducklake_requires_native_gcs_path(tmp_path, path):
    env = gcp_env()
    env["DUCKLAKE_DATA_PATH"] = path
    result = run(env, tmp_path)
    assert result.returncode == 1
    assert "gcss://bucket/prefix" in result.stderr


def test_missing_required_var_names_the_var(tmp_path):
    # r2-data-catalog without its bucket should fail naming the missing variable.
    result = run(
        {"DUCKDB_MODE": "r2-data-catalog", "DUCKDB_OTLP_TOKEN": "a-private-token-123456"},
        tmp_path,
    )
    assert result.returncode == 1
    assert "exception_type" not in result.stderr
    assert "CLOUDFLARE" in result.stderr


def _healthcheck(env: dict) -> int:
    """Run the daemon's `healthcheck` subcommand with a clean env; return its exit code."""
    full_env = {"PATH": os.environ.get("PATH", "")}
    full_env.update(env)
    return subprocess.run(
        [str(SERVER_BIN), "healthcheck"],
        env=full_env,
        capture_output=True,
        text=True,
        timeout=30,
    ).returncode


def test_grpc_healthcheck_uses_tcp_connect():
    # otap: (gRPC/HTTP2) has no HTTP /readyz, so the healthcheck TCP-connects: a bound listener
    # is healthy, the same port with nothing listening is not.
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.listen(16)
    try:
        assert _healthcheck({"DUCKDB_OTLP_LISTEN_URI": f"otap:127.0.0.1:{port}"}) == 0
    finally:
        sock.close()
    assert _healthcheck({"DUCKDB_OTLP_LISTEN_URI": f"otap:127.0.0.1:{port}"}) == 1


def test_http_healthcheck_needs_real_http_not_just_tcp():
    # otlp: (HTTP) must get a real /readyz response; a bare TCP listener that never speaks HTTP
    # is unhealthy -- proving the HTTP and gRPC probes are distinct.
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.listen(16)
    try:
        assert _healthcheck({"DUCKDB_OTLP_LISTEN_URI": f"otlp:127.0.0.1:{port}"}) == 1
    finally:
        sock.close()


@pytest.mark.parametrize("transports", ["http", "grpc", "http,grpc", " grpc, http "])
def test_transport_selection(tmp_path, transports):
    result = run({"DUCKDB_MODE": "parquet", "DUCKDB_OTLP_TRANSPORTS": transports}, tmp_path)
    assert result.returncode == 0, result.stderr
    selected = [value.strip() for value in transports.split(",")]
    # Every transport is a listener on one server: exactly one otlp_serve call, with the URIs
    # and transports as parallel lists in the order given.
    assert result.stdout.count("FROM otlp_serve(") == 1
    ports = {"http": 4318, "grpc": 4317}
    # The default bind host is loopback; the container image opts back into 0.0.0.0 via
    # DUCKDB_OTLP_HOST in its Dockerfile.
    uris = ", ".join(f"'otlp:127.0.0.1:{ports[t]}'" for t in selected)
    assert f"[{uris}]" in result.stdout
    assert "transport := [" + ", ".join(f"'{t}'" for t in selected) + "]" in result.stdout
    assert "FROM otap_serve(" not in result.stdout


@pytest.mark.parametrize("transports", ["http,", ",grpc", "http,,grpc", "http,http", "grpc,grpc", "otap", " ", "HTTP"])
def test_invalid_transports_fail_before_startup(tmp_path, transports):
    result = run({"DUCKDB_MODE": "parquet", "DUCKDB_OTLP_TRANSPORTS": transports}, tmp_path)
    assert result.returncode == 1
    assert "DUCKDB_OTLP_TRANSPORTS" in result.stderr


@pytest.mark.parametrize(
    "env,complaint",
    [
        ({"DUCKDB_OTLP_TRANSPORTS": "http,grpc", "DUCKDB_OTLP_LISTEN_URI": "otlp:localhost:9000"}, "single transport"),
        ({"DUCKDB_OTLP_TRANSPORTS": "grpc", "DUCKDB_OTLP_LISTEN_URI": "otap:localhost:9000"}, "otap:"),
        (
            {
                "DUCKDB_OTLP_TRANSPORTS": "http,grpc",
                "OTEL_HTTP_ADDR": "0.0.0.0:9000",
                "OTEL_GRPC_ADDR": "localhost:9000",
            },
            "different ports",
        ),
        ({"DUCKDB_OTLP_TRANSPORTS": "grpc", "OTEL_GRPC_ADDR": "0.0.0.0:70000"}, "port"),
    ],
)
def test_conflicting_or_invalid_listener_addresses(tmp_path, env, complaint):
    result = run({"DUCKDB_MODE": "parquet", **env}, tmp_path)
    assert result.returncode == 1
    assert complaint in result.stderr


def test_grpc_uri_override_is_canonical_and_ignores_http_threads(tmp_path):
    result = run(
        {
            "DUCKDB_MODE": "parquet",
            "DUCKDB_OTLP_TRANSPORTS": "grpc",
            "DUCKDB_OTLP_LISTEN_URI": "otlp://127.0.0.1:9000",
            "DUCKDB_OTLP_HTTP_THREADS": "4",
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "['otlp:127.0.0.1:9000']" in result.stdout
    assert "transport := ['grpc']" in result.stdout
    assert "http_threads :=" not in result.stdout


def test_standard_grpc_healthcheck_uses_configured_grpc_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(16)
        env = {"DUCKDB_OTLP_TRANSPORTS": "grpc", "OTEL_GRPC_ADDR": f"127.0.0.1:{sock.getsockname()[1]}"}
        assert _healthcheck(env) == 0
    assert _healthcheck(env) == 1


def test_healthcheck_requires_both_listeners():
    import http.server
    import threading

    class Ready(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if self.path == "/readyz" else 404)
            self.end_headers()

        def log_message(self, *args):
            pass

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Ready) as httpd, socket.socket() as grpc_sock:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        grpc_sock.bind(("127.0.0.1", 0))
        grpc_sock.listen(16)
        env = {
            "DUCKDB_OTLP_TRANSPORTS": "http,grpc",
            "OTEL_HTTP_ADDR": f"127.0.0.1:{httpd.server_port}",
            "OTEL_GRPC_ADDR": f"127.0.0.1:{grpc_sock.getsockname()[1]}",
        }
        try:
            assert _healthcheck(env) == 0
            grpc_sock.close()
            assert _healthcheck(env) == 1
        finally:
            httpd.shutdown()
            thread.join()
