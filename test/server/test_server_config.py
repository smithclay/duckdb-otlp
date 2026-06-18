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
DEFAULT_BIN = REPO_ROOT / "build" / "release" / "extension" / "otlp" / "duckdb_otlp_server"
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


def test_missing_mode_fails_fast(tmp_path):
    result = run({}, tmp_path)  # no DUCKDB_MODE
    assert result.returncode == 1
    assert "DUCKDB_MODE" in result.stderr
    # Error is unwrapped, not a JSON blob.
    assert "exception_type" not in result.stderr


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


def test_default_token_warns(tmp_path):
    result = run({"DUCKDB_MODE": "local-ducklake"}, tmp_path)  # no token set
    assert result.returncode == 0, result.stderr
    assert "WARNING: using the built-in development OTLP token" in result.stdout


def test_explicit_token_does_not_warn(tmp_path):
    result = run(
        {"DUCKDB_MODE": "local-ducklake", "DUCKDB_OTLP_TOKEN": "a-private-token-123456"},
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "WARNING: using the built-in development OTLP token" not in result.stdout


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
