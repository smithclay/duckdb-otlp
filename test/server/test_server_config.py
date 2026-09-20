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
import re
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


def test_aws_ducklake_uses_the_shared_credential_chain_and_local_catalog(tmp_path):
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
    # The same chain the parquet and s3-tables modes build. Hardcoding CHAIN instance here
    # meant AWS_PROFILE worked in one AWS mode and was silently ignored in another.
    assert "CHAIN env" in out
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
        "R2_BUCKET": "mybucket",
        "CLOUDFLARE_CATALOG_URI": "https://catalog.example/uri",
        "CLOUDFLARE_API_TOKEN": SECRET,
        "R2_ACCESS_KEY_ID": SECRET,
        "R2_SECRET_ACCESS_KEY": SECRET,
    }
    result = run(env, tmp_path)
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert SECRET not in out, "a secret value leaked into generated SQL"
    assert "getenv(" not in out, "getenv() is not available in the embedded daemon; use getvariable()"
    assert "getvariable('env_CLOUDFLARE_API_TOKEN')" in out
    assert "getvariable('env_R2_ACCESS_KEY_ID')" in out
    assert "getvariable('env_R2_SECRET_ACCESS_KEY')" in out


def test_r2_neon_ducklake_secrets_use_getvariable(tmp_path):
    env = {
        "DUCKDB_MODE": "r2-neon-ducklake",
        "DUCKDB_OTLP_TOKEN": "a-private-token-123456",
        "R2_BUCKET": "mybucket",
        "R2_ACCESS_KEY_ID": SECRET,
        "R2_SECRET_ACCESS_KEY": SECRET,
        "R2_ENDPOINT": "https://acct.r2.cloudflarestorage.com",
        "PGHOST": "db.example",
        "PGDATABASE": "lake",
        "PGUSER": "lakeuser",
        "PGPASSWORD": SECRET,
    }
    result = run(env, tmp_path)
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert SECRET not in out, "a secret value leaked into generated SQL"
    assert "getenv(" not in out, "getenv() is not available in the embedded daemon; use getvariable()"
    assert "getvariable('env_PGPASSWORD')" in out
    assert "getvariable('env_R2_ACCESS_KEY_ID')" in out


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
    env.update({"PGPORT": "5433", "PGSSLMODE": "disable", "DUCKDB_CATALOG": "trace-lake", "DUCKDB_SCHEMA": "traces"})
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


def free_port():
    """Shared by the test modules so a fix for the bind-then-reuse race lands in one place."""
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _healthcheck(env: dict) -> int:
    """Run the daemon's `doctor` subcommand with a clean env; return its exit code."""
    full_env = {"PATH": os.environ.get("PATH", "")}
    full_env.update(env)
    return subprocess.run(
        [str(SERVER_BIN), "doctor"],
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


def test_healthcheck_probes_the_configured_quack_port():
    """The Quack probe has to resolve the same address startup binds.

    It used to read only DUCKDB_QUACK_ADDR/QUACK_HTTP_ADDR and fall back to 9494, while
    ServerConfig gives DUCKDB_QUACK_PORT (what `--quack PORT` sets) precedence — so
    `--quack 9999` bound 9999 and the container HEALTHCHECK probed 9494 forever. Both now go
    through QuackAddrFromEnv.
    """
    import http.server
    import threading

    class Ready(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if self.path in ("/readyz", "/") else 404)
            self.end_headers()

        def log_message(self, *args):
            pass

    with (
        http.server.ThreadingHTTPServer(("127.0.0.1", 0), Ready) as otlp,
        http.server.ThreadingHTTPServer(("127.0.0.1", 0), Ready) as quack,
    ):
        threads = [threading.Thread(target=s.serve_forever, daemon=True) for s in (otlp, quack)]
        for thread in threads:
            thread.start()
        env = {
            "DUCKDB_OTLP_TRANSPORTS": "http",
            "OTEL_HTTP_ADDR": f"127.0.0.1:{otlp.server_port}",
            "DUCKDB_QUACK_ENABLED": "1",
            "DUCKDB_QUACK_PORT": str(quack.server_port),
        }
        try:
            assert _healthcheck(env) == 0
            # A port nothing is listening on must fail, so the pass above is not vacuous.
            assert _healthcheck({**env, "DUCKDB_QUACK_PORT": str(quack.server_port + 1)}) == 1
        finally:
            for server in (otlp, quack):
                server.shutdown()
            for thread in threads:
                thread.join()


# --- --init-sql / DUCKDB_OTLP_INIT_SQL -------------------------------------------------
#
# The escape hatch for DuckDB configuration the modes do not model. Resolved to contents
# during config resolution, so DRY_RUN both validates the path and prints the SQL -- which
# is what makes these tests possible without opening a database.


def test_init_sql_lands_between_mode_setup_and_serve(tmp_path):
    """Position is the contract: after the catalog ATTACH, before ingest starts.

    Earlier and the script could not reference the telemetry catalog; later and it could
    not configure the server that is already running.
    """
    script = tmp_path / "init.sql"
    script.write_text("SET memory_limit='2GB';\n")
    result = run({"DUCKDB_OTLP_INIT_SQL": str(script)}, tmp_path)
    assert result.returncode == 0, result.stderr
    out = result.stdout
    assert "SET memory_limit='2GB';" in out
    assert out.index("ATTACH 'ducklake:") < out.index("SET memory_limit")
    assert out.index("SET memory_limit") < out.index("FROM otlp_serve(")
    # The banner names the script, so a serve log says which file shaped the instance.
    assert f"Init SQL: {script}" in out


def test_init_sql_missing_file_is_a_hard_error(tmp_path):
    """A script that cannot be read must not degrade to "start without it": the operator
    asked for an ATTACH or a limit that the running server would silently lack."""
    result = run({"DUCKDB_OTLP_INIT_SQL": str(tmp_path / "nope.sql")}, tmp_path)
    assert result.returncode == 1
    assert "Could not open --init-sql script" in result.stderr


def test_init_sql_directory_is_rejected(tmp_path):
    # An ifstream on a directory opens on some platforms and reads nothing, which would
    # turn a mistyped path into a silent no-op.
    result = run({"DUCKDB_OTLP_INIT_SQL": str(tmp_path)}, tmp_path)
    assert result.returncode == 1
    assert "is a directory, not a SQL file" in result.stderr


def test_empty_init_sql_is_a_no_op(tmp_path):
    # An operator templating this file into a container should be able to render it empty.
    script = tmp_path / "init.sql"
    script.write_text("\n   \n")
    result = run({"DUCKDB_OTLP_INIT_SQL": str(script)}, tmp_path)
    assert result.returncode == 0, result.stderr
    assert "FROM otlp_serve(" in result.stdout


def test_every_missing_setting_is_reported_in_one_run(tmp_path):
    """Configuration errors are reported as a set, not one per run.

    Reporting the first miss and stopping turned configuring a remote lakehouse into a
    guessing game: r2-data-catalog took six runs to satisfy because each named a single
    variable. The count in the header is what makes "am I nearly there?" answerable.
    """
    result = run({"DUCKDB_MODE": "r2-data-catalog"}, tmp_path)
    assert result.returncode != 0
    assert "Missing 3 required settings" in result.stderr
    for name in ("CLOUDFLARE_API_TOKEN", "R2_BUCKET", "CLOUDFLARE_ACCOUNT_ID"):
        assert name in result.stderr


def test_a_variable_several_settings_need_is_listed_once(tmp_path):
    """CLOUDFLARE_ACCOUNT_ID feeds both the endpoint and the warehouse; one miss, one line."""
    result = run({"DUCKDB_MODE": "r2-data-catalog"}, tmp_path)
    assert result.stderr.count("CLOUDFLARE_ACCOUNT_ID") == 1


def test_r2_catalog_uri_is_derived_from_account_and_bucket(tmp_path):
    """The catalog URI is mechanically derivable from two values the mode already requires.

    It was the one setting an operator had to assemble by hand, while the warehouse -- built
    from the same two -- was already derived.
    """
    result = run(
        {
            "DUCKDB_MODE": "r2-data-catalog",
            "CLOUDFLARE_API_TOKEN": SECRET,
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "R2_BUCKET": "mybucket",
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "ENDPOINT 'https://catalog.cloudflarestorage.com/acct123/mybucket'" in result.stdout
    assert "ATTACH 'acct123_mybucket'" in result.stdout


def test_storage_credentials_fall_back_to_the_duckdb_secret_store(tmp_path):
    """No key pair in the environment is a supported configuration, not an error.

    DuckDB loads a CREATE PERSISTENT SECRET from its own store automatically, so emitting a
    CREATE OR REPLACE SECRET here would shadow the stored one with an empty credential.
    """
    result = run(
        {
            "DUCKDB_MODE": "r2-local-ducklake",
            "R2_BUCKET": "mybucket",
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "CREATE OR REPLACE SECRET r2_storage" not in result.stdout
    # The banner must say so: "using your keys" and "hoping a stored secret exists" are very
    # different deployments and the difference must not be invisible.
    assert "Credentials: DuckDB secret store" in result.stdout


def test_half_an_r2_key_pair_is_an_error_not_a_silent_fallback(tmp_path):
    """One half set is a typo. Falling through to the secret store would hide it until the
    first seal failed against R2."""
    result = run(
        {
            "DUCKDB_MODE": "r2-local-ducklake",
            "R2_BUCKET": "mybucket",
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "R2_ACCESS_KEY_ID": SECRET,
        },
        tmp_path,
    )
    assert result.returncode != 0
    assert "R2_SECRET_ACCESS_KEY" in result.stderr
    assert SECRET not in result.stderr


def test_secret_dir_is_set_before_anything_touches_the_secret_manager(tmp_path):
    """secret_directory has to be set before the first CREATE SECRET initializes the manager."""
    secrets = tmp_path / "secrets"
    result = run(
        {
            "DUCKDB_MODE": "r2-local-ducklake",
            "R2_BUCKET": "mybucket",
            "CLOUDFLARE_ACCOUNT_ID": "acct123",
            "DUCKDB_OTLP_SECRET_DIR": str(secrets),
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    sql = result.stdout[result.stdout.index("Generated initialization SQL:") :]
    assert f"SET secret_directory = '{secrets}';" in sql
    assert sql.index("SET secret_directory") < sql.index("INSTALL")


def test_mode_none_attaches_nothing_and_leaves_the_target_to_init_sql(tmp_path):
    """The escape hatch: --init-sql layered on an unwanted local-ducklake ATTACH was the only
    way to reach a layout the eight presets do not cover (S3 data, Postgres catalog)."""
    script = tmp_path / "attach.sql"
    script.write_text("ATTACH 'ducklake:postgres:dbname=lake' AS lake (DATA_PATH 's3://b/p');\n")
    result = run(
        {
            "DUCKDB_MODE": "none",
            "DUCKDB_OTLP_INIT_SQL": str(script),
            "DUCKDB_CATALOG": "lake",
        },
        tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "INSTALL ducklake" not in result.stdout
    assert "ATTACH 'ducklake:postgres:dbname=lake'" in result.stdout
    assert "catalog := 'lake'" in result.stdout


def test_mode_none_without_init_sql_writes_to_the_control_database(tmp_path):
    """A bare `none` is still a working server, not a half-configured one."""
    result = run({"DUCKDB_MODE": "none"}, tmp_path)
    assert result.returncode == 0, result.stderr
    assert "catalog := ''" in result.stdout
    assert "Mode: none" in result.stdout


# Minimal configuration for each mode, so every mode can be resolved far enough to report the
# extensions it needs. Mirrors the required-settings table in the CLI reference.
MODE_FIXTURES = {
    "local-ducklake": {},
    "none": {},
    "parquet": {},
    "parquet-s3": {"DUCKDB_MODE": "parquet", "S3_BUCKET": "b", "AWS_REGION": "us-east-1"},
    "aws-ducklake": {"DUCKLAKE_DATA_PATH": "s3://b/p", "AWS_REGION": "us-east-1"},
    "gcp-ducklake": {
        "DUCKLAKE_DATA_PATH": "gcss://b/p",
        "PGHOST": "h",
        "PGDATABASE": "d",
        "PGUSER": "u",
        "PGPASSWORD": SECRET,
    },
    "r2-local-ducklake": {"R2_BUCKET": "b", "CLOUDFLARE_ACCOUNT_ID": "a"},
    "r2-neon-ducklake": {
        "R2_BUCKET": "b",
        "CLOUDFLARE_ACCOUNT_ID": "a",
        "PGHOST": "h",
        "PGDATABASE": "d",
        "PGUSER": "u",
        "PGPASSWORD": SECRET,
    },
    "r2-data-catalog": {"R2_BUCKET": "b", "CLOUDFLARE_ACCOUNT_ID": "a", "CLOUDFLARE_API_TOKEN": SECRET},
    "s3-tables": {"S3_TABLES_BUCKET_ARN": "arn:aws:s3tables:us-west-2:1:bucket/b"},
}


def extensions_reported_by(mode: str, tmp_path) -> dict:
    """The banner's Extensions block for `mode`, as {name: source}."""
    env = dict(MODE_FIXTURES[mode])
    env.setdefault("DUCKDB_MODE", mode)
    result = run(env, tmp_path)
    assert result.returncode == 0, f"{mode}: {result.stderr}"
    reported = {}
    in_block = False
    for line in result.stdout.splitlines():
        if line.startswith("Extensions:"):
            in_block = True
            continue
        if in_block:
            if not line.strip():
                break
            name, _, note = line.strip().partition(" ")
            reported[name] = note.strip("()") or "core"
    return reported


@pytest.mark.parametrize("mode", sorted(MODE_FIXTURES))
def test_generated_sql_installs_exactly_what_the_banner_reports(mode, tmp_path):
    """The banner and the setup SQL are two renderings of one declaration.

    They used to be written out separately -- a list for the banner, hand-typed INSTALL/LOAD
    for the SQL -- so a mode could load an extension it never mentioned, or name one it never
    loaded, with no symptom beyond a misleading banner.
    """
    env = dict(MODE_FIXTURES[mode])
    env.setdefault("DUCKDB_MODE", mode)
    result = run(env, tmp_path)
    assert result.returncode == 0, result.stderr
    reported = extensions_reported_by(mode, tmp_path)

    installed = set(re.findall(r"^INSTALL ([a-z0-9_]+)", result.stdout, re.M))
    loaded = set(re.findall(r"^LOAD ([a-z0-9_]+);", result.stdout, re.M))
    # `otlp` is statically embedded: reported, never installed. Everything else is both.
    needs_install = {name for name, source in reported.items() if source != "built in"}
    assert installed == needs_install
    assert loaded == needs_install
    assert "otlp" not in installed, "the statically embedded extension must never be INSTALLed"
    # A community extension needs its repository named or the INSTALL resolves nowhere.
    for name, source in reported.items():
        if source == "community":
            assert f"INSTALL {name} FROM community;" in result.stdout


def test_the_image_primes_every_extension_some_mode_needs(tmp_path):
    """The container's offline extension cache is a third copy of this list, in another
    language and build stage, so it cannot share the declaration -- but it can be checked.

    If a mode gains an extension the image does not prime, the daemon's startup INSTALL goes to
    the network, or fails outright in an offline deployment. That is invisible until someone
    runs the image without egress.
    """
    dockerfile = (REPO_ROOT / "docker" / "duckdb-otlp-server" / "Dockerfile").read_text()
    primed = set(re.search(r"for ext in ([a-z0-9 ]+); do", dockerfile).group(1).split())
    primed |= set(re.findall(r"INSTALL ([a-z0-9_]+) FROM community", dockerfile))

    needed = set()
    for mode in MODE_FIXTURES:
        needed |= {name for name, source in extensions_reported_by(mode, tmp_path).items() if source != "built in"}

    missing = needed - primed
    assert not missing, (
        f"{sorted(missing)} is needed by a mode but not primed into the image's extension cache "
        f"(docker/duckdb-otlp-server/Dockerfile). Add it there, or the container will reach the "
        f"network on startup."
    )
