"""CLI-surface tests for the ``duckdb-otlp`` binary.

These cover the command-line layer that sits in front of ``ServerConfig``: subcommand
dispatch, flag/env precedence, the OTLP environment-variable contract, the authentication
and the authentication rules.

Everything here runs against the real binary with a *scrubbed* environment (``env -i`` in
spirit: only PATH and HOME survive), because the whole point of several of these tests is
what happens when a variable is, or is not, present. Inheriting the ambient shell would make
them pass or fail depending on who ran them.

Point the tests at a binary with DUCKDB_OTLP_SERVER_BIN, or rely on the default
``build/release`` path. If the binary is missing the whole module is skipped.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from test_server_config import REPO_ROOT, SERVER_BIN, free_port, pytestmark  # noqa: F401  (pytestmark re-export)

TOKEN = "a-private-token-123456"


def run(args, env=None, home: Path | None = None, timeout: int = 120):
    """Run the CLI with a scrubbed environment plus `env`.

    HOME is redirected into the test's tmp_path so the XDG data-directory default cannot
    touch the real user's home, and so DuckDB's extension cache stays per-test.
    """
    full_env = {"PATH": os.environ.get("PATH", "")}
    if home is not None:
        full_env["HOME"] = str(home)
    full_env.update(env or {})
    return subprocess.run(
        [str(SERVER_BIN), *args],
        env=full_env,
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(REPO_ROOT),
    )


# --------------------------------------------------------------------------------------
# Subcommand dispatch
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("args", [["help"], ["--help"], ["-h"], ["help", "doctor"], ["help", "validate"]])
def test_help_variants_exit_zero(args, tmp_path):
    result = run(args, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "duckdb-otlp" in result.stdout


def test_version_prints_a_version(tmp_path):
    result = run(["version"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert result.stdout.startswith("duckdb-otlp ")


def test_unknown_flag_is_rejected_with_guidance(tmp_path):
    result = run(["--nonsense"], home=tmp_path)
    assert result.returncode == 2  # usage error
    assert "Unknown flag" in result.stderr
    assert "duckdb-otlp help" in result.stderr


# --------------------------------------------------------------------------------------
# Zero-config defaults (the laptop path)
# --------------------------------------------------------------------------------------


def test_bare_validate_needs_no_configuration(tmp_path):
    """A bare invocation must resolve to a working local lakehouse on loopback."""
    result = run(["validate"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Mode: local-ducklake" in result.stdout
    # Loopback, both standard OTLP transports.
    assert "OTLP http: otlp:127.0.0.1:4318" in result.stdout
    assert "OTLP grpc: otlp:127.0.0.1:4317" in result.stdout
    # The data directory follows XDG rather than the container's /data.
    assert str(tmp_path / ".local" / "share" / "duckdb-otlp") in result.stdout


def test_xdg_data_home_is_honored(tmp_path):
    target = tmp_path / "xdg"
    result = run(["validate"], env={"XDG_DATA_HOME": str(target)}, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert str(target / "duckdb-otlp") in result.stdout


# --------------------------------------------------------------------------------------
# Authentication
# --------------------------------------------------------------------------------------


def test_loopback_without_token_disables_auth(tmp_path):
    result = run(["validate"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Authentication is DISABLED" in result.stdout
    # The generated SQL must ask for disable_auth, never fall back to a shared default token.
    assert "disable_auth := true" in result.stdout
    assert "token :=" not in result.stdout


def test_non_loopback_without_token_is_rejected(tmp_path):
    result = run(["validate", "--host", "0.0.0.0"], home=tmp_path)
    assert result.returncode == 1
    assert "bearer token is required" in result.stderr
    # The message must name both ways out.
    assert "DUCKDB_OTLP_TOKEN" in result.stderr
    assert "--no-auth" in result.stderr


def test_no_builtin_default_token_anywhere(tmp_path):
    """The previously hard-coded development token must be gone from the binary's behavior."""
    result = run(["validate", "--host", "0.0.0.0", "--token", TOKEN], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "dev-otlp-token" not in result.stdout


def test_non_loopback_with_token_uses_a_session_variable(tmp_path):
    result = run(["validate", "--host", "0.0.0.0", "--token", TOKEN], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "token := getvariable('duckdb_otlp_effective_token')" in result.stdout
    # The secret itself must never reach the generated SQL text.
    assert TOKEN not in result.stdout


def test_short_token_is_rejected_by_name(tmp_path):
    result = run(["validate", "--host", "0.0.0.0", "--token", "short"], home=tmp_path)
    assert result.returncode == 1
    assert "at least 16 characters" in result.stderr


def test_no_auth_on_a_public_bind_warns(tmp_path):
    result = run(["validate", "--host", "0.0.0.0", "--no-auth"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "WARNING: authentication is DISABLED by request" in result.stdout


# --------------------------------------------------------------------------------------
# Listener selection and flag/env precedence
# --------------------------------------------------------------------------------------


def test_explicit_port_selects_only_that_transport(tmp_path):
    result = run(["validate", "--grpc", "4317"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "OTLP grpc: otlp:127.0.0.1:4317" in result.stdout
    assert "OTLP http:" not in result.stdout


def test_zero_port_disables_a_transport(tmp_path):
    result = run(["validate", "--http", "0", "--grpc", "4317"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "OTLP http:" not in result.stdout
    assert "OTLP grpc:" in result.stdout


@pytest.mark.parametrize(
    "off,stays_on",
    [("--grpc", "OTLP http:"), ("--http", "OTLP grpc:")],
)
def test_zero_port_disables_only_that_transport(off, stays_on, tmp_path):
    """A port of 0 turns one transport off; it must not un-default the others.

    Only a NON-ZERO port narrows the listener set to what was named, so `--grpc 0` means
    "turn gRPC off, keep the rest" rather than "turn everything off".
    """
    result = run(["validate", off, "0"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert stays_on in result.stdout


def test_disabling_every_listener_is_an_error(tmp_path):
    result = run(["validate", "--http", "0", "--grpc", "0"], home=tmp_path)
    assert result.returncode == 1
    assert "Every listener is disabled" in result.stderr


def test_flag_beats_environment(tmp_path):
    result = run(["validate", "--http", "4318"], env={"DUCKDB_OTLP_HTTP_PORT": "9999"}, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "otlp:127.0.0.1:4318" in result.stdout
    assert "9999" not in result.stdout


def test_environment_beats_default(tmp_path):
    result = run(["validate"], env={"DUCKDB_OTLP_HTTP_PORT": "9999"}, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "otlp:127.0.0.1:9999" in result.stdout


def test_host_flag_moves_every_listener(tmp_path):
    result = run(["validate", "--host", "0.0.0.0", "--no-auth"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "otlp:0.0.0.0:4318" in result.stdout
    assert "otlp:0.0.0.0:4317" in result.stdout


def test_legacy_transport_list_still_works(tmp_path):
    """The container image and existing deployments set these; they must keep working."""
    result = run(
        ["validate"],
        env={
            "DUCKDB_OTLP_TRANSPORTS": "http,grpc",
            "OTEL_HTTP_ADDR": "127.0.0.1:5318",
            "OTEL_GRPC_ADDR": "127.0.0.1:5317",
        },
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "otlp:127.0.0.1:5318" in result.stdout
    assert "otlp:127.0.0.1:5317" in result.stdout


def test_ports_supersede_the_legacy_transport_list(tmp_path):
    """A container pins DUCKDB_OTLP_TRANSPORTS, so a --grpc flag has to win over it."""
    result = run(
        ["validate", "--grpc", "4317"],
        env={"DUCKDB_OTLP_TRANSPORTS": "http"},
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "OTLP grpc:" in result.stdout
    assert "OTLP http:" not in result.stdout


def test_duplicate_ports_are_rejected(tmp_path):
    result = run(["validate", "--http", "4317", "--grpc", "4317"], home=tmp_path)
    assert result.returncode == 1
    assert "must use different ports" in result.stderr


def test_otap_cannot_be_combined_with_standard_otlp(tmp_path):
    result = run(["validate", "--otap", "4317", "--http", "4318"], home=tmp_path)
    assert result.returncode == 1
    assert "OTAP/Arrow cannot be combined" in result.stderr


def test_otap_alone_uses_otap_serve(tmp_path):
    result = run(["validate", "--otap", "4317"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "otap:127.0.0.1:4317" in result.stdout
    assert "FROM otap_serve(" in result.stdout


# --------------------------------------------------------------------------------------
# Standard OTLP environment variables
# --------------------------------------------------------------------------------------


def test_invalid_port_in_a_legacy_addr_is_rejected(tmp_path):
    """A port that is present but out of range must fail, not silently fall back to the default."""
    result = run(
        ["validate"],
        env={"OTEL_GRPC_ADDR": "0.0.0.0:70000", "DUCKDB_OTLP_TRANSPORTS": "grpc"},
        home=tmp_path,
    )
    assert result.returncode == 1
    assert "OTEL_GRPC_ADDR" in result.stderr


@pytest.mark.parametrize(
    "name,value",
    [
        ("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "http://localhost:4318"),
        ("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://localhost:4318"),
        ("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://localhost:4318"),
        ("OTEL_EXPORTER_OTLP_CERTIFICATE", "/etc/ssl/ca.pem"),
        ("OTEL_EXPORTER_OTLP_CLIENT_KEY", "/etc/ssl/key.pem"),
        ("OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "/etc/ssl/cert.pem"),
    ],
)
def test_unsupportable_otel_vars_fail_loudly(name, value, tmp_path):
    """Silently ignoring a TLS cert or a per-signal endpoint would mislead about real behavior."""
    result = run(["validate"], env={name: value}, home=tmp_path)
    assert result.returncode == 1
    assert name in result.stderr
    assert "cannot be honored" in result.stderr


def parquet_mode_env(tmp_path):
    """A mode that needs no downloadable extension."""
    return {
        "DUCKDB_MODE": "parquet",
        "DUCKDB_OTLP_DATA_DIR": str(tmp_path / "data"),
        "DUCKDB_DATABASE": str(tmp_path / "data" / "control.duckdb"),
        "PARQUET_EXPORT_PATH": str(tmp_path / "data" / "parquet"),
    }


# --------------------------------------------------------------------------------------
# Flags are layered over the environment, not written into it
# --------------------------------------------------------------------------------------


def test_the_cli_never_mutates_the_process_environment():
    """Flag values reach ServerConfig through an EnvSource, never through setenv().

    This is a source guard rather than a behavioural test on purpose: the difference is not
    observable from outside the process (setenv() does not rewrite the exec-time block that
    /proc/<pid>/environ exposes). What it buys is that a flag value — a --token above all —
    never becomes visible to getenv() elsewhere in the process, and that there is one
    explicit configuration source instead of a global mutated in the right order by hand.
    """
    offenders = []
    for source in sorted((REPO_ROOT / "src" / "server").glob("*.cpp")):
        text = source.read_text()
        if "setenv(" in text or "_putenv_s(" in text:
            offenders.append(source.name)
    assert offenders == [], f"these write to the process environment: {offenders}"


# --------------------------------------------------------------------------------------
# Bind-address resolution
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("args", "env", "expected"),
    [
        (["--host", "::1"], {}, "otlp:[::1]:4318"),
        (["--host", "[::1]"], {}, "otlp:[::1]:4318"),
        # The IPv6 wildcard is not loopback, so it needs auth opted out like 0.0.0.0 does.
        (["--no-auth"], {"DUCKDB_OTLP_TRANSPORTS": "http", "OTEL_HTTP_ADDR": "[::]:4318"}, "otlp:[::]:4318"),
    ],
)
def test_ipv6_hosts_are_bracketed_in_the_listener_uri(args, env, expected, tmp_path):
    """An IPv6 literal has to be re-bracketed when host and port are recombined.

    Both sources hand the host over unbracketed — OtlpUri::Host() strips the brackets it
    parsed, and --host/DUCKDB_OTLP_HOST take a bare literal — so "::1" + ":4318" otherwise
    reads as one colon-separated string and the port parse fails.
    """
    result = run(["validate", *args], env=env, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert expected in result.stdout


def test_an_explicit_otel_addr_host_beats_an_inherited_duckdb_otlp_host(tmp_path):
    """The image sets DUCKDB_OTLP_HOST=0.0.0.0; narrowing the bind must not be widened back.

    Between two environment variables the more specific one wins, so a host spelled out in
    OTEL_HTTP_ADDR beats the global DUCKDB_OTLP_HOST.
    """
    result = run(
        ["validate"],
        env={
            "DUCKDB_OTLP_HOST": "0.0.0.0",
            "DUCKDB_OTLP_TRANSPORTS": "http",
            "OTEL_HTTP_ADDR": "127.0.0.1:4318",
        },
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "otlp:127.0.0.1:4318" in result.stdout


def test_the_host_flag_still_moves_every_listener(tmp_path):
    """--host is the most specific thing the user said, so it beats an OTEL_*_ADDR host too."""
    result = run(
        ["validate", "--host", "0.0.0.0", "--no-auth"],
        env={"DUCKDB_OTLP_TRANSPORTS": "http", "OTEL_HTTP_ADDR": "127.0.0.1:4318"},
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "otlp:0.0.0.0:4318" in result.stdout


def test_quack_zero_disables_quack(tmp_path):
    """`--quack 0` means off, like --http 0 / --grpc 0 — not "enable on an ephemeral port"."""
    result = run(["validate", "--quack", "0"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Quack: disabled" in result.stdout


def test_quack_port_enables_quack(tmp_path):
    result = run(["validate", "--quack", "9999", "--quack-token", "0123456789abcdef"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Quack: quack:127.0.0.1:9999" in result.stdout


# --------------------------------------------------------------------------------------
# Ergonomics: a mistake must not start a server
#
# A bare `duckdb-otlp` means serve, and unrecognized argv used to fall through to it — so
# `duckdb-otlp covert traces.pb` bound ports and created a DuckLake instead of reporting a
# typo, and the two stray words were never mentioned.
# --------------------------------------------------------------------------------------


def test_an_unrecognizable_word_gets_no_guess(tmp_path):
    """A wrong guess is worse than none; DuckDB's TopNLevenshtein always returns its best."""
    result = run(["zzzzzzzz"], home=tmp_path, timeout=30)
    assert result.returncode == 2  # usage error
    assert "Did you mean" not in result.stderr


@pytest.mark.parametrize("command", ["doctor", "healthcheck"])
def test_doctor_names_the_listener_it_could_not_reach(command, tmp_path):
    """`healthcheck` stays accepted: the container HEALTHCHECK and compose probes call it."""
    result = run([command], env={"DUCKDB_OTLP_TRANSPORTS": "grpc"}, home=tmp_path, timeout=30)
    assert result.returncode == 1
    assert "FAIL" in result.stdout and "4317" in result.stdout


def test_doctor_runs_every_check_not_just_the_first(tmp_path):
    """A partial answer — http up, grpc down — is the point of the command."""
    result = run(["doctor"], home=tmp_path, timeout=30)
    assert result.returncode == 1
    assert "4318" in result.stdout and "4317" in result.stdout


def test_doctor_reports_each_check_that_passed(tmp_path):
    import http.server
    import threading

    class Ready(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200 if self.path == "/readyz" else 404)
            self.end_headers()

        def log_message(self, *args):
            pass

    with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Ready) as httpd:
        thread = threading.Thread(target=httpd.serve_forever, daemon=True)
        thread.start()
        try:
            result = run(
                ["doctor"],
                env={"DUCKDB_OTLP_TRANSPORTS": "http", "OTEL_HTTP_ADDR": f"127.0.0.1:{httpd.server_port}"},
                home=tmp_path,
                timeout=30,
            )
            assert result.returncode == 0, result.stderr
            assert result.stdout.startswith("ok")
        finally:
            httpd.shutdown()
            thread.join()


def test_validate_does_not_claim_to_start_a_server(tmp_path):
    result = run(["validate"], env=parquet_mode_env(tmp_path), home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Starting duckdb-otlp server" not in result.stdout
    assert "Checking duckdb-otlp configuration" in result.stdout


def test_the_banner_names_where_data_lands_not_just_the_control_database(tmp_path):
    """In local-ducklake the banner named only the control DB, which holds no telemetry."""
    result = run(["validate"], env=parquet_mode_env(tmp_path), home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Data: " in result.stdout
    assert "(control)" in result.stdout


def test_help_lists_the_modes(tmp_path):
    result = run(["help"], home=tmp_path)
    assert result.returncode == 0
    for mode in ("local-ducklake", "parquet", "aws-ducklake", "s3-tables"):
        assert mode in result.stdout


# --------------------------------------------------------------------------------------
# Output discipline, exit codes, and a machine-readable mode
# --------------------------------------------------------------------------------------


def test_the_startup_banner_is_visible_while_the_server_runs(tmp_path):
    """std::cout is fully buffered when stdout is not a terminal, so a redirected log held
    the whole banner until the process exited — `docker logs` on a healthy container showed
    nothing about how it had started."""
    import time

    log = tmp_path / "server.log"
    env = {
        "PATH": os.environ.get("PATH", ""),
        "HOME": str(tmp_path),
        "DUCKDB_MODE": "parquet",
        "DUCKDB_DATABASE": ":memory:",
        "PARQUET_EXPORT_PATH": str(tmp_path / "pq"),
        "DUCKDB_OTLP_TRANSPORTS": "http",
        "OTEL_HTTP_ADDR": f"127.0.0.1:{free_port()}",
        "DUCKDB_OTLP_STARTUP_TIMEOUT": "30",
    }
    with log.open("w") as handle:
        process = subprocess.Popen([str(SERVER_BIN)], env=env, stdout=handle, stderr=handle)
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if "Starting server" in log.read_text():
                    break
                assert process.poll() is None, log.read_text()
                time.sleep(0.5)
            # Read while the process is still alive: the point is that nothing waits for exit.
            assert process.poll() is None
            running_output = log.read_text()
            assert "Mode: parquet" in running_output
            assert "OTLP http" in running_output
        finally:
            process.terminate()
            try:
                process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


def test_doctor_json_is_parseable_and_agrees_with_the_exit_code(tmp_path):
    import json

    result = run(["doctor", "--json"], env={"DUCKDB_OTLP_TRANSPORTS": "grpc"}, home=tmp_path, timeout=30)
    assert result.returncode == 1
    report = json.loads(result.stdout)
    assert report["healthy"] is False
    assert [check["ok"] for check in report["checks"]] == [False]
    assert report["checks"][0]["check"] == "OTLP grpc"


def test_an_ipv6_host_is_bracketed_in_the_quack_address_too(tmp_path):
    """The listener path bracketed and the Quack path did not, so `--host ::1 --quack 9494`
    produced "::1:9494", which nothing can parse back."""
    result = run(
        ["validate", "--host", "::1", "--quack", "9494", "--quack-token", TOKEN],
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "quack:[::1]:9494" in result.stdout
    assert "otlp:[::1]:4318" in result.stdout


@pytest.mark.parametrize(
    ("args", "belongs_to"),
    [
        (["doctor", "--init-sql", "x.sql"], "serve, validate"),
        (["validate", "--json"], "doctor"),
        (["doctor", "--dry-run"], "serve, validate"),
    ],
)
def test_a_flag_of_another_command_is_rejected_by_name(args, belongs_to, tmp_path):
    """doctor must not run operator SQL (the container HEALTHCHECK calls it on every probe)."""
    result = run(args, home=tmp_path)
    assert result.returncode == 2  # usage error
    assert "does not accept" in result.stderr
    assert belongs_to in result.stderr


@pytest.mark.parametrize(("word", "expected"), [("valdate", "validate"), ("doctr", "doctor"), ("healthchek", "doctor")])
def test_a_mistyped_subcommand_is_rejected_with_a_suggestion(word, expected, tmp_path):
    """A bare `duckdb-otlp` means serve, so a typo must not fall through and bind ports."""
    result = run([word], home=tmp_path, timeout=30)
    assert result.returncode == 2  # usage error
    assert "Unknown command" in result.stderr
    assert f"duckdb-otlp {expected}" in result.stderr


@pytest.mark.parametrize("command", ["serve", "validate", "doctor"])
def test_commands_reject_positional_arguments(command, tmp_path):
    result = run([command, "some-file.pb"], home=tmp_path, timeout=30)
    assert result.returncode == 2  # usage error
    assert "takes no arguments" in result.stderr


@pytest.mark.parametrize(
    ("args", "code"), [(["version"], 0), (["nonsense"], 2), (["doctor", "--bogus"], 2), (["doctor"], 1)]
)
def test_a_usage_error_exits_2_and_a_failure_exits_1(args, code, tmp_path):
    result = run(args, env={"DUCKDB_OTLP_TRANSPORTS": "grpc"}, home=tmp_path, timeout=30)
    assert result.returncode == code, result.stderr
