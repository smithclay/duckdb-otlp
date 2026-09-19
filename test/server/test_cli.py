"""CLI-surface tests for the ``duckdb-otlp`` binary.

These cover the command-line layer that sits in front of ``ServerConfig``: subcommand
dispatch, flag/env precedence, the OTLP environment-variable contract, the authentication
rules, and the ``convert``/``export``/``query`` subcommands.

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

from test_server_config import REPO_ROOT, SERVER_BIN, pytestmark  # noqa: F401  (pytestmark re-export)

DATA_DIR = REPO_ROOT / "test" / "data"
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


@pytest.mark.parametrize("args", [["help"], ["--help"], ["-h"], ["help", "convert"], ["help", "export"]])
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
    assert result.returncode == 1
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


def test_exporter_endpoint_is_ignored_entirely(tmp_path):
    """The single most important footgun: a shell pointing at a real collector.

    OTEL_EXPORTER_OTLP_ENDPOINT must neither move the listener nor fail the startup.
    """
    result = run(
        ["validate"],
        env={"OTEL_EXPORTER_OTLP_ENDPOINT": "https://api.honeycomb.io:443"},
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "otlp:127.0.0.1:4318" in result.stdout
    assert "honeycomb" not in result.stdout


@pytest.mark.parametrize(
    "protocol,expected,absent",
    [
        ("grpc", "OTLP grpc:", "OTLP http:"),
        ("http/protobuf", "OTLP http:", "OTLP grpc:"),
        ("http/json", "OTLP http:", "OTLP grpc:"),
    ],
)
def test_exporter_protocol_narrows_the_default(protocol, expected, absent, tmp_path):
    result = run(["validate"], env={"OTEL_EXPORTER_OTLP_PROTOCOL": protocol}, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert expected in result.stdout
    assert absent not in result.stdout
    # The banner must say why, so a narrowed listener set is never silent.
    assert f"OTEL_EXPORTER_OTLP_PROTOCOL={protocol}" in result.stdout


def test_exporter_protocol_does_not_override_explicit_ports(tmp_path):
    result = run(["validate", "--http", "4318"], env={"OTEL_EXPORTER_OTLP_PROTOCOL": "grpc"}, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "OTLP http:" in result.stdout
    assert "OTLP grpc:" not in result.stdout


def test_invalid_port_in_a_legacy_addr_is_rejected(tmp_path):
    """A port that is present but out of range must fail, not silently fall back to the default."""
    result = run(
        ["validate"],
        env={"OTEL_GRPC_ADDR": "0.0.0.0:70000", "DUCKDB_OTLP_TRANSPORTS": "grpc"},
        home=tmp_path,
    )
    assert result.returncode == 1
    assert "OTEL_GRPC_ADDR" in result.stderr


def test_invalid_exporter_protocol_is_rejected(tmp_path):
    result = run(["validate"], env={"OTEL_EXPORTER_OTLP_PROTOCOL": "carrier-pigeon"}, home=tmp_path)
    assert result.returncode == 1
    assert "OTEL_EXPORTER_OTLP_PROTOCOL" in result.stderr


def test_exporter_headers_supply_the_token(tmp_path):
    result = run(
        ["validate", "--host", "0.0.0.0"],
        env={"OTEL_EXPORTER_OTLP_HEADERS": f"Authorization=Bearer {TOKEN}"},
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "token := getvariable('duckdb_otlp_effective_token')" in result.stdout
    assert TOKEN not in result.stdout


def test_exporter_headers_pick_authorization_out_of_several(tmp_path):
    result = run(
        ["validate", "--host", "0.0.0.0"],
        env={"OTEL_EXPORTER_OTLP_HEADERS": f"x-team=infra,Authorization=Bearer {TOKEN},x-env=prod"},
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "disable_auth" not in result.stdout


def test_explicit_token_beats_exporter_headers(tmp_path):
    result = run(
        ["validate", "--host", "0.0.0.0", "--token", TOKEN],
        env={"OTEL_EXPORTER_OTLP_HEADERS": "Authorization=Bearer short"},
        home=tmp_path,
    )
    # Would fail the 16-character floor if the header had won.
    assert result.returncode == 0, result.stderr


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


# --------------------------------------------------------------------------------------
# convert
# --------------------------------------------------------------------------------------


def test_convert_needs_no_mode_or_credentials(tmp_path):
    """`convert` is the stateless path: it must work with nothing configured at all."""
    out = tmp_path / "out"
    result = run(
        ["convert", str(DATA_DIR / "otlp_traces.pb"), "--signal", "traces", "--to", str(out) + "/"],
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert (out / "traces.parquet").exists()


def test_convert_detects_the_signal(tmp_path):
    out = tmp_path / "out"
    result = run(["convert", str(DATA_DIR / "otlp_traces.pb"), "--to", str(out) + "/"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "Detected signal(s): traces" in result.stderr
    assert (out / "traces.parquet").exists()
    # A traces file must NOT be mis-detected as another signal.
    assert not (out / "logs.parquet").exists()


def test_convert_detects_every_metric_shape(tmp_path):
    out = tmp_path / "out"
    result = run(["convert", str(DATA_DIR / "metrics_all_types.jsonl"), "--to", str(out) + "/"], home=tmp_path)
    assert result.returncode == 0, result.stderr
    for shape in ("gauge", "sum", "histogram", "exp_histogram"):
        assert (out / f"metrics_{shape}.parquet").exists(), shape


def test_convert_streams_csv_to_stdout(tmp_path):
    result = run(
        ["convert", str(DATA_DIR / "logs_simple.jsonl"), "--signal", "logs", "--format", "csv"],
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert "service_name" in result.stdout.splitlines()[0]
    assert "Application started" in result.stdout


def test_convert_rejects_parquet_on_stdout(tmp_path):
    result = run(
        ["convert", str(DATA_DIR / "logs_simple.jsonl"), "--signal", "logs", "--format", "parquet"],
        home=tmp_path,
    )
    assert result.returncode == 1
    assert "--to" in result.stderr


def test_convert_refuses_to_clobber_without_overwrite(tmp_path):
    out = tmp_path / "out"
    args = ["convert", str(DATA_DIR / "logs_simple.jsonl"), "--signal", "logs", "--to", str(out) + "/"]
    assert run(args, home=tmp_path).returncode == 0
    second = run(args, home=tmp_path)
    assert second.returncode == 1
    assert "--overwrite" in second.stderr
    assert run([*args, "--overwrite"], home=tmp_path).returncode == 0


def test_convert_unions_several_inputs(tmp_path):
    out = tmp_path / "out" / "logs.csv"
    result = run(
        [
            "convert",
            str(DATA_DIR / "logs_simple.jsonl"),
            str(DATA_DIR / "logs_simple.jsonl"),
            "--signal",
            "logs",
            "--to",
            str(out),
            "--format",
            "csv",
        ],
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    # Two copies of the same file: header once, twice the rows.
    single = run(
        ["convert", str(DATA_DIR / "logs_simple.jsonl"), "--signal", "logs", "--format", "csv"],
        home=tmp_path,
    ).stdout
    single_rows = len(single.strip().splitlines()) - 1
    doubled_rows = len(out.read_text().strip().splitlines()) - 1
    assert doubled_rows == 2 * single_rows


def test_convert_without_input_explains_itself(tmp_path):
    result = run(["convert"], home=tmp_path)
    assert result.returncode == 1
    assert "at least one input file" in result.stderr


def test_convert_rejects_an_unknown_signal_by_listing_them(tmp_path):
    result = run(["convert", str(DATA_DIR / "logs_simple.jsonl"), "--signal", "sparks"], home=tmp_path)
    assert result.returncode == 1
    assert "metrics_exp_histogram" in result.stderr


def test_export_rejects_file_arguments(tmp_path):
    """The convert/export split has to be explained, not silently guessed."""
    result = run(["export", str(DATA_DIR / "logs_simple.jsonl")], home=tmp_path)
    assert result.returncode == 1
    assert "duckdb-otlp convert" in result.stderr


# --------------------------------------------------------------------------------------
# query and export against a real catalog
# --------------------------------------------------------------------------------------


def parquet_mode_env(tmp_path):
    """A mode that needs no downloadable extension, so these run offline."""
    return {
        "DUCKDB_MODE": "parquet",
        "DUCKDB_OTLP_DATA_DIR": str(tmp_path / "data"),
        "DUCKDB_DATABASE": str(tmp_path / "data" / "control.duckdb"),
        "PARQUET_EXPORT_PATH": str(tmp_path / "data" / "parquet"),
    }


def test_query_runs_ddl_without_wrapping_it(tmp_path):
    """A COPY wrapper around DDL is a syntax error; only a trailing SELECT may be redirected."""
    env = parquet_mode_env(tmp_path)
    result = run(
        ["query", "CREATE SCHEMA IF NOT EXISTS otlp; CREATE TABLE otlp.t AS SELECT 1 AS x;"],
        env=env,
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr


def test_query_selects_after_setup_statements(tmp_path):
    env = parquet_mode_env(tmp_path)
    result = run(
        [
            "query",
            "CREATE SCHEMA IF NOT EXISTS otlp; CREATE TABLE otlp.t AS SELECT 7 AS x; SELECT x FROM otlp.t;",
            "--format",
            "csv",
        ],
        env=env,
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines() == ["x", "7"]


def test_query_reads_files_without_a_catalog(tmp_path):
    env = parquet_mode_env(tmp_path)
    result = run(
        ["query", f"SELECT count(*) AS n FROM read_otlp_logs('{DATA_DIR / 'logs_simple.jsonl'}')", "--format", "csv"],
        env=env,
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip().splitlines()[0] == "n"


def test_query_from_a_file(tmp_path):
    env = parquet_mode_env(tmp_path)
    script = tmp_path / "q.sql"
    script.write_text("SELECT 42 AS answer;")
    result = run(["query", "--file", str(script), "--format", "csv"], env=env, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert "42" in result.stdout


def test_query_rejects_both_inline_sql_and_file(tmp_path):
    script = tmp_path / "q.sql"
    script.write_text("SELECT 1;")
    result = run(["query", "SELECT 1", "--file", str(script)], env=parquet_mode_env(tmp_path), home=tmp_path)
    assert result.returncode == 1
    assert "not both" in result.stderr


def test_export_round_trips_a_catalog_table(tmp_path):
    env = parquet_mode_env(tmp_path)
    seed = run(
        [
            "query",
            "CREATE SCHEMA IF NOT EXISTS otlp; "
            f"CREATE OR REPLACE TABLE otlp.otlp_logs AS SELECT * FROM read_otlp_logs('{DATA_DIR / 'logs_simple.jsonl'}');",
        ],
        env=env,
        home=tmp_path,
    )
    assert seed.returncode == 0, seed.stderr

    out = tmp_path / "dump"
    result = run(["export", "--signal", "logs", "--to", str(out) + "/"], env=env, home=tmp_path)
    assert result.returncode == 0, result.stderr
    assert (out / "logs.parquet").exists()

    back = run(
        ["query", f"SELECT count(*) AS n FROM read_parquet('{out / 'logs.parquet'}')", "--format", "csv"],
        env=env,
        home=tmp_path,
    )
    assert back.returncode == 0, back.stderr
    assert int(back.stdout.strip().splitlines()[1]) > 0


def test_export_since_filters_rows(tmp_path):
    env = parquet_mode_env(tmp_path)
    seed = run(
        [
            "query",
            "CREATE SCHEMA IF NOT EXISTS otlp; "
            f"CREATE OR REPLACE TABLE otlp.otlp_logs AS SELECT * FROM read_otlp_logs('{DATA_DIR / 'logs_simple.jsonl'}');",
        ],
        env=env,
        home=tmp_path,
    )
    assert seed.returncode == 0, seed.stderr

    # The fixture's timestamps are from 2021, so "the last hour" must select nothing.
    out = tmp_path / "recent"
    result = run(
        ["export", "--signal", "logs", "--since", "-1h", "--to", str(out) + "/", "--format", "csv"],
        env=env,
        home=tmp_path,
    )
    assert result.returncode == 0, result.stderr
    assert len((out / "logs.csv").read_text().strip().splitlines()) == 1  # header only


def test_export_rejects_an_unknown_partition_scheme(tmp_path):
    result = run(
        ["export", "--partition-by", "hour", "--to", str(tmp_path / "x")],
        env=parquet_mode_env(tmp_path),
        home=tmp_path,
    )
    assert result.returncode == 1
    assert "--partition-by" in result.stderr


# --------------------------------------------------------------------------------------
# Per-command flag tables
#
# One global flag table used to mean every command silently accepted every flag, so
# `convert --quack 9494` parsed and then did nothing. Each command now declares the flags it
# accepts, and a misdirected flag is an error that says where the flag does belong.
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("args", "belongs_to"),
    [
        (["convert", "--quack", "9494", "x.pb"], "serve"),
        (["convert", "--mode", "parquet", "x.pb"], "serve"),
        (["convert", "--partition-by", "day", "x.pb"], "export"),
        (["query", "--token", TOKEN, "SELECT 1"], "serve"),
        (["query", "--signal", "logs", "SELECT 1"], "convert"),
        (["export", "--http", "4318"], "serve"),
        (["export", "--file", "script.sql"], "query"),
        (["--since", "-24h"], "export"),
        (["--readonly"], "query"),
    ],
)
def test_a_flag_of_another_command_is_rejected_by_name(args, belongs_to, tmp_path):
    result = run(args, home=tmp_path)
    assert result.returncode == 1
    assert "does not accept" in result.stderr
    assert belongs_to in result.stderr


def test_an_unknown_flag_names_the_command_whose_help_to_read(tmp_path):
    result = run(["convert", "--nonsense", "x.pb"], home=tmp_path)
    assert result.returncode == 1
    assert "Unknown flag" in result.stderr
    assert "help convert" in result.stderr


@pytest.mark.parametrize("command", ["export", "query"])
def test_export_and_query_accept_the_catalog_selection_flags(command, tmp_path):
    """export and query read back what `serve` wrote, so they take the flags that choose it.

    Naming a catalog on the command line has to reach the same resolution `serve` uses; the
    CATALOG_CMDS group in the flag table is what extends those five flags to these commands.
    """
    seed = run(
        [
            "query",
            "CREATE SCHEMA IF NOT EXISTS otlp; "
            f"CREATE OR REPLACE TABLE otlp.otlp_logs AS SELECT * FROM read_otlp_logs('{DATA_DIR / 'logs_simple.jsonl'}');",
        ],
        env=parquet_mode_env(tmp_path),
        home=tmp_path,
    )
    assert seed.returncode == 0, seed.stderr

    # The same catalog as parquet_mode_env, but selected entirely by flag.
    args = [
        "--mode",
        "parquet",
        "--data-dir",
        str(tmp_path / "data"),
        "--database",
        str(tmp_path / "data" / "control.duckdb"),
        "--schema",
        "otlp",
    ]
    env = {"PARQUET_EXPORT_PATH": str(tmp_path / "data" / "parquet")}
    if command == "export":
        args = ["export", "--signal", "logs", "--to", str(tmp_path / "out") + "/", "--format", "csv", *args]
    else:
        args = ["query", "SELECT count(*) AS n FROM otlp_logs", "--format", "csv", *args]
    result = run(args, env=env, home=tmp_path)
    assert result.returncode == 0, result.stderr
    if command == "export":
        assert len((tmp_path / "out" / "logs.csv").read_text().strip().splitlines()) > 1
    else:
        assert int(result.stdout.strip().splitlines()[1]) > 0


def test_otap_is_a_switch_for_convert_and_a_port_for_serve(tmp_path):
    # The one flag whose meaning depends on the command: convert reads OTAP files, serve opens
    # an OTAP/Arrow listener. The flag table settles it, so neither spelling needs a special case.
    converted = run(
        ["convert", "--otap", "--signal", "logs", str(DATA_DIR / "logs_simple.jsonl"), "--format", "csv"],
        home=tmp_path,
    )
    assert converted.returncode == 1  # an OTLP fixture read as OTAP, not a usage error
    assert "does not accept" not in converted.stderr

    served = run(["validate", "--otap", "4317", "--http", "0", "--grpc", "0"], home=tmp_path)
    assert served.returncode == 0, served.stderr
    assert "otap:127.0.0.1:4317" in served.stdout


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
