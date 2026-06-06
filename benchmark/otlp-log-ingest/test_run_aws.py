import argparse
import importlib.util
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).with_name("run_aws.py")
SPEC = importlib.util.spec_from_file_location("run_aws", MODULE_PATH)
run_aws = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
sys.modules[SPEC.name] = run_aws
SPEC.loader.exec_module(run_aws)


def args(tmp_path, **overrides):
    values = {
        "command": "plan",
        "run_id": "aws-20260605-test",
        "region": "us-west-2",
        "profile": None,
        "instance_type": "c7g.xlarge",
        "consumer_cpus": 4,
        "consumer_memory_gib": 8,
        "max_buffered_bytes": 2_147_483_648,
        "seal_target_bytes": 64 * 1024 * 1024,
        "seal_max_age_ms": 5000,
        "consumer_volume_gib": 100,
        "retention_days": 1,
        "max_runtime_hours": 4,
        "image": "duckdb-otlp:test",
        "image_archive": None,
        "duckdb_cli": None,
        "rate": 175_000,
        "rate_sweep": None,
        "batch_size": 1_000,
        "concurrency": 4,
        "queue_depth": 8,
        "seed": 20_260_605,
        "target_gzip_bytes_per_record": 786,
        "smoke_rate": 10_000,
        "smoke_seconds": 30,
        "warmup_seconds": 60,
        "measured_seconds": 180,
        "soak": False,
        "soak_seconds": 1800,
        "sample_interval": 5,
        "remote_timeout": 3600,
        "retain": False,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_cloudformation_has_no_nat_or_public_route():
    template = run_aws.TEMPLATE.read_text()
    forbidden = (
        "AWS::EC2::NatGateway",
        "AWS::EC2::InternetGateway",
        "PublicSubnet",
        "NatEip",
    )
    assert not any(value in template for value in forbidden)
    assert "AWS::EC2::VPCEndpoint" in template
    assert "VpcEndpointType: Gateway" in template
    assert "MapPublicIpOnLaunch: false" in template


def test_default_topology_and_phases(tmp_path):
    parsed = args(tmp_path)
    assert run_aws.phases(parsed) == [
        run_aws.Phase("smoke", 10_000, 30),
        run_aws.Phase("warmup", 175_000, 60),
        run_aws.Phase("measured", 175_000, 180),
    ]


def test_rate_sweep_requires_capacity_points(tmp_path):
    parsed = args(tmp_path, rate_sweep="125000,150000,175000")
    with pytest.raises(run_aws.BenchError):
        run_aws.validate_config(parsed)
    parsed.rate_sweep = "125000,150000,175000,200000"
    run_aws.validate_config(parsed)


@pytest.mark.parametrize("name", ["seal_target_bytes", "seal_max_age_ms"])
def test_seal_configuration_must_be_positive(tmp_path, name):
    parsed = args(tmp_path, **{name: 0})
    with pytest.raises(run_aws.BenchError, match=name.replace("_", "-")):
        run_aws.validate_config(parsed)


def test_soak_is_explicit_and_at_least_thirty_minutes(tmp_path):
    parsed = args(tmp_path, soak=True, soak_seconds=1200)
    with pytest.raises(run_aws.BenchError):
        run_aws.validate_config(parsed)
    parsed.soak_seconds = 1800
    assert run_aws.phases(parsed)[-1] == run_aws.Phase("soak", 175_000, 1800)


def test_stack_name_and_run_id_validation():
    assert run_aws.stack_name("aws-20260605-test") == "duckdb-otlp-bench-aws-20260605-test"
    with pytest.raises(run_aws.BenchError):
        run_aws.validate_run_id("AWS unsafe")


def test_artifact_architecture_validation(tmp_path):
    cli = tmp_path / "duckdb"
    header = bytearray(20)
    header[:4] = b"\x7fELF"
    header[5] = 1
    header[18:20] = (183).to_bytes(2, "little")
    cli.write_bytes(header)
    archive = tmp_path / "image.tar"
    manifest = json.dumps([{"Config": "config.json"}]).encode()
    config = json.dumps({"architecture": "arm64", "os": "linux"}).encode()
    with tarfile.open(archive, "w") as output:
        for name, payload in (("manifest.json", manifest), ("config.json", config)):
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            output.addfile(info, io.BytesIO(payload))
    run_aws.validate_artifact_architecture(archive, cli)
    config = json.dumps({"architecture": "amd64", "os": "linux"}).encode()
    with tarfile.open(archive, "w") as output:
        for name, payload in (("manifest.json", manifest), ("config.json", config)):
            info = tarfile.TarInfo(name)
            info.size = len(payload)
            output.addfile(info, io.BytesIO(payload))
    with pytest.raises(run_aws.BenchError):
        run_aws.validate_artifact_architecture(archive, cli)


def test_ssm_command_construction_does_not_embed_tokens(monkeypatch, tmp_path):
    captured = {}

    def fake_aws(_args, *parts, **_kwargs):
        captured["parts"] = parts
        return type("Result", (), {"stdout": "command-123\n"})()

    monkeypatch.setattr(run_aws, "aws", fake_aws)
    command_id = run_aws.ssm_send(args(tmp_path), "i-123", ["source runtime.env", "echo ready"], "test")
    assert command_id == "command-123"
    encoded = captured["parts"][captured["parts"].index("--parameters") + 1]
    payload = json.loads(encoded)
    assert payload["commands"] == ["source runtime.env", "echo ready"]
    assert "TOKEN=" not in encoded


def test_start_consumer_passes_seal_configuration(monkeypatch, tmp_path):
    captured = {}

    def fake_remote(_args, _instance, commands, _comment):
        captured["commands"] = commands

    monkeypatch.setattr(run_aws, "remote", fake_remote)
    parsed = args(tmp_path, seal_target_bytes=268_435_456, seal_max_age_ms=10_000)
    state = {
        "outputs": {
            "ConsumerInstanceId": "i-123",
            "BucketName": "bench-bucket",
        }
    }
    run_aws.start_consumer(parsed, state)
    docker_run = captured["commands"][4]
    assert "-e DUCKDB_OTLP_SEAL_TARGET_BYTES=268435456" in docker_run
    assert "-e DUCKDB_OTLP_SEAL_MAX_AGE_MS=10000" in docker_run


def test_quack_query_uses_s3_result_transport(monkeypatch, tmp_path):
    captured = {}

    def fake_remote(_args, _instance, commands, _comment):
        captured["commands"] = commands
        return ""

    def fake_aws(_args, *parts, **_kwargs):
        captured.setdefault("aws", []).append(parts)
        if parts[:2] == ("s3", "cp"):
            return type("Result", (), {"stdout": '{"value":1}\n{"value":2}\n'})()
        return type("Result", (), {"stdout": ""})()

    monkeypatch.setattr(run_aws, "remote", fake_remote)
    monkeypatch.setattr(run_aws, "aws", fake_aws)
    state = {"outputs": {"ConsumerInstanceId": "i-123", "BucketName": "bench-bucket"}}
    assert run_aws.quack_query(args(tmp_path), state, "SELECT 1") == [
        {"value": 1},
        {"value": 2},
    ]
    assert captured["commands"][0].startswith("set -euo pipefail;")
    assert "export HOME=/root;" in captured["commands"][0]
    assert "aws s3 cp /tmp/duckdb-otlp-query-" in captured["commands"][0]
    assert captured["aws"][0][:2] == ("s3", "cp")
    assert captured["aws"][1][:2] == ("s3", "rm")


def test_latency_is_labelled_as_approximation():
    producer = {
        "accepted_batches": [
            {
                "first_sequence": 0,
                "records": 1000,
                "accepted_at_unix_nano": 2_000_000_000,
                "generated_first_unix_nano": 1_900_000_000,
            },
            {
                "first_sequence": 1000,
                "records": 1000,
                "accepted_at_unix_nano": 3_000_000_000,
                "generated_first_unix_nano": 2_900_000_000,
            },
        ]
    }
    seals = [
        {"success": True, "rows_committed": 1000, "completed_unix_ms": 2500},
        {"success": True, "rows_committed": 1000, "completed_unix_ms": 3500},
    ]
    result = run_aws.latency_approximation(producer, seals)
    assert "approximation" in result["method"]
    assert result["batches_mapped"] == 2
    assert result["accepted_to_durable_batch_ms"]["p50"] == 500


def test_resource_summary_includes_seal_phase_timings():
    resources = run_aws.summarize_samples(
        [],
        [
            {
                "success": True,
                "duration_ms": 100,
                "append_duration_ms": 30,
                "commit_duration_ms": 65,
                "rows_committed": 1000,
            },
            {
                "success": True,
                "duration_ms": 200,
                "append_duration_ms": 80,
                "commit_duration_ms": 110,
                "rows_committed": 2000,
            },
        ],
    )
    assert resources["seal_append_duration_ms"]["max"] == 80
    assert resources["seal_commit_duration_ms"]["max"] == 110
    assert resources["seal_duration_ms"]["max"] == 200


def test_server_sample_tolerates_quack_timeout(monkeypatch, tmp_path):
    # A mid-load Quack timeout must degrade a single sample, not abort the run,
    # and must still capture the container-CPU stats that reveal the saturation.
    def boom(*_args, **_kwargs):
        raise run_aws.BenchError("IO Error: Timeout was reached error for HTTP POST to '.../quack'")

    def fake_remote(_args, _instance, commands, _comment):
        if "docker stats" in commands[0]:
            return '{"CPUPerc":"395.0%","MemUsage":"3GiB / 8GiB"}'
        return "380.0 4096"

    monkeypatch.setattr(run_aws, "quack_query", boom)
    monkeypatch.setattr(run_aws, "remote", fake_remote)
    state = {"outputs": {"ConsumerInstanceId": "i-c", "ProducerInstanceId": "i-p"}}
    errors: list[str] = []
    sample = run_aws.server_sample(args(tmp_path), state, run_aws.Phase("warmup", 100_000, 60), errors)

    assert sample["server"] == []
    assert sample["container"] == {"CPUPerc": "395.0%", "MemUsage": "3GiB / 8GiB"}
    assert sample["producer"] == {"cpu_percent": 380.0, "rss_bytes": 4096 * 1024}
    assert len(errors) == 1 and "Timeout was reached" in errors[0]


def test_backlog_gate_ignores_dropped_samples():
    # Samples whose Quack query failed carry server == [] and must not be treated
    # as backlog observations by the unbounded-growth gate.
    phase = {
        "producer": {
            "actual_attempted_records_per_second": 100,
            "configured_offered_records_per_second": 100,
            "accepted_records": 10,
        },
        "server_final": [{"buffered_rows": 0, "seal_failures_total": 0}],
        "reconciliation": {"durable_rows": 10},
        "flush": [{"status": "ok"}],
        "samples": [{"server": []} for _ in range(8)],
    }
    assert "backlog showed sustained unbounded growth" not in run_aws.evaluate(phase)


def test_drain_backlog_waits_for_zero(monkeypatch, tmp_path):
    # Polls buffered_rows until the background sealer drains it to zero, tolerating
    # a transient query failure along the way without aborting.
    responses = [
        run_aws.BenchError("Timeout was reached"),
        [{"buffered_rows": 500_000}],
        [{"buffered_rows": 120_000}],
        [{"buffered_rows": 0}],
    ]

    def fake_quack(_args, _state, _sql):
        item = responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(run_aws, "quack_query", fake_quack)
    monkeypatch.setattr(run_aws.time, "sleep", lambda _s: None)
    result = run_aws.drain_backlog(args(tmp_path, sample_interval=0), {"outputs": {}})
    assert result == {"drained": True, "residual_buffered_rows": 0, "poll_errors": 1}


def test_drain_backlog_reports_residual_on_timeout(monkeypatch, tmp_path):
    # Polls once (sees a non-zero residual) then the deadline passes: monotonic
    # yields deadline-base, an in-window check, then an out-of-window check.
    clock = iter([0.0, 0.0, 5.0])
    monkeypatch.setattr(run_aws.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(run_aws, "quack_query", lambda *_a, **_k: [{"buffered_rows": 999}])
    monkeypatch.setattr(run_aws.time, "sleep", lambda _s: None)
    result = run_aws.drain_backlog(args(tmp_path, sample_interval=0), {"outputs": {}}, deadline_seconds=1)
    assert result["drained"] is False and result["residual_buffered_rows"] == 999


def test_success_gates_report_missing_measurements():
    phase = {
        "producer": {
            "configured_offered_records_per_second": 100,
            "actual_attempted_records_per_second": 98,
            "accepted_records": 100,
            "failed_records": 1,
            "ambiguous_transport_failure_records": 0,
            "response_status_counts": {"503": 1},
        },
        "server_final": [{"seal_failures_total": 1, "buffered_rows": 10}],
        "flush": [{"status": "error", "error": "failed"}],
        "reconciliation": {
            "durable_rows": 98,
            "missing_accepted_sequences": 2,
            "duplicate_sequences": 1,
        },
        "samples": [],
    }
    gates = run_aws.evaluate(phase)
    assert "attempted rate below 99% of configured rate" in gates
    assert "HTTP 503 backpressure responses" in gates
    assert "final flush failed" in gates
    assert "accepted sequence missing" in gates


def test_ownership_checks_all_tags(monkeypatch, tmp_path):
    payload = [
        {"Key": "duckdb-otlp:run-id", "Value": "aws-20260605-test"},
        {"Key": "duckdb-otlp:repository", "Value": "duckdb-otlp"},
        {"Key": "duckdb-otlp:purpose", "Value": "benchmark"},
    ]

    def fake_aws(*_args, **_kwargs):
        return type("Result", (), {"returncode": 0, "stdout": json.dumps(payload)})()

    monkeypatch.setattr(run_aws, "aws", fake_aws)
    assert run_aws.ownership_matches(args(tmp_path), "stack", "aws-20260605-test")
    payload[0]["Value"] = "someone-else"
    assert not run_aws.ownership_matches(args(tmp_path), "stack", "aws-20260605-test")


def test_cleanup_empties_bucket_before_deleting_stack(monkeypatch, tmp_path):
    parsed = args(tmp_path, command="cleanup")
    monkeypatch.setattr(run_aws, "OUTPUT_ROOT", tmp_path)
    run_aws.save_state(
        {
            "run_id": parsed.run_id,
            "stack_name": run_aws.stack_name(parsed.run_id),
            "outputs": {"BucketName": "owned-bucket"},
        }
    )
    monkeypatch.setattr(run_aws, "ownership_matches", lambda *_: True)
    calls = []

    def fake_aws(_args, *parts, **_kwargs):
        calls.append(parts)
        if parts[:2] == ("cloudformation", "describe-stacks"):
            return type(
                "Result",
                (),
                {"returncode": 255, "stdout": "", "stderr": "does not exist"},
            )()
        return type("Result", (), {"returncode": 0, "stdout": "{}", "stderr": ""})()

    monkeypatch.setattr(run_aws, "aws", fake_aws)
    run_aws.cleanup(parsed)
    assert calls[0][:2] == ("s3", "rm")
    assert calls[1][:2] == ("cloudformation", "delete-stack")
    assert calls[2][:2] == ("cloudformation", "wait")


def test_cleanup_is_idempotent_after_stack_deletion(monkeypatch, tmp_path):
    parsed = args(tmp_path, command="cleanup")
    monkeypatch.setattr(run_aws, "OUTPUT_ROOT", tmp_path)
    run_aws.save_state({"run_id": parsed.run_id, "outputs": {"BucketName": "gone"}})
    monkeypatch.setattr(run_aws, "ownership_matches", lambda *_: False)

    def fake_aws(*_args, **_kwargs):
        return type(
            "Result",
            (),
            {
                "returncode": 255,
                "stdout": "",
                "stderr": "Stack with id x does not exist",
            },
        )()

    monkeypatch.setattr(run_aws, "aws", fake_aws)
    state = run_aws.cleanup(parsed)
    assert state["retained"] is False
    assert state["cleaned_at"]


def test_read_committed_total_sums_counter(monkeypatch, tmp_path):
    monkeypatch.setattr(run_aws, "quack_query", lambda *_a, **_k: [{"committed_rows_total": 12345}])
    assert run_aws.read_committed_total(args(tmp_path), {"outputs": {}}) == 12345


def test_counter_reconcile_is_delta_and_defers_sequences():
    recon = run_aws.counter_reconcile(50_000, 1_000_000, 1_050_000)
    assert recon["durable_rows"] == 50_000
    assert recon["accepted_records"] == 50_000
    assert recon["missing_accepted_sequences"] is None
    assert recon["duplicate_sequences"] is None
    assert recon["sequence_validation"] == "deferred-to-ground-truth-reconcile"


def test_ground_truth_sql_shape():
    sql = run_aws.ground_truth_reconcile_sql()
    assert "GROUP BY run_id" in sql
    assert "count(DISTINCT" in sql
    assert 'benchmark.run_id' in sql and 'benchmark.sequence' in sql
    assert "FROM lake.otlp.otlp_logs" in sql


def test_host_lake_query_ships_base64_and_reads_via_s3(monkeypatch, tmp_path):
    import base64

    captured = {}

    def fake_remote(_args, _instance, commands, _comment):
        captured["command"] = commands[0]
        return ""

    def fake_aws(_args, *parts, **_kwargs):
        captured.setdefault("aws", []).append(parts)
        if parts[:2] == ("s3", "cp"):
            return type("R", (), {"stdout": '{"run_id":"r-smoke","durable_rows":3}\n'})()
        return type("R", (), {"stdout": ""})()

    monkeypatch.setattr(run_aws, "remote", fake_remote)
    monkeypatch.setattr(run_aws, "aws", fake_aws)
    state = {"outputs": {"ConsumerInstanceId": "i-c", "BucketName": "b"}}
    rows = run_aws.host_lake_query(args(tmp_path), state, 'SELECT \'$."x"\' AS run_id')
    assert rows == [{"run_id": "r-smoke", "durable_rows": 3}]
    # The SQL (with embedded double quotes) is shipped base64-encoded, never inlined.
    token = captured["command"].split("echo ", 1)[1].split(" | base64 -d", 1)[0]
    script = base64.b64decode(token).decode()
    assert "LOAD ducklake;" in script and "READ_ONLY" in script
    assert "ATTACH 'ducklake:/data/ducklake/catalog.duckdb'" in script
    assert 'COPY (' in script and "(FORMAT JSON)" in script
    assert captured["aws"][0][:2] == ("s3", "cp")
    assert captured["aws"][1][:2] == ("s3", "rm")


def test_ground_truth_reconcile_stops_daemon_and_keys_by_run_id(monkeypatch, tmp_path):
    captured = {}

    def fake_remote(_args, _instance, commands, comment):
        captured["stop"] = " ".join(commands)
        captured["comment"] = comment

    monkeypatch.setattr(run_aws, "remote", fake_remote)
    monkeypatch.setattr(
        run_aws,
        "host_lake_query",
        lambda *_a, **_k: [
            {"run_id": "aws-test-smoke", "durable_rows": 10, "unique_sequences": 10, "duplicate_sequences": 0},
            {"run_id": "aws-test-measured", "durable_rows": 90, "unique_sequences": 90, "duplicate_sequences": 0},
        ],
    )
    out = run_aws.ground_truth_reconcile(args(tmp_path), {"outputs": {"ConsumerInstanceId": "i-c", "BucketName": "b"}})
    assert "docker stop" in captured["stop"]
    assert set(out) == {"aws-test-smoke", "aws-test-measured"}
    assert out["aws-test-measured"]["durable_rows"] == 90


def test_phase_integrity_passes_clean_and_flags_loss_dup_and_missing_gt(tmp_path):
    parsed = args(tmp_path, run_id="aws-test")
    result = {"reconciliation": {"accepted_records": 100}}
    clean = run_aws.phase_integrity(
        parsed, "measured", result, {"durable_rows": 100, "unique_sequences": 100, "duplicate_sequences": 0}
    )
    assert clean["ok"] and clean["missing_accepted_sequences"] == 0

    lossy = run_aws.phase_integrity(
        parsed, "measured", result, {"durable_rows": 98, "unique_sequences": 95, "duplicate_sequences": 3}
    )
    assert not lossy["ok"]
    assert "duplicate sequence" in lossy["failed_gates"]
    assert "accepted sequence missing" in lossy["failed_gates"]
    assert lossy["missing_accepted_sequences"] == 5

    absent = run_aws.phase_integrity(parsed, "measured", result, {})
    assert not absent["ok"]
    assert "ground-truth reconcile missing for phase" in absent["failed_gates"]
