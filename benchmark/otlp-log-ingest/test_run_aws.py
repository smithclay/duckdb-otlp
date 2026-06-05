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


def test_quack_query_uses_staged_root_extension_cache(monkeypatch, tmp_path):
    captured = {}

    def fake_remote(_args, _instance, commands, _comment):
        captured["commands"] = commands
        return '{"value":1}\n{"value":2}\n'

    monkeypatch.setattr(run_aws, "remote", fake_remote)
    state = {"outputs": {"ConsumerInstanceId": "i-123"}}
    assert run_aws.quack_query(args(tmp_path), state, "SELECT 1") == [
        {"value": 1},
        {"value": 2},
    ]
    assert captured["commands"][0].startswith("export HOME=/root;")


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
