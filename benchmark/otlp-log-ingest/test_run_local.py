import json
from pathlib import Path

import pytest

import run_local


def test_host_architecture_mapping():
    assert run_local.host_docker_platform("arm64") == "linux/arm64"
    assert run_local.host_docker_platform("x86_64") == "linux/amd64"
    with pytest.raises(run_local.BenchError):
        run_local.host_docker_platform("mips")


def test_disk_space_calculation(monkeypatch, tmp_path):
    spec = run_local.PhaseSpec("measured", 175_000, 60)
    estimate = run_local.estimated_phase_bytes(spec)
    monkeypatch.setattr(run_local, "available_bytes", lambda _: estimate + run_local.EMERGENCY_FREE + 1)
    result = run_local.check_disk(tmp_path, spec)
    assert result["estimated_phase_bytes"] == estimate


def test_disk_space_rejects_unsafe_run(monkeypatch, tmp_path):
    spec = run_local.PhaseSpec("measured", 175_000, 60)
    monkeypatch.setattr(run_local, "available_bytes", lambda _: run_local.EMERGENCY_FREE)
    with pytest.raises(run_local.BenchError):
        run_local.check_disk(tmp_path, spec)


def test_output_ceiling_constant():
    assert run_local.OUTPUT_CEILING == 100 * 1024**3
    assert run_local.DISK_POLL_SECONDS <= 5


def test_lock_handling(tmp_path):
    path = tmp_path / "lock"
    with run_local.ExclusiveLock(path):
        with pytest.raises(run_local.BenchError):
            with run_local.ExclusiveLock(path):
                pass


def test_phase_gating():
    phase = {
        "producer": {
            "configured_offered_records_per_second": 100,
            "actual_attempted_records_per_second": 100,
            "accepted_records": 100,
            "failed_records": 0,
            "scheduler_missed_records": 0,
            "response_status_counts": {"202": 1},
        },
        "server": [{"seal_failures_total": 0, "buffered_rows": 0}],
        "flush": [{"status": "flushed", "error": None}],
        "reconciliation": {
            "durable_rows": 100,
            "unique_sequences": 100,
            "duplicate_sequences": 0,
            "missing_accepted_sequences": 0,
        },
        "server_fatal_error": False,
        "emergency_stop_reason": "",
    }
    assert run_local.evaluate_phase(phase) == (True, [])
    phase["producer"]["response_status_counts"]["503"] = 1
    success, gates = run_local.evaluate_phase(phase)
    assert not success
    assert "HTTP 503 backpressure responses" in gates


def test_scheduler_misses_are_evidence_not_a_gate_above_99_percent():
    phase = {
        "producer": {
            "configured_offered_records_per_second": 100,
            "actual_attempted_records_per_second": 99.5,
            "accepted_records": 995,
            "failed_records": 0,
            "scheduler_missed_records": 5,
            "response_status_counts": {"202": 10},
        },
        "server": [{"seal_failures_total": 0, "buffered_rows": 0}],
        "flush": [{"status": "flushed", "error": None}],
        "reconciliation": {
            "durable_rows": 995,
            "unique_sequences": 995,
            "duplicate_sequences": 0,
            "missing_accepted_sequences": 0,
        },
        "server_fatal_error": False,
        "emergency_stop_reason": "",
    }
    assert run_local.evaluate_phase(phase) == (True, [])


def test_reconciliation_gate_detects_missing_and_duplicates():
    phase = {
        "producer": {
            "configured_offered_records_per_second": 100,
            "actual_attempted_records_per_second": 100,
            "accepted_records": 100,
            "failed_records": 0,
            "scheduler_missed_records": 0,
            "response_status_counts": {"202": 1},
        },
        "server": [{"seal_failures_total": 0, "buffered_rows": 0}],
        "flush": [{"status": "flushed", "error": None}],
        "reconciliation": {
            "durable_rows": 100,
            "unique_sequences": 98,
            "duplicate_sequences": 2,
            "missing_accepted_sequences": 2,
        },
        "server_fatal_error": False,
        "emergency_stop_reason": "",
    }
    success, gates = run_local.evaluate_phase(phase)
    assert not success
    assert "duplicate benchmark.sequence values" in gates
    assert "missing accepted benchmark.sequence values" in gates


def test_cleanup_targeting(tmp_path):
    output = tmp_path / "run"
    inside = output / "data" / "smoke"
    inside.mkdir(parents=True)
    run_local.ensure_cleanup_target(output, inside)
    outside = tmp_path / "unrelated"
    outside.mkdir()
    with pytest.raises(run_local.BenchError):
        run_local.ensure_cleanup_target(output, outside)


def test_plan_records_three_minute_measurement():
    payload = run_local.plan()
    measured = next(item for item in payload["phases"] if item["name"] == "measured")
    assert measured["seconds"] == 180
    assert measured["compressed_ingress_bytes"] == 175_000 * 180 * 786
