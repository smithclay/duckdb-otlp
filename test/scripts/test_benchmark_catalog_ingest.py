import importlib.util
import sys
from pathlib import Path

import pytest


SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark_catalog_ingest.py"
SPEC = importlib.util.spec_from_file_location("benchmark_catalog_ingest", SCRIPT)
assert SPEC and SPEC.loader
benchmark = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = benchmark
SPEC.loader.exec_module(benchmark)


def test_host_docker_platform():
    assert benchmark.host_docker_platform("arm64") == "linux/arm64"
    assert benchmark.host_docker_platform("aarch64") == "linux/arm64"
    assert benchmark.host_docker_platform("x86_64") == "linux/amd64"
    with pytest.raises(benchmark.BenchError):
        benchmark.host_docker_platform("mips")


def test_producer_result_adapter_preserves_catalog_report_contract():
    load = benchmark.producer_result_to_load(
        {
            "duration_seconds": 2,
            "configured_offered_records_per_second": 100,
            "attempted_records": 190,
            "accepted_records": 180,
            "rejected_records": 10,
            "failed_records": 0,
            "scheduler_missed_records": 10,
            "requests_attempted": 19,
            "gzip_bytes": 1900,
            "accepted_gzip_bytes": 1800,
            "accepted_records_per_second": 90,
            "response_status_counts": {"202": 18, "503": 1},
            "request_latency_ms": {"mean": 1.5, "p50": 1, "p95": 2, "p99": 3, "max": 4},
        },
        batch_size=10,
    )

    assert load["target_records_per_second"] == 100
    assert load["accepted_records"] == 180
    assert load["dropped_records"] == 20
    assert load["accepted_body_mib_per_second"] == 900 / (1024 * 1024)
    assert load["errors_by_status"] == {"503": 1}
    assert load["post_latency_ms_avg"] == 1.5
    assert load["post_latency_ms_p95"] == 2
