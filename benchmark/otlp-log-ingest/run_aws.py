#!/usr/bin/env python3
"""Run the disposable two-node AWS duckdb-otlp DuckLake/S3 benchmark."""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import secrets
import statistics
import subprocess
import sys
import tarfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DIR = Path(__file__).resolve().parent
PRODUCER_DIR = BENCHMARK_DIR / "producer"
TEMPLATE = BENCHMARK_DIR / "aws" / "template.yaml"
OUTPUT_ROOT = BENCHMARK_DIR / "output" / "aws"
DEFAULT_IMAGE = "ghcr.io/smithclay/duckdb-otlp:latest"
DEFAULT_RATES = (125_000, 150_000, 175_000, 200_000)


class BenchError(RuntimeError):
    pass


@dataclass(frozen=True)
class Phase:
    name: str
    rate: int
    seconds: int


def run_cmd(
    args: list[str], *, check: bool = True, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(args, text=True, capture_output=True, check=False, env=env)
    if check and result.returncode:
        raise BenchError(
            f"command failed ({result.returncode}): {' '.join(args)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def aws_base(args: argparse.Namespace) -> list[str]:
    command = ["aws"]
    if args.profile:
        command += ["--profile", args.profile]
    if args.region:
        command += ["--region", args.region]
    return command


def aws(args: argparse.Namespace, *parts: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return run_cmd([*aws_base(args), *parts], check=check)


def unique_run_id() -> str:
    return f"aws-{dt.datetime.now(dt.UTC).strftime('%Y%m%d-%H%M%S')}-{secrets.token_hex(3)}"


def stack_name(run_id: str) -> str:
    return f"duckdb-otlp-bench-{run_id}"


def validate_run_id(run_id: str) -> None:
    if not (8 <= len(run_id) <= 48) or any(ch not in "abcdefghijklmnopqrstuvwxyz0123456789-" for ch in run_id):
        raise BenchError("run ID must be 8-48 lowercase letters, digits, or hyphens")


def output_dir(run_id: str) -> Path:
    return OUTPUT_ROOT / run_id


def state_path(run_id: str) -> Path:
    return output_dir(run_id) / "state.json"


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def load_state(run_id: str) -> dict[str, Any]:
    path = state_path(run_id)
    if not path.exists():
        raise BenchError(f"run state not found: {path}")
    return json.loads(path.read_text())


def save_state(state: dict[str, Any]) -> None:
    write_json(state_path(state["run_id"]), state)


def validate_config(args: argparse.Namespace) -> None:
    validate_run_id(args.run_id)
    for name in ("rate", "batch_size", "instance_type", "region"):
        if not getattr(args, name):
            raise BenchError(f"--{name.replace('_', '-')} is required")
    for name in (
        "smoke_seconds",
        "warmup_seconds",
        "measured_seconds",
        "retention_days",
        "max_runtime_hours",
        "consumer_cpus",
        "consumer_memory_gib",
        "max_buffered_bytes",
    ):
        if getattr(args, name) <= 0:
            raise BenchError(f"--{name.replace('_', '-')} must be positive")
    if args.soak and args.soak_seconds < 1800:
        raise BenchError("--soak-seconds must be at least 1800")
    if args.rate_sweep:
        rates = parse_rates(args.rate_sweep)
        if not set(DEFAULT_RATES).issubset(rates):
            raise BenchError("--rate-sweep must include 125000,150000,175000,200000")
    if args.command in {"smoke", "run"}:
        if not args.image_archive or not args.image_archive.is_file():
            raise BenchError("--image-archive must name an existing ARM64 docker save archive")
        if not args.duckdb_cli or not args.duckdb_cli.is_file():
            raise BenchError("--duckdb-cli must name an existing ARM64 DuckDB CLI binary")
        validate_artifact_architecture(args.image_archive, args.duckdb_cli)


def validate_artifact_architecture(image_archive: Path, duckdb_cli: Path) -> None:
    header = duckdb_cli.read_bytes()[:20]
    if len(header) < 20 or header[:4] != b"\x7fELF":
        raise BenchError("--duckdb-cli must be a Linux ELF executable")
    byte_order = "little" if header[5] == 1 else "big"
    if int.from_bytes(header[18:20], byte_order) != 183:
        raise BenchError("--duckdb-cli must target ARM64/AArch64")
    try:
        with tarfile.open(image_archive, "r:*") as archive:
            manifest_file = archive.extractfile("manifest.json")
            if manifest_file is None:
                raise BenchError("--image-archive is not a docker save archive")
            manifest = json.load(manifest_file)
            config_name = manifest[0]["Config"]
            config_file = archive.extractfile(config_name)
            if config_file is None:
                raise BenchError("docker image config is missing from --image-archive")
            config = json.load(config_file)
    except (tarfile.TarError, KeyError, IndexError, json.JSONDecodeError) as exc:
        raise BenchError(f"could not inspect --image-archive: {exc}") from exc
    if config.get("architecture") != "arm64" or config.get("os") != "linux":
        raise BenchError("--image-archive must contain a linux/arm64 daemon image")


def parse_rates(value: str) -> set[int]:
    try:
        rates = {int(item.strip()) for item in value.split(",") if item.strip()}
    except ValueError as exc:
        raise BenchError("rate sweep values must be integers") from exc
    if not rates or min(rates) <= 0:
        raise BenchError("rate sweep values must be positive")
    return rates


def phases(args: argparse.Namespace, smoke_only: bool = False) -> list[Phase]:
    result = [Phase("smoke", 10_000, args.smoke_seconds)]
    if smoke_only:
        return result
    result.append(Phase("warmup", args.rate, args.warmup_seconds))
    if args.rate_sweep:
        result.extend(
            Phase(f"rate-{rate}", rate, args.measured_seconds) for rate in sorted(parse_rates(args.rate_sweep))
        )
    else:
        result.append(Phase("measured", args.rate, args.measured_seconds))
    if args.soak:
        result.append(Phase("soak", args.rate, args.soak_seconds))
    return result


def plan(args: argparse.Namespace) -> dict[str, Any]:
    validate_config(args)
    validation = aws(
        args,
        "cloudformation",
        "validate-template",
        "--template-body",
        f"file://{TEMPLATE}",
    )
    planned_phases = phases(args)
    payload = {
        "run_id": args.run_id,
        "stack_name": stack_name(args.run_id),
        "template": str(TEMPLATE),
        "architecture": "arm64",
        "topology": {
            "producer": args.instance_type,
            "consumer": args.instance_type,
            "public_ips": False,
        },
        "network": {
            "ssm": True,
            "s3_gateway_endpoint": True,
            "nat_gateway": False,
            "internet_gateway": False,
            "ssh": False,
        },
        "phases": [phase.__dict__ for phase in planned_phases],
        "estimated_target_gbps": args.rate * args.target_gzip_bytes_per_record * 8 / 1e9,
        "estimated_compressed_ingress_bytes": sum(
            phase.rate * phase.seconds * args.target_gzip_bytes_per_record for phase in planned_phases
        ),
        "cost_controls": {
            "same_availability_zone": True,
            "offline_s3_staged_artifacts": True,
            "automatic_instance_shutdown_hours": args.max_runtime_hours,
            "s3_retention_days": args.retention_days,
            "paid_s3_request_metrics": False,
        },
        "cloudformation_validation": json.loads(validation.stdout),
        "creates_resources": False,
    }
    write_json(output_dir(args.run_id) / "plan.json", payload)
    return payload


def stack_outputs(args: argparse.Namespace, stack: str) -> dict[str, str]:
    result = aws(
        args,
        "cloudformation",
        "describe-stacks",
        "--stack-name",
        stack,
        "--query",
        "Stacks[0].Outputs",
        "--output",
        "json",
    )
    return {item["OutputKey"]: item["OutputValue"] for item in json.loads(result.stdout)}


def wait_ssm(args: argparse.Namespace, instance_ids: list[str], timeout: int = 600) -> None:
    deadline = time.monotonic() + timeout
    pending = set(instance_ids)
    while pending and time.monotonic() < deadline:
        result = aws(
            args,
            "ssm",
            "describe-instance-information",
            "--filters",
            "Key=InstanceIds,Values=" + ",".join(sorted(pending)),
            "--query",
            "InstanceInformationList[].InstanceId",
            "--output",
            "json",
        )
        pending -= set(json.loads(result.stdout))
        if pending:
            time.sleep(5)
    if pending:
        raise BenchError(f"instances did not register with SSM: {', '.join(sorted(pending))}")


def provision(args: argparse.Namespace) -> dict[str, Any]:
    validate_config(args)
    stack = stack_name(args.run_id)
    created_at = dt.datetime.now(dt.UTC).isoformat()
    aws(
        args,
        "cloudformation",
        "deploy",
        "--stack-name",
        stack,
        "--template-file",
        str(TEMPLATE),
        "--capabilities",
        "CAPABILITY_IAM",
        "--parameter-overrides",
        f"RunId={args.run_id}",
        f"CreatedAt={created_at}",
        f"InstanceType={args.instance_type}",
        f"RetentionDays={args.retention_days}",
        f"MaxRuntimeHours={args.max_runtime_hours}",
        f"ConsumerVolumeGiB={args.consumer_volume_gib}",
        "--tags",
        f"duckdb-otlp:run-id={args.run_id}",
        "duckdb-otlp:repository=duckdb-otlp",
        "duckdb-otlp:purpose=benchmark",
        f"duckdb-otlp:created-at={created_at}",
    )
    outputs = stack_outputs(args, stack)
    wait_ssm(args, [outputs["ProducerInstanceId"], outputs["ConsumerInstanceId"]])
    for instance_id in (outputs["ProducerInstanceId"], outputs["ConsumerInstanceId"]):
        remote(
            args,
            instance_id,
            ["for i in $(seq 1 180); do " "test -f /opt/duckdb-otlp-benchmark/ready && exit 0; sleep 1; done; exit 1"],
            "wait for instance bootstrap",
        )
    state = {
        "run_id": args.run_id,
        "stack_name": stack,
        "created_at": created_at,
        "region": args.region,
        "profile": args.profile,
        "outputs": outputs,
        "image": args.image,
        "retained": bool(args.retain),
        "phases": {},
    }
    save_state(state)
    return state


def ssm_send(args: argparse.Namespace, instance_id: str, commands: list[str], comment: str) -> str:
    parameters = json.dumps({"commands": commands, "executionTimeout": [str(args.remote_timeout)]})
    result = aws(
        args,
        "ssm",
        "send-command",
        "--instance-ids",
        instance_id,
        "--document-name",
        "AWS-RunShellScript",
        "--comment",
        comment,
        "--parameters",
        parameters,
        "--query",
        "Command.CommandId",
        "--output",
        "text",
    )
    return result.stdout.strip()


def ssm_result(args: argparse.Namespace, command_id: str, instance_id: str, wait: bool = True) -> dict[str, Any]:
    while True:
        result = aws(
            args,
            "ssm",
            "get-command-invocation",
            "--command-id",
            command_id,
            "--instance-id",
            instance_id,
            "--output",
            "json",
            check=False,
        )
        if result.returncode:
            if wait:
                time.sleep(2)
                continue
            return {"Status": "Pending"}
        payload = json.loads(result.stdout)
        if not wait or payload["Status"] not in {"Pending", "InProgress", "Delayed"}:
            return payload
        time.sleep(2)


def remote(args: argparse.Namespace, instance_id: str, commands: list[str], comment: str) -> str:
    command_id = ssm_send(args, instance_id, commands, comment)
    result = ssm_result(args, command_id, instance_id)
    if result["Status"] != "Success":
        raise BenchError(
            f"remote command failed ({result['Status']}): {comment}\n"
            f"{result.get('StandardOutputContent', '')}\n{result.get('StandardErrorContent', '')}"
        )
    return result.get("StandardOutputContent", "")


def build_and_upload_producer(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    artifact = output_dir(args.run_id) / "bin" / "otlp-log-producer"
    artifact.parent.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.update({"GOOS": "linux", "GOARCH": "arm64", "CGO_ENABLED": "0"})
    result = subprocess.run(
        ["go", "build", "-trimpath", "-o", str(artifact), "."],
        cwd=PRODUCER_DIR,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise BenchError(result.stderr)
    bucket = state["outputs"]["BucketName"]
    key = f"{args.run_id}/artifacts/otlp-log-producer"
    aws(args, "s3", "cp", str(artifact), f"s3://{bucket}/{key}", "--only-show-errors")
    producer = state["outputs"]["ProducerInstanceId"]
    remote(
        args,
        producer,
        [
            f"aws s3 cp s3://{bucket}/{key} /opt/duckdb-otlp-benchmark/otlp-log-producer --only-show-errors",
            "chmod 0755 /opt/duckdb-otlp-benchmark/otlp-log-producer",
        ],
        "install shared benchmark producer",
    )
    return {"bucket": bucket, "key": key, "local_path": str(artifact)}


def stage_runtime_artifacts(args: argparse.Namespace, state: dict[str, Any]) -> None:
    bucket = state["outputs"]["BucketName"]
    prefix = f"{args.run_id}/artifacts"
    image_archive = args.image_archive
    if image_archive.suffix != ".gz":
        compressed = output_dir(args.run_id) / "bin" / (image_archive.name + ".gz")
        compressed.parent.mkdir(parents=True, exist_ok=True)
        with image_archive.open("rb") as source, gzip.open(compressed, "wb", compresslevel=1) as target:
            while chunk := source.read(1024 * 1024):
                target.write(chunk)
        image_archive = compressed
    runtime_env = output_dir(args.run_id) / "bin" / "runtime.env"
    runtime_env.write_text(f"OTLP_TOKEN={secrets.token_urlsafe(32)}\nQUACK_TOKEN={secrets.token_urlsafe(32)}\n")
    runtime_env.chmod(0o600)
    uploads = (
        (image_archive, f"{prefix}/daemon-image.tar.gz"),
        (args.duckdb_cli, f"{prefix}/duckdb"),
        (runtime_env, f"{prefix}/runtime.env"),
    )
    for source, key in uploads:
        aws(args, "s3", "cp", str(source), f"s3://{bucket}/{key}", "--only-show-errors")
    producer = state["outputs"]["ProducerInstanceId"]
    consumer = state["outputs"]["ConsumerInstanceId"]
    common = [
        f"aws s3 cp s3://{bucket}/{prefix}/runtime.env /opt/duckdb-otlp-benchmark/runtime.env --only-show-errors",
        "chmod 0600 /opt/duckdb-otlp-benchmark/runtime.env",
    ]
    remote(args, producer, common, "stage producer runtime configuration")
    remote(
        args,
        consumer,
        [
            *common,
            f"aws s3 cp s3://{bucket}/{prefix}/daemon-image.tar.gz /tmp/daemon-image.tar.gz --only-show-errors",
            "gzip -dc /tmp/daemon-image.tar.gz | docker load",
            "docker rm -f duckdb-otlp-extension-cache >/dev/null 2>&1 || true",
            f"docker create --name duckdb-otlp-extension-cache {args.image}",
            "mkdir -p /root/.duckdb",
            "docker cp duckdb-otlp-extension-cache:/duckdb-home/.duckdb/. /root/.duckdb/",
            "docker rm duckdb-otlp-extension-cache",
            f"aws s3 cp s3://{bucket}/{prefix}/duckdb /usr/local/bin/duckdb --only-show-errors",
            "chmod 0755 /usr/local/bin/duckdb",
        ],
        "stage offline consumer artifacts",
    )
    runtime_env.unlink(missing_ok=True)


def start_consumer(args: argparse.Namespace, state: dict[str, Any]) -> None:
    out = state["outputs"]
    consumer = out["ConsumerInstanceId"]
    bucket = out["BucketName"]
    prefix = f"{args.run_id}/ducklake"
    remote(
        args,
        consumer,
        [
            "set -euo pipefail",
            "source /opt/duckdb-otlp-benchmark/runtime.env",
            "docker rm -f duckdb-otlp-benchmark >/dev/null 2>&1 || true",
            "mkdir -p /data/ducklake && chown -R 65532:65532 /data",
            "docker run -d --name duckdb-otlp-benchmark --restart=no "
            f"--cpus={args.consumer_cpus:g} --memory={args.consumer_memory_gib}g "
            f"--memory-swap={args.consumer_memory_gib}g "
            "-p 4318:4318 -p 9494:9494 -v /data:/data:rw "
            "-e DUCKDB_MODE=aws-ducklake -e DUCKDB_DATABASE=/data/control.duckdb "
            "-e DUCKDB_OTLP_DATA_DIR=/data -e DUCKLAKE_CATALOG_PATH=/data/ducklake/catalog.duckdb "
            f"-e DUCKLAKE_DATA_PATH=s3://{bucket}/{prefix} -e AWS_REGION={args.region} "
            "-e DUCKLAKE_NAME=lake -e DUCKDB_CATALOG=lake -e DUCKDB_SCHEMA=otlp "
            "-e OTEL_HTTP_ADDR=0.0.0.0:4318 -e DUCKDB_QUACK_ENABLED=1 -e DUCKDB_QUACK_ADDR=0.0.0.0:9494 "
            "-e DUCKDB_OTLP_HTTP_THREADS=4 -e DUCKDB_OTLP_MAX_BODY_BYTES=2097152 "
            f"-e DUCKDB_OTLP_MAX_BUFFERED_BYTES={args.max_buffered_bytes} "
            '-e DUCKDB_OTLP_TOKEN="$OTLP_TOKEN" -e DUCKDB_QUACK_TOKEN="$QUACK_TOKEN" '
            f"{args.image}",
            "for i in $(seq 1 180); do docker exec duckdb-otlp-benchmark "
            "/usr/local/bin/duckdb-otlp-server healthcheck && exit 0; sleep 1; done",
            "docker logs duckdb-otlp-benchmark >&2",
            "exit 1",
        ],
        "start duckdb-otlp consumer",
    )


def quack_query(args: argparse.Namespace, state: dict[str, Any], sql: str) -> list[dict[str, Any]]:
    out = state["outputs"]
    sql_literal = sql.replace("'", "''")
    command = (
        'export HOME=/root; source /opt/duckdb-otlp-benchmark/runtime.env; Q="$QUACK_TOKEN"; '
        "rm -f /tmp/query.json; "
        '/usr/local/bin/duckdb -unsigned :memory: -c "LOAD quack; '
        f"COPY (FROM quack_query('quack:127.0.0.1:9494','{sql_literal}',token='${{Q}}')) "
        "TO '/tmp/query.json' (FORMAT JSON);\" >/dev/null; cat /tmp/query.json"
    )
    raw = remote(args, out["ConsumerInstanceId"], [command], "query benchmark consumer").strip()
    return [json.loads(line) for line in raw.splitlines() if line.strip()]


def calibrate(args: argparse.Namespace) -> dict[str, Any]:
    binary = output_dir(args.run_id) / "calibration-producer"
    result = subprocess.run(
        [
            "go",
            "run",
            ".",
            "--mode",
            "calibrate",
            "--batch-size",
            str(args.batch_size),
            "--seed",
            str(args.seed),
            "--target-gzip-bytes-per-record",
            str(args.target_gzip_bytes_per_record),
        ],
        cwd=PRODUCER_DIR,
        text=True,
        capture_output=True,
        check=False,
    )
    del binary
    if result.returncode:
        raise BenchError(result.stderr)
    return json.loads(result.stdout)


def start_phase(
    args: argparse.Namespace,
    state: dict[str, Any],
    phase: Phase,
    calibration: dict[str, Any],
) -> str:
    out = state["outputs"]
    run_id = f"{args.run_id}-{phase.name}"
    bucket = out["BucketName"]
    result_path = f"/opt/duckdb-otlp-benchmark/{phase.name}-producer.json"
    stdout_path = f"/opt/duckdb-otlp-benchmark/{phase.name}-producer.log"
    pid_path = f"/opt/duckdb-otlp-benchmark/{phase.name}.pid"
    command = [
        "set -euo pipefail",
        "source /opt/duckdb-otlp-benchmark/runtime.env",
        'TOKEN="$OTLP_TOKEN"',
        "/opt/duckdb-otlp-benchmark/otlp-log-producer --mode run "
        f"--url http://{out['ConsumerPrivateIp']}:4318/v1/logs --token \"$TOKEN\" "
        f"--run-id {run_id} --scenario aws-ducklake-s3-{phase.name} --rate {phase.rate} "
        f"--duration {phase.seconds}s --batch-size {args.batch_size} --concurrency {args.concurrency} "
        f"--queue-depth {args.queue_depth} --seed {args.seed} "
        f"--target-gzip-bytes-per-record {args.target_gzip_bytes_per_record} "
        f"--body-payload-bytes {calibration['body_payload_bytes']} --output {result_path} "
        f"> {stdout_path} 2>&1 &",
        "PID=$!",
        f'echo "$PID" > {pid_path}',
        'wait "$PID"',
        f"aws s3 cp {result_path} s3://{bucket}/{args.run_id}/raw/{phase.name}-producer.json --only-show-errors",
        f"aws s3 cp {stdout_path} s3://{bucket}/{args.run_id}/raw/{phase.name}-producer.log --only-show-errors",
    ]
    return ssm_send(args, out["ProducerInstanceId"], command, f"run {phase.name} load")


def server_sample(args: argparse.Namespace, state: dict[str, Any], phase: Phase) -> dict[str, Any]:
    rows = quack_query(args, state, "SELECT * FROM otlp_server_list()")
    raw_stats = remote(
        args,
        state["outputs"]["ConsumerInstanceId"],
        ["docker stats --no-stream --format '{{json .}}' duckdb-otlp-benchmark"],
        "sample consumer container",
    ).strip()
    producer_stats = remote(
        args,
        state["outputs"]["ProducerInstanceId"],
        [
            f"PID=$(cat /opt/duckdb-otlp-benchmark/{phase.name}.pid 2>/dev/null || true); "
            'test -n "$PID" && ps -o %cpu=,rss= -p "$PID" || true'
        ],
        "sample producer process",
    ).split()
    return {
        "sampled_at": dt.datetime.now(dt.UTC).isoformat(),
        "server": rows,
        "container": json.loads(raw_stats) if raw_stats else None,
        "producer": (
            {
                "cpu_percent": float(producer_stats[0]),
                "rss_bytes": int(producer_stats[1]) * 1024,
            }
            if len(producer_stats) == 2
            else None
        ),
    }


def download_json(args: argparse.Namespace, bucket: str, key: str) -> dict[str, Any]:
    result = aws(args, "s3", "cp", f"s3://{bucket}/{key}", "-", "--only-show-errors")
    return json.loads(result.stdout)


def reconcile(
    args: argparse.Namespace,
    state: dict[str, Any],
    run_id: str,
    accepted: int,
    *,
    verify_sequences: bool = True,
) -> dict[str, Any]:
    where = f"""WHERE json_extract_string(log_attributes, '$."benchmark.run_id"') = '{run_id.replace("'", "''")}'"""
    if not verify_sequences:
        rows = quack_query(
            args,
            state,
            "SELECT count(*) AS durable_rows FROM lake.otlp.otlp_logs",
        )
        result = rows[0] if rows else {}
        result.update(
            {
                "accepted_records": accepted,
                "sequence_validation": "skipped for cheap smoke",
            }
        )
        return result
    sql = f"""
WITH rows AS (
  SELECT try_cast(json_extract_string(log_attributes, '$."benchmark.sequence"') AS UBIGINT) AS sequence
  FROM lake.otlp.otlp_logs
  {where}
)
SELECT count(*) AS durable_rows,
       count(DISTINCT sequence) AS unique_sequences,
       count(*) - count(DISTINCT sequence) AS duplicate_sequences,
       min(sequence) AS min_sequence,
       max(sequence) AS max_sequence
FROM rows
"""
    rows = quack_query(args, state, sql)
    result = rows[0] if rows else {}
    result["accepted_records"] = accepted
    result["missing_accepted_sequences"] = max(0, accepted - int(result.get("unique_sequences") or 0))
    return result


def phase_seals(producer: dict[str, Any], seals: list[dict[str, Any]]) -> list[dict[str, Any]]:
    started_at = producer.get("started_at")
    started_ms = (
        int(dt.datetime.fromisoformat(started_at.replace("Z", "+00:00")).timestamp() * 1000) if started_at else 0
    )
    return [row for row in seals if int(row.get("completed_unix_ms") or 0) >= started_ms]


def latency_approximation(producer: dict[str, Any], seals: list[dict[str, Any]]) -> dict[str, Any]:
    batches = sorted(
        producer.get("accepted_batches") or [],
        key=lambda row: row["accepted_at_unix_nano"],
    )
    successful = [
        row for row in phase_seals(producer, seals) if row.get("success") and int(row.get("rows_committed") or 0) > 0
    ]
    successful.sort(key=lambda row: int(row["completed_unix_ms"]))
    values_accepted: list[float] = []
    values_generated: list[float] = []
    batch_index = 0
    credited = 0
    accepted_cumulative = 0
    for seal in successful:
        credited += int(seal["rows_committed"])
        while batch_index < len(batches) and accepted_cumulative + int(batches[batch_index]["records"]) <= credited:
            batch = batches[batch_index]
            completed_ns = int(seal["completed_unix_ms"]) * 1_000_000
            values_accepted.append((completed_ns - int(batch["accepted_at_unix_nano"])) / 1e6)
            values_generated.append((completed_ns - int(batch["generated_first_unix_nano"])) / 1e6)
            accepted_cumulative += int(batch["records"])
            batch_index += 1
    return {
        "method": "seal-bucket cumulative-row approximation; concurrent batch append order may differ from sequence order",
        "accepted_to_durable_batch_ms": percentile(values_accepted),
        "generated_to_durable_batch_ms": percentile(values_generated),
        "batches_mapped": batch_index,
        "batches_total": len(batches),
    }


def percentile(values: list[float]) -> dict[str, float | int | None]:
    if not values:
        return {"count": 0, "p50": None, "p95": None, "p99": None, "max": None}
    ordered = sorted(values)

    def pick(q: float) -> float:
        return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * q))]

    return {
        "count": len(values),
        "p50": pick(0.50),
        "p95": pick(0.95),
        "p99": pick(0.99),
        "max": ordered[-1],
    }


def parse_size(value: str) -> float | None:
    units = {
        "B": 1,
        "kB": 1000,
        "KB": 1000,
        "KiB": 1024,
        "MB": 1000**2,
        "MiB": 1024**2,
        "GB": 1000**3,
        "GiB": 1024**3,
    }
    value = value.strip()
    for unit, multiplier in units.items():
        if value.endswith(unit):
            try:
                return float(value[: -len(unit)].strip()) * multiplier
            except ValueError:
                return None
    return None


def summarize_samples(samples: list[dict[str, Any]], seals: list[dict[str, Any]]) -> dict[str, Any]:
    consumer_cpu: list[float] = []
    consumer_memory: list[float] = []
    network_rx: list[float] = []
    network_tx: list[float] = []
    producer_cpu: list[float] = []
    producer_rss: list[int] = []
    active_requests: list[int] = []
    buffered_rows: list[int] = []
    admitted_bytes: list[int] = []
    oldest_age: list[int] = []
    for sample in samples:
        container = sample.get("container") or {}
        try:
            consumer_cpu.append(float(str(container.get("CPUPerc", "")).rstrip("%")))
        except ValueError:
            pass
        memory = parse_size(str(container.get("MemUsage", "")).split("/", 1)[0])
        if memory is not None:
            consumer_memory.append(memory)
        rx, _, tx = str(container.get("NetIO", "")).partition("/")
        parsed_rx, parsed_tx = parse_size(rx), parse_size(tx)
        if parsed_rx is not None:
            network_rx.append(parsed_rx)
        if parsed_tx is not None:
            network_tx.append(parsed_tx)
        producer = sample.get("producer") or {}
        if producer.get("cpu_percent") is not None:
            producer_cpu.append(float(producer["cpu_percent"]))
        if producer.get("rss_bytes") is not None:
            producer_rss.append(int(producer["rss_bytes"]))
        server = (sample.get("server") or [{}])[0]
        active_requests.append(int(server.get("active_requests") or 0))
        buffered_rows.append(int(server.get("buffered_rows") or 0))
        admitted_bytes.append(int(server.get("admitted_bytes") or 0))
        if server.get("oldest_buffered_age_ms") is not None:
            oldest_age.append(int(server["oldest_buffered_age_ms"]))
    successful_seals = [row for row in seals if row.get("success")]
    seal_durations = [float(row["duration_ms"]) for row in successful_seals]
    seal_rows = [int(row["rows_committed"]) for row in successful_seals]
    return {
        "producer_cpu_percent_average": (statistics.fmean(producer_cpu) if producer_cpu else None),
        "producer_cpu_percent_peak": max(producer_cpu) if producer_cpu else None,
        "producer_rss_bytes_peak": max(producer_rss) if producer_rss else None,
        "consumer_cpu_percent_average": (statistics.fmean(consumer_cpu) if consumer_cpu else None),
        "consumer_cpu_percent_peak": max(consumer_cpu) if consumer_cpu else None,
        "consumer_memory_bytes_peak": max(consumer_memory) if consumer_memory else None,
        "consumer_network_receive_bytes": (network_rx[-1] - network_rx[0] if len(network_rx) > 1 else None),
        "consumer_network_transmit_bytes": (network_tx[-1] - network_tx[0] if len(network_tx) > 1 else None),
        "active_requests_max": max(active_requests) if active_requests else None,
        "buffered_rows_max": max(buffered_rows) if buffered_rows else None,
        "admitted_bytes_max": max(admitted_bytes) if admitted_bytes else None,
        "oldest_buffered_age_ms_max": max(oldest_age) if oldest_age else None,
        "backlog_rows": buffered_rows,
        "seal_count": len(successful_seals),
        "seal_rows": percentile([float(value) for value in seal_rows]),
        "seal_duration_ms": percentile(seal_durations),
        "seal_failures": sum(1 for row in seals if not row.get("success")),
    }


def storage_stats(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    bucket = state["outputs"]["BucketName"]
    prefix = f"{args.run_id}/ducklake/"
    result = aws(
        args,
        "s3api",
        "list-objects-v2",
        "--bucket",
        bucket,
        "--prefix",
        prefix,
        "--output",
        "json",
    )
    objects = json.loads(result.stdout).get("Contents") or []
    parquet_sizes = [int(item["Size"]) for item in objects if item["Key"].endswith(".parquet")]
    return {
        "object_count": len(objects),
        "total_bytes": sum(int(item["Size"]) for item in objects),
        "parquet_file_count": len(parquet_sizes),
        "parquet_total_bytes": sum(parquet_sizes),
        "parquet_min_bytes": min(parquet_sizes) if parquet_sizes else None,
        "parquet_mean_bytes": (statistics.fmean(parquet_sizes) if parquet_sizes else None),
        "parquet_max_bytes": max(parquet_sizes) if parquet_sizes else None,
        "s3_request_metrics": {
            "available": False,
            "reason": "paid per-request CloudWatch metrics are intentionally not enabled",
        },
    }


def cloudwatch_metrics(args: argparse.Namespace, instance_id: str, start: str, end: str) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for name, statistic in (
        ("CPUUtilization", "Average"),
        ("NetworkIn", "Sum"),
        ("NetworkOut", "Sum"),
    ):
        result = aws(
            args,
            "cloudwatch",
            "get-metric-statistics",
            "--namespace",
            "AWS/EC2",
            "--metric-name",
            name,
            "--dimensions",
            f"Name=InstanceId,Value={instance_id}",
            "--start-time",
            start,
            "--end-time",
            end,
            "--period",
            "60",
            "--statistics",
            statistic,
            "--output",
            "json",
        )
        metrics[name] = json.loads(result.stdout).get("Datapoints") or []
    return metrics


def evaluate(phase: dict[str, Any]) -> list[str]:
    producer = phase["producer"]
    server = phase["server_final"][0] if phase["server_final"] else {}
    reconciliation = phase["reconciliation"]
    statuses = producer.get("response_status_counts") or {}
    gates: list[str] = []
    if float(producer.get("actual_attempted_records_per_second") or 0) < 0.99 * float(
        producer.get("configured_offered_records_per_second") or 0
    ):
        gates.append("attempted rate below 99% of configured rate")
    if int(statuses.get("413") or 0):
        gates.append("unexpected HTTP 413 responses")
    if int(statuses.get("503") or 0):
        gates.append("HTTP 503 backpressure responses")
    if int(producer.get("failed_records") or 0) or int(producer.get("ambiguous_transport_failure_records") or 0):
        gates.append("producer transport, payload, or ambiguous failures")
    if int(server.get("seal_failures_total") or 0):
        gates.append("seal failures")
    if int(server.get("buffered_rows") or 0):
        gates.append("buffered rows remained after final flush")
    if phase.get("flush", [{}])[0].get("status") == "error" or phase.get("flush", [{}])[0].get("error"):
        gates.append("final flush failed")
    if int(reconciliation.get("durable_rows") or 0) != int(producer.get("accepted_records") or 0):
        gates.append("durable rows did not equal accepted records")
    if int(reconciliation.get("missing_accepted_sequences") or 0):
        gates.append("accepted sequence missing")
    if int(reconciliation.get("duplicate_sequences") or 0):
        gates.append("duplicate sequence")
    samples = phase.get("samples") or []
    buffered = [int(sample["server"][0].get("buffered_rows") or 0) for sample in samples if sample.get("server")]
    if (
        len(buffered) >= 4
        and buffered[-1] > buffered[len(buffered) // 2]
        and all(
            later >= earlier
            for earlier, later in zip(buffered[len(buffered) // 2 :], buffered[len(buffered) // 2 + 1 :])
        )
    ):
        gates.append("backlog showed sustained unbounded growth")
    return gates


def run_phase(
    args: argparse.Namespace,
    state: dict[str, Any],
    phase: Phase,
    calibration: dict[str, Any],
) -> dict[str, Any]:
    started = dt.datetime.now(dt.UTC)
    command_id = start_phase(args, state, phase, calibration)
    samples: list[dict[str, Any]] = []
    while True:
        invocation = ssm_result(args, command_id, state["outputs"]["ProducerInstanceId"], wait=False)
        samples.append(server_sample(args, state, phase))
        if invocation["Status"] not in {"Pending", "InProgress", "Delayed"}:
            if invocation["Status"] != "Success":
                raise BenchError(invocation.get("StandardErrorContent") or invocation["Status"])
            break
        time.sleep(args.sample_interval)
    bucket = state["outputs"]["BucketName"]
    producer = download_json(args, bucket, f"{args.run_id}/raw/{phase.name}-producer.json")
    flush_start = time.monotonic()
    flush = quack_query(args, state, "SELECT * FROM otlp_flush('otlp:0.0.0.0:4318')")
    flush_seconds = time.monotonic() - flush_start
    server_final = quack_query(args, state, "SELECT * FROM otlp_server_list()")
    seals = quack_query(
        args,
        state,
        "SELECT seal_sequence, started_unix_ms, completed_unix_ms, duration_ms, "
        "rows_committed, admitted_bytes, success, error FROM otlp_seal_list()",
    )
    current_seals = phase_seals(producer, seals)
    phase_run_id = f"{args.run_id}-{phase.name}"
    reconciliation = reconcile(
        args,
        state,
        phase_run_id,
        int(producer.get("accepted_records") or 0),
        verify_sequences=phase.name != "smoke",
    )
    finished = dt.datetime.now(dt.UTC)
    result = {
        "spec": phase.__dict__,
        "started_at": started.isoformat(),
        "finished_at": finished.isoformat(),
        "producer": producer,
        "samples": samples,
        "flush": flush,
        "final_flush_seconds": flush_seconds,
        "server_final": server_final,
        "seals": seals,
        "reconciliation": reconciliation,
        "durability_latency": latency_approximation(producer, seals),
        "resources": summarize_samples(samples, current_seals),
        "consumer_cloudwatch": cloudwatch_metrics(
            args,
            state["outputs"]["ConsumerInstanceId"],
            started.isoformat(),
            finished.isoformat(),
        ),
        "producer_cloudwatch": cloudwatch_metrics(
            args,
            state["outputs"]["ProducerInstanceId"],
            started.isoformat(),
            finished.isoformat(),
        ),
        "storage": storage_stats(args, state),
    }
    result["failed_gates"] = evaluate(result)
    result["success"] = not result["failed_gates"]
    write_json(output_dir(args.run_id) / f"{phase.name}.json", result)
    return result


def run_benchmark(args: argparse.Namespace, smoke_only: bool) -> dict[str, Any]:
    state = load_state(args.run_id)
    try:
        calibration = calibrate(args)
        write_json(output_dir(args.run_id) / "calibration.json", calibration)
        build_and_upload_producer(args, state)
        stage_runtime_artifacts(args, state)
        start_consumer(args, state)
        for phase in phases(args, smoke_only):
            result = run_phase(args, state, phase, calibration)
            state["phases"][phase.name] = {
                "success": result["success"],
                "failed_gates": result["failed_gates"],
                "result": str(output_dir(args.run_id) / f"{phase.name}.json"),
            }
            save_state(state)
            if not result["success"] and phase.name in {"smoke", "warmup"}:
                break
        write_reports(args, state, calibration)
        return state
    finally:
        if not args.retain:
            cleanup(args)


def write_reports(args: argparse.Namespace, state: dict[str, Any], calibration: dict[str, Any]) -> None:
    results = {}
    for name in state["phases"]:
        path = output_dir(args.run_id) / f"{name}.json"
        if path.exists():
            results[name] = json.loads(path.read_text())
    consolidated = {
        "schema_version": 1,
        "run_id": args.run_id,
        "topology": {
            "producer_instance_type": args.instance_type,
            "consumer_instance_type": args.instance_type,
            "architecture": "arm64",
            "ducklake_catalog": "consumer EBS",
            "parquet_data": f"s3://{state['outputs']['BucketName']}/{args.run_id}/ducklake",
        },
        "calibration": calibration,
        "phases": results,
        "success": bool(results) and all(item["success"] for item in results.values()),
    }
    write_json(output_dir(args.run_id) / "benchmark-results.json", consolidated)
    lines = [
        f"# AWS duckdb-otlp benchmark: {args.run_id}",
        "",
        f"- Result: **{'pass' if consolidated['success'] else 'fail'}**",
        f"- Instances: `{args.instance_type}` ARM64 producer and consumer",
        f"- DuckLake data: `s3://{state['outputs']['BucketName']}/{args.run_id}/ducklake`",
        f"- Calibration: {calibration['calibrated_gzip_bytes_per_record']:.2f} gzip bytes/record",
        "",
    ]
    for name, result in results.items():
        producer = result["producer"]
        latency = result["durability_latency"]
        resources = result["resources"]
        lines += [
            f"## {name}",
            "",
            f"- Result: **{'pass' if result['success'] else 'fail'}**",
            f"- Generated / accepted: {producer.get('generated_records', 0):,} / "
            f"{producer.get('accepted_records', 0):,}",
            f"- Rejected / failed / ambiguous: {producer.get('rejected_records', 0):,} / "
            f"{producer.get('failed_records', 0):,} / "
            f"{producer.get('ambiguous_transport_failure_records', 0):,}",
            f"- Attempted rate: {producer.get('actual_attempted_records_per_second', 0):,.1f} records/s",
            f"- Compressed ingress: {producer.get('gzip_gigabits_per_second', 0):.3f} Gbps",
            f"- HTTP p50/p95/p99/max: {producer['request_latency_ms']['p50']:.3f} / "
            f"{producer['request_latency_ms']['p95']:.3f} / {producer['request_latency_ms']['p99']:.3f} / "
            f"{producer['request_latency_ms']['max']:.3f} ms",
            f"- Generated-to-accepted p95: {producer['generated_to_accepted_batch_ms']['p95']:.3f} ms",
            f"- Producer CPU average/peak: {resources.get('producer_cpu_percent_average')} / "
            f"{resources.get('producer_cpu_percent_peak')}; peak RSS: {resources.get('producer_rss_bytes_peak')}",
            f"- Consumer CPU average/peak: {resources.get('consumer_cpu_percent_average')} / "
            f"{resources.get('consumer_cpu_percent_peak')}; peak memory: "
            f"{resources.get('consumer_memory_bytes_peak')}",
            f"- Consumer network receive/transmit bytes: {resources.get('consumer_network_receive_bytes')} / "
            f"{resources.get('consumer_network_transmit_bytes')}",
            f"- Active requests / buffered rows / admitted bytes maxima: "
            f"{resources.get('active_requests_max')} / {resources.get('buffered_rows_max')} / "
            f"{resources.get('admitted_bytes_max')}",
            f"- Oldest buffered-record age max: {resources.get('oldest_buffered_age_ms_max')} ms",
            f"- Seals / failures: {resources.get('seal_count')} / {resources.get('seal_failures')}; "
            f"seal duration p95: {resources['seal_duration_ms'].get('p95')} ms",
            f"- Final flush: {result['final_flush_seconds']:.3f}s",
            f"- Missing / duplicate: {result['reconciliation'].get('missing_accepted_sequences', 0)} / "
            f"{result['reconciliation'].get('duplicate_sequences', 0)}",
            f"- Accepted-to-durable p95: {latency['accepted_to_durable_batch_ms'].get('p95')} ms "
            "(seal-bucket approximation)",
            f"- Parquet files / bytes: {result['storage']['parquet_file_count']} / "
            f"{result['storage']['parquet_total_bytes']}",
            f"- Failed gates: {', '.join(result['failed_gates']) if result['failed_gates'] else 'none'}",
            "",
        ]
    (output_dir(args.run_id) / "report.md").write_text("\n".join(lines))


def ownership_matches(args: argparse.Namespace, stack: str, run_id: str) -> bool:
    result = aws(
        args,
        "cloudformation",
        "describe-stacks",
        "--stack-name",
        stack,
        "--query",
        "Stacks[0].Tags",
        "--output",
        "json",
        check=False,
    )
    if result.returncode:
        return False
    tags = {item["Key"]: item["Value"] for item in json.loads(result.stdout)}
    return (
        tags.get("duckdb-otlp:run-id") == run_id
        and tags.get("duckdb-otlp:repository") == "duckdb-otlp"
        and tags.get("duckdb-otlp:purpose") == "benchmark"
    )


def cleanup(args: argparse.Namespace) -> dict[str, Any]:
    stack = stack_name(args.run_id)
    if not ownership_matches(args, stack, args.run_id):
        probe = aws(
            args,
            "cloudformation",
            "describe-stacks",
            "--stack-name",
            stack,
            "--output",
            "json",
            check=False,
        )
        if probe.returncode and "does not exist" in getattr(probe, "stderr", ""):
            state = load_state(args.run_id)
            state.setdefault("cleaned_at", dt.datetime.now(dt.UTC).isoformat())
            state["retained"] = False
            save_state(state)
            return state
        raise BenchError(f"refusing to delete stack without matching ownership tags: {stack}")
    state = load_state(args.run_id)
    bucket = state["outputs"]["BucketName"]
    for attempt in range(2):
        aws(
            args,
            "s3",
            "rm",
            f"s3://{bucket}",
            "--recursive",
            "--only-show-errors",
            check=False,
        )
        aws(args, "cloudformation", "delete-stack", "--stack-name", stack)
        aws(
            args,
            "cloudformation",
            "wait",
            "stack-delete-complete",
            "--stack-name",
            stack,
            check=False,
        )
        probe = aws(
            args,
            "cloudformation",
            "describe-stacks",
            "--stack-name",
            stack,
            "--query",
            "Stacks[0].StackStatus",
            "--output",
            "text",
            check=False,
        )
        if probe.returncode and "does not exist" in probe.stderr:
            break
        if attempt == 1:
            status = probe.stdout.strip() or probe.stderr.strip()
            raise BenchError(f"stack cleanup did not complete: {status}")
    state["cleaned_at"] = dt.datetime.now(dt.UTC).isoformat()
    state["retained"] = False
    save_state(state)
    return state


def status(args: argparse.Namespace) -> dict[str, Any]:
    stack = stack_name(args.run_id)
    result = aws(
        args,
        "cloudformation",
        "describe-stacks",
        "--stack-name",
        stack,
        "--query",
        "Stacks[0].{Status:StackStatus,Created:CreationTime,Tags:Tags}",
        "--output",
        "json",
        check=False,
    )
    return {
        "run_id": args.run_id,
        "stack": json.loads(result.stdout) if result.returncode == 0 else None,
        "local_state": (json.loads(state_path(args.run_id).read_text()) if state_path(args.run_id).exists() else None),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "provision", "smoke", "run", "status", "cleanup"))
    parser.add_argument("--run-id", default=unique_run_id())
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION"),
    )
    parser.add_argument("--profile", default=os.environ.get("AWS_PROFILE"))
    parser.add_argument("--instance-type", default="c7g.xlarge")
    parser.add_argument("--consumer-cpus", type=float, default=4)
    parser.add_argument("--consumer-memory-gib", type=int, default=8)
    parser.add_argument("--max-buffered-bytes", type=int, default=2_147_483_648)
    parser.add_argument("--consumer-volume-gib", type=int, default=100)
    parser.add_argument("--retention-days", type=int, default=1)
    parser.add_argument("--max-runtime-hours", type=int, default=4)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--image-archive", type=Path)
    parser.add_argument("--duckdb-cli", type=Path)
    parser.add_argument("--rate", type=int, default=175_000)
    parser.add_argument("--rate-sweep")
    parser.add_argument("--batch-size", type=int, default=1_000)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--queue-depth", type=int, default=8)
    parser.add_argument("--seed", type=int, default=20_260_605)
    parser.add_argument("--target-gzip-bytes-per-record", type=float, default=786)
    parser.add_argument("--smoke-seconds", type=int, default=30)
    parser.add_argument("--warmup-seconds", type=int, default=60)
    parser.add_argument("--measured-seconds", type=int, default=180)
    parser.add_argument("--soak", action="store_true")
    parser.add_argument("--soak-seconds", type=int, default=1800)
    parser.add_argument("--sample-interval", type=float, default=5)
    parser.add_argument("--remote-timeout", type=int, default=3600)
    parser.add_argument("--retain", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "plan":
            result = plan(args)
        elif args.command == "provision":
            result = provision(args)
        elif args.command == "smoke":
            result = run_benchmark(args, True)
        elif args.command == "run":
            result = run_benchmark(args, False)
        elif args.command == "status":
            result = status(args)
        else:
            result = cleanup(args)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except BenchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
