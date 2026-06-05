#!/usr/bin/env python3
"""Run the bounded local Mac Studio duckdb-otlp capacity benchmark."""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import fcntl
import http.client
import json
import os
import platform
import secrets
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
BENCHMARK_DIR = Path(__file__).resolve().parent
PRODUCER_DIR = BENCHMARK_DIR / "producer"
OUTPUT_ROOT = BENCHMARK_DIR / "output"
LOCK_PATH = OUTPUT_ROOT / ".local-capacity.lock"
ACTIVE_PATH = OUTPUT_ROOT / "active.json"
IMAGE = "duckdb-otlp:local-capacity"
RATE = 175_000
BATCH_SIZE = 1_000
CONCURRENCY = 4
QUEUE_DEPTH = 8
SEED = 20_260_605
TARGET_GZIP_BPR = 786.0
SMOKE_SECONDS = 15
WARMUP_SECONDS = 60
MEASURED_SECONDS = 180
MIN_FREE_FULL_RUN = 90 * 1024**3
MIN_FREE_BEFORE_MEASURED = 65 * 1024**3
EMERGENCY_FREE = 25 * 1024**3
OUTPUT_CEILING = 100 * 1024**3
DISK_OVERHEAD_FACTOR = 1.75
DISK_FIXED_OVERHEAD = 5 * 1024**3
DISK_POLL_SECONDS = 2
MAX_BODY_BYTES = 2 * 1024**2
MAX_BUFFERED_BYTES = 2 * 1024**3


class BenchError(RuntimeError):
    pass


@dataclass
class PhaseSpec:
    name: str
    rate: int
    seconds: int


@dataclass
class Context:
    run_id: str
    output_dir: Path
    platform: str
    image: str
    producer_bin: Path
    otlp_port: int
    quack_port: int
    otlp_token: str
    quack_token: str
    container_name: str
    calibration: dict[str, Any] = field(default_factory=dict)
    phases: dict[str, Any] = field(default_factory=dict)
    host_contention: dict[str, Any] = field(default_factory=dict)
    started_at: str = field(default_factory=lambda: dt.datetime.now(dt.UTC).isoformat())


class ExclusiveLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle: Any = None

    def __enter__(self) -> "ExclusiveLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise BenchError(f"another local benchmark holds {self.path}") from exc
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(json.dumps({"pid": os.getpid(), "started_at": dt.datetime.now(dt.UTC).isoformat()}))
        self.handle.flush()
        return self

    def __exit__(self, *_: Any) -> None:
        if self.handle:
            fcntl.flock(self.handle, fcntl.LOCK_UN)
            self.handle.close()


def run_cmd(
    args: list[str],
    *,
    cwd: Path = ROOT,
    check: bool = True,
    timeout: float | None = None,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        args,
        cwd=cwd,
        check=False,
        timeout=timeout,
        input=input_text,
        text=True,
        capture_output=True,
        env=env,
    )
    if check and result.returncode != 0:
        raise BenchError(
            f"command failed ({result.returncode}): {' '.join(args)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result


def require_program(name: str) -> None:
    if shutil.which(name) is None:
        raise BenchError(f"required executable not found on PATH: {name}")


def host_docker_platform(machine: str | None = None) -> str:
    machine = (machine or platform.machine()).lower()
    if machine in {"arm64", "aarch64"}:
        return "linux/arm64"
    if machine in {"x86_64", "amd64"}:
        return "linux/amd64"
    raise BenchError(f"unsupported host architecture: {machine}")


def available_bytes(path: Path) -> int:
    return shutil.disk_usage(path).free


def directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    result = run_cmd(["du", "-sk", str(path)], check=False)
    if result.returncode == 0 and result.stdout.split():
        return int(result.stdout.split()[0]) * 1024
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def estimated_phase_bytes(spec: PhaseSpec) -> int:
    compressed = spec.rate * spec.seconds * TARGET_GZIP_BPR
    return int(compressed * DISK_OVERHEAD_FACTOR + DISK_FIXED_OVERHEAD)


def check_disk(path: Path, spec: PhaseSpec, minimum_free: int = 0) -> dict[str, int]:
    free = available_bytes(path)
    required = max(minimum_free, EMERGENCY_FREE + estimated_phase_bytes(spec))
    if free < required:
        raise BenchError(
            f"disk safety check failed for {spec.name}: free={format_bytes(free)}, required={format_bytes(required)}"
        )
    return {"free_bytes": free, "required_bytes": required, "estimated_phase_bytes": estimated_phase_bytes(spec)}


def ensure_orbstack() -> None:
    result = run_cmd(["orb", "status"], check=False)
    if result.returncode != 0:
        run_cmd(["orb", "start"], timeout=120)
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        if run_cmd(["docker", "info"], check=False, timeout=10).returncode == 0:
            return
        time.sleep(1)
    raise BenchError("OrbStack Docker did not become ready")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def create_context(image: str) -> Context:
    now = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    run_id = f"mac-studio-{now}-{secrets.token_hex(3)}"
    output_dir = OUTPUT_ROOT / run_id
    output_dir.mkdir(parents=True)
    return Context(
        run_id=run_id,
        output_dir=output_dir,
        platform=host_docker_platform(),
        image=image,
        producer_bin=output_dir / "bin" / "otlp-log-producer",
        otlp_port=free_port(),
        quack_port=free_port(),
        otlp_token=f"otlp-{secrets.token_urlsafe(24)}",
        quack_token=f"quack-{secrets.token_urlsafe(24)}",
        container_name=f"duckdb-otlp-capacity-{run_id}",
    )


def write_active(ctx: Context) -> None:
    ACTIVE_PATH.write_text(
        json.dumps(
            {
                "run_id": ctx.run_id,
                "output_dir": str(ctx.output_dir),
                "container_name": ctx.container_name,
                "pid": os.getpid(),
            },
            indent=2,
        )
        + "\n"
    )


def validate_prerequisites() -> dict[str, Any]:
    for program in ("go", "docker", "orb", "duckdb", "uv", "make", "cmake", "ninja"):
        require_program(program)
    platform_name = host_docker_platform()
    free = available_bytes(OUTPUT_ROOT if OUTPUT_ROOT.exists() else BENCHMARK_DIR)
    if free < MIN_FREE_FULL_RUN:
        raise BenchError(
            f"full-run preflight requires at least {format_bytes(MIN_FREE_FULL_RUN)} free; found {format_bytes(free)}"
        )
    return {"platform": platform_name, "free_bytes": free}


def refuse_existing_container() -> None:
    result = run_cmd(
        ["docker", "ps", "-a", "--filter", "name=^/duckdb-otlp-capacity-", "--format", "{{.Names}}"], check=False
    )
    names = [line for line in result.stdout.splitlines() if line.strip()]
    if names:
        raise BenchError(f"existing benchmark container(s) found: {', '.join(names)}")


def collect_host_contention() -> dict[str, Any]:
    containers = run_cmd(["docker", "ps", "--format", "{{json .}}"], check=False)
    stats = run_cmd(["docker", "stats", "--no-stream", "--format", "{{json .}}"], check=False)
    return {
        "running_containers": [json.loads(line) for line in containers.stdout.splitlines() if line],
        "container_stats_before_benchmark": [json.loads(line) for line in stats.stdout.splitlines() if line],
    }


def build_artifacts(ctx: Context, skip_image_build: bool) -> None:
    ctx.producer_bin.parent.mkdir(parents=True, exist_ok=True)
    run_cmd(["go", "build", "-trimpath", "-o", str(ctx.producer_bin), "."], cwd=PRODUCER_DIR)
    if skip_image_build:
        return
    env = dict(os.environ)
    env["VCPKG_TOOLCHAIN_PATH"] = str(ROOT / "vcpkg/scripts/buildsystems/vcpkg.cmake")
    env["GEN"] = "ninja"
    env["DOCKER_IMAGE"] = ctx.image
    env["DOCKER_LOCAL_PLATFORM"] = ctx.platform
    run_cmd(["make", "docker-image-local"], env=env, timeout=7200)


def calibrate_producer(ctx: Context) -> dict[str, Any]:
    path = ctx.output_dir / "calibration.json"
    run_cmd(
        [
            str(ctx.producer_bin),
            "--mode",
            "calibrate",
            "--batch-size",
            str(BATCH_SIZE),
            "--scenario",
            "local-capacity",
            "--seed",
            str(SEED),
            "--target-gzip-bytes-per-record",
            str(TARGET_GZIP_BPR),
            "--output",
            str(path),
        ]
    )
    calibration = json.loads(path.read_text())
    if calibration["calibration_error_percent"] > 1:
        raise BenchError("producer calibration exceeded 1% tolerance")
    ctx.calibration = calibration
    return calibration


def phase_data_dir(ctx: Context, phase: str) -> Path:
    return ctx.output_dir / "data" / phase


def prepare_phase_storage(ctx: Context, phase: str) -> Path:
    path = phase_data_dir(ctx, phase)
    if path.exists():
        ensure_cleanup_target(ctx.output_dir, path)
        shutil.rmtree(path)
    path.mkdir(parents=True)
    path.chmod(0o777)
    (path / "ducklake").mkdir()
    (path / "ducklake").chmod(0o777)
    (path / "ducklake" / "storage").mkdir()
    (path / "ducklake" / "storage").chmod(0o777)
    return path


def start_container(ctx: Context, data_dir: Path) -> None:
    args = [
        "docker",
        "run",
        "-d",
        "--name",
        ctx.container_name,
        "--platform",
        ctx.platform,
        "--cpus",
        "4",
        "--memory",
        "8g",
        "--memory-swap",
        "8g",
        "-p",
        f"127.0.0.1:{ctx.otlp_port}:4318",
        "-p",
        f"127.0.0.1:{ctx.quack_port}:9494",
        "-v",
        f"{data_dir}:/data:rw",
    ]
    env = {
        "DUCKDB_MODE": "local-ducklake",
        "DUCKDB_DATABASE": "/data/control.duckdb",
        "DUCKDB_OTLP_DATA_DIR": "/data",
        "DUCKLAKE_NAME": "lake",
        "DUCKLAKE_CATALOG_PATH": "/data/ducklake/catalog.duckdb",
        "DUCKLAKE_DATA_PATH": "/data/ducklake/storage",
        "DUCKDB_CATALOG": "lake",
        "DUCKDB_SCHEMA": "main",
        "DUCKDB_OTLP_TOKEN": ctx.otlp_token,
        "DUCKDB_QUACK_ENABLED": "1",
        "DUCKDB_QUACK_TOKEN": ctx.quack_token,
        "DUCKDB_OTLP_HTTP_THREADS": "4",
        "DUCKDB_OTLP_MAX_BODY_BYTES": str(MAX_BODY_BYTES),
        "DUCKDB_OTLP_MAX_BUFFERED_BYTES": str(MAX_BUFFERED_BYTES),
        "OTEL_HTTP_ADDR": "0.0.0.0:4318",
        "DUCKDB_QUACK_ADDR": "0.0.0.0:9494",
    }
    for key, value in sorted(env.items()):
        args.extend(["-e", f"{key}={value}"])
    args.append(ctx.image)
    run_cmd(args)
    os_name = run_cmd(["docker", "inspect", "-f", "{{.Platform}}", ctx.container_name]).stdout.strip()
    architecture = run_cmd(["docker", "image", "inspect", "-f", "{{.Architecture}}", ctx.image]).stdout.strip()
    expected = ctx.platform
    actual = f"{os_name}/{architecture}"
    if actual != expected:
        raise BenchError(f"container architecture mismatch: expected {expected}, got {actual}")
    wait_ready(ctx)


def wait_ready(ctx: Context) -> None:
    deadline = time.monotonic() + 180
    last_error = ""
    while time.monotonic() < deadline:
        try:
            request = urllib.request.Request(f"http://127.0.0.1:{ctx.otlp_port}/readyz")
            with urllib.request.urlopen(request, timeout=2) as response:
                if response.status == 200:
                    query(ctx, "SELECT count(*) AS servers FROM otlp_server_list()", "ready")
                    return
        except Exception as exc:
            last_error = str(exc)
        time.sleep(1)
    logs = run_cmd(["docker", "logs", ctx.container_name], check=False).stderr
    raise BenchError(f"consumer did not become ready: {last_error}\n{logs[-4000:]}")


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def query(ctx: Context, sql: str, name: str, timeout: float = 300) -> list[dict[str, Any]]:
    temp = ctx.output_dir / "tmp"
    temp.mkdir(exist_ok=True)
    fd, filename = tempfile.mkstemp(prefix=f"{name}-", suffix=".json", dir=temp)
    os.close(fd)
    Path(filename).unlink()
    expression = (
        "FROM quack_query("
        f"{sql_quote(f'quack:127.0.0.1:{ctx.quack_port}')},"
        f"{sql_quote(sql)},token={sql_quote(ctx.quack_token)})"
    )
    control = f"INSTALL quack;\nLOAD quack;\nCOPY ({expression}) TO {sql_quote(filename)} (FORMAT JSON);\n"
    try:
        run_cmd(["duckdb", "-unsigned", ":memory:"], input_text=control, timeout=timeout)
        raw = Path(filename).read_text() if Path(filename).exists() else ""
    finally:
        Path(filename).unlink(missing_ok=True)
    if not raw.strip():
        return []
    payload = json.loads(raw)
    return payload if isinstance(payload, list) else [payload]


def stop_container(ctx: Context) -> None:
    run_cmd(["docker", "stop", "--time", "120", ctx.container_name], check=False, timeout=150)
    run_cmd(["docker", "rm", "-f", ctx.container_name], check=False, timeout=30)


def container_running(ctx: Context) -> bool:
    result = run_cmd(["docker", "inspect", "-f", "{{.State.Running}}", ctx.container_name], check=False)
    return result.returncode == 0 and result.stdout.strip() == "true"


class Sampler:
    def __init__(self, ctx: Context, data_dir: Path):
        self.ctx = ctx
        self.data_dir = data_dir
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self.stop_event = threading.Event()
        self.emergency_reason = ""
        self.process: subprocess.Popen[str] | None = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self, process: subprocess.Popen[str]) -> None:
        self.process = process
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.thread.join(timeout=10)

    def _run(self) -> None:
        consecutive_seal_failures = 0
        while not self.stop_event.wait(DISK_POLL_SECONDS):
            sample: dict[str, Any] = {"time": dt.datetime.now(dt.UTC).isoformat()}
            try:
                free = available_bytes(self.ctx.output_dir)
                used = directory_bytes(self.ctx.output_dir)
                sample.update({"free_disk_bytes": free, "benchmark_output_bytes": used})
                if free < EMERGENCY_FREE:
                    self._emergency(f"free disk fell below {format_bytes(EMERGENCY_FREE)}")
                if used > OUTPUT_CEILING:
                    self._emergency(f"benchmark output exceeded {format_bytes(OUTPUT_CEILING)}")
            except OSError as exc:
                self._emergency(f"filesystem error: {exc}")
            stats = run_cmd(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", self.ctx.container_name], check=False
            )
            if stats.returncode == 0 and stats.stdout.strip():
                with contextlib.suppress(json.JSONDecodeError):
                    sample["container"] = json.loads(stats.stdout)
            if self.process:
                process_stats = run_cmd(
                    ["ps", "-o", "%cpu=,rss=", "-p", str(self.process.pid)],
                    check=False,
                )
                fields = process_stats.stdout.split()
                if len(fields) == 2:
                    with contextlib.suppress(ValueError):
                        sample["producer"] = {
                            "cpu_percent": float(fields[0]),
                            "rss_bytes": int(fields[1]) * 1024,
                        }
            try:
                rows = query(self.ctx, "SELECT * FROM otlp_server_list()", "sample", timeout=30)
                sample["server"] = rows
                failures = sum(int(row.get("seal_failures_total") or 0) for row in rows)
                consecutive_seal_failures = consecutive_seal_failures + 1 if failures else 0
                if consecutive_seal_failures >= 2:
                    self._emergency("repeated seal failures")
            except Exception as exc:
                self.errors.append(str(exc))
            self.samples.append(sample)

    def _emergency(self, reason: str) -> None:
        if self.emergency_reason:
            return
        self.emergency_reason = reason
        if self.process and self.process.poll() is None:
            self.process.send_signal(signal.SIGTERM)


def run_phase(ctx: Context, spec: PhaseSpec) -> dict[str, Any]:
    preflight = check_disk(
        ctx.output_dir,
        spec,
        MIN_FREE_BEFORE_MEASURED if spec.name == "measured" else 0,
    )
    data_dir = prepare_phase_storage(ctx, spec.name)
    start_container(ctx, data_dir)
    producer_path = ctx.output_dir / f"{spec.name}-producer.json"
    args = [
        str(ctx.producer_bin),
        "--mode",
        "run",
        "--url",
        f"http://127.0.0.1:{ctx.otlp_port}/v1/logs",
        "--token",
        ctx.otlp_token,
        "--run-id",
        f"{ctx.run_id}-{spec.name}",
        "--scenario",
        f"local-capacity-{spec.name}",
        "--rate",
        str(spec.rate),
        "--duration",
        f"{spec.seconds}s",
        "--batch-size",
        str(BATCH_SIZE),
        "--concurrency",
        str(CONCURRENCY),
        "--queue-depth",
        str(QUEUE_DEPTH),
        "--seed",
        str(SEED),
        "--target-gzip-bytes-per-record",
        str(TARGET_GZIP_BPR),
        "--body-payload-bytes",
        str(ctx.calibration["body_payload_bytes"]),
        "--output",
        str(producer_path),
    ]
    process = subprocess.Popen(args, cwd=ROOT, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    sampler = Sampler(ctx, data_dir)
    sampler.start(process)
    stdout, stderr = process.communicate()
    sampler.stop()
    (ctx.output_dir / f"{spec.name}-producer.stdout.log").write_text(stdout)
    (ctx.output_dir / f"{spec.name}-producer.stderr.log").write_text(stderr)
    if not producer_path.exists():
        raise BenchError(f"{spec.name} producer did not write JSON (exit {process.returncode}): {stderr[-2000:]}")
    producer = json.loads(producer_path.read_text())
    flush_started = time.monotonic()
    flush = query(ctx, "SELECT * FROM otlp_flush('otlp:0.0.0.0:4318')", f"{spec.name}-flush", timeout=600)
    flush_seconds = time.monotonic() - flush_started
    server = query(ctx, "SELECT * FROM otlp_server_list()", f"{spec.name}-server")
    reconciliation = reconcile(ctx, f"{ctx.run_id}-{spec.name}", producer, spec)
    storage = storage_stats(data_dir)
    resources = summarize_samples(sampler.samples)
    logs = run_cmd(["docker", "logs", ctx.container_name], check=False)
    (ctx.output_dir / f"{spec.name}-container.log").write_text(logs.stdout + logs.stderr)
    fatal = "ERROR:" in logs.stdout or "ERROR:" in logs.stderr
    phase = {
        "spec": vars(spec),
        "preflight": preflight,
        "producer": producer,
        "producer_exit_code": process.returncode,
        "sampler_errors": sampler.errors,
        "emergency_stop_reason": sampler.emergency_reason,
        "flush": flush,
        "final_flush_seconds": flush_seconds,
        "server": server,
        "reconciliation": reconciliation,
        "resources": resources,
        "storage": storage,
        "server_fatal_error": fatal,
    }
    phase["success"], phase["failed_gates"] = evaluate_phase(phase)
    (ctx.output_dir / f"{spec.name}.json").write_text(json.dumps(phase, indent=2) + "\n")
    stop_container(ctx)
    return phase


def reconcile(ctx: Context, run_id: str, producer: dict[str, Any], spec: PhaseSpec) -> dict[str, Any]:
    sql = f"""
WITH rows AS (
  SELECT try_cast(json_extract_string(log_attributes, '$."benchmark.sequence"') AS UBIGINT) AS sequence
  FROM otlp_logs
  WHERE json_extract_string(log_attributes, '$."benchmark.run_id"') = {sql_quote(run_id)}
)
SELECT count(*) AS durable_rows,
       count(DISTINCT sequence) AS unique_sequences,
       count(*) - count(DISTINCT sequence) AS duplicate_sequences,
       min(sequence) AS min_sequence,
       max(sequence) AS max_sequence
FROM rows
"""
    rows = query(ctx, sql, f"{spec.name}-reconcile", timeout=1200)
    result = rows[0] if rows else {}
    accepted = int(producer.get("accepted_records") or 0)
    unique = int(result.get("unique_sequences") or 0)
    result["accepted_records"] = accepted
    result["missing_accepted_sequences"] = max(0, accepted - unique)
    return result


def evaluate_phase(phase: dict[str, Any]) -> tuple[bool, list[str]]:
    producer = phase["producer"]
    server = phase["server"]
    reconciliation = phase["reconciliation"]
    configured = float(producer.get("configured_offered_records_per_second") or 0)
    attempted = float(producer.get("actual_attempted_records_per_second") or 0)
    statuses = producer.get("response_status_counts") or {}
    gates: list[str] = []
    if configured == 0 or attempted < configured * 0.99:
        gates.append("attempted rate below 99% of configured rate")
    if int(statuses.get("413") or 0):
        gates.append("unexpected HTTP 413 responses")
    if int(statuses.get("503") or 0):
        gates.append("HTTP 503 backpressure responses")
    if int(producer.get("failed_records") or 0):
        gates.append("producer transport or payload failures")
    if any(int(row.get("seal_failures_total") or 0) for row in server):
        gates.append("seal failures")
    if any(int(row.get("buffered_rows") or 0) for row in server):
        gates.append("buffered rows remained after flush")
    if any(row.get("status") == "error" or row.get("error") for row in phase["flush"]):
        gates.append("final flush failed")
    if int(reconciliation.get("durable_rows") or 0) != int(producer.get("accepted_records") or 0):
        gates.append("accepted rows did not equal durable rows")
    if int(reconciliation.get("duplicate_sequences") or 0):
        gates.append("duplicate benchmark.sequence values")
    if int(reconciliation.get("missing_accepted_sequences") or 0):
        gates.append("missing accepted benchmark.sequence values")
    if phase.get("server_fatal_error"):
        gates.append("server fatal error")
    if phase.get("emergency_stop_reason"):
        gates.append(f"emergency stop: {phase['emergency_stop_reason']}")
    return not gates, gates


def parse_size_bytes(value: str) -> float | None:
    value = value.strip()
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
    for unit, multiplier in units.items():
        if value.endswith(unit):
            with contextlib.suppress(ValueError):
                return float(value[: -len(unit)].strip()) * multiplier
    return None


def summarize_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    cpu: list[float] = []
    memory: list[float] = []
    buffered: list[int] = []
    active: list[int] = []
    disk: list[int] = []
    free: list[int] = []
    net_rx: list[float] = []
    net_tx: list[float] = []
    producer_cpu: list[float] = []
    producer_rss: list[int] = []
    for sample in samples:
        container = sample.get("container") or {}
        with contextlib.suppress(ValueError):
            cpu.append(float(str(container.get("CPUPerc", "")).rstrip("%")))
        usage = str(container.get("MemUsage", "")).split("/", 1)[0]
        parsed_memory = parse_size_bytes(usage)
        if parsed_memory is not None:
            memory.append(parsed_memory)
        rx, _, tx = str(container.get("NetIO", "")).partition("/")
        parsed_rx, parsed_tx = parse_size_bytes(rx), parse_size_bytes(tx)
        if parsed_rx is not None:
            net_rx.append(parsed_rx)
        if parsed_tx is not None:
            net_tx.append(parsed_tx)
        producer = sample.get("producer") or {}
        if producer.get("cpu_percent") is not None:
            producer_cpu.append(float(producer["cpu_percent"]))
        if producer.get("rss_bytes") is not None:
            producer_rss.append(int(producer["rss_bytes"]))
        rows = sample.get("server") or []
        buffered.append(sum(int(row.get("buffered_rows") or 0) for row in rows))
        active.append(sum(int(row.get("active_requests") or 0) for row in rows))
        disk.append(int(sample.get("benchmark_output_bytes") or 0))
        free.append(int(sample.get("free_disk_bytes") or 0))
    return {
        "samples": len(samples),
        "container_cpu_percent_average": statistics.fmean(cpu) if cpu else None,
        "container_cpu_percent_peak": max(cpu) if cpu else None,
        "container_memory_bytes_peak": max(memory) if memory else None,
        "container_network_receive_bytes": net_rx[-1] - net_rx[0] if len(net_rx) > 1 else None,
        "container_network_transmit_bytes": net_tx[-1] - net_tx[0] if len(net_tx) > 1 else None,
        "producer_cpu_percent_average": statistics.fmean(producer_cpu) if producer_cpu else None,
        "producer_cpu_percent_peak": max(producer_cpu) if producer_cpu else None,
        "producer_rss_bytes_peak": max(producer_rss) if producer_rss else None,
        "maximum_buffered_rows": max(buffered) if buffered else None,
        "maximum_active_requests": max(active) if active else None,
        "peak_benchmark_output_bytes": max(disk) if disk else None,
        "minimum_free_disk_bytes": min(free) if free else None,
        "samples_raw": samples,
    }


def storage_stats(data_dir: Path) -> dict[str, Any]:
    metadata = data_dir / "ducklake" / "catalog.duckdb"
    parquet = list((data_dir / "ducklake" / "storage").rglob("*.parquet"))
    sizes = [path.stat().st_size for path in parquet]
    return {
        "ducklake_metadata_bytes": metadata.stat().st_size if metadata.exists() else 0,
        "parquet_file_count": len(parquet),
        "parquet_total_bytes": sum(sizes),
        "parquet_average_file_bytes": statistics.fmean(sizes) if sizes else 0,
        "total_phase_storage_bytes": directory_bytes(data_dir),
    }


def remove_phase_storage(ctx: Context, phase: str) -> None:
    path = phase_data_dir(ctx, phase)
    if path.exists():
        ensure_cleanup_target(ctx.output_dir, path)
        shutil.rmtree(path)


def ensure_cleanup_target(output_dir: Path, target: Path) -> None:
    output = output_dir.resolve()
    resolved = target.resolve()
    if output not in resolved.parents:
        raise BenchError(f"refusing to delete path outside benchmark output: {resolved}")


def result_document(ctx: Context, skipped_gate: str | None = None) -> dict[str, Any]:
    measured = ctx.phases.get("measured") or {}
    return {
        "run_id": ctx.run_id,
        "started_at": ctx.started_at,
        "finished_at": dt.datetime.now(dt.UTC).isoformat(),
        "host": {
            "architecture": platform.machine(),
            "docker_platform": ctx.platform,
            "shared_host_contention": True,
            "contention_inventory": ctx.host_contention,
        },
        "producer_configuration": {
            "rate": RATE,
            "batch_size": BATCH_SIZE,
            "concurrency": CONCURRENCY,
            "queue_depth": QUEUE_DEPTH,
            "seed": SEED,
            "target_gzip_bytes_per_record": TARGET_GZIP_BPR,
            "warmup_seconds": WARMUP_SECONDS,
            "measured_seconds": MEASURED_SECONDS,
        },
        "consumer_configuration": {
            "cpus": 4,
            "memory_bytes": 8 * 1024**3,
            "http_threads": 4,
            "max_body_bytes": MAX_BODY_BYTES,
            "max_buffered_bytes": MAX_BUFFERED_BYTES,
            "mode": "local-ducklake",
            "platform": ctx.platform,
        },
        "calibration": ctx.calibration,
        "phases": ctx.phases,
        "capacity_sustained": bool(measured.get("success")),
        "skipped_gate": skipped_gate,
        "comparability_limitations": [
            "Producer and consumer share one physical Mac and contend for host CPU, memory, and disk.",
            "OrbStack localhost networking is not equivalent to networking between isolated EC2 instances.",
            "Local disk is not S3 and local DuckLake commits do not measure S3 request latency.",
            "The workload calibrates compressed request bytes per record, not the unpublished source schema.",
            "Passing is only a local CPU, parsing, buffering, Parquet, and seal capacity gate.",
        ],
    }


def write_reports(ctx: Context, skipped_gate: str | None = None) -> None:
    result = result_document(ctx, skipped_gate)
    (ctx.output_dir / "benchmark-results.json").write_text(json.dumps(result, indent=2) + "\n")
    (ctx.output_dir / "report.md").write_text(render_markdown(result))


def render_markdown(result: dict[str, Any]) -> str:
    producer_config = result["producer_configuration"]
    consumer_config = result["consumer_configuration"]
    calibration = result["calibration"]
    lines = [
        f"# Local duckdb-otlp capacity benchmark: {result['run_id']}",
        "",
        f"- Host/container architecture: `{result['host']['architecture']}` / `{result['host']['docker_platform']}`",
        f"- Consumer limit: {consumer_config['cpus']} CPUs, {consumer_config['memory_bytes'] / 1024**3:.0f} GiB RAM",
        f"- Producer: {producer_config['concurrency']} workers, batch size {producer_config['batch_size']}, "
        f"deterministic seed {producer_config['seed']}",
        f"- Warm-up: {producer_config['warmup_seconds']}s at {producer_config['rate']:,} records/s",
        f"- Measured phase: {producer_config['measured_seconds']}s at {producer_config['rate']:,} records/s",
        f"- Calibration: {calibration.get('calibrated_gzip_bytes_per_record', 0):.3f} gzip bytes/record "
        f"({calibration.get('calibration_error_percent', 0):.3f}% error)",
        f"- Capacity sustained: **{'yes' if result['capacity_sustained'] else 'no'}**",
    ]
    if result.get("skipped_gate"):
        lines.append(f"- Measured phase skipped: {result['skipped_gate']}")
    lines.extend(["", "## Phases", ""])
    for name, phase in result["phases"].items():
        producer = phase["producer"]
        reconciliation = phase["reconciliation"]
        server = phase["server"][0] if phase["server"] else {}
        resources = phase["resources"]
        storage = phase["storage"]
        seals = int(server.get("seals_total") or 0)
        rows_per_seal = int(reconciliation.get("durable_rows") or 0) / seals if seals else 0
        lines.extend(
            [
                f"### {name}",
                "",
                f"- Result: **{'pass' if phase['success'] else 'fail'}**",
                f"- Attempted rate: {producer.get('actual_attempted_records_per_second', 0):,.1f} records/s",
                f"- Accepted rate: {producer.get('accepted_records_per_second', 0):,.1f} records/s",
                f"- Gzip ingress: {producer.get('gzip_gigabits_per_second', 0):.3f} Gbps",
                f"- Gzip bytes/record: {producer.get('gzip_bytes_per_record', 0):.3f}",
                f"- Scheduler missed records/deadlines: {producer.get('scheduler_missed_records', 0):,} / "
                f"{producer.get('scheduler_missed_deadlines', 0):,}",
                f"- HTTP p95/p99/max: {producer['request_latency_ms']['p95']:.3f} / "
                f"{producer['request_latency_ms']['p99']:.3f} / {producer['request_latency_ms']['max']:.3f} ms",
                f"- Accepted / durable: {producer.get('accepted_records', 0):,} / "
                f"{int(reconciliation.get('durable_rows') or 0):,}",
                f"- Missing / duplicate sequences: {int(reconciliation.get('missing_accepted_sequences') or 0):,} / "
                f"{int(reconciliation.get('duplicate_sequences') or 0):,}",
                f"- Seals / rows per seal / failures: {seals:,} / {rows_per_seal:,.1f} / "
                f"{int(server.get('seal_failures_total') or 0):,}",
                f"- Maximum buffered rows: {int(resources.get('maximum_buffered_rows') or 0):,}",
                f"- Final flush: {phase.get('final_flush_seconds', 0):.3f}s",
                f"- Consumer CPU average/peak: {resources.get('container_cpu_percent_average', 0):.1f}% / "
                f"{resources.get('container_cpu_percent_peak', 0):.1f}%",
                f"- Consumer peak memory: {format_bytes(resources.get('container_memory_bytes_peak') or 0)}",
                f"- Producer CPU average/peak: {resources.get('producer_cpu_percent_average', 0):.1f}% / "
                f"{resources.get('producer_cpu_percent_peak', 0):.1f}%",
                f"- Producer peak RSS: {format_bytes(resources.get('producer_rss_bytes_peak') or 0)}",
                f"- Peak benchmark disk usage: {format_bytes(resources.get('peak_benchmark_output_bytes') or 0)}",
                f"- Parquet: {storage.get('parquet_file_count', 0):,} files, "
                f"{format_bytes(storage.get('parquet_total_bytes') or 0)} total, "
                f"{format_bytes(storage.get('parquet_average_file_bytes') or 0)} average",
                f"- Failed gates: {', '.join(phase['failed_gates']) if phase['failed_gates'] else 'none'}",
                "",
            ]
        )
    measured = result["phases"].get("measured") or {}
    if measured:
        producer = measured["producer"]
        resources = measured["resources"]
        if producer.get("scheduler_missed_records") and (resources.get("container_cpu_percent_peak") or 0) < 350:
            bottleneck = (
                "The producer/shared host was the observed constraint: the producer missed scheduler slots while the "
                "consumer stayed well below its four-CPU and 8-GiB limits."
            )
        else:
            bottleneck = "No clear saturation bottleneck was observed within the configured phase gates."
        lines.extend(["## Bottleneck", "", bottleneck, ""])
    contention = result["host"]["contention_inventory"]
    lines.extend(
        [
            "## Host contention",
            "",
            f"- {len(contention.get('running_containers') or [])} unrelated containers were already running.",
            "- They were preserved and their pre-benchmark Docker statistics are recorded in `benchmark-results.json`.",
            "",
        ]
    )
    lines.extend(
        [
            "## Limitations",
            "",
            "- Producer and consumer share one physical Mac, so host contention is part of the result.",
            "- OrbStack localhost networking is not isolated EC2 networking.",
            "- Local disk is not S3; local DuckLake commits do not include S3 request latency.",
            "- The 786-byte calibration is a compressed-volume proxy for an unpublished source workload.",
            "",
        ]
    )
    return "\n".join(lines)


def run_workflow(args: argparse.Namespace, smoke_only: bool = False) -> Context:
    validate_prerequisites()
    ensure_orbstack()
    refuse_existing_container()
    ctx = create_context(args.image)
    ctx.host_contention = collect_host_contention()
    write_active(ctx)
    skipped_gate: str | None = None
    try:
        build_artifacts(ctx, args.skip_image_build)
        calibrate_producer(ctx)
        phases = [PhaseSpec("smoke", 10_000, SMOKE_SECONDS)]
        if not smoke_only:
            phases.extend([PhaseSpec("warmup", RATE, WARMUP_SECONDS), PhaseSpec("measured", RATE, MEASURED_SECONDS)])
        for spec in phases:
            if spec.name == "measured" and not ctx.phases["warmup"]["success"]:
                skipped_gate = "warm-up failed: " + ", ".join(ctx.phases["warmup"]["failed_gates"])
                break
            phase = run_phase(ctx, spec)
            ctx.phases[spec.name] = phase
            if spec.name == "smoke" and not phase["success"]:
                skipped_gate = "smoke failed: " + ", ".join(phase["failed_gates"])
                break
            if spec.name in {"smoke", "warmup"}:
                remove_phase_storage(ctx, spec.name)
        write_reports(ctx, skipped_gate)
        return ctx
    except Exception as exc:
        skipped_gate = str(exc)
        with contextlib.suppress(Exception):
            write_reports(ctx, skipped_gate)
        raise
    finally:
        if container_running(ctx):
            with contextlib.suppress(Exception):
                query(ctx, "SELECT * FROM otlp_flush('otlp:0.0.0.0:4318')", "failure-flush", timeout=120)
        stop_container(ctx)
        if not args.keep_data:
            for phase in ("smoke", "warmup", "measured"):
                with contextlib.suppress(Exception):
                    remove_phase_storage(ctx, phase)
        for temporary in (ctx.output_dir / "bin", ctx.output_dir / "tmp"):
            if temporary.exists():
                ensure_cleanup_target(ctx.output_dir, temporary)
                shutil.rmtree(temporary)
        ACTIVE_PATH.unlink(missing_ok=True)


def cleanup() -> None:
    if ACTIVE_PATH.exists():
        payload = json.loads(ACTIVE_PATH.read_text())
        container = payload.get("container_name")
        if container and container.startswith("duckdb-otlp-capacity-"):
            run_cmd(["docker", "rm", "-f", container], check=False)
        ACTIVE_PATH.unlink(missing_ok=True)


def status() -> dict[str, Any]:
    active = json.loads(ACTIVE_PATH.read_text()) if ACTIVE_PATH.exists() else None
    containers = run_cmd(
        ["docker", "ps", "-a", "--filter", "name=^/duckdb-otlp-capacity-", "--format", "{{json .}}"], check=False
    )
    return {
        "active": active,
        "containers": [json.loads(line) for line in containers.stdout.splitlines() if line],
        "free_bytes": available_bytes(OUTPUT_ROOT if OUTPUT_ROOT.exists() else BENCHMARK_DIR),
    }


def format_bytes(value: int | float) -> str:
    return f"{value / 1024**3:.2f} GiB"


def plan() -> dict[str, Any]:
    specs = [
        PhaseSpec("smoke", 10_000, SMOKE_SECONDS),
        PhaseSpec("warmup", RATE, WARMUP_SECONDS),
        PhaseSpec("measured", RATE, MEASURED_SECONDS),
    ]
    return {
        "platform": host_docker_platform(),
        "phases": [
            {
                **vars(spec),
                "compressed_ingress_bytes": int(spec.rate * spec.seconds * TARGET_GZIP_BPR),
                "disk_safety_estimate_bytes": estimated_phase_bytes(spec),
            }
            for spec in specs
        ],
        "consumer": {"cpus": 4, "memory_bytes": 8 * 1024**3},
        "producer": {"concurrency": CONCURRENCY, "queue_depth": QUEUE_DEPTH},
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("plan", "smoke", "run", "status", "cleanup"), nargs="?", default="run")
    parser.add_argument("--image", default=IMAGE)
    parser.add_argument("--skip-image-build", action="store_true")
    parser.add_argument("--keep-data", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.command == "plan":
            print(json.dumps(plan(), indent=2))
            return 0
        if args.command == "status":
            print(json.dumps(status(), indent=2))
            return 0
        if args.command == "cleanup":
            cleanup()
            return 0
        with ExclusiveLock(LOCK_PATH):
            ctx = run_workflow(args, smoke_only=args.command == "smoke")
        print(ctx.output_dir)
        return 0
    except BenchError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
