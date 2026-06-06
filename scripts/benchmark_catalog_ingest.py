#!/usr/bin/env python3
"""Run disposable duckdb-otlp catalog ingest benchmarks.

The harness starts the published duckdb-otlp Docker image with DUCKDB_MODE,
runs the shared Go OTLP producer at a target rate, records Docker CPU/memory
samples, samples server seal/backlog counters through Quack, and writes a
consolidated JSON/Markdown report.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import http.client
import json
import math
import socket
import os
import platform
import random
import re
import secrets
import shutil
import statistics
import string
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


DEFAULT_IMAGE = "ghcr.io/smithclay/duckdb-otlp:latest"
DEFAULT_TOKEN = "duckdb-otlp-benchmark-token"
DEFAULT_AWS_PROFILE = "cli-dev"
DEFAULT_RATE = 50_000
DEFAULT_RATE_SWEEP = "10000,25000,50000,75000,100000"
DEFAULT_BATCH_SIZE = 5_000
DEFAULT_BODY_PAYLOAD_BYTES = 32
DEFAULT_PRODUCER_CONCURRENCY = 4
DEFAULT_PRODUCER_QUEUE_DEPTH = 8
ROOT = Path(__file__).resolve().parents[1]
PRODUCER_DIR = ROOT / "benchmark" / "otlp-log-ingest" / "producer"
DEFAULT_TABLES = (
    "otlp_logs",
    "otlp_traces",
    "otlp_metrics_gauge",
    "otlp_metrics_sum",
    "otlp_metrics_histogram",
    "otlp_metrics_exp_histogram",
)
SCENARIOS = (
    "local-ducklake",
    "r2-data-catalog",
    "s3-tables",
    "r2-neon-ducklake",
    "r2-local-ducklake",
)


class BenchError(RuntimeError):
    pass


@dataclass
class CommandResult:
    args: list[str]
    returncode: int
    stdout: str
    stderr: str


@dataclass
class ScenarioContext:
    name: str
    run_id: str
    env: dict[str, str]
    output_dir: Path
    keep: bool
    image: str
    platform: str | None
    port: int
    token: str
    container_name: str
    producer_bin: Path
    dry_run: bool = False
    quack_port: int = 0
    catalog: str = "lake"
    schema: str = "otlp"
    docker_env: dict[str, str] = field(default_factory=dict)
    docker_volumes: list[tuple[str, str, str]] = field(default_factory=list)
    resources: dict[str, Any] = field(default_factory=dict)
    cleanup: list[tuple[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class TrialRun:
    scenario: str
    trial: int
    rate: int
    order: int


def eprint(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def load_env_file(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line.removeprefix("export ").strip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        if key:
            env[key] = value
    return env


def merged_env(env_file: Path) -> dict[str, str]:
    env = dict(os.environ)
    env.update(load_env_file(env_file))
    return env


def require_program(name: str) -> None:
    if shutil.which(name) is None:
        raise BenchError(f"required executable not found on PATH: {name}")


def require_env(env: dict[str, str], *names: str) -> None:
    missing = [name for name in names if not env.get(name)]
    if missing:
        raise BenchError(f"missing required environment variable(s): {', '.join(missing)}")


def wrangler_env(env: dict[str, str]) -> dict[str, str]:
    clean = dict(env)
    clean.pop("CLOUDFLARE_API_TOKEN", None)
    return clean


def wrangler_cmd(ctx: ScenarioContext, *args: str, check: bool = True) -> CommandResult:
    return run_cmd(["wrangler", *args, "--env-file", "/dev/null"], env=wrangler_env(ctx.env), check=check)


def first_env(env: dict[str, str], *names: str) -> str | None:
    for name in names:
        value = env.get(name)
        if value:
            return value
    return None


def r2_access_key(env: dict[str, str]) -> str | None:
    return first_env(
        env,
        "CLOUDFLARE_ACCESS_KEY_ID",
        "R2_ACCESS_KEY_ID",
        "CLOUDFLARE_S3_ACCESS_KEY_ID",
        "CLOUDFLARE_R2_ACCESS_KEY_ID",
        "CLOUDFLARE_S3_KEY_ID",
    )


def r2_secret_key(env: dict[str, str]) -> str | None:
    return first_env(
        env,
        "CLOUDFLARE_SECRET_ACCESS_KEY",
        "R2_SECRET_ACCESS_KEY",
        "CLOUDFLARE_S3_SECRET_ACCESS_KEY",
        "CLOUDFLARE_R2_SECRET_ACCESS_KEY",
        "CLOUDFLARE_S3_SECRET_KEY",
    )


def aws_cli_base(env: dict[str, str], profile: str | None = None, region: str | None = None) -> list[str]:
    args = ["aws"]
    active_profile = profile or env.get("AWS_PROFILE") or DEFAULT_AWS_PROFILE
    if active_profile:
        args.extend(["--profile", active_profile])
    active_region = region or env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION")
    if active_region:
        args.extend(["--region", active_region])
    return args


def aws_cli(
    env: dict[str, str], service: str, operation: str, *args: str, profile: str | None = None, region: str | None = None
) -> list[str]:
    return [*aws_cli_base(env, profile, region), service, operation, *args]


def aws_region(env: dict[str, str], profile: str | None = None) -> str:
    configured = env.get("AWS_REGION") or env.get("AWS_DEFAULT_REGION")
    if configured:
        return configured
    result = run_cmd(
        [*aws_cli_base(env, profile), "configure", "get", "region"],
        env=env,
        check=False,
    )
    region = result.stdout.strip()
    if region:
        return region
    raise BenchError("could not determine AWS region from AWS_REGION/AWS_DEFAULT_REGION or aws configure")


def cloudflare_account_id(ctx: ScenarioContext) -> str:
    if ctx.env.get("CLOUDFLARE_ACCOUNT_ID"):
        return ctx.env["CLOUDFLARE_ACCOUNT_ID"]
    require_program("wrangler")
    result = wrangler_cmd(ctx, "whoami")
    match = re.search(r"\b[a-f0-9]{32}\b", result.stdout)
    if not match:
        raise BenchError("could not infer CLOUDFLARE_ACCOUNT_ID from wrangler whoami")
    account_id = match.group(0)
    ctx.env["CLOUDFLARE_ACCOUNT_ID"] = account_id
    return account_id


def r2_s3_endpoint(ctx: ScenarioContext) -> str:
    host = first_env(ctx.env, "CLOUDFLARE_S3_API_HOST", "R2_ENDPOINT")
    if host:
        return host if host.startswith("http://") or host.startswith("https://") else f"https://{host}"
    return f"https://{cloudflare_account_id(ctx)}.r2.cloudflarestorage.com"


def r2_s3_cli_env(ctx: ScenarioContext) -> dict[str, str]:
    access_key = r2_access_key(ctx.env)
    secret_key = r2_secret_key(ctx.env)
    if not access_key or not secret_key:
        raise BenchError(
            "missing R2 S3 credentials; set R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY "
            "or CLOUDFLARE_S3_ACCESS_KEY_ID/CLOUDFLARE_S3_SECRET_ACCESS_KEY"
        )
    env = dict(ctx.env)
    for key in (
        "AWS_PROFILE",
        "AWS_DEFAULT_PROFILE",
        "AWS_SESSION_TOKEN",
        "AWS_SECURITY_TOKEN",
    ):
        env.pop(key, None)
    env["AWS_ACCESS_KEY_ID"] = access_key
    env["AWS_SECRET_ACCESS_KEY"] = secret_key
    env["AWS_REGION"] = "auto"
    env["AWS_DEFAULT_REGION"] = "auto"
    return env


def r2_s3api(ctx: ScenarioContext, *args: str, check: bool = True) -> CommandResult:
    return run_cmd(
        ["aws", "s3api", "--endpoint-url", r2_s3_endpoint(ctx), *args],
        env=r2_s3_cli_env(ctx),
        check=check,
    )


def preflight_r2_s3_write(ctx: ScenarioContext, bucket: str, prefix: str) -> None:
    if ctx.dry_run:
        return
    probe = ctx.output_dir / f"{slug(ctx.name)}-r2-preflight.txt"
    probe.write_text("duckdb-otlp benchmark preflight\n")
    key = f"{prefix.rstrip('/')}/_duckdb_otlp_preflight.txt" if prefix else "_duckdb_otlp_preflight.txt"
    result = r2_s3api(ctx, "put-object", "--bucket", bucket, "--key", key, "--body", str(probe), check=False)
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise BenchError(
            "R2 S3 credentials cannot write to the disposable bucket. "
            "Create/update the R2 S3 access key with object write permission for newly-created benchmark buckets, "
            "then rerun. "
            f"Bucket: {bucket}. AWS CLI error: {detail}"
        )
    r2_s3api(ctx, "delete-object", "--bucket", bucket, "--key", key, check=False)


def neon_project_id(ctx: ScenarioContext) -> str:
    if ctx.env.get("NEON_PROJECT_ID"):
        return ctx.env["NEON_PROJECT_ID"]
    require_program("neonctl")
    result = run_cmd(["neonctl", "projects", "list", "--output", "json"], env=ctx.env)
    payload = json.loads(result.stdout)
    projects = payload.get("projects") or []
    if len(projects) != 1:
        raise BenchError("set NEON_PROJECT_ID or make neonctl projects list return exactly one project")
    project_id = projects[0]["id"]
    ctx.env["NEON_PROJECT_ID"] = project_id
    return project_id


def run_cmd(
    args: list[str],
    *,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
    input_text: str | None = None,
    check: bool = True,
    timeout: float | None = None,
) -> CommandResult:
    proc = subprocess.run(
        args,
        input=input_text,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        cwd=str(cwd) if cwd else None,
        timeout=timeout,
    )
    result = CommandResult(args, proc.returncode, proc.stdout, proc.stderr)
    if check and proc.returncode != 0:
        cmd = " ".join(args)
        raise BenchError(
            f"command failed ({proc.returncode}): {cmd}\n" f"stdout:\n{proc.stdout}\n\nstderr:\n{proc.stderr}"
        )
    return result


def sensitive_values(*envs: dict[str, str]) -> list[str]:
    markers = ("TOKEN", "SECRET", "PASSWORD", "KEY", "CREDENTIAL", "DATABASE_URL", "CONNECTION_STRING")
    values: list[str] = []
    for env in envs:
        for key, value in env.items():
            if value and len(value) >= 6 and any(marker in key.upper() for marker in markers):
                values.append(value)
    return sorted(set(values), key=len, reverse=True)


def redact_text(text: str, *envs: dict[str, str]) -> str:
    redacted = text
    for value in sensitive_values(*envs):
        redacted = redacted.replace(value, "<redacted>")
    return redacted


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def slug(value: str) -> str:
    value = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return re.sub(r"-+", "-", value)


def unique_name(prefix: str, run_id: str, suffix: str, max_len: int = 63) -> str:
    rand = "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(8))
    prefix = slug(prefix)
    run_id = slug(run_id)
    suffix = slug(suffix)
    reserved = len(prefix) + len(run_id) + len(rand) + 3
    suffix_budget = max(1, max_len - reserved)
    base = f"{prefix}-{run_id}-{suffix[:suffix_budget].rstrip('-')}-{rand}"
    return base[:max_len].rstrip("-")


def make_run_id() -> str:
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%d%H%M%S")
    rand = "".join(secrets.choice(string.ascii_lowercase + string.digits) for _ in range(6))
    return f"{timestamp}-{rand}"


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def host_docker_platform(machine: str | None = None) -> str:
    machine = (machine or platform.machine()).lower()
    if machine in {"arm64", "aarch64"}:
        return "linux/arm64"
    if machine in {"x86_64", "amd64"}:
        return "linux/amd64"
    raise BenchError(f"unsupported host architecture: {machine}")


def build_producer(output_dir: Path, configured: Path | None) -> Path:
    if configured:
        producer = configured.resolve()
        if not producer.is_file():
            raise BenchError(f"producer executable not found: {producer}")
        return producer
    require_program("go")
    producer = output_dir / "bin" / "otlp-bench-producer"
    producer.parent.mkdir(parents=True, exist_ok=True)
    run_cmd(["go", "build", "-trimpath", "-o", str(producer), "."], cwd=PRODUCER_DIR)
    return producer


def cf_api(
    ctx: ScenarioContext,
    method: str,
    path: str,
    *,
    body: bytes | None = None,
    ok: tuple[int, ...] = (200,),
) -> Any:
    require_env(ctx.env, "CLOUDFLARE_API_TOKEN")
    url = f"https://api.cloudflare.com/client/v4{path}"
    req = urllib.request.Request(url, data=body, method=method)
    req.add_header("Authorization", f"Bearer {ctx.env['CLOUDFLARE_API_TOKEN']}")
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=60) as response:
            payload = response.read().decode()
            if response.status not in ok:
                raise BenchError(f"Cloudflare API returned {response.status}: {payload}")
            return json.loads(payload) if payload else {}
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode(errors="replace")
        if exc.code not in ok:
            raise BenchError(f"Cloudflare API returned {exc.code}: {payload}") from exc
        return json.loads(payload) if payload else {}


def r2_object_keys(ctx: ScenarioContext, bucket: str) -> list[str]:
    account_id = cloudflare_account_id(ctx)
    keys: list[str] = []
    cursor = ""
    while True:
        query = "?per_page=1000"
        if cursor:
            query += "&cursor=" + urllib.parse.quote(cursor)
        payload = cf_api(ctx, "GET", f"/accounts/{account_id}/r2/buckets/{bucket}/objects{query}")
        result = payload.get("result", [])
        if isinstance(result, dict):
            objects = result.get("objects", [])
            cursor = result.get("cursor") or ""
            truncated = bool(result.get("truncated"))
        else:
            objects = result
            cursor = ""
            truncated = False
        keys.extend(obj["key"] for obj in objects if obj.get("key"))
        if not truncated and not cursor:
            return keys


def r2_object_summary(ctx: ScenarioContext, bucket: str) -> dict[str, Any]:
    if r2_access_key(ctx.env) and r2_secret_key(ctx.env):
        result = r2_s3api(ctx, "list-objects-v2", "--bucket", bucket, "--output", "json", check=False)
        if result.returncode == 0:
            payload = json.loads(result.stdout or "{}")
            objects = payload.get("Contents", [])
            keys = [obj["Key"] for obj in objects if obj.get("Key")]
            return {
                "bucket": bucket,
                "objects": len(keys),
                "parquet_files": sum(1 for key in keys if key.endswith(".parquet")),
                "metadata_files": sum(1 for key in keys if "metadata" in key or key.endswith(".json")),
                "bytes": sum(int(obj.get("Size") or 0) for obj in objects),
                "listing_api": "s3",
            }
        if not ctx.env.get("CLOUDFLARE_API_TOKEN"):
            return {"bucket": bucket, "error": result.stderr.strip(), "listing_api": "s3"}
    account_id = cloudflare_account_id(ctx)
    keys = r2_object_keys(ctx, bucket)
    sizes: dict[str, int] = {}
    for key in keys:
        encoded = urllib.parse.quote(key, safe="")
        with contextlib.suppress(Exception):
            payload = cf_api(ctx, "GET", f"/accounts/{account_id}/r2/buckets/{bucket}/objects/{encoded}")
            result = payload.get("result") or {}
            sizes[key] = int(result.get("size") or 0)
    return {
        "bucket": bucket,
        "objects": len(keys),
        "parquet_files": sum(1 for key in keys if key.endswith(".parquet")),
        "metadata_files": sum(1 for key in keys if "metadata" in key or key.endswith(".json")),
        "bytes": sum(sizes.values()) if sizes else None,
        "listing_api": "cloudflare",
    }


def delete_r2_objects(ctx: ScenarioContext, bucket: str) -> None:
    if r2_access_key(ctx.env) and r2_secret_key(ctx.env):
        keys = r2_object_keys_s3(ctx, bucket)
        if keys is not None:
            for key in keys:
                r2_s3api(ctx, "delete-object", "--bucket", bucket, "--key", key, check=False)
            return
    account_id = cloudflare_account_id(ctx)
    for key in r2_object_keys(ctx, bucket):
        encoded = urllib.parse.quote(key, safe="")
        cf_api(ctx, "DELETE", f"/accounts/{account_id}/r2/buckets/{bucket}/objects/{encoded}", ok=(200, 204))


def delete_r2_bucket(ctx: ScenarioContext, bucket: str) -> None:
    if r2_access_key(ctx.env) and r2_secret_key(ctx.env):
        result = r2_s3api(ctx, "delete-bucket", "--bucket", bucket, check=False)
        if result.returncode == 0:
            return
    wrangler_cmd(ctx, "r2", "bucket", "delete", bucket, check=False)


def r2_object_keys_s3(ctx: ScenarioContext, bucket: str) -> list[str] | None:
    result = r2_s3api(ctx, "list-objects-v2", "--bucket", bucket, "--output", "json", check=False)
    if result.returncode != 0:
        return None
    payload = json.loads(result.stdout or "{}")
    return [obj["Key"] for obj in payload.get("Contents", []) if obj.get("Key")]


def create_r2_bucket(ctx: ScenarioContext, purpose: str) -> str:
    bucket = unique_name("duckdb-otlp-bench", ctx.run_id, purpose)
    if ctx.dry_run:
        return bucket
    if r2_access_key(ctx.env) and r2_secret_key(ctx.env):
        result = r2_s3api(ctx, "create-bucket", "--bucket", bucket, check=False)
        if result.returncode == 0:
            ctx.cleanup.append(("r2_bucket", bucket))
            return bucket
        eprint(f"[{ctx.name}] R2 S3 create-bucket failed; falling back to Wrangler OAuth")
    cloudflare_account_id(ctx)
    require_program("wrangler")
    wrangler_cmd(ctx, "r2", "bucket", "create", bucket)
    ctx.cleanup.append(("r2_bucket", bucket))
    return bucket


def r2_data_catalog_setup(ctx: ScenarioContext) -> None:
    bucket = create_r2_bucket(ctx, "r2catalog")
    if not ctx.dry_run:
        wrangler_cmd(ctx, "r2", "bucket", "catalog", "enable", bucket)
        ctx.cleanup.append(("r2_catalog", bucket))
    account_id = cloudflare_account_id(ctx)
    catalog_uri = f"https://catalog.cloudflarestorage.com/{account_id}/{bucket}"
    warehouse = f"{account_id}_{bucket}"
    ctx.resources.update({"r2_bucket": bucket, "r2_catalog_uri": catalog_uri, "r2_warehouse": warehouse})
    r2_storage_secret_sql(ctx)
    ctx.docker_env.update(
        {
            "DUCKDB_MODE": "r2-data-catalog",
            "CLOUDFLARE_ACCOUNT_ID": account_id,
            "CLOUDFLARE_R2_BUCKET": bucket,
            "CLOUDFLARE_CATALOG_URI": catalog_uri,
            "CLOUDFLARE_CATALOG_TOKEN": ctx.env["CLOUDFLARE_API_TOKEN"],
            "CLOUDFLARE_WAREHOUSE": warehouse,
        }
    )


def local_ducklake_setup(ctx: ScenarioContext) -> None:
    metadata_path = "/tmp/duckdb-otlp-bench.ducklake"
    data_path = "/tmp/duckdb-otlp-bench-files/"
    ctx.resources.update({"metadata_path": metadata_path, "data_path": data_path})
    ctx.docker_env.update(
        {
            "DUCKDB_MODE": "local-ducklake",
            "DUCKLAKE_NAME": ctx.catalog,
            "DUCKLAKE_CATALOG_PATH": metadata_path,
            "DUCKLAKE_DATA_PATH": data_path,
        }
    )


def pass_aws_credentials_to_container(ctx: ScenarioContext, profile: str | None, region: str) -> None:
    ctx.docker_env["AWS_REGION"] = region
    ctx.docker_env["AWS_DEFAULT_REGION"] = region
    if profile:
        if not ctx.dry_run:
            ctx.docker_env.update(aws_export_credentials(ctx.env, profile))
        return

    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_SECURITY_TOKEN"):
        if ctx.env.get(key):
            ctx.docker_env[key] = ctx.env[key]


def aws_export_credentials(env: dict[str, str], profile: str | None = None) -> dict[str, str]:
    result = run_cmd([*aws_cli_base(env, profile), "configure", "export-credentials", "--format", "env"], env=env)
    creds: dict[str, str] = {}
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.startswith("export "):
            line = line.removeprefix("export ")
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        creds[key] = value.strip().strip("'").strip('"')
    return {key: value for key, value in creds.items() if key.startswith("AWS_") and value}


def s3_tables_template() -> str:
    return """AWSTemplateFormatVersion: '2010-09-09'
Description: Disposable Amazon S3 Tables resources for duckdb-otlp benchmarks.
Parameters:
  TableBucketName:
    Type: String
  NamespaceName:
    Type: String
    Default: otlp
Resources:
  OtlpTableBucket:
    Type: AWS::S3Tables::TableBucket
    Properties:
      TableBucketName: !Ref TableBucketName
      Tags:
        - Key: project
          Value: duckdb-otlp
        - Key: purpose
          Value: benchmark
  OtlpNamespace:
    Type: AWS::S3Tables::Namespace
    Properties:
      TableBucketARN: !GetAtt OtlpTableBucket.TableBucketARN
      Namespace: !Ref NamespaceName
Outputs:
  TableBucketName:
    Value: !Ref TableBucketName
  TableBucketArn:
    Value: !GetAtt OtlpTableBucket.TableBucketARN
  NamespaceName:
    Value: !Ref NamespaceName
"""


def s3_tables_setup(ctx: ScenarioContext) -> None:
    profile = ctx.env.get("AWS_PROFILE") or DEFAULT_AWS_PROFILE
    region = aws_region(ctx.env, profile)
    if ctx.dry_run:
        account = ctx.env.get("AWS_ACCOUNT_ID", "000000000000")
    else:
        require_program("aws")
        account = run_cmd(
            aws_cli(
                ctx.env,
                "sts",
                "get-caller-identity",
                "--query",
                "Account",
                "--output",
                "text",
                profile=profile,
            ),
            env=ctx.env,
        ).stdout.strip()
    stack = unique_name("duckdb-otlp-bench", ctx.run_id, "s3tables")
    table_bucket = unique_name("duckdb-otlp-bench", ctx.run_id, f"s3tables-{account}-{region}")
    template = ctx.output_dir / f"{stack}.yaml"
    template.write_text(s3_tables_template())
    if ctx.dry_run:
        table_bucket_arn = f"arn:aws:s3tables:{region}:{account}:bucket/{table_bucket}"
    else:
        run_cmd(
            aws_cli(
                ctx.env,
                "cloudformation",
                "deploy",
                "--stack-name",
                stack,
                "--template-file",
                str(template),
                "--parameter-overrides",
                f"TableBucketName={table_bucket}",
                "NamespaceName=otlp",
                profile=profile,
                region=region,
            ),
            env=ctx.env,
        )
        ctx.cleanup.append(("cloudformation_stack", {"stack": stack, "profile": profile, "region": region}))
        table_bucket_arn = run_cmd(
            aws_cli(
                ctx.env,
                "cloudformation",
                "describe-stacks",
                "--stack-name",
                stack,
                "--query",
                "Stacks[0].Outputs[?OutputKey=='TableBucketArn'].OutputValue | [0]",
                "--output",
                "text",
                profile=profile,
                region=region,
            ),
            env=ctx.env,
        ).stdout.strip()
        # Empty the table bucket before the stack delete (registered after the stack so the
        # reversed() cleanup runs it first), otherwise CloudFormation can't delete the
        # populated bucket and it leaks.
        ctx.cleanup.append(("s3_tables_bucket", {"arn": table_bucket_arn, "profile": profile, "region": region}))
    ctx.resources.update(
        {
            "aws_profile": profile or "default",
            "aws_region": region,
            "s3_tables_stack": stack,
            "s3_tables_bucket": table_bucket,
            "s3_tables_bucket_arn": table_bucket_arn,
        }
    )
    pass_aws_credentials_to_container(ctx, profile, region)
    ctx.docker_env.update(
        {
            "DUCKDB_MODE": "s3-tables",
            "S3_TABLES_BUCKET_ARN": table_bucket_arn,
        }
    )


def r2_storage_secret_sql(ctx: ScenarioContext) -> str:
    access_key = r2_access_key(ctx.env)
    secret_key = r2_secret_key(ctx.env)
    if not access_key or not secret_key:
        raise BenchError(
            "missing R2 S3 credentials; set R2_ACCESS_KEY_ID/R2_SECRET_ACCESS_KEY "
            "or CLOUDFLARE_S3_ACCESS_KEY_ID/CLOUDFLARE_S3_SECRET_ACCESS_KEY"
        )
    ctx.docker_env["R2_ACCESS_KEY_ID"] = access_key
    ctx.docker_env["R2_SECRET_ACCESS_KEY"] = secret_key
    ctx.docker_env["CLOUDFLARE_ACCESS_KEY_ID"] = access_key
    ctx.docker_env["CLOUDFLARE_SECRET_ACCESS_KEY"] = secret_key
    ctx.docker_env["CLOUDFLARE_R2_ENDPOINT"] = r2_s3_endpoint(ctx)
    return ""


def neon_connection_string(ctx: ScenarioContext) -> tuple[str, str | None]:
    if ctx.env.get("NEON_DATABASE_URL"):
        return ctx.env["NEON_DATABASE_URL"], None
    project_id = neon_project_id(ctx)
    branch_name = unique_name("duckdb-otlp-bench", ctx.run_id, "neon")
    if ctx.dry_run:
        return "postgresql://user:password@host.neon.tech/neondb?sslmode=require", branch_name
    require_program("neonctl")
    create = run_cmd(
        [
            "neonctl",
            "branches",
            "create",
            "--project-id",
            project_id,
            "--name",
            branch_name,
            "--output",
            "json",
        ],
        env=ctx.env,
    )
    payload = json.loads(create.stdout)
    branch = payload.get("branch") or payload
    branch_id = branch.get("id") or branch.get("branch_id") or branch_name
    ctx.cleanup.append(("neon_branch", branch_id))
    database, role = neon_database_and_role(ctx, project_id, branch_id)
    conn = run_cmd(
        [
            "neonctl",
            "connection-string",
            "--project-id",
            project_id,
            "--branch",
            branch_id,
            "--database-name",
            database,
            "--role-name",
            role,
        ],
        env=ctx.env,
    ).stdout.strip()
    return conn, branch_id


def neon_database_and_role(ctx: ScenarioContext, project_id: str, branch_id: str) -> tuple[str, str]:
    configured_database = ctx.env.get("NEON_DATABASE")
    configured_role = ctx.env.get("NEON_ROLE")
    if configured_database and configured_role:
        return configured_database, configured_role

    databases_result = run_cmd(
        [
            "neonctl",
            "databases",
            "list",
            "--project-id",
            project_id,
            "--branch",
            branch_id,
            "--output",
            "json",
        ],
        env=ctx.env,
    )
    databases = json.loads(databases_result.stdout or "[]")
    if not databases:
        raise BenchError(f"no Neon databases found on branch {branch_id}")
    database_row = next((db for db in databases if db.get("name") == configured_database), databases[0])
    database = configured_database or database_row["name"]
    role = configured_role or database_row.get("owner_name")
    if role:
        return database, role

    roles_result = run_cmd(
        [
            "neonctl",
            "roles",
            "list",
            "--project-id",
            project_id,
            "--branch",
            branch_id,
            "--output",
            "json",
        ],
        env=ctx.env,
    )
    roles = json.loads(roles_result.stdout or "[]")
    if not roles:
        raise BenchError(f"no Neon roles found on branch {branch_id}")
    return database, roles[0]["name"]


def r2_neon_ducklake_setup(ctx: ScenarioContext) -> None:
    bucket = create_r2_bucket(ctx, "ducklake-neon")
    prefix = f"bench/{ctx.run_id}/"
    ctx.resources.update({"r2_bucket": bucket, "r2_prefix": prefix})
    preflight_r2_s3_write(ctx, bucket, prefix)
    postgres_url, branch_id = neon_connection_string(ctx)
    pg = parse_postgres_url(postgres_url)
    ctx.resources.update({"neon_branch_id": branch_id})
    ctx.docker_env.update(
        {
            "DUCKDB_MODE": "r2-neon-ducklake",
            "CLOUDFLARE_R2_BUCKET": bucket,
            "CLOUDFLARE_R2_PREFIX": prefix,
            "NEON_PGHOST": pg["host"],
            "NEON_PGPORT": pg["port"],
            "NEON_PGDATABASE": pg["database"],
            "NEON_PGUSER": pg["user"],
            "NEON_PGPASSWORD": pg["password"],
            "NEON_PGSSLMODE": pg["sslmode"],
        }
    )
    r2_storage_secret_sql(ctx)


def parse_postgres_url(url: str) -> dict[str, str]:
    parsed = urllib.parse.urlparse(url)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise BenchError("Neon connection string must use postgres:// or postgresql://")
    query = urllib.parse.parse_qs(parsed.query)
    return {
        "host": parsed.hostname or "",
        "port": str(parsed.port or 5432),
        "database": parsed.path.lstrip("/") or "neondb",
        "user": urllib.parse.unquote(parsed.username or ""),
        "password": urllib.parse.unquote(parsed.password or ""),
        "sslmode": query.get("sslmode", ["require"])[0],
    }


def r2_local_ducklake_setup(ctx: ScenarioContext) -> None:
    bucket = create_r2_bucket(ctx, "ducklake-local")
    prefix = f"bench/{ctx.run_id}/"
    ctx.resources.update({"r2_bucket": bucket, "r2_prefix": prefix})
    preflight_r2_s3_write(ctx, bucket, prefix)
    r2_storage_secret_sql(ctx)
    ctx.docker_env.update(
        {
            "DUCKDB_MODE": "r2-local-ducklake",
            "CLOUDFLARE_R2_BUCKET": bucket,
            "CLOUDFLARE_R2_PREFIX": prefix,
            "DUCKLAKE_CATALOG_PATH": "/tmp/duckdb-otlp-bench.ducklake",
        }
    )


SETUP_BY_SCENARIO = {
    "local-ducklake": local_ducklake_setup,
    "r2-data-catalog": r2_data_catalog_setup,
    "s3-tables": s3_tables_setup,
    "r2-neon-ducklake": r2_neon_ducklake_setup,
    "r2-local-ducklake": r2_local_ducklake_setup,
}


def start_container(ctx: ScenarioContext) -> None:
    configure_container_env(ctx)
    require_program("docker")
    # The daemon image is distroless (no shell/duckdb); Quack queries run from a host duckdb.
    require_program("duckdb")
    run_cmd(["docker", "rm", "-f", ctx.container_name], check=False)
    args = docker_run_args(ctx)
    run_cmd(args)


def configure_container_env(ctx: ScenarioContext) -> None:
    ctx.docker_env.update(
        {
            "DUCKDB_CATALOG": ctx.catalog,
            "DUCKDB_SCHEMA": ctx.schema,
            "DUCKDB_DATABASE": "/tmp/duckdb-otlp-bench-control.duckdb",
            "DUCKDB_OTLP_TOKEN": ctx.token,
            "DUCKDB_QUACK_ENABLED": "1",
            "DUCKDB_QUACK_TOKEN": ctx.token + "-quack",
            "OTEL_HTTP_ADDR": "0.0.0.0:4318",
        }
    )


def docker_run_args(ctx: ScenarioContext) -> list[str]:
    args = [
        "docker",
        "run",
        "-d",
        "--name",
        ctx.container_name,
        "-p",
        f"127.0.0.1:{ctx.port}:4318",
        "-p",
        f"127.0.0.1:{ctx.quack_port}:9494",
    ]
    if ctx.platform:
        args[2:2] = ["--platform", ctx.platform]
    for source, target, mode in ctx.docker_volumes:
        args.extend(["-v", f"{source}:{target}:{mode}"])
    for key, value in sorted(ctx.docker_env.items()):
        if value:
            args.extend(["-e", f"{key}={value}"])
    args.append(ctx.image)
    return args


def wait_for_health(port: int, timeout: float = 300) -> None:
    deadline = time.monotonic() + timeout
    last_error = ""
    while time.monotonic() < deadline:
        try:
            conn = http.client.HTTPConnection("127.0.0.1", port, timeout=2)
            conn.request("GET", "/healthz")
            resp = conn.getresponse()
            body = resp.read()
            conn.close()
            if resp.status == 200 and b"ok" in body:
                return
        except OSError as exc:
            last_error = str(exc)
        time.sleep(1)
    raise BenchError(f"server did not become healthy on port {port}: {last_error}")


def parse_cpu(value: str) -> float | None:
    with contextlib.suppress(ValueError):
        return float(value.strip().rstrip("%"))
    return None


def parse_size_mib(value: str) -> float | None:
    match = re.match(r"([0-9.]+)\s*([KMGTP]?i?B)", value.strip())
    if not match:
        return None
    number = float(match.group(1))
    unit = match.group(2).lower()
    scale = {
        "b": 1 / (1024 * 1024),
        "kb": 1 / 1024,
        "kib": 1 / 1024,
        "mb": 1,
        "mib": 1,
        "gb": 1024,
        "gib": 1024,
        "tb": 1024 * 1024,
        "tib": 1024 * 1024,
    }.get(unit)
    return number * scale if scale else None


def parse_mib(value: str) -> float | None:
    return parse_size_mib(value.split("/", 1)[0])


def parse_net_io(value: str) -> tuple[float | None, float | None]:
    rx, sep, tx = value.partition("/")
    if not sep:
        return None, None
    return parse_size_mib(rx), parse_size_mib(tx)


class DockerStatsSampler:
    def __init__(self, container: str):
        self.container = container
        self.samples: list[dict[str, Any]] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "DockerStatsSampler":
        self._thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        self._stop.set()
        self._thread.join(timeout=5)

    def _run(self) -> None:
        while not self._stop.is_set():
            result = run_cmd(
                ["docker", "stats", "--no-stream", "--format", "{{json .}}", self.container],
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                with contextlib.suppress(json.JSONDecodeError):
                    payload = json.loads(result.stdout)
                    network_rx_mib, network_tx_mib = parse_net_io(payload.get("NetIO", ""))
                    self.samples.append(
                        {
                            "time": dt.datetime.now(dt.UTC).isoformat(),
                            "cpu_percent": parse_cpu(payload.get("CPUPerc", "")),
                            "memory_mib": parse_mib(payload.get("MemUsage", "")),
                            "network_rx_mib": network_rx_mib,
                            "network_tx_mib": network_tx_mib,
                            "raw": payload,
                        }
                    )
            time.sleep(1)

    def summary(self) -> dict[str, Any]:
        cpus = [sample["cpu_percent"] for sample in self.samples if sample.get("cpu_percent") is not None]
        mems = [sample["memory_mib"] for sample in self.samples if sample.get("memory_mib") is not None]
        rx = [sample["network_rx_mib"] for sample in self.samples if sample.get("network_rx_mib") is not None]
        tx = [sample["network_tx_mib"] for sample in self.samples if sample.get("network_tx_mib") is not None]
        return {
            "samples": len(self.samples),
            "cpu_percent_avg": statistics.fmean(cpus) if cpus else None,
            "cpu_percent_max": max(cpus) if cpus else None,
            "memory_mib_avg": statistics.fmean(mems) if mems else None,
            "memory_mib_max": max(mems) if mems else None,
            "network_rx_mib_delta": rx[-1] - rx[0] if len(rx) >= 2 else None,
            "network_tx_mib_delta": tx[-1] - tx[0] if len(tx) >= 2 else None,
            "network_rx_mib_start": rx[0] if rx else None,
            "network_rx_mib_end": rx[-1] if rx else None,
            "network_tx_mib_start": tx[0] if tx else None,
            "network_tx_mib_end": tx[-1] if tx else None,
        }


class ServerStatsSampler:
    def __init__(self, ctx: ScenarioContext, interval: float):
        self.ctx = ctx
        self.interval = interval
        self.samples: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def __enter__(self) -> "ServerStatsSampler":
        if self.interval > 0:
            self._thread.start()
        return self

    def __exit__(self, *_: Any) -> None:
        if self.interval > 0:
            self._stop.set()
            self._thread.join(timeout=max(5, self.interval * 2))

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                rows = query_json(self.ctx, "SELECT * FROM otlp_server_list()", "server-sample", timeout=30)
                self.samples.append(server_sample_summary(rows))
            except Exception as exc:
                self.errors.append(str(exc))
            self._stop.wait(self.interval)

    def summary(self) -> dict[str, Any]:
        buffered = numeric_values(self.samples, "buffered_rows")
        seal_age = numeric_values(self.samples, "last_seal_age_ms")
        seals = numeric_values(self.samples, "seals_total")
        failures = numeric_values(self.samples, "seal_failures_total")
        return {
            "samples": len(self.samples),
            "errors": self.errors[:10],
            "max_buffered_rows": max(buffered) if buffered else None,
            "max_last_seal_age_ms": max(seal_age) if seal_age else None,
            "max_seals_total": max(seals) if seals else None,
            "max_seal_failures_total": max(failures) if failures else None,
            "samples_tail": self.samples[-10:],
        }


def server_sample_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "time": dt.datetime.now(dt.UTC).isoformat(),
        "buffered_rows": sum(int(row.get("buffered_rows") or 0) for row in rows),
        "last_seal_age_ms": max(
            (int(row.get("last_seal_age_ms")) for row in rows if row.get("last_seal_age_ms") is not None),
            default=None,
        ),
        "seals_total": max((int(row.get("seals_total") or 0) for row in rows), default=0),
        "seal_failures_total": sum(int(row.get("seal_failures_total") or 0) for row in rows),
        "rows": rows,
    }


def producer_result_to_load(producer: dict[str, Any], batch_size: int) -> dict[str, Any]:
    elapsed = float(producer.get("duration_seconds") or 0)
    attempted = int(producer.get("attempted_records") or 0)
    accepted = int(producer.get("accepted_records") or 0)
    rejected = int(producer.get("rejected_records") or 0)
    failed = int(producer.get("failed_records") or 0)
    missed = int(producer.get("scheduler_missed_records") or 0)
    attempted_body_bytes = int(producer.get("gzip_bytes") or 0)
    accepted_body_bytes = int(producer.get("accepted_gzip_bytes") or 0)
    latency = producer.get("request_latency_ms") or {}
    return {
        "duration_seconds": elapsed,
        "target_records_per_second": int(producer.get("configured_offered_records_per_second") or 0),
        "batch_size": batch_size,
        "attempted_records": attempted,
        "accepted_records": accepted,
        "dropped_records": rejected + failed + missed,
        "attempted_body_bytes": attempted_body_bytes,
        "accepted_body_bytes": accepted_body_bytes,
        "batches": int(producer.get("requests_attempted") or 0),
        "errors_by_status": {
            status: count for status, count in (producer.get("response_status_counts") or {}).items() if status != "202"
        },
        "accepted_records_per_second": producer.get("accepted_records_per_second"),
        "accepted_body_bytes_per_second": accepted_body_bytes / elapsed if elapsed else None,
        "accepted_body_mib_per_second": accepted_body_bytes / elapsed / (1024 * 1024) if elapsed else None,
        "accepted_body_bytes_per_record": accepted_body_bytes / accepted if accepted else None,
        "post_latency_ms_avg": latency.get("mean"),
        "post_latency_ms_p50": latency.get("p50"),
        "post_latency_ms_p95": latency.get("p95"),
        "post_latency_ms_p99": latency.get("p99"),
        "post_latency_ms_max": latency.get("max"),
    }


def send_load(
    ctx: ScenarioContext,
    duration: float,
    rate: int,
    batch_size: int,
    body_payload_bytes: int,
    concurrency: int,
    queue_depth: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    output = ctx.output_dir / f"{slug(ctx.container_name)}-producer.json"
    result = run_cmd(
        [
            str(ctx.producer_bin),
            "--mode",
            "run",
            "--url",
            f"http://127.0.0.1:{ctx.port}/v1/logs",
            "--token",
            ctx.token,
            "--run-id",
            ctx.container_name,
            "--scenario",
            ctx.name,
            "--rate",
            str(rate),
            "--duration",
            f"{duration}s",
            "--batch-size",
            str(batch_size),
            "--body-payload-bytes",
            str(body_payload_bytes),
            "--concurrency",
            str(concurrency),
            "--queue-depth",
            str(queue_depth),
            "--output",
            str(output),
        ],
        check=False,
        timeout=duration + 120,
    )
    if not output.exists():
        raise BenchError(f"producer did not write {output} (exit {result.returncode}): {result.stderr[-2000:]}")
    producer = json.loads(output.read_text())
    if result.returncode != 0:
        raise BenchError(
            f"producer failed with exit {result.returncode}: {producer.get('error') or result.stderr[-2000:]}"
        )
    if int(producer.get("schema_version") or 0) != 2:
        raise BenchError(f"unsupported producer result schema: {producer.get('schema_version')}")
    return producer_result_to_load(producer, batch_size), producer


# The daemon image is distroless (no shell/duckdb inside the container), so Quack queries
# run from the host duckdb CLI against the published Quack port (ctx.quack_port -> 9494).
def quack_query_expr(ctx: ScenarioContext, sql: str) -> str:
    return "\n".join(
        [
            "FROM quack_query(",
            f"  {sql_quote(f'quack:127.0.0.1:{ctx.quack_port}')},",
            f"  {sql_quote(sql)},",
            f"  token = {sql_quote(ctx.token + '-quack')}",
            ")",
        ]
    )


def run_host_duckdb(control: str, timeout: float = 60) -> str:
    return run_cmd(["duckdb", "-unsigned", ":memory:"], input_text=control, timeout=timeout).stdout


def quack_exec(ctx: ScenarioContext, sql: str) -> None:
    run_host_duckdb("INSTALL quack;\nLOAD quack;\n" + quack_query_expr(ctx, sql) + ";\n")


def query_json(ctx: ScenarioContext, query: str, name: str, timeout: float = 60) -> list[dict[str, Any]]:
    fd, tmp = tempfile.mkstemp(prefix=f"bench-{slug(name)}-", suffix=".json")
    os.close(fd)
    os.unlink(tmp)  # COPY ... TO creates the file fresh
    out_path = Path(tmp)
    control = (
        "INSTALL quack;\nLOAD quack;\nCOPY (\n"
        + quack_query_expr(ctx, query)
        + f"\n) TO {sql_quote(str(out_path))} (FORMAT JSON);\n"
    )
    try:
        run_host_duckdb(control, timeout=timeout)
        raw = out_path.read_text().strip() if out_path.exists() else ""
    finally:
        with contextlib.suppress(FileNotFoundError):
            out_path.unlink()
    if not raw:
        return []
    with contextlib.suppress(json.JSONDecodeError):
        payload = json.loads(raw)
        return payload if isinstance(payload, list) else [payload]
    return [json.loads(line) for line in raw.splitlines() if line]


def collect_db_stats(ctx: ScenarioContext) -> dict[str, Any]:
    flush_start = time.monotonic()
    flush = query_json(ctx, "SELECT * FROM otlp_flush('otlp:0.0.0.0:4318')", "flush")
    flush_seconds = time.monotonic() - flush_start
    server_start = time.monotonic()
    server = query_json(ctx, "SELECT * FROM otlp_server_list()", "server-list")
    server_seconds = time.monotonic() - server_start
    parts = []
    for table in DEFAULT_TABLES:
        parts.append(
            f"SELECT {sql_quote(table)} AS table_name, count(*) AS row_count FROM {ctx.catalog}.{ctx.schema}.{table}"
        )
    count_start = time.monotonic()
    counts = query_json(ctx, "\nUNION ALL\n".join(parts), "row-counts")
    count_seconds = time.monotonic() - count_start
    return {
        "flush": flush,
        "server": server,
        "table_counts": counts,
        "timings": {
            "flush_seconds": flush_seconds,
            "server_list_seconds": server_seconds,
            "row_count_seconds": count_seconds,
        },
    }


def validate_db_stats(load: dict[str, Any], db: dict[str, Any]) -> None:
    flush = db.get("flush") or []
    if any(row.get("status") == "error" or row.get("error") for row in flush):
        raise BenchError(f"flush failed: {json.dumps(flush, sort_keys=True)}")
    server = db.get("server") or []
    buffered_rows = sum(int(row.get("buffered_rows") or 0) for row in server)
    if buffered_rows:
        raise BenchError(f"{buffered_rows} rows remained buffered after flush")
    table_counts = db.get("table_counts") or []
    logs_rows = next(
        (int(row.get("row_count") or 0) for row in table_counts if row.get("table_name") == "otlp_logs"), 0
    )
    accepted = int(load.get("accepted_records") or 0)
    if logs_rows != accepted:
        raise BenchError(f"committed otlp_logs rows ({logs_rows}) did not match accepted records ({accepted})")


def committed_log_rows(db: dict[str, Any]) -> int:
    table_counts = db.get("table_counts") or []
    return next((int(row.get("row_count") or 0) for row in table_counts if row.get("table_name") == "otlp_logs"), 0)


def benchmark_metrics(load: dict[str, Any], db: dict[str, Any], measured_seconds: float) -> dict[str, Any]:
    rows = committed_log_rows(db)
    flush_seconds = (db.get("timings") or {}).get("flush_seconds")
    server = db.get("server") or []
    seals_total = max((int(row.get("seals_total") or 0) for row in server), default=0)
    seal_failures_total = sum(int(row.get("seal_failures_total") or 0) for row in server)
    storage_seconds = measured_seconds
    return {
        "committed_log_rows": rows,
        "measured_seconds": storage_seconds,
        "durable_records_per_second": rows / storage_seconds if storage_seconds else None,
        "flush_seconds": flush_seconds,
        "flush_records_per_second": rows / flush_seconds if rows and flush_seconds else None,
        "accepted_records_per_second": load.get("accepted_records_per_second"),
        "dropped_records": load.get("dropped_records"),
        "seals_total": seals_total,
        "seal_failures_total": seal_failures_total,
        "rows_per_seal": rows / seals_total if seals_total else None,
    }


def drop_tables(ctx: ScenarioContext) -> None:
    statements = ["SELECT status FROM otlp_stop('otlp:0.0.0.0:4318');"]
    statements.extend(f"DROP TABLE IF EXISTS {ctx.catalog}.{ctx.schema}.{table};" for table in DEFAULT_TABLES)
    statements.append(f"DETACH {ctx.catalog};")
    quack_exec(ctx, "\n".join(statements) + "\n")
    time.sleep(2)


def stop_container(ctx: ScenarioContext) -> None:
    run_cmd(["docker", "stop", ctx.container_name], check=False, timeout=90)
    run_cmd(["docker", "rm", "-f", ctx.container_name], check=False)


def s3_tables_summary(ctx: ScenarioContext) -> dict[str, Any]:
    profile = ctx.resources.get("aws_profile")
    if profile == "default":
        profile = None
    region = ctx.resources.get("aws_region")
    bucket_arn = ctx.resources.get("s3_tables_bucket_arn")
    if not (profile and region and bucket_arn):
        return {}
    result = run_cmd(
        aws_cli(
            ctx.env,
            "s3tables",
            "list-tables",
            "--table-bucket-arn",
            bucket_arn,
            "--namespace",
            "otlp",
            "--output",
            "json",
            profile=profile,
            region=region,
        ),
        env=ctx.env,
        check=False,
    )
    if result.returncode != 0:
        return {"error": result.stderr.strip()}
    payload = json.loads(result.stdout or "{}")
    return {"table_count": len(payload.get("tables", [])), "tables": payload.get("tables", [])}


def storage_summary(ctx: ScenarioContext) -> dict[str, Any]:
    if ctx.name == "local-ducklake":
        data_path = ctx.resources.get("data_path")
        if not data_path:
            return {}
        # Distroless image has no shell/find; list files via the daemon's own glob() over Quack.
        glob_pattern = data_path.rstrip("/") + "/**"
        try:
            rows = query_json(ctx, f"SELECT file FROM glob({sql_quote(glob_pattern)})", "storage-files")
        except BenchError as exc:
            return {"error": str(exc)}
        files = [row["file"] for row in rows if row.get("file")]
        return {
            "path": data_path,
            "objects": len(files),
            "parquet_files": sum(1 for path in files if path.endswith(".parquet")),
            "files": files[:50],
        }
    if ctx.name.startswith("r2-") or ctx.name == "r2-data-catalog":
        bucket = ctx.resources.get("r2_bucket")
        return r2_object_summary(ctx, bucket) if bucket else {}
    if ctx.name == "s3-tables":
        return s3_tables_summary(ctx)
    return {"note": "object files are managed by the provider and are not enumerated by this harness"}


def empty_s3_tables_bucket(ctx: ScenarioContext, arn: str, profile: str | None, region: str) -> None:
    """Delete every table and namespace in an S3 Tables bucket so CloudFormation can delete the
    (now-empty) bucket. CloudFormation delete-stack fails on a populated table bucket, which
    otherwise leaks the bucket and leaves the stack in DELETE_FAILED."""
    ns_result = run_cmd(
        aws_cli(
            ctx.env,
            "s3tables",
            "list-namespaces",
            "--table-bucket-arn",
            arn,
            "--query",
            "namespaces[].namespace[0]",
            "--output",
            "text",
            profile=profile,
            region=region,
        ),
        env=ctx.env,
        check=False,
    )
    namespaces = ns_result.stdout.split() if ns_result.returncode == 0 else []
    for namespace in namespaces:
        tbl_result = run_cmd(
            aws_cli(
                ctx.env,
                "s3tables",
                "list-tables",
                "--table-bucket-arn",
                arn,
                "--namespace",
                namespace,
                "--query",
                "tables[].name",
                "--output",
                "text",
                profile=profile,
                region=region,
            ),
            env=ctx.env,
            check=False,
        )
        for name in tbl_result.stdout.split() if tbl_result.returncode == 0 else []:
            run_cmd(
                aws_cli(
                    ctx.env,
                    "s3tables",
                    "delete-table",
                    "--table-bucket-arn",
                    arn,
                    "--namespace",
                    namespace,
                    "--name",
                    name,
                    profile=profile,
                    region=region,
                ),
                env=ctx.env,
                check=False,
            )
        run_cmd(
            aws_cli(
                ctx.env,
                "s3tables",
                "delete-namespace",
                "--table-bucket-arn",
                arn,
                "--namespace",
                namespace,
                profile=profile,
                region=region,
            ),
            env=ctx.env,
            check=False,
        )


def cleanup_resource(ctx: ScenarioContext, item: tuple[str, Any]) -> None:
    kind, value = item
    if kind == "r2_catalog":
        wrangler_cmd(ctx, "r2", "bucket", "catalog", "disable", value, check=False)
    elif kind == "r2_bucket":
        with contextlib.suppress(Exception):
            delete_r2_objects(ctx, value)
        delete_r2_bucket(ctx, value)
    elif kind == "cloudformation_stack":
        profile = value.get("profile")
        run_cmd(
            aws_cli(
                ctx.env,
                "cloudformation",
                "delete-stack",
                "--stack-name",
                value["stack"],
                profile=profile,
                region=value["region"],
            ),
            env=ctx.env,
            check=False,
        )
        run_cmd(
            aws_cli(
                ctx.env,
                "cloudformation",
                "wait",
                "stack-delete-complete",
                "--stack-name",
                value["stack"],
                profile=profile,
                region=value["region"],
            ),
            env=ctx.env,
            check=False,
        )
    elif kind == "s3_tables_bucket":
        empty_s3_tables_bucket(ctx, value["arn"], value.get("profile"), value["region"])
    elif kind == "neon_branch" and value:
        project_id = ctx.env.get("NEON_PROJECT_ID") or neon_project_id(ctx)
        run_cmd(
            [
                "neonctl",
                "branches",
                "delete",
                str(value),
                "--project-id",
                project_id,
                "--yes",
            ],
            env=ctx.env,
            check=False,
        )


def cleanup(ctx: ScenarioContext, *, drop: bool) -> None:
    if not ctx.keep and drop:
        with contextlib.suppress(Exception):
            drop_tables(ctx)
    if not ctx.keep:
        stop_container(ctx)
        for item in reversed(ctx.cleanup):
            with contextlib.suppress(Exception):
                cleanup_resource(ctx, item)


def run_scenario(
    args: argparse.Namespace,
    env: dict[str, str],
    run: TrialRun,
    run_id: str,
    output_dir: Path,
    producer_bin: Path,
) -> dict[str, Any]:
    name = run.scenario
    ctx = ScenarioContext(
        name=name,
        run_id=run_id,
        env=env,
        output_dir=output_dir,
        keep=args.keep,
        image=args.image,
        platform=args.platform,
        port=args.port,
        token=args.token,
        container_name=unique_name("duckdb-otlp", run_id, f"{name}-t{run.trial}-r{run.rate}", max_len=48),
        producer_bin=producer_bin,
        dry_run=args.dry_run,
        quack_port=free_port(),
    )
    started = False
    drop = False
    result: dict[str, Any] = {
        "scenario": name,
        "trial": run.trial,
        "rate": run.rate,
        "order": run.order,
        "resources": ctx.resources,
    }
    try:
        eprint(f"[{name} t{run.trial} r{run.rate:,}] preparing disposable resources")
        SETUP_BY_SCENARIO[name](ctx)
        configure_container_env(ctx)
        if args.dry_run:
            result.update(
                {
                    "status": "dry-run",
                    "docker_env": json.loads(redact_text(json.dumps(ctx.docker_env, sort_keys=True), ctx.env)),
                    "docker_volumes": ctx.docker_volumes,
                    "resources": ctx.resources,
                }
            )
            cleanup(ctx, drop=False)
            return result
        eprint(f"[{name} t{run.trial} r{run.rate:,}] starting {ctx.image}")
        start_container(ctx)
        started = True
        wait_for_health(ctx.port, timeout=args.startup_timeout)
        eprint(f"[{name} t{run.trial} r{run.rate:,}] sending records for {args.duration}s")
        measured_start = time.monotonic()
        with DockerStatsSampler(ctx.container_name) as docker_sampler, ServerStatsSampler(
            ctx, args.sample_interval
        ) as server_sampler:
            load, producer = send_load(
                ctx,
                args.duration,
                run.rate,
                args.batch_size,
                args.body_payload_bytes,
                args.producer_concurrency,
                args.producer_queue_depth,
            )
        stats = docker_sampler.summary()
        server_stats = server_sampler.summary()
        eprint(f"[{name} t{run.trial} r{run.rate:,}] flushing and collecting counters")
        db = collect_db_stats(ctx)
        measured_seconds = time.monotonic() - measured_start
        validate_db_stats(load, db)
        drop = True
        storage = storage_summary(ctx)
        metrics = benchmark_metrics(load, db, measured_seconds)
        result.update(
            {
                "status": "ok",
                "container": ctx.container_name,
                "resources": ctx.resources,
                "load": load,
                "producer": producer,
                "metrics": metrics,
                "docker_stats": stats,
                "server_stats": server_stats,
                "db": db,
                "storage": storage,
            }
        )
        return result
    except Exception as exc:
        logs = ""
        if started:
            logs = run_cmd(["docker", "logs", "--tail", "200", ctx.container_name], check=False).stdout
        result.update(
            {
                "status": "error",
                "error": redact_text(str(exc), ctx.env, ctx.docker_env),
                "container_logs_tail": redact_text(logs, ctx.env, ctx.docker_env),
                "resources": ctx.resources,
            }
        )
        return result
    finally:
        if not args.keep:
            eprint(f"[{name} t{run.trial} r{run.rate:,}] cleaning up")
        cleanup(ctx, drop=drop)


def coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    with contextlib.suppress(TypeError, ValueError):
        return float(value)
    return None


def numeric_values(items: list[dict[str, Any]], key: str) -> list[float]:
    values = []
    for item in items:
        value = coerce_float(item.get(key))
        if value is not None and math.isfinite(value):
            values.append(value)
    return values


def metric_values(results: list[dict[str, Any]], section: str, key: str) -> list[float]:
    return numeric_values([result.get(section) or {} for result in results], key)


def mean_ci95(values: list[float]) -> tuple[float, float | None] | None:
    if not values:
        return None
    mean = statistics.fmean(values)
    if len(values) < 2:
        return mean, None
    ci95 = 1.96 * statistics.stdev(values) / math.sqrt(len(values))
    return mean, ci95


def percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, math.ceil((pct / 100) * len(ordered)) - 1))
    return ordered[index]


def format_mean_ci(values: list[float]) -> str:
    summary = mean_ci95(values)
    if summary is None:
        return "n/a"
    mean, ci95 = summary
    if ci95 is None:
        return format_number(mean)
    return f"{format_number(mean)} ± {format_number(ci95)}"


def storage_file_count(result: dict[str, Any]) -> Any:
    storage = result.get("storage") or {}
    return storage.get("objects", storage.get("table_count", "n/a"))


def result_groups(results: list[dict[str, Any]]) -> dict[tuple[str, int], list[dict[str, Any]]]:
    groups: dict[tuple[str, int], list[dict[str, Any]]] = {}
    for result in results:
        key = (result["scenario"], int(result.get("rate") or 0))
        groups.setdefault(key, []).append(result)
    return dict(sorted(groups.items(), key=lambda item: (item[0][0], item[0][1])))


def write_report(results: list[dict[str, Any]], output_dir: Path, source: str, run_config: dict[str, Any]) -> None:
    report = output_dir / "report.md"
    lines = [
        "# duckdb-otlp catalog ingest benchmark",
        "",
        f"Run time: {dt.datetime.now(dt.UTC).isoformat()}",
        "",
        "Run config:",
        "",
        "```json",
        json.dumps(run_config, indent=2, sort_keys=True),
        "```",
        "",
        f"Inspired by the OpenTelemetry Collector load-test shape: {source}",
        "",
        "The producer sends gzip-compressed OTLP protobuf. Accepted MiB/s is compressed request-body throughput.",
        "",
        "## Summary",
        "",
        "| Scenario | Offered rows/s | OK/Trials | Durable rows/s mean ± 95% CI | Accepted MiB/s mean ± 95% CI | Container TX MiB median | Accepted rows/s mean ± 95% CI | POST p95 ms mean | Flush seconds p95 | Max buffered rows p95 | Avg CPU % mean | Storage files median |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for (scenario, rate), group in result_groups(results).items():
        ok = [result for result in group if result.get("status") == "ok"]
        durable = metric_values(ok, "metrics", "durable_records_per_second")
        accepted = metric_values(ok, "metrics", "accepted_records_per_second")
        accepted_mib = metric_values(ok, "load", "accepted_body_mib_per_second")
        network_tx = metric_values(ok, "docker_stats", "network_tx_mib_delta")
        post_p95 = metric_values(ok, "load", "post_latency_ms_p95")
        flush = metric_values(ok, "metrics", "flush_seconds")
        buffered = metric_values(ok, "server_stats", "max_buffered_rows")
        cpu = metric_values(ok, "docker_stats", "cpu_percent_avg")
        files = numeric_values([{"files": storage_file_count(result)} for result in ok], "files")
        lines.append(
            "| {scenario} | {rate} | {ok_trials}/{trials} | {durable} | {accepted_mib} | {network_tx} | {accepted} | {post_p95} | {flush_p95} | {buffered_p95} | {cpu_avg} | {files_median} |".format(
                scenario=scenario,
                rate=format_number(rate),
                ok_trials=len(ok),
                trials=len(group),
                durable=format_mean_ci(durable),
                accepted_mib=format_mean_ci(accepted_mib),
                network_tx=format_number(percentile(network_tx, 50)),
                accepted=format_mean_ci(accepted),
                post_p95=format_mean_ci(post_p95),
                flush_p95=format_number(percentile(flush, 95)),
                buffered_p95=format_number(percentile(buffered, 95)),
                cpu_avg=format_mean_ci(cpu),
                files_median=format_number(percentile(files, 50)),
            )
        )

    lines.extend(
        [
            "",
            "## Trial Results",
            "",
            "| Order | Scenario | Trial | Offered rows/s | Status | Durable rows/s | Accepted MiB/s | Container RX MiB | Container TX MiB | Accepted rows/s | POST p95 ms | Dropped | Flush seconds | Max buffered rows | Seals | Avg CPU % | Storage files |",
            "| ---: | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
        ]
    )
    for result in sorted(results, key=lambda item: int(item.get("order") or 0)):
        metrics = result.get("metrics") or {}
        stats = result.get("docker_stats") or {}
        server_stats = result.get("server_stats") or {}
        load = result.get("load") or {}
        lines.append(
            "| {order} | {scenario} | {trial} | {rate} | {status} | {durable} | {accepted_mib} | {network_rx} | {network_tx} | {accepted} | {post_p95} | {dropped} | {flush} | {buffered} | {seals} | {cpu} | {files} |".format(
                order=result.get("order", "n/a"),
                scenario=result["scenario"],
                trial=result.get("trial", "n/a"),
                rate=format_number(result.get("rate")),
                status=result["status"],
                durable=format_number(metrics.get("durable_records_per_second")),
                accepted_mib=format_number(load.get("accepted_body_mib_per_second")),
                network_rx=format_number(stats.get("network_rx_mib_delta")),
                network_tx=format_number(stats.get("network_tx_mib_delta")),
                accepted=format_number(metrics.get("accepted_records_per_second")),
                post_p95=format_number(load.get("post_latency_ms_p95")),
                dropped=format_number(metrics.get("dropped_records")),
                flush=format_number(metrics.get("flush_seconds")),
                buffered=format_number(server_stats.get("max_buffered_rows")),
                seals=format_number(metrics.get("seals_total")),
                cpu=format_number(stats.get("cpu_percent_avg")),
                files=storage_file_count(result),
            )
        )

    lines.extend(["", "## Details", ""])
    for result in results:
        lines.extend([f"### {result['scenario']} trial {result.get('trial')} rate {result.get('rate')}", ""])
        lines.append(f"Status: `{result['status']}`")
        if result.get("error"):
            lines.append("")
            lines.append("Error:")
            lines.append("")
            lines.append("```text")
            lines.append(result["error"])
            lines.append("```")
        lines.append("")
        lines.append("```json")
        lines.append(json.dumps(result, indent=2, sort_keys=True))
        lines.append("```")
        lines.append("")
    report.write_text("\n".join(lines))


def format_number(value: Any) -> str:
    if value is None:
        return "n/a"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", action="append", choices=SCENARIOS + ("all",), default=None)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--image", default=os.environ.get("DUCKDB_OTLP_BENCH_IMAGE", DEFAULT_IMAGE))
    parser.add_argument(
        "--platform",
        default=os.environ.get("DUCKDB_OTLP_BENCH_PLATFORM") or host_docker_platform(),
        help="Docker platform. Defaults to the host CPU architecture.",
    )
    parser.add_argument("--duration", type=float, default=60)
    parser.add_argument(
        "--rate",
        type=int,
        default=int(os.environ.get("DUCKDB_OTLP_BENCH_RATE", DEFAULT_RATE)),
        help="Offered rows/s for a single-rate run. Defaults near the size-trigger seal regime.",
    )
    parser.add_argument(
        "--rate-sweep",
        default=os.environ.get("DUCKDB_OTLP_BENCH_RATE_SWEEP"),
        help=(
            "Comma-separated offered rows/s values. Overrides --rate when provided. "
            f"Recommended sweep: {DEFAULT_RATE_SWEEP}."
        ),
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=int(os.environ.get("DUCKDB_OTLP_BENCH_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
        help="Rows per OTLP request batch.",
    )
    parser.add_argument(
        "--body-payload-bytes",
        type=int,
        default=int(os.environ.get("DUCKDB_OTLP_BENCH_BODY_PAYLOAD_BYTES", DEFAULT_BODY_PAYLOAD_BYTES)),
        help="Deterministic log body bytes per record.",
    )
    parser.add_argument(
        "--producer-concurrency",
        type=int,
        default=DEFAULT_PRODUCER_CONCURRENCY,
        help="Concurrent OTLP request workers.",
    )
    parser.add_argument(
        "--producer-queue-depth",
        type=int,
        default=DEFAULT_PRODUCER_QUEUE_DEPTH,
        help="Bounded producer request queue depth.",
    )
    parser.add_argument(
        "--producer-bin",
        type=Path,
        help="Use an existing shared Go producer executable instead of building it.",
    )
    parser.add_argument("--trials", type=int, default=1, help="Independent repetitions per scenario/rate.")
    parser.add_argument(
        "--sample-interval",
        type=float,
        default=5,
        help="Seconds between otlp_server_list samples during load. Set 0 to disable.",
    )
    parser.add_argument("--random-seed", type=int, help="Seed for randomized execution order.")
    parser.add_argument(
        "--no-randomize", action="store_true", help="Preserve scenario/rate order instead of shuffling."
    )
    parser.add_argument("--startup-timeout", type=float, default=300)
    parser.add_argument("--port", type=int, default=0, help="Host port. Defaults to an available local port.")
    parser.add_argument("--token", default=DEFAULT_TOKEN)
    parser.add_argument("--output-dir", type=Path, default=Path("output/catalog-benchmarks"))
    parser.add_argument("--keep", action="store_true", help="Keep containers and cloud resources for debugging.")
    parser.add_argument(
        "--dry-run", action="store_true", help="Prepare scenario SQL/resources plan without starting load."
    )
    return parser.parse_args()


def selected_scenarios(values: list[str]) -> list[str]:
    if not values:
        return list(SCENARIOS)
    if "all" in values:
        return list(SCENARIOS)
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value not in seen:
            ordered.append(value)
            seen.add(value)
    return ordered


def selected_rates(args: argparse.Namespace) -> list[int]:
    if not args.rate_sweep:
        return [args.rate]
    rates: list[int] = []
    for raw in args.rate_sweep.split(","):
        value = raw.strip()
        if not value:
            continue
        try:
            rate = int(value)
        except ValueError as exc:
            raise BenchError(f"invalid --rate-sweep value: {value}") from exc
        if rate <= 0:
            raise BenchError("--rate-sweep values must be greater than zero")
        rates.append(rate)
    if not rates:
        raise BenchError("--rate-sweep must include at least one positive integer")
    return rates


def validate_args(args: argparse.Namespace) -> None:
    if args.duration <= 0:
        raise BenchError("--duration must be greater than zero")
    if args.rate <= 0:
        raise BenchError("--rate must be greater than zero")
    if args.batch_size <= 0:
        raise BenchError("--batch-size must be greater than zero")
    if args.body_payload_bytes <= 0:
        raise BenchError("--body-payload-bytes must be greater than zero")
    if args.producer_concurrency <= 0:
        raise BenchError("--producer-concurrency must be greater than zero")
    if args.producer_queue_depth <= 0:
        raise BenchError("--producer-queue-depth must be greater than zero")
    if args.trials <= 0:
        raise BenchError("--trials must be greater than zero")
    if args.sample_interval < 0:
        raise BenchError("--sample-interval must be zero or greater")


def build_trial_runs(
    scenarios: list[str], rates: list[int], trials: int, rng: random.Random, randomize: bool
) -> list[TrialRun]:
    runs: list[TrialRun] = []
    order = 1
    for trial in range(1, trials + 1):
        block = [(scenario, rate) for scenario in scenarios for rate in rates]
        if randomize:
            rng.shuffle(block)
        for scenario, rate in block:
            runs.append(TrialRun(scenario=scenario, trial=trial, rate=rate, order=order))
            order += 1
    return runs


def main() -> int:
    args = parse_args()
    validate_args(args)
    env = merged_env(args.env_file)
    run_id = make_run_id()
    output_dir = args.output_dir / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    producer_bin = build_producer(output_dir, args.producer_bin) if not args.dry_run else Path("dry-run")
    scenarios = selected_scenarios(args.scenario)
    rates = selected_rates(args)
    random_seed = args.random_seed if args.random_seed is not None else secrets.randbelow(2**32)
    rng = random.Random(random_seed)
    runs = build_trial_runs(scenarios, rates, args.trials, rng, not args.no_randomize)
    results = []
    requested_port = args.port
    for run in runs:
        args.port = free_port() if requested_port == 0 else requested_port
        results.append(run_scenario(args, env, run, run_id, output_dir, producer_bin))
    (output_dir / "results.json").write_text(json.dumps(results, indent=2, sort_keys=True))
    source = "https://open-telemetry.github.io/opentelemetry-collector-contrib/benchmarks/loadtests/"
    run_config = {
        "run_id": run_id,
        "scenarios": scenarios,
        "rates": rates,
        "trials": args.trials,
        "duration_seconds": args.duration,
        "batch_size": args.batch_size,
        "body_payload_bytes": args.body_payload_bytes,
        "producer_concurrency": args.producer_concurrency,
        "producer_queue_depth": args.producer_queue_depth,
        "producer_schema_version": 2,
        "sample_interval_seconds": args.sample_interval,
        "randomized": not args.no_randomize,
        "random_seed": random_seed,
        "image": args.image,
        "platform": args.platform,
    }
    write_report(results, output_dir, source, run_config)
    eprint(f"wrote {output_dir / 'results.json'}")
    eprint(f"wrote {output_dir / 'report.md'}")
    return 1 if any(result["status"] == "error" for result in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
