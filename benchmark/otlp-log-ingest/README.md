# Local Mac Studio capacity benchmark

This benchmark drives the native `duckdb-otlp-server` image with a compiled Go
OTLP/HTTP protobuf producer. It calibrates complete gzip-compressed requests to
approximately 786 bytes per log record, then runs:

1. 15 seconds at 10,000 records/s.
2. 60 seconds of warm-up at 175,000 records/s.
3. 180 measured seconds at 175,000 records/s.

The consumer is pinned to the host CPU architecture and limited to 4 CPUs and
8 GiB RAM. The producer uses four request workers and a bounded eight-request
queue.

```bash
uv run --with pytest --with black python benchmark/otlp-log-ingest/run_local.py plan
uv run --with pytest --with black pytest benchmark/otlp-log-ingest/test_run_local.py
uv run python benchmark/otlp-log-ingest/run_local.py smoke
uv run python benchmark/otlp-log-ingest/run_local.py run
uv run python benchmark/otlp-log-ingest/run_local.py status
uv run python benchmark/otlp-log-ingest/run_local.py cleanup
```

All generated data lives under `benchmark/otlp-log-ingest/output/<run-id>`.
Warm-up and smoke storage are deleted before the next phase. Unless
`--keep-data` is passed, measured DuckLake data is also removed after storage
statistics and reports are written.

This is a shared-host local capacity gate. OrbStack localhost networking is not
isolated EC2 networking, local disk is not S3, and the compressed-size
calibration is only a proxy for the unpublished source workload.

The Go module in `producer/` is also the canonical workload generator for
`scripts/benchmark_catalog_ingest.py`. The catalog harness uses a fixed small
record body while this capacity suite uses compressed-size calibration. Python
owns provisioning, observation, reconciliation, and reporting; Go owns OTLP
payload generation, pacing, HTTP transport, and producer-side metrics.

## Run the AWS DuckLake/S3 benchmark

`run_aws.py` provisions two private ARM64 EC2 instances with CloudFormation:
the producer runs the shared Go binary directly, while the consumer runs the
existing daemon container with its DuckLake catalogue on encrypted EBS and
Parquet data in regional S3.

The stack deliberately has no NAT gateway, internet gateway, public subnet,
public IP, or SSH ingress. Stage the ARM64 daemon image and matching ARM64
DuckDB CLI from the operator machine; instances download them through the S3
gateway endpoint. The only interface endpoints are SSM and SSM Messages.

Prepare offline artefacts:

```bash
docker save duckdb-otlp:daemon -o /tmp/duckdb-otlp-daemon-arm64.tar
gzip -1 /tmp/duckdb-otlp-daemon-arm64.tar
chmod +x /path/to/duckdb-arm64
```

Use one run ID for the complete lifecycle:

```bash
RUN_ID=aws-$(date -u +%Y%m%d-%H%M%S)-test

# Validation only: no resources are created.
uv run python benchmark/otlp-log-ingest/run_aws.py plan \
  --run-id "$RUN_ID" --region us-west-2

# This is the first billable command.
uv run python benchmark/otlp-log-ingest/run_aws.py provision \
  --run-id "$RUN_ID" --region us-west-2

uv run python benchmark/otlp-log-ingest/run_aws.py smoke \
  --run-id "$RUN_ID" --region us-west-2 \
  --image duckdb-otlp:daemon \
  --image-archive /tmp/duckdb-otlp-daemon-arm64.tar.gz \
  --duckdb-cli /path/to/duckdb-arm64 \
  --consumer-cpus 2 --consumer-memory-gib 3 \
  --max-buffered-bytes 536870912 \
  --retain

uv run python benchmark/otlp-log-ingest/run_aws.py run \
  --run-id "$RUN_ID" --region us-west-2 \
  --image duckdb-otlp:daemon \
  --image-archive /tmp/duckdb-otlp-daemon-arm64.tar.gz \
  --duckdb-cli /path/to/duckdb-arm64

uv run python benchmark/otlp-log-ingest/run_aws.py status \
  --run-id "$RUN_ID" --region us-west-2

uv run python benchmark/otlp-log-ingest/run_aws.py cleanup \
  --run-id "$RUN_ID" --region us-west-2
```

The default 4 CPU/8 GiB container limits target `c7g.xlarge`. Reduce the
explicit consumer limits for a cheap `t4g.medium` smoke as shown above.
Use `--seal-target-bytes` and `--seal-max-age-ms` to vary the daemon's
background seal size and age triggers. Their defaults are 64 MiB and 5000 ms;
`--max-buffered-bytes` is the separate admission/backpressure cap.

`run` performs a 30-second smoke phase, 60-second warm-up, and 180-second
measurement at 175,000 records/s with batches of 1,000. Use
`--rate-sweep 125000,150000,175000,200000` for the optional sweep. A 30-minute
soak runs only with `--soak`.

Use `--retain` only for deliberate debugging. Otherwise `smoke` and `run`
empty the S3 bucket and delete the stack on success or failure. The instances
also schedule an automatic shutdown after `--max-runtime-hours` (four hours by
default), limiting compute exposure if orchestration is interrupted. Stopped
instances, EBS, S3, and interface endpoints still incur cost until `cleanup`
completes.

The main costs are EC2 runtime, EBS, S3 storage and requests, and two SSM
interface endpoints. Producer-to-consumer traffic stays within one
Availability Zone. Consumer-to-S3 traffic uses the S3 gateway endpoint, which
has no hourly or data-processing charge. Detailed S3 request metrics are not
enabled because they add monitoring cost; reports mark them unavailable.

Results and raw samples are written under
`benchmark/otlp-log-ingest/output/aws/<run-id>`. Durability latency is reported
as a seal-bucket approximation based on producer accepted-batch ranges and
generic daemon seal history; it is not presented as exact per-record latency.
