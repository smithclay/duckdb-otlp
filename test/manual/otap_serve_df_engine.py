#!/usr/bin/env -S uv run --script
# /// script
# requires-python = ">=3.9"
# dependencies = [
#   "duckdb==1.5.4",
# ]
# ///
"""End-to-end smoke: real otel-arrow OTAP producer -> our otap_serve gRPC ingest.

This wires the upstream OTAP Dataflow Engine example
(https://github.com/open-telemetry/otel-arrow#otap-dataflow-engine-example) to
the extension's live OTAP/Arrow gRPC server and checks logs flow through end to
end. It is deliberately boring:

  1. build `df_engine` from a local otel-arrow checkout (cargo, incremental).
     NOTE: with default features -- the README's `--no-default-features` is for
     the minimal syslog example and strips `dev-tools` (the traffic_generator
     receiver) and `crypto-ring` (the rustls provider), both of which we need.
  2. start `otap_serve(disable_auth := true)` on a loopback port
  3. run df_engine with a generated config: a `traffic_generator` receiver
     (logs only, bounded) -> an `otap` exporter pointed at our port
  4. poll until the generated logs land in `otlp_logs`, then verify count +
     that rows carry real structured content (a service name)
  5. tear everything down

Why `disable_auth`: the otel-arrow OTAP exporter (`GrpcClientSettings`) has no
way to attach a bearer token -- no config field, no env var. So a real OTAP
producer can only reach an anonymous listener. `disable_auth` is the opt-in for
exactly this (trusted local networks / tokenless producers).

The traffic generator fetches the semantic-conventions registry over the network
at startup (it generates from the live semconv model), which takes ~20-40s before
any logs flow -- so DEADLINE_SECONDS is generous. Override REGISTRY_PATH to point
at a different registry.

Run (requires a built loadable extension and a local otel-arrow checkout):

    uv run --script test/manual/otap_serve_df_engine.py

    OTEL_ARROW_DIR=~/workspace/otel-arrow \\
    OTLP_EXTENSION=build/release/extension/otlp/otlp.duckdb_extension \\
    OTLP_PORT=4327 SIGNAL_COUNT=2000 \\
        uv run --script test/manual/otap_serve_df_engine.py
"""

import os
import pathlib
import subprocess
import sys
import tempfile
import time

EXTENSION = os.environ.get("OTLP_EXTENSION", "build/release/extension/otlp/otlp.duckdb_extension")
OTEL_ARROW_DIR = pathlib.Path(os.environ.get("OTEL_ARROW_DIR", "~/workspace/otel-arrow")).expanduser()
PORT = int(os.environ.get("OTLP_PORT", "4327"))
SIGNAL_COUNT = int(os.environ.get("SIGNAL_COUNT", "2000"))
SIGNALS_PER_SECOND = int(os.environ.get("SIGNALS_PER_SECOND", "2000"))
# Generous: the first run git-clones the semconv registry before producing.
DEADLINE_SECONDS = float(os.environ.get("DEADLINE_SECONDS", "180"))
SKIP_BUILD = os.environ.get("SKIP_BUILD") == "1"
REGISTRY_PATH = os.environ.get("REGISTRY_PATH", "https://github.com/open-telemetry/semantic-conventions.git[model]")

DATAFLOW_DIR = OTEL_ARROW_DIR / "rust" / "otap-dataflow"
DF_ENGINE_BIN = DATAFLOW_DIR / "target" / "debug" / "df_engine"
URI = f"otap:127.0.0.1:{PORT}"

CONFIG_TEMPLATE = """version: otel_dataflow/v1
engine: {{}}
groups:
  default:
    pipelines:
      main:
        policies:
          channel_capacity:
            control:
              node: 100
              pipeline: 100
            pdata: 128
        nodes:
          receiver:
            type: receiver:traffic_generator
            config:
              traffic_config:
                signals_per_second: {sps}
                max_signal_count: {count}
                metric_weight: 0
                trace_weight: 0
                log_weight: 30
              registry_path: {registry}
          exporter:
            type: exporter:otap
            config:
              grpc_endpoint: "http://127.0.0.1:{port}"
              # Left on the exporter defaults: compression_method=zstd (grpc-encoding:
              # zstd at the transport) and payload_compression=zstd (the Arrow payload).
              # The server accepts both, so this exercises the realistic producer-default
              # path -- no compression workaround needed.
        connections:
          - from: receiver
            to: exporter
"""


def eprint(msg: str) -> None:
    print(msg, file=sys.stderr, flush=True)


def build_df_engine() -> None:
    if SKIP_BUILD and DF_ENGINE_BIN.exists():
        eprint(f"[df_engine] SKIP_BUILD=1, using existing {DF_ENGINE_BIN}")
        return
    eprint("[df_engine] cargo build --bin df_engine (default features; incremental)")
    proc = subprocess.run(
        ["cargo", "build", "--bin", "df_engine"],
        cwd=DATAFLOW_DIR,
    )
    if proc.returncode != 0:
        raise SystemExit(f"df_engine build failed (cwd={DATAFLOW_DIR})")
    if not DF_ENGINE_BIN.exists():
        raise SystemExit(f"df_engine binary not found at {DF_ENGINE_BIN} after build")


def main() -> int:
    import duckdb

    if not pathlib.Path(EXTENSION).exists():
        raise SystemExit(f"extension not found: {EXTENSION} (build it first)")
    if not DATAFLOW_DIR.is_dir():
        raise SystemExit(f"otel-arrow dataflow dir not found: {DATAFLOW_DIR} (set OTEL_ARROW_DIR)")

    build_df_engine()

    config_text = CONFIG_TEMPLATE.format(sps=SIGNALS_PER_SECOND, count=SIGNAL_COUNT, registry=REGISTRY_PATH, port=PORT)
    cfg = tempfile.NamedTemporaryFile("w", suffix=".yaml", prefix="otap-e2e-", delete=False)
    cfg.write(config_text)
    cfg.close()
    cfg_path = cfg.name

    con = duckdb.connect(config={"allow_unsigned_extensions": "true"})
    con.execute(f"LOAD '{EXTENSION}'")
    con.execute(f"SELECT * FROM otap_serve('{URI}', disable_auth := true)")
    eprint(f"[serve] otap_serve listening on {URI} (anonymous)")

    df_log = open(cfg_path + ".df_engine.log", "w")
    eprint(f"[df_engine] starting -> {URI}; log: {df_log.name}")
    df = subprocess.Popen(
        [str(DF_ENGINE_BIN), "-c", cfg_path],
        cwd=DATAFLOW_DIR,
        stdout=df_log,
        stderr=subprocess.STDOUT,
    )

    failures = []
    landed = 0
    try:
        deadline = time.monotonic() + DEADLINE_SECONDS
        while time.monotonic() < deadline:
            con.execute(f"SELECT * FROM otlp_flush('{URI}')")
            landed = con.execute("SELECT count(*) FROM otlp_logs").fetchone()[0]
            if landed >= SIGNAL_COUNT:
                break
            if df.poll() is not None:
                failures.append(f"df_engine exited early (code {df.returncode}) with only {landed} rows")
                break
            time.sleep(1)
        else:
            failures.append(f"timed out after {DEADLINE_SECONDS:.0f}s with {landed}/{SIGNAL_COUNT} rows")

        con.execute(f"SELECT * FROM otlp_flush('{URI}')")
        landed = con.execute("SELECT count(*) FROM otlp_logs").fetchone()[0]
        if landed < SIGNAL_COUNT:
            failures.append(f"only {landed}/{SIGNAL_COUNT} log rows landed")

        # Content spot-check: a real OTAP decode yields structured rows, not just a
        # row count. Every generated log should carry a resource service name.
        with_service = con.execute(
            "SELECT count(*) FROM otlp_logs WHERE service_name IS NOT NULL AND service_name <> ''"
        ).fetchone()[0]
        if with_service == 0:
            failures.append("no log rows carry a service_name (OTAP decode produced empty content?)")
    finally:
        df.terminate()
        try:
            df.wait(timeout=10)
        except subprocess.TimeoutExpired:
            df.kill()
        df_log.close()
        stop = con.execute(f"SELECT * FROM otlp_stop('{URI}')").fetchone()
        if stop and stop[1] != 0:
            failures.append(f"otlp_stop dropped {stop[1]} rows (expected clean drain)")
        os.unlink(cfg_path)

    if failures:
        print("FAIL")
        for f in failures:
            print(f"  - {f}")
        tail = pathlib.Path(df_log.name).read_text(errors="replace").splitlines()[-25:]
        if tail:
            print("--- df_engine log (tail) ---")
            print("\n".join(tail))
            print("--- end df_engine log ---")
        return 1
    os.unlink(df_log.name)
    print(f"OK: otel-arrow df_engine streamed {landed} logs over OTAP/gRPC into otlp_logs (anonymous serve)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
