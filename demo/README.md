# Demo reel

The GIF at the top of the repository README, and the MP4 beside it.

```sh
./demo/record.sh
```

That writes `demo/duckdb-otlp.gif` (for the README) and `demo/duckdb-otlp.mp4`
(for social posts, where a video beats a GIF on both quality and size).

## What it shows

Three beats, deliberately: **receive → ingest → query**.

1. `duckdb-otlp serve` with no flags at all — OTLP/HTTP on 4318, OTLP/gRPC on
   4317, a local DuckLake, no configuration file.
2. `demo/seed.py` stands in for your own apps or agents, sending synthetic
   traffic across five services over OTLP/HTTP. Point the real OpenTelemetry
   Demo, an SDK, or a collector at the same port and nothing else changes.
3. Plain `duckdb` runs a p95-by-service query straight over the Parquet the
   server has already written — *while the server keeps receiving*.

Anything outside those three beats was cut. The reel is a hook that autoplays
muted on a timeline, not a tutorial, so it stays under a minute.

The queries in beat 3 use `duckdb` rather than `duckdb-otlp query` for a
concrete reason: in a catalog mode the running server holds the control
database's write lock, so `duckdb-otlp query` cannot read it until the server
stops (even with `--readonly`). Reading the Parquet directly is both the only
live option and the better story — the data is open the moment it lands.

## The data is synthetic

`demo/seed.py` generates every span, log, and metric locally; nothing on screen
is recorded traffic, and the reel never claims otherwise -- the caption points
viewers at the OpenTelemetry Demo as something they could send, not as the
source of what is being shown. `payment-service` is deliberately the slow, failing one, so
the p95 query has something to find. Pass `--seed N` to fix the RNG.

## Requirements

`vhs`, `ffmpeg`, `duckdb`, `python3`, and a built `duckdb-otlp`
(`GEN=ninja make`, or point `DUCKDB_OTLP_BIN` at one).

**VHS 0.12.0 cannot record.** It runs the tape, prints `Creating <file>...`,
exits 0 and writes nothing — [charmbracelet/vhs#787][vhs787]. It captures the
frames and then never encodes them, so the failure looks exactly like success.
`record.sh` detects that version and stops. Use 0.11.0:

```sh
curl -fsSL -o /tmp/vhs.tar.gz \
  https://github.com/charmbracelet/vhs/releases/download/v0.11.0/vhs_0.11.0_Darwin_arm64.tar.gz
tar -xzf /tmp/vhs.tar.gz -C /tmp
VHS_BIN=/tmp/vhs_0.11.0_Darwin_arm64/vhs ./demo/record.sh
```

[vhs787]: https://github.com/charmbracelet/vhs/issues/787

## Editing the reel

`demo/demo.tape` is the script — VHS records exactly what it says, so the reel
is reproducible and reviewable in a diff. Setup that a viewer should not watch
(the workspace, `PATH`, the prompt) lives in `record.sh` and the tape's `Hide`
block, never on camera.
