# ===========================================================================
#  duckdb-otlp -- demo reel
#
#  Record with ./demo/record.sh, which builds the off-camera workspace this
#  tape expects ($DEMO_WORKDIR, duckdb-otlp on PATH, DUCKDB_OTLP_DATA_DIR).
#  Running `vhs demo/demo.tape` directly will not work.
#
#  Three beats, and nothing else: RECEIVE -> INGEST -> QUERY. Anything that is
#  not one of those three is cut, however good it looks -- this is a hook that
#  autoplays muted on a timeline, not a tutorial. The one addition is a closing
#  install line, because a reel with no next action wastes the attention it won.
#
#  The typed `#` lines are the narration; they are real shell comments, so
#  nothing on screen is staged. Every number shown is produced by the run.
#  Pacing: a caption gets ~900ms to land before its command runs; a result
#  gets ~4s. SQL is typed faster than prose (Type@18ms) -- it is read as a
#  shape, not word by word, and at prose speed it dominates the runtime.
# ===========================================================================

Output demo/duckdb-otlp.gif
Output demo/duckdb-otlp.mp4

Require duckdb
Require python3

Set Shell bash
Set FontFamily "JetBrains Mono"
Set FontSize 16
Set Width 1200
Set Height 640
Set Padding 25
Set Theme "Catppuccin Mocha"
Set TypingSpeed 28ms
Set Framerate 10
Set WaitTimeout 90s

# --- off camera: clean workspace, plain prompt -----------------------------
# PS1 is "$ " rather than a fancy glyph: a prompt character the recording font
# lacks renders as a stray ")" in the GIF.
Hide
Type `export PS1="$ "` Enter
Type "cd $DEMO_WORKDIR" Enter
Type "clear" Enter
Show

# --- 1. RECEIVE: one binary, zero configuration ----------------------------
Type "# OpenTelemetry, queryable with SQL -- no collector, no cluster, no vendor."
Sleep 900ms
Enter
Type "duckdb-otlp serve &"
Sleep 500ms
Enter
Wait+Screen@60s /Send OTLP/
Sleep 2600ms

Hide
Type "clear" Enter
Show

# --- 2. INGEST: real OTLP over the wire ------------------------------------
Type "# Point any OTLP exporter at :4318. Here, a checkout flow."
Sleep 900ms
Enter
Type "python3 seed.py --traces 2000"
Sleep 400ms
Enter
Wait+Screen@60s /buffered/
Sleep 2400ms

Hide
Type "clear" Enter
Show

# --- 3. QUERY: SQL over telemetry that is already open Parquet -------------
Type "# It lands as Parquet. Query it with plain DuckDB -- while ingest runs."
Sleep 1000ms
Enter
Type "duckdb -dark-mode"
Sleep 300ms
Enter
Sleep 1500ms

Type@18ms "SELECT service_name, count(*) AS spans,"
Enter
Type@18ms "       round(quantile_cont(duration_time_unix_nano, 0.95)/1e6, 1) AS p95_ms,"
Enter
Type@18ms "       sum(status_code = 2) AS errors"
Enter
Type@18ms "FROM 'otel/ducklake/storage/main/otlp_traces/*.parquet'"
Enter
Type@18ms "GROUP BY 1 ORDER BY p95_ms DESC;"
Sleep 500ms
Enter
Sleep 4200ms

# --- close: the one next action -------------------------------------------
Type ".quit"
Enter
Sleep 700ms
Type "# curl -fsSL https://smithclay.github.io/duckdb-otlp/install.sh | sh"
Sleep 1000ms
Enter
Sleep 3200ms
