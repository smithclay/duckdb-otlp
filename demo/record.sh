#!/usr/bin/env bash
# Record the demo reel. Everything the tape needs is set up here, off camera, so
# demo.tape holds only what a viewer should see.
#
#   ./demo/record.sh            # record into demo/duckdb-otlp.{gif,mp4}
#
# Requires: vhs, ffmpeg, duckdb, python3, and a built duckdb-otlp binary.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

BIN="${DUCKDB_OTLP_BIN:-$REPO_ROOT/build/release/extension/otlp/duckdb-otlp}"
if [[ ! -x "$BIN" ]]; then
  echo "record.sh: no duckdb-otlp binary at $BIN" >&2
  echo "record.sh: build one with \`GEN=ninja make\` or set DUCKDB_OTLP_BIN." >&2
  exit 1
fi

for tool in duckdb python3 ffmpeg; do
  command -v "$tool" >/dev/null || { echo "record.sh: $tool is not installed." >&2; exit 1; }
done

# VHS 0.12.0 runs the tape, prints "Creating <file>...", exits 0 and writes nothing --
# charmbracelet/vhs#787. It captures frames fine and then never encodes them, so the
# failure is silent and looks like success. Pin to 0.11.0 until that is fixed upstream.
VHS="${VHS_BIN:-vhs}"
command -v "$VHS" >/dev/null || { echo "record.sh: vhs is not installed." >&2; exit 1; }
VHS_VERSION="$("$VHS" --version 2>/dev/null | grep -oE 'v?[0-9]+\.[0-9]+\.[0-9]+' | head -1)"
if [[ "$VHS_VERSION" == "v0.12.0" || "$VHS_VERSION" == "0.12.0" ]]; then
  cat >&2 <<'EOF'
record.sh: vhs 0.12.0 cannot record -- it exits 0 and writes no file
           (https://github.com/charmbracelet/vhs/issues/787).

           Install 0.11.0 and point VHS_BIN at it:

             curl -fsSL -o /tmp/vhs.tar.gz \
               https://github.com/charmbracelet/vhs/releases/download/v0.11.0/vhs_0.11.0_Darwin_arm64.tar.gz
             tar -xzf /tmp/vhs.tar.gz -C /tmp
             VHS_BIN=/tmp/vhs_0.11.0_Darwin_arm64/vhs ./demo/record.sh
EOF
  exit 1
fi

# A fresh workspace every take, so the reel never shows a previous run's data and
# the row counts on screen are always the ones this run produced.
WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/duckdb-otlp-demo.XXXXXX")"
cleanup() {
  pkill -f "$BIN serve" 2>/dev/null || true
  rm -rf "$WORKDIR"
}
trap cleanup EXIT

cp "$REPO_ROOT/demo/seed.py" "$WORKDIR/seed.py"

# The tape reads these; exporting them keeps absolute paths out of the tape itself.
export DEMO_WORKDIR="$WORKDIR"
export PATH="$(dirname "$BIN"):$PATH"
# Keeps the banner's data path short and the take reproducible. Set here rather
# than as a `serve` flag so the reel can show `duckdb-otlp serve` with no flags.
export DUCKDB_OTLP_DATA_DIR="./otel"

echo "record.sh: workspace $WORKDIR"
"$VHS" "$REPO_ROOT/demo/demo.tape"

echo
echo "record.sh: wrote"
for f in demo/duckdb-otlp.gif demo/duckdb-otlp.mp4; do
  [[ -f "$f" ]] && printf '  %-28s %s\n' "$f" "$(du -h "$f" | cut -f1)"
done
