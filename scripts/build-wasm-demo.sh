#!/usr/bin/env bash
#
# Build the browser-demo wasm extension at site/public/wasm-demo/otlp.duckdb_extension.wasm.
#
# WHY THIS IS SPECIAL (and not just `make wasm_eh`):
# The demo page runs @duckdb/duckdb-wasm@1.33.1-dev56.0 (DuckDB v1.5.4), whose runtime is
# built with wasm-native exceptions. A loadable extension only links into that runtime if:
#   1. it is built against the SAME DuckDB the runtime uses — the v1.5.4 tag; and
#   2. it is compiled with the SAME emscripten the runtime uses — 3.1.71; and
#   3. the Rust FFI lib (otlp2records) is built WITHOUT unwinding (panic=immediate-abort).
#      Rust on wasm32-unknown-emscripten emits legacy SjLj `invoke_*` exception/longjmp
#      trampolines that the wasm-native-EH host does NOT provide; immediate-abort drops them.
#      (Verified: without it the extension throws "function signature mismatch" at load.)
#
# NOTE: duckdb-wasm applies its own patches to DuckDB before building the runtime, but those
# are runtime-build concerns (extension-install URLs, repo preferences, wasm code-size) — none
# change a shared struct's layout or a host-resolved symbol, so the extension is ABI-identical
# with or without them. Measured: a no-patch build has the IDENTICAL wasm import surface (372
# imports, 0 invoke_*) and loads + runs read_otlp_logs in dev56 headless Chrome. We therefore
# build against the plain v1.5.4 tag and do NOT vendor/apply the patches.
#
# This is a DEMO-ONLY build. The repo's native submodule pin now also tracks DuckDB v1.5.4, so
# the DuckDB *version* matches — this build still diverges from a plain `make wasm_eh` only in
# the emscripten pin and the immediate-abort Rust lib. The submodule + Makefile are restored on
# exit. Re-run after changing the extension sources or bumping the demo's duckdb-wasm version
# (and update the version in site/public/wasm-demo/app.js).
#
# Prereqs: rustup nightly + rust-src (for -Zbuild-std), an emsdk with 3.1.71, vcpkg bootstrapped.
set -euo pipefail

DUCKDB_TAG=${DUCKDB_TAG:-v1.5.4}
VERSION_FIELD=${VERSION_FIELD:-v1.5.4}
EMSDK_DIR=${EMSDK_DIR:-${EMSDK:-$HOME/workspace/emsdk}}

REPO=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO"
DEMO_OUT="$REPO/site/public/wasm-demo/otlp.duckdb_extension.wasm"

ORIG_DUCKDB=$(git -C duckdb rev-parse HEAD)
cleanup() {
  echo "Restoring duckdb submodule + Makefile…"
  git -C duckdb checkout -f "$ORIG_DUCKDB" >/dev/null 2>&1 || true
  git -C duckdb clean -fdq >/dev/null 2>&1 || true
  git checkout -- Makefile >/dev/null 2>&1 || true
}
trap cleanup EXIT

# emscripten 3.1.71 (must match duckdb-wasm dev56)
[ -f "$EMSDK_DIR/emsdk_env.sh" ] || { echo "ERROR: emsdk not found at $EMSDK_DIR (set EMSDK_DIR)"; exit 1; }
# shellcheck disable=SC1091
source "$EMSDK_DIR/emsdk_env.sh" >/dev/null 2>&1
echo "Using $(emcc --version | head -1)"
rustup component add rust-src --toolchain nightly >/dev/null 2>&1 || true

# DuckDB v1.5.4 (plain tag — no duckdb-wasm patches; see header NOTE for why they're unneeded)
git -C duckdb fetch --depth 1 origin tag "$DUCKDB_TAG" 2>/dev/null || true
git -C duckdb checkout -q "$DUCKDB_TAG"

# Stamp the metadata version + build the Rust lib with panic=immediate-abort (drops SjLj invoke_*)
# Force the metadata stamp to the demo's target DuckDB version regardless of the Makefile
# default (which tracks the native submodule pin — now also v1.5.4, but may diverge later).
sed -i.bak -E "s/VERSION_FIELD=\"v[0-9.]+\"/VERSION_FIELD=\"$VERSION_FIELD\"/" Makefile
sed -i.bak 's|cd external/otlp2records && cargo build --target wasm32-unknown-emscripten --release --features ffi|cd external/otlp2records \&\& RUSTFLAGS="-Zunstable-options -Cpanic=immediate-abort" cargo +nightly build --target wasm32-unknown-emscripten --release --features ffi -Z build-std=std,panic_abort|' Makefile
rm -f Makefile.bak

rm -rf build/wasm_eh
VCPKG_TOOLCHAIN_PATH="$REPO/vcpkg/scripts/buildsystems/vcpkg.cmake" make wasm_eh

cp build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm "$DEMO_OUT"
echo "OK: wrote $DEMO_OUT ($(wc -c < "$DEMO_OUT") bytes)"
echo "Verify in a browser, or with a headless duckdb-wasm 1.33.1-dev56.0 load test."
