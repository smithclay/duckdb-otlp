#!/usr/bin/env bash
#
# Build the browser-demo wasm extension at site/public/wasm-demo/otlp.duckdb_extension.wasm.
#
# WHY THIS IS SPECIAL (and not just `make wasm_eh`):
# The demo page runs @duckdb/duckdb-wasm@1.33.1-dev56.0 (DuckDB v1.5.4), whose runtime is
# built with wasm-native exceptions. A loadable extension only links into that runtime if:
#   1. it is built against the SAME DuckDB the runtime uses — v1.5.4 + duckdb-wasm's own
#      DuckDB patches (scripts/wasm-demo/patches/duckdb/, applied with `patch -p1 --forward`);
#   2. it is compiled with the SAME emscripten the runtime uses — 3.1.71; and
#   3. the Rust FFI lib (otlp2records) is built WITHOUT unwinding (panic=immediate-abort).
#      Rust on wasm32-unknown-emscripten emits legacy SjLj `invoke_*` exception/longjmp
#      trampolines that the wasm-native-EH host does NOT provide; immediate-abort drops them.
#      (Verified: without it the extension throws "function signature mismatch" at load.)
#
# This is a DEMO-ONLY build, decoupled from the repo's pinned DuckDB submodule (v1.5.3) used
# for native builds. The submodule + Makefile are restored on exit. Re-run after changing
# the extension sources or bumping the demo's duckdb-wasm version (also re-vendor patches,
# see scripts/wasm-demo/patches/README.md, and update the version in site/public/wasm-demo/app.js).
#
# Prereqs: rustup nightly + rust-src (for -Zbuild-std), an emsdk with 3.1.71, vcpkg bootstrapped.
set -euo pipefail

DUCKDB_TAG=${DUCKDB_TAG:-v1.5.4}
VERSION_FIELD=${VERSION_FIELD:-v1.5.4}
EMSDK_DIR=${EMSDK_DIR:-${EMSDK:-$HOME/workspace/emsdk}}

REPO=$(cd "$(dirname "$0")/.." && pwd)
cd "$REPO"
PATCHES="$REPO/scripts/wasm-demo/patches/duckdb"
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

# DuckDB v1.5.4 + duckdb-wasm patches
git -C duckdb fetch --depth 1 origin tag "$DUCKDB_TAG" 2>/dev/null || true
git -C duckdb checkout -q "$DUCKDB_TAG"
cat "$PATCHES"/*.patch | patch -p1 --forward -d duckdb || true   # --forward skips already-upstream hunks

# Stamp the metadata version + build the Rust lib with panic=immediate-abort (drops SjLj invoke_*)
sed -i.bak "s/VERSION_FIELD=\"v1.5.3\"/VERSION_FIELD=\"$VERSION_FIELD\"/" Makefile
sed -i.bak 's|cd external/otlp2records && cargo build --target wasm32-unknown-emscripten --release --features ffi|cd external/otlp2records \&\& RUSTFLAGS="-Zunstable-options -Cpanic=immediate-abort" cargo +nightly build --target wasm32-unknown-emscripten --release --features ffi -Z build-std=std,panic_abort|' Makefile
rm -f Makefile.bak

rm -rf build/wasm_eh
VCPKG_TOOLCHAIN_PATH="$REPO/vcpkg/scripts/buildsystems/vcpkg.cmake" make wasm_eh

cp build/wasm_eh/extension/otlp/otlp.duckdb_extension.wasm "$DEMO_OUT"
echo "OK: wrote $DEMO_OUT ($(wc -c < "$DEMO_OUT") bytes)"
echo "Verify in a browser, or with a headless duckdb-wasm 1.33.1-dev56.0 load test."
