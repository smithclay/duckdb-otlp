# duckdb-wasm DuckDB patches (vendored)

`duckdb/*.patch` are copied verbatim from
[`duckdb/duckdb-wasm`](https://github.com/duckdb/duckdb-wasm) at commit
`f61c03b709792b8661590bb9cff438a1d389b325` (npm `@duckdb/duckdb-wasm@1.33.1-dev56.0`,
which bundles DuckDB **v1.5.4**), path `patches/duckdb/`.

duckdb-wasm applies these to its DuckDB submodule before building the `.wasm` runtime.
To build a loadable extension that is ABI-compatible with that runtime, the extension
must be compiled against the **same patched DuckDB**. `scripts/build-wasm-demo.sh`
applies them with `patch -p1 --forward` (the way duckdb-wasm's Makefile does — some hunks
are already upstream in the v1.5.4 tag and are skipped).

Re-vendor when bumping the demo's duckdb-wasm version:

    V=<duckdb-wasm npm version>
    REF=$(npm view @duckdb/duckdb-wasm@$V gitHead)
    for p in all_of_them.patch invalid_arrow.patch; do
      gh api "repos/duckdb/duckdb-wasm/contents/patches/duckdb/$p?ref=$REF" --jq .content \
        | base64 -d > duckdb/$p
    done
