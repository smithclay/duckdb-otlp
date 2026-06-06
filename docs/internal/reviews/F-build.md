# Reviewer F — Build / CI / Packaging / Native-vs-WASM Parity

**Scope:** the invariants that only bite in production: the distroless image graph,
the offline extension cache, the CI gating that protects the published artifact, and
native-vs-WASM parity. Read-only review; every claim is grounded in a cited file.

## Summary

The packaging *mechanics* are sound and well-documented. The distroless guarantee
holds exactly as `AGENTS.md` advertises (`runtime-prebuilt` never touches the
`builder` stage), the offline extension cache is version-coupled correctly
(`HOME` and `DUCKDB_VERSION` agree across `deps`/`runtime-base`/the pinned DuckDB
submodule), the `HEALTHCHECK` is the daemon's own `healthcheck` subcommand, and
`libz.so.1` is staged as the one lib distroless/cc lacks. Native-vs-WASM parity is
real: the file-read table functions compile and *function* on `wasm_eh` (protobuf
included), and `otlp_serve` throws a clean `NotImplementedException` rather than
link-failing.

The real risk is **CI gating asymmetry**. The cheap PR gate (`daemon-compile`)
*compiles* the amd64 daemon and runs DRY_RUN config tests, but it never builds the
runtime image and never runs the e2e. The only thing that boots the actual published
distroless image and exercises ingest→seal→DuckLake-commit (`docker-smoke`) runs
**only on `main`/tags/dispatch**. So a Dockerfile-runtime regression, an arm64-only
break, or an ingest/seal regression passes PR CI green and first fails on the publish
run — the published image can break with a green PR. That is the headline (BLD-001).
Two secondary findings: a **stale "gzip e2e is the canary" claim in `CMakeLists.txt`**
that is no longer true after the smoke harness was swapped to JSON-only (BLD-002),
and the **`AGENTS.md` schema column-count contract is wrong** on four of six signals
(BLD-003).

Headline severity counts: 1 high, 3 medium, 3 nit.

---

## BLD-001 — PR gate compiles the daemon but never builds or boots the published image (high)

**Location:** `.github/workflows/MainDistributionPipeline.yml:45-77` (`daemon-compile`),
`:121-176` (`docker-smoke`, `if` at `:128`), `:79-119` (`daemon-linux`, `if` at `:81`),
`:178-243` (`docker-image`, `if` at `:180`).

**Category:** CI gating / publish safety. **Confidence:** high.

**What & why.** The PR / feature-branch gate is `daemon-compile`
(`:47` `if: pull_request || (push && ref != main && !tag)`). It builds the
`daemon-export` target for **`linux/amd64` only** (`:65`) and runs
`uv run --with pytest pytest test/server` (`:77`). `test/server/test_server_config.py`
is purely a `DRY_RUN=1` env→SQL config-generation test (it does not open a DB or start a
listener — see its own docstring, `test/server/test_server_config.py:1-14,38-54`). So the
PR gate validates: *the amd64 daemon links, and config SQL generation is correct.*

It validates none of the following, all of which are gated behind `main`/tags/dispatch:

- **The runtime image building/booting.** `docker-smoke` (`:126`, `if` at `:128`) is the
  only job that assembles `runtime-prebuilt` and boots the distroless container
  (`scripts/smoke_test.py`: distroless boot → OTLP/JSON POST → `otlp_flush` over Quack →
  assert committed DuckLake row count). A regression in any `runtime-base`/`deps` change
  (libz staging at Dockerfile `:80-85`, the primed extension cache at `:71-75`/`:94`, the
  `HEALTHCHECK` at `:107-108`, `LD_LIBRARY_PATH`/`HOME` env) is invisible on a PR.
- **The arm64 daemon.** `daemon-linux` builds both arches (`:84-92`) but only on main; a
  PR can break the arm64 build and stay green.
- **Ingest/seal/DuckLake-commit.** Same — only `docker-smoke` exercises it, post-merge.

Net: *a PR can go fully green while the published image fails to build, fails to boot, or
silently drops/loses ingested rows.* The first signal is the publish run on `main`, where
it is already too late to block.

**Proposed minimal action.** Run `docker-smoke` (build `runtime-prebuilt` from the
amd64 `daemon-export` artifact + `scripts/smoke_test.py`) on PRs as well — it is amd64-only
and already cache-warmed by `daemon-compile`'s `scope=daemon-amd64`. If the full e2e is too
slow for every PR, at minimum build the `runtime-prebuilt` image on PRs (boot it without the
Quack round-trip) so Dockerfile-runtime regressions are caught pre-merge. Optionally add a
PR-time arm64 *compile* of `daemon-export` to catch arch-specific breaks.

---

## BLD-002 — `CMakeLists.txt` cites a "docker-smoke gzip e2e" canary that no longer exists (medium)

**Location:** `CMakeLists.txt:69-76`; CI `docker-smoke` runs `scripts/smoke_test.py`
(`MainDistributionPipeline.yml:171-176`); `scripts/smoke_test.py:107-118` (POST is plain JSON).

**Category:** CI coverage / stale invariant. **Confidence:** high.

**What & why.** `CMakeLists.txt:69-74` documents a genuinely fragile, load-bearing link
ordering: the zlib-enabled OTLP archive must link *before* `duckdb_static`, because both
contain weak inline cpp-httplib symbols and the wrong order selects DuckDB's **non-zlib**
request decompressor — which makes the daemon reject gzip OTLP bodies with HTTP 415. The
comment then asserts: *"the docker-smoke gzip e2e is the canary that catches a regression."*

That canary no longer exists. The recent commit `ad725fcb`
("ci(smoke): replace benchmark-harness smoke gate with minimal smoke_test.py") swapped the
gate to `scripts/smoke_test.py`, which POSTs **uncompressed** `application/json`
(`smoke_test.py:107-118`, `Content-Type: application/json`, no `Content-Encoding: gzip`). It
never exercises the gzip decompression path. The gzip producer now lives only in the
benchmark harness (`scripts/benchmark_catalog_ingest.py:1710` — "The producer sends
gzip-compressed OTLP protobuf"), which `docker-smoke-minimal-gate` explicitly removed from
the CI gate.

So the documented protection for a silent, linker-order-dependent regression is gone, and
the comment now lies about coverage. A future DuckDB httplib re-vendor or linker change
could regress gzip ingest and *no CI job would catch it* — only the comment claims one does.

**Proposed minimal action.** Either (a) add a gzip-encoded POST to `scripts/smoke_test.py`
(send the same JSON `Content-Encoding: gzip`, assert 202) so the canary is restored, or
(b) update the `CMakeLists.txt:74` comment to state plainly that gzip ingest is **no longer
covered by CI** and add a targeted SQLLogicTest/manual check. (a) is preferred — it keeps the
invariant honest at near-zero cost.

---

## BLD-003 — `AGENTS.md` advertises wrong schema column counts (medium)

**Location:** `AGENTS.md` "Extension Type" + "Schema Design" sections;
ground truth in `external/otlp2records/src/schema/arrow.rs:8-163`.

**Category:** authoritative-contract accuracy. **Confidence:** high (counted fields directly).

**What & why.** `AGENTS.md` is the authoritative agent build/packaging contract; future agents
will trust its numbers. It states traces 25 / logs 15 / gauge 16 / sum 18 / histogram 22 /
exp_histogram 27. Counting the actual Arrow `Schema::new(vec![...])` field lists in
`arrow.rs`:

- logs (`:9-28`): **18** fields — AGENTS.md says 15.
- traces (`:32-57`): **24** fields — AGENTS.md says 25.
- gauge (`:61-79`): **17** fields — AGENTS.md says 16.
- sum (`:83-103`): **19** fields — AGENTS.md says 18.
- histogram (`:107-130`): **22** — correct.
- exp_histogram (`:134-162`): **27** — correct.

So the true tuple is **24/18/17/19/22/27**; AGENTS.md's **25/15/16/18/22/27** is wrong on
four of six signals (matches the ORIENTATION lead, `ORIENTATION.md:30`). Pre-1.0 is exactly
when this contract gets frozen and inherited.

**Proposed minimal action.** Correct the six counts in `AGENTS.md` to 24/18/17/19/22/27 in
both the "Extension Type" bullet list and "Schema Design". (Cross-check `schemas.md`/`api.md`
per the ORIENTATION note, but those are out of this reviewer's primary scope.)

---

## BLD-004 — Only `OtlpServeBind` carries the WASM guard; sibling lifecycle fns rely on an empty registry (nit)

**Location:** `src/otlp_start_stop.cpp:43-45` (only `OtlpServeBind` throws on WASM);
`OtlpStopBind:219`, `OtlpServerListBind:257`, `OtlpSealListBind:370`, `OtlpFlushBind:426`
have no `#ifdef __EMSCRIPTEN__`; backstop is `src/otlp_storage.cpp:28-29`
(`CreateServer` throws on WASM) and the WASM `OtlpServer` ctor stub at
`src/otlp_server_http.cpp:262-265`.

**Category:** native-vs-WASM parity. **Confidence:** high. **Severity:** nit (behavior is
correct, just inconsistent).

**What & why.** Charter point 4 asks whether the server functions throw *cleanly* on WASM
rather than link-failing or silently no-op'ing. They do not link-fail: `otlp_server_http.cpp`
wraps all httplib-touching code in `#ifndef __EMSCRIPTEN__` (`:8-10,17,258-279`) and provides
WASM stubs (ctor throws `NotImplementedException`, `:264`), and `otlp_server.cpp` has no
httplib usage so its seal/buffer methods compile on WASM but are unreachable. `otlp_serve`
throws a clear `NotImplementedException` at bind time (`otlp_start_stop.cpp:44`).

The inconsistency: `otlp_stop`/`otlp_flush`/`otlp_server_list`/`otlp_seal_list` are *not*
guarded at bind time. On WASM they operate over a registry that is always empty (no server can
be created — `CreateServer` throws), so `otlp_stop`/`otlp_flush` return "not found" and
`*_list` return empty rows. That is a benign no-op, not a crash — acceptable, but it means
those four functions present a misleading "success-ish" surface on a platform where the whole
ingest subsystem is unavailable, instead of the clear "not implemented for the wasm platform"
that `otlp_serve` gives. This is the "silently no-op" edge the charter flags as undesirable.

**Proposed minimal action.** For symmetry and honesty, add the same
`#ifdef __EMSCRIPTEN__ throw NotImplementedException(...)` to the four sibling bind functions
so the entire ingest control plane reports "not implemented on wasm" uniformly. Low risk;
purely additive.

---

## BLD-005 — Vendored Rust backend is not format/lint/test-gated by this repo's CI (nit)

**Location:** no `cargo fmt`/`clippy`/`cargo test` in `.github/workflows/*`,
`Makefile`, or `.pre-commit-config.yaml`; `make format-check` only checks `src test`
(`extension-ci-tools/makefiles/duckdb_extension.Makefile:260-261`); the Rust crate is a
pinned submodule `external/otlp2records (v0.8.4)` with its own `.github/workflows`.

**Category:** CI coverage. **Confidence:** high. **Severity:** nit.

**What & why.** The Rust parsing core is roughly half the system's logic, but this repo's CI
gates only the C++/Python (`code-quality-check` → `make format-check`/`tidy-check`, which
target `src`/`test` only). Rust quality is enforced upstream in the `otlp2records` repo, and
the submodule is pinned to a released tag (`v0.8.4`), so the *shipped* code is gated where it
lives. The gap is only relevant if someone edits the vendored Rust in-tree (e.g. a local patch
before bumping the tag): such an edit would compile via `FindOtlp2Records` but is never
`cargo fmt`/`clippy`/`cargo test`'d by this repo. The Rust unit tests in
`bindings/wasm.rs:239-482` and elsewhere are also never run by this repo's pipeline.

**Proposed minimal action.** Accept as-is (pinned submodule is the right model), or — if local
Rust edits are expected during hardening — add a small `cargo test --features ffi` +
`cargo fmt --check` job scoped to `external/otlp2records` to catch in-tree drift before a tag
bump.

---

## BLD-006 — Tooling-version drift between pre-commit and CI; local `format-check`/`tidy-check` is environmentally broken (nit)

**Location:** `.pre-commit-config.yaml` (clang-format `v11.0.1`, black `24.10.0`,
cmake-format `v0.6.13`); CI installs `black==24.*` + `clang_format==11.0.1` + `clang-tidy`
(`extension-ci-tools/.github/workflows/_extension_code_quality.yml`, Install steps);
local failure documented in `ORIENTATION.md:7-8` and `format-check-venv-workaround` memory.

**Category:** tooling consistency. **Confidence:** high. **Severity:** nit.

**What & why.** Charter point 6 asks whether format/tidy run green in CI and whether the
local environmental failure is itself a finding.

- **CI is fine and does gate.** `code-quality-check` (`MainDistributionPipeline.yml:245-251`)
  calls the reusable `_extension_code_quality.yml`, which installs `clang-format-11`,
  `black==24.*`, `clang_format==11.0.1`, `cmake-format`, and `clang-tidy` fresh and runs
  `make format-check` / `make tidy-check`. So the local venv breakage
  (`ORIENTATION.md:7-8`: `format.py` aborts because the repo venv lacks `black`/`clang-format`)
  is **local-only** and does NOT mask anything in CI. Good. It is, however, real developer
  friction and the `format-check-venv-workaround` memory exists precisely because the bundled
  venv shebang is broken — worth a one-line fix or a documented bootstrap.
- **Two small drifts:** (1) CI installs `black==24.*` (floating minor) while pre-commit pins
  `black==24.10.0` (exact) — a black 24.x bugfix could reformat differently between the two
  paths. (2) `cmake-format`/`cmake-lint` are pinned in pre-commit (`v0.6.13`) and run there,
  but `make format-check`'s `format.py` also runs cmake-format with the CI-installed
  `cmake-format` (unpinned in the reusable workflow's Install step). Low probability of a real
  divergence, but it is exactly the kind of "CI drifts from pre-commit" that the charter calls
  out.

**Proposed minimal action.** Pin `black==24.10.0` (exact) in the install path to match
pre-commit, and pin `cmake-format`/`cmake_format` to `0.6.13`. Separately, fix or document the
local format venv so `make format-check` works for contributors (this is the recurring
`format-check-venv-workaround` pain).

---

## Confirmed-clean invariants (audit points that hold)

- **Distroless guarantee (point 1):** `runtime-prebuilt` (`Dockerfile:112-114`) is
  `FROM runtime-base` + `COPY docker-bin/${TARGETARCH}/duckdb-otlp-server`; `runtime-base`
  (`:87-110`) is `FROM gcr.io/distroless/cc-debian12:nonroot` and COPYs only from `deps`.
  Neither references `builder`. **Packaging never recompiles DuckDB.** Final base, libz staging
  (`:80-85,91`), and the self-`healthcheck` HEALTHCHECK (`:107-108` →
  `src/server/main.cpp:262-263` → `OtlpLoopbackHttpStatusOk` on `/readyz`) are all as
  AGENTS.md describes. CI `docker-smoke`/`docker-image` both build `--target runtime-prebuilt`
  (`:158,237`).
- **Offline-cache coupling (point 2):** `HOME=/duckdb-home` matches between `deps` (`:50`) and
  `runtime-base` (`:96`); `DUCKDB_VERSION=v1.5.3` is the default ARG in both `builder` (`:21`)
  and `deps` (`:45`), the DuckDB submodule is pinned to `v1.5.3` (`git submodule status`), and
  the daemon compiles with `OVERRIDE_GIT_DESCRIBE=${DUCKDB_VERSION}` (`:38`). The `deps` stage
  primes ducklake/iceberg/httpfs/aws/postgres/quack into that `HOME` (`:71-75`) and CI passes
  **no `build-args`**, so every build uses the same pinned version. No silent re-download /
  offline-failure divergence found. (`otlp` is statically embedded and intentionally not
  installed — `:69-70`.)
- **CI publish targets match (point 3, the mechanics):** PR `daemon-compile` and publish
  `daemon-linux` share `daemon-export` and `scope=daemon-amd64` cache (`:64-68` vs `:108-112`);
  `docker-smoke` runs `scripts/smoke_test.py` against the packaged amd64 `runtime-prebuilt`
  image (`:153-176`), per commit `ad725fcb`. The *flags/targets/version* match — the gap is the
  *gating asymmetry* in BLD-001, not a target/flag mismatch.
- **Native-vs-WASM parity, protobuf (points 4 & 5):** the shipped `wasm_eh` extension uses the
  **FFI** Rust path (`Makefile:27` builds `--features ffi`, not `--features wasm`;
  `extension_config.cmake:5-11` links `libotlp2records.a`; `Makefile:54` exports
  `_otlp_transform`/`_otlp_transform_metrics_all`/etc.). The FFI surface accepts
  Auto/Protobuf/Json/Jsonl (`bindings/ffi.rs:56-73`) and `decode/` is not feature-gated
  (`lib.rs:28`), so **protobuf decode is present in the WASM build.** `read_otlp.cpp` defaults
  to `OTLP_FORMAT_AUTO` with no WASM restriction (`:103`). The demo ships `.pb` samples
  (`site/public/wasm-demo/samples/{logs,metrics,traces}.pb`, registered in `app.js:112-114`).
- **README vs AGENTS.md (point 6 — RESOLVED, premise was stale):** there is **no
  contradiction** in the current tree. README states "WASM supports JSON, JSONL, and protobuf
  file reads" (`README.md:117,158`) and AGENTS.md agrees ("WASM builds support both JSON and
  Protobuf formats"). The alleged "WASM demo is JSON-only" wording does not exist in
  `README.md`. **Ground truth: the shipped WASM build (and demo) includes the protobuf decode
  path; both docs are correct.**
