# DuckDB OpenTelemetry Extension Site

[mdBook](https://rust-lang.github.io/mdBook/) documentation site for the DuckDB OpenTelemetry Extension.

Pages live in `src/`, one directory per page (`src/guides/stream-to-parquet/index.md`
publishes as `/guides/stream-to-parquet/`). `src/SUMMARY.md` is the sidebar, and a
page it does not list is not built. Every other file in `src/` is copied to the site
verbatim: `install.sh`, the WASM demo in `src/wasm-demo/`, and example configs.

The only build dependency is the `mdbook` binary. CI installs the version pinned
in `../.github/actions/setup-mdbook/action.yml`; install the same one locally from
the [mdBook releases](https://github.com/rust-lang/mdBook/releases).

```bash
mdbook serve --open                      # live preview at http://localhost:3000/
mdbook build                             # writes dist/
python3 ../scripts/check_site_links.py dist   # fails on broken internal links
```

Documentation PRs run the build and the link check before merge (`site-check.yml`).

## Pages publishing

`.github/workflows/pages.yml` deploys the site and the unsigned DuckDB extension
repository as one snapshot. Docs-only deploys recover the extension repository
from `extension-repository.tar.gz` on the published site, so they do not depend on
expiring Actions artifacts. The first deploy of this workflow can recover the old
site's binaries from its `/extensions/` index. Missing binaries stop deployment
before the existing site is replaced.

The archive contains only the extension index, binaries, and `.nojekyll`; every
successful deployment republishes it. Docs-only updates preserve the published
DuckDB version. To publish new binaries (or bootstrap an empty Pages site), run
the **Pages** workflow on `main` with **publish_extension** enabled. Release
publishing also builds new binaries and attaches the archive to the release.

To verify the repository assembly locally, from the repository root:

```bash
uv run python -m unittest discover -s test/pages -v
uv run python scripts/pages_extension_repository.py \
  --base-url https://smithclay.github.io/duckdb-otlp \
  --destination site/dist
```
