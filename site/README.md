# DuckDB OpenTelemetry Extension Site

Astro/Starlight documentation site for the DuckDB OpenTelemetry Extension.

Docs live directly in `src/content/docs/`. The embedded WASM demo lives in `public/wasm-demo/`.

```bash
npm ci
npm run dev
npm run check
npm run build
```

CI uses Node 24. Documentation PRs run a clean lockfile install, `astro check`,
and a production build before merge. Upgrade Astro and Starlight together; Renovate
groups their updates. TypeScript stays below 7 until `@astrojs/check` supports it.

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
