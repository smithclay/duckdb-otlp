# DuckDB OTLP site

Astro/Starlight documentation site for DuckDB OTLP.

The build syncs markdown from `../docs` into Starlight content and copies `../demo` into `public/demo`, so the repository docs and WASM demo stay the source of truth.

```bash
npm install
npm run dev
npm run build
```
