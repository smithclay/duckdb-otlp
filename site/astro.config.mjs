import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import starlightClientMermaid from '@pasqal-io/starlight-client-mermaid';

export default defineConfig({
  site: 'https://smithclay.github.io',
  base: '/duckdb-otlp',
  integrations: [
    starlight({
      title: 'DuckDB OpenTelemetry Extension',
      description: 'duckdb-otlp: query OpenTelemetry traces, logs, and metrics with DuckDB.',
      logo: {
        src: './public/duckdb-otlp.png',
        alt: 'duckdb-otlp logo',
      },
      customCss: ['./src/styles/brand.css'],
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/smithclay/duckdb-otlp',
        },
      ],
      plugins: [starlightClientMermaid()],
      sidebar: [
        {
          label: 'Start Here',
          items: [
            { label: 'Overview', link: '/' },
            { label: 'Get Started', link: '/get-started/' },
            { label: 'WASM Demo', link: '/demo/' },
            { label: 'Live Ingest Quickstart', link: '/quickstart/serve/' },
          ],
        },
        {
          label: 'Setup',
          items: [
            { label: 'Installation', link: '/setup/installation/' },
            { label: 'Collector', link: '/setup/collector/' },
            { label: 'OpenTelemetry Demo', link: '/setup/otel-demo/' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'Overview', link: '/guides/' },
            { label: 'Analyze Telemetry', link: '/guides/analyze-telemetry/' },
            { label: 'Stream to Local DuckLake', link: '/guides/stream-to-local-ducklake/' },
            { label: 'Query with Quack', link: '/guides/query-with-quack/' },
            { label: 'Stream to Remote DuckLake', link: '/guides/stream-to-remote-ducklake/' },
            { label: 'Store Agent Traces in DuckLake', link: '/guides/store-agent-traces-local-ducklake/' },
            { label: 'Stream to S3 Tables', link: '/guides/stream-to-s3-tables/' },
            { label: 'Stream to R2 Data Catalog', link: '/guides/stream-to-r2-data-catalog/' },
            { label: 'Export to Parquet', link: '/guides/exporting-to-parquet/' },
            { label: 'Error Handling', link: '/guides/error-handling/' },
          ],
        },
        {
          label: 'Reference',
          items: [
            { label: 'API', link: '/reference/api/' },
            { label: 'Schemas', link: '/reference/schemas/' },
            { label: 'Live Ingest', link: '/reference/serve/' },
            { label: 'Performance', link: '/reference/performance/' },
          ],
        },
        {
          label: 'Architecture',
          items: [{ label: 'Architecture', link: '/architecture/' }],
        },
      ],
    }),
  ],
});
