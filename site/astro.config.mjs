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
            { label: 'Sample Data', link: '/setup/sample-data/' },
            { label: 'OpenTelemetry Demo', link: '/setup/otel-demo/' },
          ],
        },
        {
          label: 'Guides',
          items: [
            { label: 'Overview', link: '/guides/' },
            { label: 'Analyze Traces', link: '/guides/analyzing-traces/' },
            { label: 'Filter Logs', link: '/guides/filtering-logs/' },
            { label: 'Work with Metrics', link: '/guides/working-with-metrics/' },
            { label: 'Stream to DuckLake', link: '/guides/stream-to-ducklake/' },
            { label: 'Stream to Iceberg', link: '/guides/stream-to-iceberg/' },
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
