# Schema Reference

The extension emits strongly-typed tables inspired by the [OpenTelemetry ClickHouse exporter schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter). All column names use `snake_case`.

| Table function | Columns | Notes |
| --- | --- | --- |
| `read_otlp_traces` | 25 | Spans with identifiers, scope metadata, attributes, events, and links |
| `read_otlp_logs` | 15 | Log records with severity, body, resource attributes, and trace correlation |
| `read_otlp_metrics_gauge` | 16 | Gauge metrics with value and metadata |
| `read_otlp_metrics_sum` | 18 | Sum/counter metrics with aggregation temporality |

> **Note**: `read_otlp_metrics` (union schema), `read_otlp_metrics_histogram`, `read_otlp_metrics_exp_histogram`, and `read_otlp_metrics_summary` are not yet implemented. Use the gauge and sum helpers for now.

## Traces (`read_otlp_traces`)

25 columns total:

| Column | Type | Description |
| --- | --- | --- |
| `timestamp` | TIMESTAMP_MS | Span start time |
| `end_timestamp` | BIGINT | Span end time (nanoseconds since epoch) |
| `duration` | BIGINT | Span duration in nanoseconds |
| `trace_id` | VARCHAR | Trace identifier (hex string) |
| `span_id` | VARCHAR | Span identifier (hex string) |
| `parent_span_id` | VARCHAR | Parent span identifier (hex string) |
| `trace_state` | VARCHAR | W3C trace state |
| `service_name` | VARCHAR | Service name from resource attributes |
| `service_namespace` | VARCHAR | Service namespace from resource attributes |
| `service_instance_id` | VARCHAR | Service instance ID from resource attributes |
| `span_name` | VARCHAR | Operation name |
| `span_kind` | INTEGER | Span kind (0=unspecified, 1=internal, 2=server, 3=client, 4=producer, 5=consumer) |
| `status_code` | INTEGER | Status code (0=unset, 1=ok, 2=error) |
| `status_message` | VARCHAR | Status description |
| `resource_attributes` | VARCHAR | Resource attributes as JSON string |
| `scope_name` | VARCHAR | Instrumentation scope name |
| `scope_version` | VARCHAR | Instrumentation scope version |
| `scope_attributes` | VARCHAR | Scope attributes as JSON string |
| `span_attributes` | VARCHAR | Span attributes as JSON string |
| `events_json` | VARCHAR | Span events as JSON array |
| `links_json` | VARCHAR | Span links as JSON array |
| `dropped_attributes_count` | INTEGER | Number of dropped attributes |
| `dropped_events_count` | INTEGER | Number of dropped events |
| `dropped_links_count` | INTEGER | Number of dropped links |
| `flags` | INTEGER | Trace flags |

Each span is emitted once, even if multiple files are scanned. Use standard DuckDB SQL to join traces back to logs or metrics via `trace_id`/`span_id`.

## Logs (`read_otlp_logs`)

15 columns total:

| Column | Type | Description |
| --- | --- | --- |
| `timestamp` | TIMESTAMP_MS | Log timestamp |
| `observed_timestamp` | BIGINT | Time when the log was observed (nanoseconds since epoch) |
| `trace_id` | VARCHAR | Trace identifier for correlation (hex string) |
| `span_id` | VARCHAR | Span identifier for correlation (hex string) |
| `service_name` | VARCHAR | Service name from resource attributes |
| `service_namespace` | VARCHAR | Service namespace from resource attributes |
| `service_instance_id` | VARCHAR | Service instance ID from resource attributes |
| `severity_number` | INTEGER | Numeric severity level (1-24) |
| `severity_text` | VARCHAR | Severity text (e.g., "INFO", "ERROR") |
| `body` | VARCHAR | Log message body |
| `resource_attributes` | VARCHAR | Resource attributes as JSON string |
| `scope_name` | VARCHAR | Instrumentation scope name |
| `scope_version` | VARCHAR | Instrumentation scope version |
| `scope_attributes` | VARCHAR | Scope attributes as JSON string |
| `log_attributes` | VARCHAR | Log record attributes as JSON string |

## Metrics

### Gauge Metrics (`read_otlp_metrics_gauge`)

16 columns total:

| Column | Type | Description |
| --- | --- | --- |
| `timestamp` | TIMESTAMP_MS | Data point timestamp |
| `start_timestamp` | BIGINT | Start time (nanoseconds since epoch) |
| `metric_name` | VARCHAR | Metric name |
| `metric_description` | VARCHAR | Metric description |
| `metric_unit` | VARCHAR | Metric unit |
| `value` | DOUBLE | Gauge value |
| `service_name` | VARCHAR | Service name from resource attributes |
| `service_namespace` | VARCHAR | Service namespace from resource attributes |
| `service_instance_id` | VARCHAR | Service instance ID from resource attributes |
| `resource_attributes` | VARCHAR | Resource attributes as JSON string |
| `scope_name` | VARCHAR | Instrumentation scope name |
| `scope_version` | VARCHAR | Instrumentation scope version |
| `scope_attributes` | VARCHAR | Scope attributes as JSON string |
| `metric_attributes` | VARCHAR | Data point attributes as JSON string |
| `flags` | INTEGER | Data point flags |
| `exemplars_json` | VARCHAR | Exemplars as JSON array |

### Sum Metrics (`read_otlp_metrics_sum`)

18 columns total (gauge columns plus):

| Column | Type | Description |
| --- | --- | --- |
| `aggregation_temporality` | INTEGER | Aggregation temporality (1=delta, 2=cumulative) |
| `is_monotonic` | BOOLEAN | Whether the sum is monotonic (counter vs. up-down counter) |

All gauge columns are included, plus the two sum-specific columns above.

### Creating typed archive tables

```sql
CREATE TABLE archive_gauge AS
SELECT * FROM read_otlp_metrics_gauge('otel-export/telemetry.jsonl');

CREATE TABLE archive_sum AS
SELECT * FROM read_otlp_metrics_sum('otel-export/telemetry.jsonl');
```

See the [cookbook](../guides/cookbook.md) for more recipes.

## Type system notes

- `trace_id` and `span_id` are VARCHAR hex strings. Use `unhex()` to convert to binary if needed.
- Attribute columns store JSON strings. Parse with DuckDB's JSON functions: `json_extract(resource_attributes, '$.key')`.
- Timestamps: `timestamp` is TIMESTAMP_MS for easy querying; `end_timestamp`, `observed_timestamp`, and `start_timestamp` are BIGINT nanoseconds for precision.
- Events and links are stored as JSON arrays in `events_json` and `links_json`.

---

Looking for end-to-end workflows? Head back to the [cookbook](../guides/cookbook.md). Need help generating telemetry? The [collector setup guide](../setup/collector.md) covers the OpenTelemetry Collector setup.
