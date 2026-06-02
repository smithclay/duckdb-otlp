---
title: "Schema Reference"
---

The extension emits typed tables based on the [OpenTelemetry ClickHouse exporter schema](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/exporter/clickhouseexporter) and aligns fields with the [OpenTelemetry Arrow data model](https://github.com/open-telemetry/otel-arrow/blob/main/docs/data_model.md) where the models overlap. All column names use `snake_case`.

| Table function | Columns | Notes |
| --- | --- | --- |
| `read_otlp_traces` | 24 | Spans with identifiers, scope metadata, attributes, events, and links |
| `read_otlp_logs` | 18 | Log records with severity, body, resource attributes, and trace correlation |
| `read_otlp_metrics_gauge` | 17 | Gauge metrics with numeric value columns and metadata |
| `read_otlp_metrics_sum` | 19 | Sum/counter metrics with aggregation temporality |
| `read_otlp_metrics_exp_histogram` | 27 | Exponential histogram metrics with bucket data |
| `read_otlp_metrics_histogram` | 22 | Standard histogram metrics with explicit bucket bounds |

`read_otlp_metrics` (union schema) and `read_otlp_metrics_summary` are registered placeholders.

## Traces (`read_otlp_traces`)

24 columns total:

| Column | Type | Description |
| --- | --- | --- |
| `start_time_unix_nano` | TIMESTAMP_NS | Span start time |
| `duration_time_unix_nano` | BIGINT | Span duration in nanoseconds |
| `trace_id` | VARCHAR | Trace identifier (hex string) |
| `span_id` | VARCHAR | Span identifier (hex string) |
| `parent_span_id` | VARCHAR | Parent span identifier (hex string) |
| `trace_state` | VARCHAR | W3C trace state |
| `service_name` | VARCHAR | Service name from resource attributes |
| `service_namespace` | VARCHAR | Service namespace from resource attributes |
| `service_instance_id` | VARCHAR | Service instance ID from resource attributes |
| `name` | VARCHAR | Operation name |
| `kind` | INTEGER | Span kind (0=unspecified, 1=internal, 2=server, 3=client, 4=producer, 5=consumer) |
| `status_code` | INTEGER | Status code (0=unset, 1=ok, 2=error) |
| `status_status_message` | VARCHAR | Status description |
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

18 columns total:

| Column | Type | Description |
| --- | --- | --- |
| `time_unix_nano` | TIMESTAMP_NS | Log timestamp |
| `observed_time_unix_nano` | TIMESTAMP_NS | Time when the log was observed |
| `trace_id` | VARCHAR | Trace identifier for correlation (hex string) |
| `span_id` | VARCHAR | Span identifier for correlation (hex string) |
| `service_name` | VARCHAR | Service name from resource attributes |
| `service_namespace` | VARCHAR | Service namespace from resource attributes |
| `service_instance_id` | VARCHAR | Service instance ID from resource attributes |
| `severity_number` | INTEGER | Numeric severity level (1-24) |
| `severity_text` | VARCHAR | Severity text (e.g., "INFO", "ERROR") |
| `event_name` | VARCHAR | Event name attribute, when present |
| `body` | VARCHAR | Log message body |
| `resource_attributes` | VARCHAR | Resource attributes as JSON string |
| `scope_name` | VARCHAR | Instrumentation scope name |
| `scope_version` | VARCHAR | Instrumentation scope version |
| `scope_attributes` | VARCHAR | Scope attributes as JSON string |
| `log_attributes` | VARCHAR | Log record attributes as JSON string |
| `dropped_attributes_count` | INTEGER | Number of dropped attributes |
| `flags` | INTEGER | Trace flags |

## Metrics

### Gauge Metrics (`read_otlp_metrics_gauge`)

17 columns total:

| Column | Type | Description |
| --- | --- | --- |
| `time_unix_nano` | TIMESTAMP_NS | Data point timestamp |
| `start_time_unix_nano` | TIMESTAMP_NS | Start time |
| `name` | VARCHAR | Metric name |
| `description` | VARCHAR | Metric description |
| `unit` | VARCHAR | Metric unit |
| `int_value` | BIGINT | Integer value, when encoded as an integer |
| `double_value` | DOUBLE | Floating point value, when encoded as a double |
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

19 columns total (gauge columns plus):

| Column | Type | Description |
| --- | --- | --- |
| `aggregation_temporality` | INTEGER | Aggregation temporality (1=delta, 2=cumulative) |
| `is_monotonic` | BOOLEAN | Whether the sum is monotonic (counter vs. up-down counter) |

All gauge columns are included, plus the two sum-specific columns above.

### Histogram Metrics (`read_otlp_metrics_histogram`)

22 columns total:

| Column | Type | Description |
| --- | --- | --- |
| `time_unix_nano` | TIMESTAMP_NS | Data point timestamp |
| `start_time_unix_nano` | TIMESTAMP_NS | Start time |
| `name` | VARCHAR | Metric name |
| `description` | VARCHAR | Metric description |
| `unit` | VARCHAR | Metric unit |
| `count` | BIGINT | Total count of observations |
| `sum` | DOUBLE | Sum of all observations (optional) |
| `min` | DOUBLE | Minimum observed value (optional) |
| `max` | DOUBLE | Maximum observed value (optional) |
| `bucket_counts` | VARCHAR | Bucket counts as JSON array of integers |
| `explicit_bounds` | VARCHAR | Explicit bucket boundaries as JSON array of floats |
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
| `aggregation_temporality` | INTEGER | Aggregation temporality (1=delta, 2=cumulative) |

### Exponential Histogram Metrics (`read_otlp_metrics_exp_histogram`)

27 columns total:

| Column | Type | Description |
| --- | --- | --- |
| `time_unix_nano` | TIMESTAMP_NS | Data point timestamp |
| `start_time_unix_nano` | TIMESTAMP_NS | Start time |
| `name` | VARCHAR | Metric name |
| `description` | VARCHAR | Metric description |
| `unit` | VARCHAR | Metric unit |
| `count` | BIGINT | Total count of observations |
| `sum` | DOUBLE | Sum of all observations (optional) |
| `min` | DOUBLE | Minimum observed value (optional) |
| `max` | DOUBLE | Maximum observed value (optional) |
| `scale` | INTEGER | Scale factor for bucket boundaries |
| `zero_count` | BIGINT | Count of observations at zero |
| `zero_threshold` | DOUBLE | Boundary for zero bucket (optional) |
| `positive_offset` | INTEGER | Starting index for positive buckets |
| `positive_bucket_counts` | VARCHAR | Positive bucket counts as JSON array |
| `negative_offset` | INTEGER | Starting index for negative buckets |
| `negative_bucket_counts` | VARCHAR | Negative bucket counts as JSON array |
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
| `aggregation_temporality` | INTEGER | Aggregation temporality (1=delta, 2=cumulative) |

## Type system notes

- `trace_id` and `span_id` are VARCHAR hex strings. Use `unhex()` to convert to binary if needed.
- Attribute columns store JSON strings. Parse with DuckDB's JSON functions: `json_extract(resource_attributes, '$.key')`.
- Timestamps: file readers expose nanosecond timestamp columns such as `time_unix_nano` and `start_time_unix_nano` as `TIMESTAMP_NS`. Live ingest tables keep the same column names but store those values as DuckDB `TIMESTAMP` for catalog compatibility.
- Events and links are stored as JSON arrays in `events_json` and `links_json`.

---

For task-oriented queries and exports, use the [how-to guides](../../guides/).
