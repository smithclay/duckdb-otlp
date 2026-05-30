# Metrics Schema

The source of truth is [schemas.md#metrics](schemas.md#metrics).

Use shape-specific readers:

| Function | Shape |
| --- | --- |
| `read_otlp_metrics_gauge(path)` | Gauge |
| `read_otlp_metrics_sum(path)` | Sum/counter |
| `read_otlp_metrics_histogram(path)` | Standard histogram |
| `read_otlp_metrics_exp_histogram(path)` | Exponential histogram |

`read_otlp_metrics(path)` and `read_otlp_metrics_summary(path)` are not implemented yet.
