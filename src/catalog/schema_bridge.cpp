#include "catalog/schema_bridge.hpp"
#include "schema/otlp_metrics_union_schema.hpp"
#include "schema/otlp_metrics_schemas.hpp"

namespace duckdb {

//! Schema Bridge: Data Transfer Between Union and Typed Schemas
//!
//! This module documents the recommended patterns for transferring OTLP metrics data
//! between the two schema formats:
//! - Union schema (27 columns) - used by read_otlp_metrics() and otlp_metrics_union()
//! - Typed schemas (10-19 columns) - used by ATTACH mode tables
//!
//! Pattern 1: File → Archive (Union → Typed)
//! Load metrics from files into typed permanent tables:
//! NOTE: ATTACH tables are read-only (only accept data via gRPC), so use permanent tables instead
//!
//!   CREATE TABLE archive_gauge AS
//!   SELECT Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
//!          ResourceAttributes, ScopeName, ScopeVersion, Attributes, Value
//!   FROM read_otlp_metrics('file.jsonl')
//!   WHERE MetricType = 'gauge';
//!
//! Pattern 2: ATTACH → Archive (Typed → Typed)
//! Archive live streaming data to permanent tables:
//!
//!   CREATE TABLE archived_gauge AS
//!   SELECT * FROM live.otel_metrics_gauge
//!   WHERE Timestamp < now() - INTERVAL '7 days';
//!
//! Pattern 3: ATTACH → Union View (Typed → Union)
//! Query all metric types together using union view:
//!
//!   SELECT * FROM otlp_metrics_union('live')
//!   WHERE ServiceName = 'my-service';
//!
//! Pattern 4: Bulk Transfer All Metric Types
//! Load all metrics from file into separate typed tables:
//!
//!   -- Gauge metrics
//!   CREATE TABLE archive_gauge AS
//!   SELECT Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
//!          ResourceAttributes, ScopeName, ScopeVersion, Attributes, Value
//!   FROM read_otlp_metrics('metrics.jsonl') WHERE MetricType = 'gauge';
//!
//!   -- Sum metrics
//!   CREATE TABLE archive_sum AS
//!   SELECT Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
//!          ResourceAttributes, ScopeName, ScopeVersion, Attributes,
//!          Value, AggregationTemporality, IsMonotonic
//!   FROM read_otlp_metrics('metrics.jsonl') WHERE MetricType = 'sum';
//!
//!   -- Histogram metrics
//!   CREATE TABLE archive_histogram AS
//!   SELECT Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
//!          ResourceAttributes, ScopeName, ScopeVersion, Attributes,
//!          Count, Sum, BucketCounts, ExplicitBounds, Min, Max
//!   FROM read_otlp_metrics('metrics.jsonl') WHERE MetricType = 'histogram';
//!
//!   -- Exponential Histogram metrics
//!   CREATE TABLE archive_exp_histogram AS
//!   SELECT Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
//!          ResourceAttributes, ScopeName, ScopeVersion, Attributes,
//!          Count, Sum, Scale, ZeroCount, PositiveOffset, PositiveBucketCounts,
//!          NegativeOffset, NegativeBucketCounts, Min, Max
//!   FROM read_otlp_metrics('metrics.jsonl') WHERE MetricType = 'exponential_histogram';
//!
//!   -- Summary metrics
//!   CREATE TABLE archive_summary AS
//!   SELECT Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
//!          ResourceAttributes, ScopeName, ScopeVersion, Attributes,
//!          Count, Sum, QuantileValues, QuantileQuantiles
//!   FROM read_otlp_metrics('metrics.jsonl') WHERE MetricType = 'summary';

} // namespace duckdb
