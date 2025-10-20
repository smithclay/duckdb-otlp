#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Schema Bridge: Data Transfer Patterns for OTLP Metrics
//!
//! This module documents recommended SQL patterns for transferring metrics data between:
//! - Union schema (27 columns) - used by read_otlp_metrics() and otlp_metrics_union()
//! - Typed schemas (10-19 columns) - used by ATTACH mode tables (otel_metrics_*)
//!
//! # Schema Overview
//!
//! Union Schema (27 columns):
//!   - 9 base columns: Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
//!                     ResourceAttributes, ScopeName, ScopeVersion, Attributes
//!   - 1 type discriminator: MetricType (gauge|sum|histogram|exponential_histogram|summary)
//!   - 17 type-specific columns (most NULL for any given row)
//!
//! Typed Schemas (5 tables):
//!   - otel_metrics_gauge (10 columns): base + Value
//!   - otel_metrics_sum (12 columns): base + Value, AggregationTemporality, IsMonotonic
//!   - otel_metrics_histogram (15 columns): base + Count, Sum, BucketCounts, ExplicitBounds, Min, Max
//!   - otel_metrics_exp_histogram (19 columns): base + Count, Sum, Scale, ZeroCount, offsets, buckets, Min, Max
//!   - otel_metrics_summary (13 columns): base + Count, Sum, QuantileValues, QuantileQuantiles
//!
//! # Recommended Patterns
//!
//! See schema_bridge.cpp for detailed SQL examples of:
//!   - Pattern 1: File → ATTACH (load metrics from files into live tables)
//!   - Pattern 2: ATTACH → Archive (persist live data to permanent tables)
//!   - Pattern 3: ATTACH → Union View (query all types together)
//!   - Pattern 4: Bulk Transfer (transfer all metric types at once)

} // namespace duckdb
