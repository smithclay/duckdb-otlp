#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Schema bridge functions for converting between union and typed metrics schemas
//!
//! These functions enable data transfer between:
//! - File reading mode (union schema with 27 columns)
//! - ATTACH mode (5 separate typed schemas with 10-19 columns)
//!
//! Usage example:
//!   INSERT INTO live.otel_metrics_gauge
//!   SELECT project_to_gauge(columns)
//!   FROM read_otlp_metrics('file.jsonl')
//!   WHERE MetricType = 'gauge';

class SchemaProjectionFunctions {
public:
	//! Project union schema row (27 columns) to gauge schema (10 columns)
	//! Extracts: base columns (0-8) + Value (10)
	static void ProjectToGauge(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project union schema row (27 columns) to sum schema (12 columns)
	//! Extracts: base columns (0-8) + Value (10) + AggregationTemporality (11) + IsMonotonic (12)
	static void ProjectToSum(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project union schema row (27 columns) to histogram schema (15 columns)
	//! Extracts: base columns (0-8) + Count (13) + Sum (14) + BucketCounts (15) +
	//!           ExplicitBounds (16) + Min (25) + Max (26)
	static void ProjectToHistogram(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project union schema row (27 columns) to exp_histogram schema (19 columns)
	//! Extracts: base columns (0-8) + Count (13) + Sum (14) + Scale (17) + ZeroCount (18) +
	//!           PositiveOffset (19) + PositiveBucketCounts (20) + NegativeOffset (21) +
	//!           NegativeBucketCounts (22) + Min (25) + Max (26)
	static void ProjectToExpHistogram(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project union schema row (27 columns) to summary schema (13 columns)
	//! Extracts: base columns (0-8) + Count (13) + Sum (14) +
	//!           QuantileValues (23) + QuantileQuantiles (24)
	static void ProjectToSummary(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project gauge schema row (10 columns) to union schema (27 columns)
	//! Sets: base columns (0-8) + MetricType='gauge' (9) + Value (10), rest NULL
	static void ProjectFromGauge(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project sum schema row (12 columns) to union schema (27 columns)
	//! Sets: base columns (0-8) + MetricType='sum' (9) + Value (10) +
	//!       AggregationTemporality (11) + IsMonotonic (12), rest NULL
	static void ProjectFromSum(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project histogram schema row (15 columns) to union schema (27 columns)
	//! Sets: base columns (0-8) + MetricType='histogram' (9) + Count (13) + Sum (14) +
	//!       BucketCounts (15) + ExplicitBounds (16) + Min (25) + Max (26), rest NULL
	static void ProjectFromHistogram(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project exp_histogram schema row (19 columns) to union schema (27 columns)
	//! Sets: base columns (0-8) + MetricType='exponential_histogram' (9) + Count (13) +
	//!       Sum (14) + Scale (17) + ZeroCount (18) + PositiveOffset (19) +
	//!       PositiveBucketCounts (20) + NegativeOffset (21) + NegativeBucketCounts (22) +
	//!       Min (25) + Max (26), rest NULL
	static void ProjectFromExpHistogram(DataChunk &args, ExpressionState &state, Vector &result);

	//! Project summary schema row (13 columns) to union schema (27 columns)
	//! Sets: base columns (0-8) + MetricType='summary' (9) + Count (13) + Sum (14) +
	//!       QuantileValues (23) + QuantileQuantiles (24), rest NULL
	static void ProjectFromSummary(DataChunk &args, ExpressionState &state, Vector &result);
};

//! Register all schema projection functions with DuckDB
void RegisterSchemaProjectionFunctions(DatabaseInstance &db);

} // namespace duckdb
