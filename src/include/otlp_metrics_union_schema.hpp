#pragma once

#include "duckdb.hpp"
#include "otlp_metrics_schemas.hpp"

namespace duckdb {

//! Union schema for all OTLP metrics types
//! Combines all metric-specific columns with a metric_type discriminator
class OTLPMetricsUnionSchema {
public:
	// Column indices (base columns 0-8 from OTLPMetricsBaseSchema)
	static constexpr idx_t COL_TIMESTAMP = 0;
	static constexpr idx_t COL_SERVICE_NAME = 1;
	static constexpr idx_t COL_METRIC_NAME = 2;
	static constexpr idx_t COL_METRIC_DESCRIPTION = 3;
	static constexpr idx_t COL_METRIC_UNIT = 4;
	static constexpr idx_t COL_RESOURCE_ATTRIBUTES = 5;
	static constexpr idx_t COL_SCOPE_NAME = 6;
	static constexpr idx_t COL_SCOPE_VERSION = 7;
	static constexpr idx_t COL_ATTRIBUTES = 8;

	// Type discriminator
	static constexpr idx_t COL_METRIC_TYPE = 9;

	// Gauge-specific (1 column)
	static constexpr idx_t COL_VALUE = 10;

	// Sum-specific (2 additional columns)
	static constexpr idx_t COL_AGGREGATION_TEMPORALITY = 11;
	static constexpr idx_t COL_IS_MONOTONIC = 12;

	// Histogram-specific (4 additional columns)
	static constexpr idx_t COL_COUNT = 13;
	static constexpr idx_t COL_SUM = 14;
	static constexpr idx_t COL_BUCKET_COUNTS = 15;
	static constexpr idx_t COL_EXPLICIT_BOUNDS = 16;

	// Exponential Histogram-specific (4 additional columns)
	static constexpr idx_t COL_SCALE = 17;
	static constexpr idx_t COL_ZERO_COUNT = 18;
	static constexpr idx_t COL_POSITIVE_OFFSET = 19;
	static constexpr idx_t COL_POSITIVE_BUCKET_COUNTS = 20;
	static constexpr idx_t COL_NEGATIVE_OFFSET = 21;
	static constexpr idx_t COL_NEGATIVE_BUCKET_COUNTS = 22;

	// Summary-specific (2 additional columns)
	static constexpr idx_t COL_QUANTILE_VALUES = 23;
	static constexpr idx_t COL_QUANTILE_QUANTILES = 24;

	// Common optional columns (shared by histogram, exp_histogram)
	static constexpr idx_t COL_MIN = 25;
	static constexpr idx_t COL_MAX = 26;

	static constexpr idx_t COLUMN_COUNT = 27;

	//! Get column names
	static vector<string> GetColumnNames() {
		return {"Timestamp",
		        "ServiceName",
		        "MetricName",
		        "MetricDescription",
		        "MetricUnit",
		        "ResourceAttributes",
		        "ScopeName",
		        "ScopeVersion",
		        "Attributes",
		        "MetricType",
		        "Value",
		        "AggregationTemporality",
		        "IsMonotonic",
		        "Count",
		        "Sum",
		        "BucketCounts",
		        "ExplicitBounds",
		        "Scale",
		        "ZeroCount",
		        "PositiveOffset",
		        "PositiveBucketCounts",
		        "NegativeOffset",
		        "NegativeBucketCounts",
		        "QuantileValues",
		        "QuantileQuantiles",
		        "Min",
		        "Max"};
	}

	//! Get column types
	static vector<LogicalType> GetColumnTypes() {
		auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);
		auto ubigint_list = LogicalType::LIST(LogicalType::UBIGINT);
		auto double_list = LogicalType::LIST(LogicalType::DOUBLE);

		return {LogicalType::TIMESTAMP_NS, // Timestamp
		        LogicalType::VARCHAR,      // ServiceName
		        LogicalType::VARCHAR,      // MetricName
		        LogicalType::VARCHAR,      // MetricDescription
		        LogicalType::VARCHAR,      // MetricUnit
		        map_type,                  // ResourceAttributes
		        LogicalType::VARCHAR,      // ScopeName
		        LogicalType::VARCHAR,      // ScopeVersion
		        map_type,                  // Attributes
		        LogicalType::VARCHAR,      // MetricType
		        LogicalType::DOUBLE,       // Value (gauge, sum)
		        LogicalType::INTEGER,      // AggregationTemporality (sum)
		        LogicalType::BOOLEAN,      // IsMonotonic (sum)
		        LogicalType::UBIGINT,      // Count (histogram, exp_histogram, summary)
		        LogicalType::DOUBLE,       // Sum (histogram, exp_histogram, summary)
		        ubigint_list,              // BucketCounts (histogram)
		        double_list,               // ExplicitBounds (histogram)
		        LogicalType::INTEGER,      // Scale (exp_histogram)
		        LogicalType::UBIGINT,      // ZeroCount (exp_histogram)
		        LogicalType::INTEGER,      // PositiveOffset (exp_histogram)
		        ubigint_list,              // PositiveBucketCounts (exp_histogram)
		        LogicalType::INTEGER,      // NegativeOffset (exp_histogram)
		        ubigint_list,              // NegativeBucketCounts (exp_histogram)
		        double_list,               // QuantileValues (summary)
		        double_list,               // QuantileQuantiles (summary)
		        LogicalType::DOUBLE,       // Min (histogram, exp_histogram)
		        LogicalType::DOUBLE};      // Max (histogram, exp_histogram)
	}
};

} // namespace duckdb
