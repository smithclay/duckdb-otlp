#pragma once

#include "duckdb.hpp"

namespace duckdb {

//! Base columns shared by all metric types
class OTLPMetricsBaseSchema {
public:
	// Common column indices (all metric tables share these)
	static constexpr idx_t COL_TIMESTAMP = 0;
	static constexpr idx_t COL_SERVICE_NAME = 1;
	static constexpr idx_t COL_METRIC_NAME = 2;
	static constexpr idx_t COL_METRIC_DESCRIPTION = 3;
	static constexpr idx_t COL_METRIC_UNIT = 4;
	static constexpr idx_t COL_RESOURCE_ATTRIBUTES = 5;
	static constexpr idx_t COL_SCOPE_NAME = 6;
	static constexpr idx_t COL_SCOPE_VERSION = 7;
	static constexpr idx_t COL_ATTRIBUTES = 8;
	static constexpr idx_t BASE_COLUMN_COUNT = 9;

	//! Get common column names
	static vector<string> GetBaseColumnNames() {
		return {"Timestamp",          "ServiceName", "MetricName",   "MetricDescription", "MetricUnit",
		        "ResourceAttributes", "ScopeName",   "ScopeVersion", "Attributes"};
	}

	//! Get common column types
	static vector<LogicalType> GetBaseColumnTypes() {
		auto map_type = LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR);

		return {
		    LogicalType::TIMESTAMP_NS, // Timestamp
		    LogicalType::VARCHAR,      // ServiceName
		    LogicalType::VARCHAR,      // MetricName
		    LogicalType::VARCHAR,      // MetricDescription
		    LogicalType::VARCHAR,      // MetricUnit
		    map_type,                  // ResourceAttributes
		    LogicalType::VARCHAR,      // ScopeName
		    LogicalType::VARCHAR,      // ScopeVersion
		    map_type                   // Attributes
		};
	}
};

//! Schema for otel_metrics_gauge
class OTLPMetricsGaugeSchema : public OTLPMetricsBaseSchema {
public:
	static constexpr idx_t COL_VALUE = BASE_COLUMN_COUNT;
	static constexpr idx_t COLUMN_COUNT = BASE_COLUMN_COUNT + 1;

	static vector<string> GetColumnNames() {
		auto names = GetBaseColumnNames();
		names.push_back("Value");
		return names;
	}

	static vector<LogicalType> GetColumnTypes() {
		auto types = GetBaseColumnTypes();
		types.push_back(LogicalType::DOUBLE); // Value
		return types;
	}
};

//! Schema for otel_metrics_sum
class OTLPMetricsSumSchema : public OTLPMetricsBaseSchema {
public:
	static constexpr idx_t COL_VALUE = BASE_COLUMN_COUNT;
	static constexpr idx_t COL_AGGREGATION_TEMPORALITY = BASE_COLUMN_COUNT + 1;
	static constexpr idx_t COL_IS_MONOTONIC = BASE_COLUMN_COUNT + 2;
	static constexpr idx_t COLUMN_COUNT = BASE_COLUMN_COUNT + 3;

	static vector<string> GetColumnNames() {
		auto names = GetBaseColumnNames();
		names.push_back("Value");
		names.push_back("AggregationTemporality");
		names.push_back("IsMonotonic");
		return names;
	}

	static vector<LogicalType> GetColumnTypes() {
		auto types = GetBaseColumnTypes();
		types.push_back(LogicalType::DOUBLE);  // Value
		types.push_back(LogicalType::INTEGER); // AggregationTemporality
		types.push_back(LogicalType::BOOLEAN); // IsMonotonic
		return types;
	}
};

//! Schema for otel_metrics_histogram
class OTLPMetricsHistogramSchema : public OTLPMetricsBaseSchema {
public:
	static constexpr idx_t COL_COUNT = BASE_COLUMN_COUNT;
	static constexpr idx_t COL_SUM = BASE_COLUMN_COUNT + 1;
	static constexpr idx_t COL_BUCKET_COUNTS = BASE_COLUMN_COUNT + 2;
	static constexpr idx_t COL_EXPLICIT_BOUNDS = BASE_COLUMN_COUNT + 3;
	static constexpr idx_t COL_MIN = BASE_COLUMN_COUNT + 4;
	static constexpr idx_t COL_MAX = BASE_COLUMN_COUNT + 5;
	static constexpr idx_t COLUMN_COUNT = BASE_COLUMN_COUNT + 6;

	static vector<string> GetColumnNames() {
		auto names = GetBaseColumnNames();
		names.push_back("Count");
		names.push_back("Sum");
		names.push_back("BucketCounts");
		names.push_back("ExplicitBounds");
		names.push_back("Min");
		names.push_back("Max");
		return names;
	}

	static vector<LogicalType> GetColumnTypes() {
		auto types = GetBaseColumnTypes();
		types.push_back(LogicalType::UBIGINT);                    // Count
		types.push_back(LogicalType::DOUBLE);                     // Sum
		types.push_back(LogicalType::LIST(LogicalType::UBIGINT)); // BucketCounts
		types.push_back(LogicalType::LIST(LogicalType::DOUBLE));  // ExplicitBounds
		types.push_back(LogicalType::DOUBLE);                     // Min
		types.push_back(LogicalType::DOUBLE);                     // Max
		return types;
	}
};

//! Schema for otel_metrics_exp_histogram
class OTLPMetricsExpHistogramSchema : public OTLPMetricsBaseSchema {
public:
	static constexpr idx_t COL_COUNT = BASE_COLUMN_COUNT;
	static constexpr idx_t COL_SUM = BASE_COLUMN_COUNT + 1;
	static constexpr idx_t COL_SCALE = BASE_COLUMN_COUNT + 2;
	static constexpr idx_t COL_ZERO_COUNT = BASE_COLUMN_COUNT + 3;
	static constexpr idx_t COL_POSITIVE_OFFSET = BASE_COLUMN_COUNT + 4;
	static constexpr idx_t COL_POSITIVE_BUCKET_COUNTS = BASE_COLUMN_COUNT + 5;
	static constexpr idx_t COL_NEGATIVE_OFFSET = BASE_COLUMN_COUNT + 6;
	static constexpr idx_t COL_NEGATIVE_BUCKET_COUNTS = BASE_COLUMN_COUNT + 7;
	static constexpr idx_t COL_MIN = BASE_COLUMN_COUNT + 8;
	static constexpr idx_t COL_MAX = BASE_COLUMN_COUNT + 9;
	static constexpr idx_t COLUMN_COUNT = BASE_COLUMN_COUNT + 10;

	static vector<string> GetColumnNames() {
		auto names = GetBaseColumnNames();
		names.push_back("Count");
		names.push_back("Sum");
		names.push_back("Scale");
		names.push_back("ZeroCount");
		names.push_back("PositiveOffset");
		names.push_back("PositiveBucketCounts");
		names.push_back("NegativeOffset");
		names.push_back("NegativeBucketCounts");
		names.push_back("Min");
		names.push_back("Max");
		return names;
	}

	static vector<LogicalType> GetColumnTypes() {
		auto types = GetBaseColumnTypes();
		types.push_back(LogicalType::UBIGINT);                    // Count
		types.push_back(LogicalType::DOUBLE);                     // Sum
		types.push_back(LogicalType::INTEGER);                    // Scale
		types.push_back(LogicalType::UBIGINT);                    // ZeroCount
		types.push_back(LogicalType::INTEGER);                    // PositiveOffset
		types.push_back(LogicalType::LIST(LogicalType::UBIGINT)); // PositiveBucketCounts
		types.push_back(LogicalType::INTEGER);                    // NegativeOffset
		types.push_back(LogicalType::LIST(LogicalType::UBIGINT)); // NegativeBucketCounts
		types.push_back(LogicalType::DOUBLE);                     // Min
		types.push_back(LogicalType::DOUBLE);                     // Max
		return types;
	}
};

//! Schema for otel_metrics_summary
class OTLPMetricsSummarySchema : public OTLPMetricsBaseSchema {
public:
	static constexpr idx_t COL_COUNT = BASE_COLUMN_COUNT;
	static constexpr idx_t COL_SUM = BASE_COLUMN_COUNT + 1;
	static constexpr idx_t COL_QUANTILE_VALUES = BASE_COLUMN_COUNT + 2;
	static constexpr idx_t COL_QUANTILE_QUANTILES = BASE_COLUMN_COUNT + 3;
	static constexpr idx_t COLUMN_COUNT = BASE_COLUMN_COUNT + 4;

	static vector<string> GetColumnNames() {
		auto names = GetBaseColumnNames();
		names.push_back("Count");
		names.push_back("Sum");
		names.push_back("QuantileValues");
		names.push_back("QuantileQuantiles");
		return names;
	}

	static vector<LogicalType> GetColumnTypes() {
		auto types = GetBaseColumnTypes();
		types.push_back(LogicalType::UBIGINT);                   // Count
		types.push_back(LogicalType::DOUBLE);                    // Sum
		types.push_back(LogicalType::LIST(LogicalType::DOUBLE)); // QuantileValues
		types.push_back(LogicalType::LIST(LogicalType::DOUBLE)); // QuantileQuantiles
		return types;
	}
};

} // namespace duckdb
