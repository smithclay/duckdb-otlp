#pragma once

#include "duckdb.hpp"
#include "schema/otlp_metrics_schemas.hpp"
#include <optional>

namespace duckdb {

//! Data struct for building gauge metric rows
struct MetricsGaugeData {
	timestamp_ns_t timestamp;
	string service_name;
	string metric_name;
	string metric_description;
	string metric_unit;
	Value resource_attributes; // MAP<VARCHAR,VARCHAR>
	string scope_name;
	string scope_version;
	Value attributes; // MAP<VARCHAR,VARCHAR>
	double value;
};

vector<Value> BuildMetricsGaugeRow(const MetricsGaugeData &d);

//! Data struct for building sum metric rows
struct MetricsSumData {
	timestamp_ns_t timestamp;
	string service_name;
	string metric_name;
	string metric_description;
	string metric_unit;
	Value resource_attributes; // MAP<VARCHAR,VARCHAR>
	string scope_name;
	string scope_version;
	Value attributes; // MAP<VARCHAR,VARCHAR>
	double value;
	std::optional<int32_t> aggregation_temporality; // OTLP enum (0=UNSPECIFIED, 1=DELTA, 2=CUMULATIVE)
	bool is_monotonic;
};

vector<Value> BuildMetricsSumRow(const MetricsSumData &d);

//! Data struct for building histogram metric rows
struct MetricsHistogramData {
	timestamp_ns_t timestamp;
	string service_name;
	string metric_name;
	string metric_description;
	string metric_unit;
	Value resource_attributes; // MAP<VARCHAR,VARCHAR>
	string scope_name;
	string scope_version;
	Value attributes; // MAP<VARCHAR,VARCHAR>
	uint64_t count;
	std::optional<double> sum;
	vector<Value> bucket_counts;   // LIST<UBIGINT> (elements)
	vector<Value> explicit_bounds; // LIST<DOUBLE> (elements)
	std::optional<double> min_value;
	std::optional<double> max_value;
};

vector<Value> BuildMetricsHistogramRow(const MetricsHistogramData &d);

//! Data struct for building exponential histogram metric rows
struct MetricsExpHistogramData {
	timestamp_ns_t timestamp;
	string service_name;
	string metric_name;
	string metric_description;
	string metric_unit;
	Value resource_attributes; // MAP<VARCHAR,VARCHAR>
	string scope_name;
	string scope_version;
	Value attributes; // MAP<VARCHAR,VARCHAR>
	uint64_t count;
	std::optional<double> sum;
	int32_t scale;
	uint64_t zero_count;
	int32_t positive_offset;
	vector<Value> positive_bucket_counts; // LIST<UBIGINT> (elements)
	int32_t negative_offset;
	vector<Value> negative_bucket_counts; // LIST<UBIGINT> (elements)
	std::optional<double> min_value;
	std::optional<double> max_value;
};

vector<Value> BuildMetricsExpHistogramRow(const MetricsExpHistogramData &d);

//! Data struct for building summary metric rows
struct MetricsSummaryData {
	timestamp_ns_t timestamp;
	string service_name;
	string metric_name;
	string metric_description;
	string metric_unit;
	Value resource_attributes; // MAP<VARCHAR,VARCHAR>
	string scope_name;
	string scope_version;
	Value attributes; // MAP<VARCHAR,VARCHAR>
	uint64_t count;
	std::optional<double> sum;
	vector<Value> quantile_values;    // LIST<DOUBLE> (elements)
	vector<Value> quantile_quantiles; // LIST<DOUBLE> (elements)
};

vector<Value> BuildMetricsSummaryRow(const MetricsSummaryData &d);

} // namespace duckdb
