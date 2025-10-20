#include "receiver/row_builders_metrics.hpp"

namespace duckdb {

vector<Value> BuildMetricsGaugeRow(const MetricsGaugeData &d) {
	vector<Value> row(OTLPMetricsGaugeSchema::COLUMN_COUNT);
	row[OTLPMetricsGaugeSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(d.timestamp);
	row[OTLPMetricsGaugeSchema::COL_SERVICE_NAME] = Value(d.service_name);
	row[OTLPMetricsGaugeSchema::COL_METRIC_NAME] = Value(d.metric_name);
	row[OTLPMetricsGaugeSchema::COL_METRIC_DESCRIPTION] = Value(d.metric_description);
	row[OTLPMetricsGaugeSchema::COL_METRIC_UNIT] = Value(d.metric_unit);
	row[OTLPMetricsGaugeSchema::COL_RESOURCE_ATTRIBUTES] = d.resource_attributes;
	row[OTLPMetricsGaugeSchema::COL_SCOPE_NAME] = Value(d.scope_name);
	row[OTLPMetricsGaugeSchema::COL_SCOPE_VERSION] = Value(d.scope_version);
	row[OTLPMetricsGaugeSchema::COL_ATTRIBUTES] = d.attributes;
	row[OTLPMetricsGaugeSchema::COL_VALUE] = Value::DOUBLE(d.value);
	return row;
}

vector<Value> BuildMetricsSumRow(const MetricsSumData &d) {
	vector<Value> row(OTLPMetricsSumSchema::COLUMN_COUNT);
	row[OTLPMetricsSumSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(d.timestamp);
	row[OTLPMetricsSumSchema::COL_SERVICE_NAME] = Value(d.service_name);
	row[OTLPMetricsSumSchema::COL_METRIC_NAME] = Value(d.metric_name);
	row[OTLPMetricsSumSchema::COL_METRIC_DESCRIPTION] = Value(d.metric_description);
	row[OTLPMetricsSumSchema::COL_METRIC_UNIT] = Value(d.metric_unit);
	row[OTLPMetricsSumSchema::COL_RESOURCE_ATTRIBUTES] = d.resource_attributes;
	row[OTLPMetricsSumSchema::COL_SCOPE_NAME] = Value(d.scope_name);
	row[OTLPMetricsSumSchema::COL_SCOPE_VERSION] = Value(d.scope_version);
	row[OTLPMetricsSumSchema::COL_ATTRIBUTES] = d.attributes;
	row[OTLPMetricsSumSchema::COL_VALUE] = Value::DOUBLE(d.value);
	if (d.aggregation_temporality) {
		row[OTLPMetricsSumSchema::COL_AGGREGATION_TEMPORALITY] = Value::INTEGER(*d.aggregation_temporality);
	} else {
		row[OTLPMetricsSumSchema::COL_AGGREGATION_TEMPORALITY] = Value();
	}
	row[OTLPMetricsSumSchema::COL_IS_MONOTONIC] = Value::BOOLEAN(d.is_monotonic);
	return row;
}

vector<Value> BuildMetricsHistogramRow(const MetricsHistogramData &d) {
	vector<Value> row(OTLPMetricsHistogramSchema::COLUMN_COUNT);
	row[OTLPMetricsHistogramSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(d.timestamp);
	row[OTLPMetricsHistogramSchema::COL_SERVICE_NAME] = Value(d.service_name);
	row[OTLPMetricsHistogramSchema::COL_METRIC_NAME] = Value(d.metric_name);
	row[OTLPMetricsHistogramSchema::COL_METRIC_DESCRIPTION] = Value(d.metric_description);
	row[OTLPMetricsHistogramSchema::COL_METRIC_UNIT] = Value(d.metric_unit);
	row[OTLPMetricsHistogramSchema::COL_RESOURCE_ATTRIBUTES] = d.resource_attributes;
	row[OTLPMetricsHistogramSchema::COL_SCOPE_NAME] = Value(d.scope_name);
	row[OTLPMetricsHistogramSchema::COL_SCOPE_VERSION] = Value(d.scope_version);
	row[OTLPMetricsHistogramSchema::COL_ATTRIBUTES] = d.attributes;
	row[OTLPMetricsHistogramSchema::COL_COUNT] = Value::UBIGINT(d.count);
	if (d.sum) {
		row[OTLPMetricsHistogramSchema::COL_SUM] = Value::DOUBLE(*d.sum);
	} else {
		row[OTLPMetricsHistogramSchema::COL_SUM] = Value();
	}
	row[OTLPMetricsHistogramSchema::COL_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, d.bucket_counts);
	row[OTLPMetricsHistogramSchema::COL_EXPLICIT_BOUNDS] = Value::LIST(LogicalType::DOUBLE, d.explicit_bounds);
	if (d.min_value) {
		row[OTLPMetricsHistogramSchema::COL_MIN] = Value::DOUBLE(*d.min_value);
	} else {
		row[OTLPMetricsHistogramSchema::COL_MIN] = Value();
	}
	if (d.max_value) {
		row[OTLPMetricsHistogramSchema::COL_MAX] = Value::DOUBLE(*d.max_value);
	} else {
		row[OTLPMetricsHistogramSchema::COL_MAX] = Value();
	}
	return row;
}

vector<Value> BuildMetricsExpHistogramRow(const MetricsExpHistogramData &d) {
	vector<Value> row(OTLPMetricsExpHistogramSchema::COLUMN_COUNT);
	row[OTLPMetricsExpHistogramSchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(d.timestamp);
	row[OTLPMetricsExpHistogramSchema::COL_SERVICE_NAME] = Value(d.service_name);
	row[OTLPMetricsExpHistogramSchema::COL_METRIC_NAME] = Value(d.metric_name);
	row[OTLPMetricsExpHistogramSchema::COL_METRIC_DESCRIPTION] = Value(d.metric_description);
	row[OTLPMetricsExpHistogramSchema::COL_METRIC_UNIT] = Value(d.metric_unit);
	row[OTLPMetricsExpHistogramSchema::COL_RESOURCE_ATTRIBUTES] = d.resource_attributes;
	row[OTLPMetricsExpHistogramSchema::COL_SCOPE_NAME] = Value(d.scope_name);
	row[OTLPMetricsExpHistogramSchema::COL_SCOPE_VERSION] = Value(d.scope_version);
	row[OTLPMetricsExpHistogramSchema::COL_ATTRIBUTES] = d.attributes;
	row[OTLPMetricsExpHistogramSchema::COL_COUNT] = Value::UBIGINT(d.count);
	if (d.sum) {
		row[OTLPMetricsExpHistogramSchema::COL_SUM] = Value::DOUBLE(*d.sum);
	} else {
		row[OTLPMetricsExpHistogramSchema::COL_SUM] = Value();
	}
	row[OTLPMetricsExpHistogramSchema::COL_SCALE] = Value::INTEGER(d.scale);
	row[OTLPMetricsExpHistogramSchema::COL_ZERO_COUNT] = Value::UBIGINT(d.zero_count);
	row[OTLPMetricsExpHistogramSchema::COL_POSITIVE_OFFSET] = Value::INTEGER(d.positive_offset);
	row[OTLPMetricsExpHistogramSchema::COL_POSITIVE_BUCKET_COUNTS] =
	    Value::LIST(LogicalType::UBIGINT, d.positive_bucket_counts);
	row[OTLPMetricsExpHistogramSchema::COL_NEGATIVE_OFFSET] = Value::INTEGER(d.negative_offset);
	row[OTLPMetricsExpHistogramSchema::COL_NEGATIVE_BUCKET_COUNTS] =
	    Value::LIST(LogicalType::UBIGINT, d.negative_bucket_counts);
	if (d.min_value) {
		row[OTLPMetricsExpHistogramSchema::COL_MIN] = Value::DOUBLE(*d.min_value);
	} else {
		row[OTLPMetricsExpHistogramSchema::COL_MIN] = Value();
	}
	if (d.max_value) {
		row[OTLPMetricsExpHistogramSchema::COL_MAX] = Value::DOUBLE(*d.max_value);
	} else {
		row[OTLPMetricsExpHistogramSchema::COL_MAX] = Value();
	}
	return row;
}

vector<Value> BuildMetricsSummaryRow(const MetricsSummaryData &d) {
	vector<Value> row(OTLPMetricsSummarySchema::COLUMN_COUNT);
	row[OTLPMetricsSummarySchema::COL_TIMESTAMP] = Value::TIMESTAMPNS(d.timestamp);
	row[OTLPMetricsSummarySchema::COL_SERVICE_NAME] = Value(d.service_name);
	row[OTLPMetricsSummarySchema::COL_METRIC_NAME] = Value(d.metric_name);
	row[OTLPMetricsSummarySchema::COL_METRIC_DESCRIPTION] = Value(d.metric_description);
	row[OTLPMetricsSummarySchema::COL_METRIC_UNIT] = Value(d.metric_unit);
	row[OTLPMetricsSummarySchema::COL_RESOURCE_ATTRIBUTES] = d.resource_attributes;
	row[OTLPMetricsSummarySchema::COL_SCOPE_NAME] = Value(d.scope_name);
	row[OTLPMetricsSummarySchema::COL_SCOPE_VERSION] = Value(d.scope_version);
	row[OTLPMetricsSummarySchema::COL_ATTRIBUTES] = d.attributes;
	row[OTLPMetricsSummarySchema::COL_COUNT] = Value::UBIGINT(d.count);
	if (d.sum) {
		row[OTLPMetricsSummarySchema::COL_SUM] = Value::DOUBLE(*d.sum);
	} else {
		row[OTLPMetricsSummarySchema::COL_SUM] = Value();
	}
	row[OTLPMetricsSummarySchema::COL_QUANTILE_VALUES] = Value::LIST(LogicalType::DOUBLE, d.quantile_values);
	row[OTLPMetricsSummarySchema::COL_QUANTILE_QUANTILES] = Value::LIST(LogicalType::DOUBLE, d.quantile_quantiles);
	return row;
}

} // namespace duckdb
