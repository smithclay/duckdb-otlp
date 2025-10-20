#include "receiver/row_builders.hpp"
#include "schema/otlp_metrics_union_schema.hpp"

namespace duckdb {

vector<Value> TransformGaugeRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);
	for (idx_t i = 0; i < 9; i++)
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("gauge");
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = row.size() > 9 ? row[9] : Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);
	union_row[OTLPMetricsUnionSchema::COL_COUNT] = Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_SUM] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_SCALE] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_MIN] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_MAX] = Value(LogicalType::DOUBLE);
	return union_row;
}

vector<Value> TransformSumRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);
	for (idx_t i = 0; i < 9; i++)
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("sum");
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = row.size() > 9 ? row[9] : Value();
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = row.size() > 10 ? row[10] : Value();
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = row.size() > 11 ? row[11] : Value(LogicalType::BOOLEAN);
	union_row[OTLPMetricsUnionSchema::COL_COUNT] = Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_SUM] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_SCALE] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_MIN] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_MAX] = Value(LogicalType::DOUBLE);
	return union_row;
}

vector<Value> TransformHistogramRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);
	for (idx_t i = 0; i < 9; i++)
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("histogram");
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = Value();
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value();
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value();
	union_row[OTLPMetricsUnionSchema::COL_COUNT] = row.size() > 9 ? row[9] : Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_SUM] = row.size() > 10 ? row[10] : Value();
	union_row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] =
	    row.size() > 11 ? row[11] : Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] =
	    row.size() > 12 ? row[12] : Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_MIN] = row.size() > 13 ? row[13] : Value();
	union_row[OTLPMetricsUnionSchema::COL_MAX] = row.size() > 14 ? row[14] : Value();
	union_row[OTLPMetricsUnionSchema::COL_SCALE] = Value();
	union_row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value();
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value();
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value();
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] = Value::LIST(LogicalType::DOUBLE, {});
	return union_row;
}

vector<Value> TransformExpHistogramRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);
	for (idx_t i = 0; i < 9; i++)
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("exponential_histogram");
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = Value();
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value();
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value();
	union_row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_COUNT] = row.size() > 9 ? row[9] : Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_SUM] = row.size() > 10 ? row[10] : Value();
	union_row[OTLPMetricsUnionSchema::COL_SCALE] = row.size() > 11 ? row[11] : Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = row.size() > 12 ? row[12] : Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = row.size() > 13 ? row[13] : Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] =
	    row.size() > 14 ? row[14] : Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = row.size() > 15 ? row[15] : Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] =
	    row.size() > 16 ? row[16] : Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_MIN] = row.size() > 17 ? row[17] : Value();
	union_row[OTLPMetricsUnionSchema::COL_MAX] = row.size() > 18 ? row[18] : Value();
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] = Value::LIST(LogicalType::DOUBLE, {});
	return union_row;
}

vector<Value> TransformSummaryRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);
	for (idx_t i = 0; i < 9; i++)
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("summary");
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = Value();
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value();
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value();
	union_row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_SCALE] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_MIN] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_MAX] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_COUNT] = row.size() > 9 ? row[9] : Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_SUM] = row.size() > 10 ? row[10] : Value();
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] =
	    row.size() > 11 ? row[11] : Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] =
	    row.size() > 12 ? row[12] : Value::LIST(LogicalType::DOUBLE, {});
	return union_row;
}

} // namespace duckdb
