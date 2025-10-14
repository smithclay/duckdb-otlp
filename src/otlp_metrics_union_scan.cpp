#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "ring_buffer.hpp"
#include "otlp_metrics_union_schema.hpp"
#include "otlp_metrics_union_scan.hpp"

namespace duckdb {

//! Transform a gauge row to union schema (27 columns)
static vector<Value> TransformGaugeRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);

	// Copy base columns (0-8)
	for (idx_t i = 0; i < 9; i++) {
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	}

	// Add MetricType discriminator
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("gauge");

	// Gauge-specific: Value
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = row.size() > 9 ? row[9] : Value(LogicalType::DOUBLE);

	// NULL for other type-specific columns
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

//! Transform a sum row to union schema (27 columns)
static vector<Value> TransformSumRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);

	// Copy base columns (0-8)
	for (idx_t i = 0; i < 9; i++) {
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	}

	// Add MetricType discriminator
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("sum");

	// Sum-specific: Value, AggregationTemporality, IsMonotonic
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = row.size() > 9 ? row[9] : Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] =
	    row.size() > 10 ? row[10] : Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = row.size() > 11 ? row[11] : Value(LogicalType::BOOLEAN);

	// NULL for histogram/exp_histogram/summary columns
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

//! Transform a histogram row to union schema (27 columns)
static vector<Value> TransformHistogramRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);

	// Copy base columns (0-8)
	for (idx_t i = 0; i < 9; i++) {
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	}

	// Add MetricType discriminator
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("histogram");

	// NULL for gauge/sum specific
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);

	// Histogram-specific: Count, Sum, BucketCounts, ExplicitBounds, Min, Max
	union_row[OTLPMetricsUnionSchema::COL_COUNT] = row.size() > 9 ? row[9] : Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_SUM] = row.size() > 10 ? row[10] : Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] =
	    row.size() > 11 ? row[11] : Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] =
	    row.size() > 12 ? row[12] : Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_MIN] = row.size() > 13 ? row[13] : Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_MAX] = row.size() > 14 ? row[14] : Value(LogicalType::DOUBLE);

	// NULL for exp_histogram/summary columns
	union_row[OTLPMetricsUnionSchema::COL_SCALE] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] = Value::LIST(LogicalType::DOUBLE, {});

	return union_row;
}

//! Transform an exponential histogram row to union schema (27 columns)
static vector<Value> TransformExpHistogramRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);

	// Copy base columns (0-8)
	for (idx_t i = 0; i < 9; i++) {
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	}

	// Add MetricType discriminator
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("exponential_histogram");

	// NULL for gauge/sum/histogram specific
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);
	union_row[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS] = Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS] = Value::LIST(LogicalType::DOUBLE, {});

	// Exp histogram-specific: Count, Sum, Scale, ZeroCount, PositiveOffset, PositiveBucketCounts, NegativeOffset,
	// NegativeBucketCounts, Min, Max
	union_row[OTLPMetricsUnionSchema::COL_COUNT] = row.size() > 9 ? row[9] : Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_SUM] = row.size() > 10 ? row[10] : Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_SCALE] = row.size() > 11 ? row[11] : Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_ZERO_COUNT] = row.size() > 12 ? row[12] : Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET] = row.size() > 13 ? row[13] : Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS] =
	    row.size() > 14 ? row[14] : Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET] = row.size() > 15 ? row[15] : Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS] =
	    row.size() > 16 ? row[16] : Value::LIST(LogicalType::UBIGINT, {});
	union_row[OTLPMetricsUnionSchema::COL_MIN] = row.size() > 17 ? row[17] : Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_MAX] = row.size() > 18 ? row[18] : Value(LogicalType::DOUBLE);

	// NULL for summary columns
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] = Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] = Value::LIST(LogicalType::DOUBLE, {});

	return union_row;
}

//! Transform a summary row to union schema (27 columns)
static vector<Value> TransformSummaryRow(const vector<Value> &row) {
	vector<Value> union_row(OTLPMetricsUnionSchema::COLUMN_COUNT);

	// Copy base columns (0-8)
	for (idx_t i = 0; i < 9; i++) {
		union_row[i] = i < row.size() ? row[i] : Value(LogicalType::VARCHAR);
	}

	// Add MetricType discriminator
	union_row[OTLPMetricsUnionSchema::COL_METRIC_TYPE] = Value("summary");

	// NULL for gauge/sum/histogram/exp_histogram specific
	union_row[OTLPMetricsUnionSchema::COL_VALUE] = Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY] = Value(LogicalType::INTEGER);
	union_row[OTLPMetricsUnionSchema::COL_IS_MONOTONIC] = Value(LogicalType::BOOLEAN);
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

	// Summary-specific: Count, Sum, QuantileValues, QuantileQuantiles
	union_row[OTLPMetricsUnionSchema::COL_COUNT] = row.size() > 9 ? row[9] : Value(LogicalType::UBIGINT);
	union_row[OTLPMetricsUnionSchema::COL_SUM] = row.size() > 10 ? row[10] : Value(LogicalType::DOUBLE);
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES] =
	    row.size() > 11 ? row[11] : Value::LIST(LogicalType::DOUBLE, {});
	union_row[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES] =
	    row.size() > 12 ? row[12] : Value::LIST(LogicalType::DOUBLE, {});

	return union_row;
}

//! Init global state - read all rows from all 5 metric buffers and transform to union schema
unique_ptr<GlobalTableFunctionState> OTLPMetricsUnionScanInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<OTLPMetricsUnionScanBindData>();
	auto state = make_uniq<OTLPMetricsUnionScanState>();

	// Order: gauge, sum, histogram, exp_histogram, summary
	for (idx_t i = 0; i < bind_data.buffers.size(); i++) {
		auto &buffer = bind_data.buffers[i];
		auto rows = buffer->ReadAll();

		// Transform each row to union schema
		for (auto &row : rows) {
			vector<Value> transformed_row;
			switch (i) {
			case 0: // gauge
				transformed_row = TransformGaugeRow(row);
				break;
			case 1: // sum
				transformed_row = TransformSumRow(row);
				break;
			case 2: // histogram
				transformed_row = TransformHistogramRow(row);
				break;
			case 3: // exponential_histogram
				transformed_row = TransformExpHistogramRow(row);
				break;
			case 4: // summary
				transformed_row = TransformSummaryRow(row);
				break;
			}
			state->rows.push_back(std::move(transformed_row));
		}
	}

	return std::move(state);
}

//! Scan function - return rows
void OTLPMetricsUnionScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &state = data.global_state->Cast<OTLPMetricsUnionScanState>();

	idx_t count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, state.rows.size() - state.current_row);

	if (count == 0) {
		output.SetCardinality(0);
		return;
	}

	// Each row is a vector<Value> with 27 columns
	// Copy values from rows into output DataChunk columns
	idx_t column_count = output.ColumnCount();

	for (idx_t col_idx = 0; col_idx < column_count; col_idx++) {
		auto &vec = output.data[col_idx];

		for (idx_t row_idx = 0; row_idx < count; row_idx++) {
			auto &row = state.rows[state.current_row + row_idx];

			// Safety check: ensure row has enough columns
			if (col_idx < row.size()) {
				// Copy Value to vector at position row_idx
				auto &value = row[col_idx];
				vec.SetValue(row_idx, value);
			} else {
				// Missing column - set to NULL
				FlatVector::SetNull(vec, row_idx, true);
			}
		}
	}

	state.current_row += count;
	output.SetCardinality(count);
}

} // namespace duckdb
