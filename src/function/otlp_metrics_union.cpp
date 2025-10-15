#include "function/otlp_metrics_union.hpp"
#include "catalog/otlp_catalog.hpp"
#include "storage/otlp_storage_info.hpp"
#include "schema/otlp_metrics_schemas.hpp"
#include "schema/otlp_metrics_union_schema.hpp"

namespace duckdb {

struct MetricsUnionBindData : public TableFunctionData {
	string catalog_name;
};

struct MetricsUnionGlobalState : public GlobalTableFunctionState {
	shared_ptr<OTLPStorageInfo> storage;
	// per-type snapshot of chunks and indices
	vector<shared_ptr<const ColumnarStoredChunk>> snaps[5];
	idx_t chunk_idx[5] {0, 0, 0, 0, 0};
	idx_t row_offset[5] {0, 0, 0, 0, 0};
	int current_type = 0; // 0=gauge,1=sum,2=hist,3=exp_hist,4=summary

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> MetricsUnionBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	if (input.inputs.size() != 1) {
		throw BinderException("otlp_metrics_union requires exactly one argument: catalog name (e.g., 'live')");
	}
	auto catalog_name = input.inputs[0].ToString();
	auto &cat = Catalog::GetCatalog(context, catalog_name);
	auto &otlp = cat.Cast<OTLPCatalog>();

	auto bind = make_uniq<MetricsUnionBindData>();
	bind->catalog_name = catalog_name;

	// Return union schema
	names = OTLPMetricsUnionSchema::GetColumnNames();
	return_types = OTLPMetricsUnionSchema::GetColumnTypes();
	return std::move(bind);
}

static unique_ptr<GlobalTableFunctionState> MetricsUnionInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<MetricsUnionBindData>();
	auto &cat = Catalog::GetCatalog(context, bind.catalog_name);
	auto &otlp = cat.Cast<OTLPCatalog>();

	auto state = make_uniq<MetricsUnionGlobalState>();
	// Access storage info (expose accessor on catalog)
	state->storage = otlp.GetStorageInfo();
	if (!state->storage) {
		throw BinderException("otlp_metrics_union: catalog '%s' is not an OTLP catalog", bind.catalog_name);
	}
	// Take snapshots in a fixed order
	state->snaps[0] = state->storage->metrics_gauge_buffer->Snapshot();
	state->snaps[1] = state->storage->metrics_sum_buffer->Snapshot();
	state->snaps[2] = state->storage->metrics_histogram_buffer->Snapshot();
	state->snaps[3] = state->storage->metrics_exp_histogram_buffer->Snapshot();
	state->snaps[4] = state->storage->metrics_summary_buffer->Snapshot();
	return std::move(state);
}

static void EmitNullsForRange(DataChunk &out, idx_t row, idx_t start_col, idx_t end_col) {
	for (idx_t c = start_col; c <= end_col; c++) {
		FlatVector::SetNull(out.data[c], row, true);
	}
}

static void MetricsUnionScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &st = data.global_state->Cast<MetricsUnionGlobalState>();

	idx_t produced = 0;
	output.SetCardinality(0);

	auto emit_row = [&](const ColumnarStoredChunk &src, idx_t src_row, int type) {
		// Base columns 0..8: Timestamp, ServiceName, MetricName, MetricDescription, MetricUnit,
		// ResourceAttributes, ScopeName, ScopeVersion, Attributes
		// Copy via GetValue -> SetValue for correctness; this path is not the ingest hot path
		for (idx_t c = 0; c < 9; c++) {
			output.data[c].SetValue(produced, src.chunk->data[c].GetValue(src_row));
		}
		// MetricType
		static const char *kinds[] = {"gauge", "sum", "histogram", "exp_histogram", "summary"};
		output.data[OTLPMetricsUnionSchema::COL_METRIC_TYPE].SetValue(produced, Value(kinds[type]));

		// Fill type-specific
		// Initialize all optional/type-specific columns to NULL
		for (idx_t c = 10; c < OTLPMetricsUnionSchema::COLUMN_COUNT; c++) {
			FlatVector::SetNull(output.data[c], produced, true);
		}

		switch (type) {
		case 0: { // gauge
			// Value
			output.data[OTLPMetricsUnionSchema::COL_VALUE].SetValue(
			    produced, src.chunk->data[OTLPMetricsGaugeSchema::COL_VALUE].GetValue(src_row));
			break;
		}
		case 1: { // sum
			output.data[OTLPMetricsUnionSchema::COL_VALUE].SetValue(
			    produced, src.chunk->data[OTLPMetricsSumSchema::COL_VALUE].GetValue(src_row));
			// AggregationTemporality: cast to INTEGER if needed
			auto v = src.chunk->data[OTLPMetricsSumSchema::COL_AGGREGATION_TEMPORALITY].GetValue(src_row);
			if (v.type().id() == LogicalTypeId::INTEGER) {
				output.data[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY].SetValue(produced, v);
			} else {
				// try parse int from string
				int32_t ival = 0;
				if (v.type().id() == LogicalTypeId::VARCHAR) {
					auto s = v.ToString();
					try {
						ival = stoi(s);
					} catch (...) {
						ival = 0;
					}
				}
				output.data[OTLPMetricsUnionSchema::COL_AGGREGATION_TEMPORALITY].SetValue(produced,
				                                                                          Value::INTEGER(ival));
			}
			output.data[OTLPMetricsUnionSchema::COL_IS_MONOTONIC].SetValue(
			    produced, src.chunk->data[OTLPMetricsSumSchema::COL_IS_MONOTONIC].GetValue(src_row));
			break;
		}
		case 2: { // histogram
			output.data[OTLPMetricsUnionSchema::COL_COUNT].SetValue(
			    produced, src.chunk->data[OTLPMetricsHistogramSchema::COL_COUNT].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_SUM].SetValue(
			    produced, src.chunk->data[OTLPMetricsHistogramSchema::COL_SUM].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_BUCKET_COUNTS].SetValue(
			    produced, src.chunk->data[OTLPMetricsHistogramSchema::COL_BUCKET_COUNTS].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_EXPLICIT_BOUNDS].SetValue(
			    produced, src.chunk->data[OTLPMetricsHistogramSchema::COL_EXPLICIT_BOUNDS].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_MIN].SetValue(
			    produced, src.chunk->data[OTLPMetricsHistogramSchema::COL_MIN].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_MAX].SetValue(
			    produced, src.chunk->data[OTLPMetricsHistogramSchema::COL_MAX].GetValue(src_row));
			break;
		}
		case 3: { // exp_histogram
			output.data[OTLPMetricsUnionSchema::COL_COUNT].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_COUNT].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_SUM].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_SUM].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_SCALE].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_SCALE].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_ZERO_COUNT].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_ZERO_COUNT].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_POSITIVE_OFFSET].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_POSITIVE_OFFSET].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_POSITIVE_BUCKET_COUNTS].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_POSITIVE_BUCKET_COUNTS].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_NEGATIVE_OFFSET].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_NEGATIVE_OFFSET].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_NEGATIVE_BUCKET_COUNTS].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_NEGATIVE_BUCKET_COUNTS].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_MIN].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_MIN].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_MAX].SetValue(
			    produced, src.chunk->data[OTLPMetricsExpHistogramSchema::COL_MAX].GetValue(src_row));
			break;
		}
		case 4: { // summary
			output.data[OTLPMetricsUnionSchema::COL_COUNT].SetValue(
			    produced, src.chunk->data[OTLPMetricsSummarySchema::COL_COUNT].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_SUM].SetValue(
			    produced, src.chunk->data[OTLPMetricsSummarySchema::COL_SUM].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_QUANTILE_VALUES].SetValue(
			    produced, src.chunk->data[OTLPMetricsSummarySchema::COL_QUANTILE_VALUES].GetValue(src_row));
			output.data[OTLPMetricsUnionSchema::COL_QUANTILE_QUANTILES].SetValue(
			    produced, src.chunk->data[OTLPMetricsSummarySchema::COL_QUANTILE_QUANTILES].GetValue(src_row));
			break;
		}
		}
		produced++;
	};

	while (produced < STANDARD_VECTOR_SIZE && st.current_type < 5) {
		auto t = st.current_type;
		if (st.chunk_idx[t] >= st.snaps[t].size()) {
			st.current_type++;
			continue;
		}
		auto &schunk = *st.snaps[t][st.chunk_idx[t]];
		if (st.row_offset[t] >= schunk.size) {
			st.chunk_idx[t]++;
			st.row_offset[t] = 0;
			continue;
		}
		emit_row(schunk, st.row_offset[t], t);
		st.row_offset[t]++;
	}

	output.SetCardinality(produced);
}

TableFunction GetOTLPMetricsUnionFunction() {
	TableFunction tf("otlp_metrics_union", {LogicalType::VARCHAR}, MetricsUnionScan, MetricsUnionBind,
	                 MetricsUnionInit);
	tf.projection_pushdown = false;
	tf.filter_pushdown = false;
	return tf;
}

} // namespace duckdb
