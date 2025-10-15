#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "table/otlp_columnar_scan.hpp"

namespace duckdb {

unique_ptr<GlobalTableFunctionState> OTLPColumnarScanInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind = input.bind_data->Cast<OTLPColumnarScanBindData>();
	auto state = make_uniq<OTLPColumnarScanState>();
	if (bind.buffer) {
		state->snapshot = bind.buffer->Snapshot();
	}
	// Projection mapping
	if (input.projection_ids.empty()) {
		for (idx_t i = 0; i < input.column_ids.size(); i++)
			state->out_to_base.push_back(input.column_ids[i]);
	} else {
		for (auto proj : input.projection_ids) {
			if (proj < input.column_ids.size())
				state->out_to_base.push_back(input.column_ids[proj]);
		}
	}
	// Filters
	if (input.filters)
		state->filters = input.filters->Copy();
	// Precompute timestamp bounds from filters on column 0
	if (state->filters) {
		// detect service/metric equals filters by column name
		auto &bind = input.bind_data->Cast<OTLPColumnarScanBindData>();
		idx_t svc_idx = DConstants::INVALID_INDEX;
		idx_t met_idx = DConstants::INVALID_INDEX;
		for (idx_t i = 0; i < bind.column_names.size(); i++) {
			if (bind.column_names[i] == "ServiceName")
				svc_idx = i;
			else if (bind.column_names[i] == "MetricName")
				met_idx = i;
		}
		for (auto &kv : state->filters->filters) {
			if (kv.first != 0)
				continue;
			auto &f = *kv.second;
			if (f.filter_type == TableFilterType::CONSTANT_COMPARISON) {
				auto &cf = f.Cast<ConstantFilter>();
				int64_t cmp_us = 0;
				if (cf.constant.type().id() == LogicalTypeId::TIMESTAMP ||
				    cf.constant.type().id() == LogicalTypeId::TIMESTAMP_NS) {
					auto ts = cf.constant.GetValue<timestamp_t>();
					cmp_us = Timestamp::GetEpochMicroSeconds(ts);
				} else if (cf.constant.type().id() == LogicalTypeId::BIGINT) {
					cmp_us = cf.constant.GetValue<int64_t>();
				} else {
					continue;
				}
				switch (cf.comparison_type) {
				case ExpressionType::COMPARE_GREATERTHAN:
				case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
					state->ts_min_us = state->ts_min_us ? std::max<int64_t>(*state->ts_min_us, cmp_us)
					                                    : std::optional<int64_t>(cmp_us);
					break;
				case ExpressionType::COMPARE_LESSTHAN:
				case ExpressionType::COMPARE_LESSTHANOREQUALTO:
					state->ts_max_us = state->ts_max_us ? std::min<int64_t>(*state->ts_max_us, cmp_us)
					                                    : std::optional<int64_t>(cmp_us);
					break;
				case ExpressionType::COMPARE_EQUAL:
					state->ts_min_us = state->ts_min_us ? std::max<int64_t>(*state->ts_min_us, cmp_us)
					                                    : std::optional<int64_t>(cmp_us);
					state->ts_max_us = state->ts_max_us ? std::min<int64_t>(*state->ts_max_us, cmp_us)
					                                    : std::optional<int64_t>(cmp_us);
					break;
				default:
					break;
				}
			}
		}
		// extract service equals
		if (svc_idx != DConstants::INVALID_INDEX) {
			auto it = state->filters->filters.find(svc_idx);
			if (it != state->filters->filters.end() &&
			    it->second->filter_type == TableFilterType::CONSTANT_COMPARISON) {
				auto &cf = it->second->Cast<ConstantFilter>();
				if (cf.comparison_type == ExpressionType::COMPARE_EQUAL &&
				    cf.constant.type().id() == LogicalTypeId::VARCHAR) {
					state->service_eq = cf.constant.GetValue<string>();
				}
			}
		}
		// extract metric equals
		if (met_idx != DConstants::INVALID_INDEX) {
			auto it = state->filters->filters.find(met_idx);
			if (it != state->filters->filters.end() &&
			    it->second->filter_type == TableFilterType::CONSTANT_COMPARISON) {
				auto &cf = it->second->Cast<ConstantFilter>();
				if (cf.comparison_type == ExpressionType::COMPARE_EQUAL &&
				    cf.constant.type().id() == LogicalTypeId::VARCHAR) {
					state->metric_eq = cf.constant.GetValue<string>();
				}
			}
		}
	}
	return std::move(state);
}

unique_ptr<LocalTableFunctionState> OTLPColumnarScanInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                              GlobalTableFunctionState *global_state) {
	auto &g = global_state->Cast<OTLPColumnarScanState>();
	auto l = make_uniq<OTLPColumnarLocalState>();
	// assign first chunk lazily in scan function
	l->chunk_idx = DConstants::INVALID_INDEX;
	l->row_offset = 0;
	return std::move(l);
}

void OTLPColumnarScanFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &g = data.global_state->Cast<OTLPColumnarScanState>();
	auto &l = data.local_state->Cast<OTLPColumnarLocalState>();

	auto row_passes_filters = [&](const DataChunk &chunk, idx_t row) -> bool {
		if (!g.filters)
			return true;
		for (auto &entry : g.filters->filters) {
			auto col = entry.first;
			auto &filter = *entry.second;
			auto &vec = chunk.data[col];
			auto val = vec.GetValue(row);
			if (val.IsNull()) {
				if (filter.filter_type != TableFilterType::IS_NULL)
					return false;
				continue;
			}
			if (filter.filter_type == TableFilterType::CONSTANT_COMPARISON) {
				auto &cf = filter.Cast<ConstantFilter>();
				if (!cf.Compare(val))
					return false;
			} else {
				// unsupported filter kinds fall back to true
				continue;
			}
		}
		return true;
	};

	auto fetch_next_chunk = [&]() -> bool {
		while (true) {
			auto idx = g.next_chunk.fetch_add(1);
			if (idx >= g.snapshot.size())
				return false;
			auto &schunk = g.snapshot[idx];
			// chunk skipping based on timestamp bounds
			bool intersects = true;
			if (g.ts_min_us && schunk->ts_max_us < *g.ts_min_us)
				intersects = false;
			if (g.ts_max_us && schunk->ts_min_us > *g.ts_max_us)
				intersects = false;
			// service/metric skipping
			if (intersects && g.service_eq) {
				if (schunk->svc_has && !schunk->svc_mixed && schunk->svc_value != *g.service_eq)
					intersects = false;
			}
			if (intersects && g.metric_eq) {
				if (schunk->met_has && !schunk->met_mixed && schunk->met_value != *g.metric_eq)
					intersects = false;
			}
			if (!intersects)
				continue;
			l.chunk_idx = idx;
			l.row_offset = 0;
			l.sel_matches.clear();
			l.sel_count = 0;
			l.sel_pos = 0;
			return true;
		}
	};

	if (l.chunk_idx == DConstants::INVALID_INDEX) {
		if (!fetch_next_chunk()) {
			output.SetCardinality(0);
			return;
		}
	}

	idx_t produced = 0;
	while (produced < STANDARD_VECTOR_SIZE) {
		if (l.chunk_idx >= g.snapshot.size())
			break;
		auto &schunk = g.snapshot[l.chunk_idx];
		auto &chunk = *schunk->chunk;
		if (l.row_offset >= schunk->size) {
			if (!fetch_next_chunk())
				break;
			continue;
		}

		// Fast path: zero-copy slice when no filters
		if (!g.filters) {
			idx_t to_copy = MinValue<idx_t>(STANDARD_VECTOR_SIZE - produced, schunk->size - l.row_offset);
			for (idx_t out_c = 0; out_c < g.out_to_base.size(); out_c++) {
				auto base_c = g.out_to_base[out_c];
				output.data[out_c].Slice(chunk.data[base_c], l.row_offset, l.row_offset + to_copy);
			}
			produced += to_copy;
			l.row_offset += to_copy;
			if (l.row_offset >= schunk->size) {
				if (!fetch_next_chunk())
					break;
			}
			continue;
		}

		// Filtered path: build selection vector once per chunk, then slice
		if (l.sel_count == 0 && l.sel_pos == 0) {
			// initial selection is the full chunk
			idx_t count = schunk->size;
			SelectionVector base_sel(count);
			for (idx_t i = 0; i < count; i++)
				base_sel.set_index(i, i);

			// apply timestamp bounds if present to reduce work early
			const SelectionVector *current_sel_ptr = &base_sel;
			SelectionVector tmp_sel1(count), tmp_sel2(count);
			idx_t current_count = count;

			if (g.ts_min_us) {
				Vector bound(LogicalType::TIMESTAMP_NS);
				auto ts_min_ns = Timestamp::TimestampNsFromEpochMicros(*g.ts_min_us);
				bound.Reference(Value::TIMESTAMPNS(ts_min_ns));
				auto &ts_vec = chunk.data[0];
				auto true_count = VectorOperations::GreaterThanEquals(
				    const_cast<Vector &>(ts_vec), bound, current_sel_ptr, current_count, &tmp_sel1, nullptr, nullptr);
				current_sel_ptr = &tmp_sel1;
				current_count = true_count;
			}
			if (g.ts_max_us && current_count > 0) {
				Vector bound(LogicalType::TIMESTAMP_NS);
				auto ts_max_ns = Timestamp::TimestampNsFromEpochMicros(*g.ts_max_us);
				bound.Reference(Value::TIMESTAMPNS(ts_max_ns));
				auto &ts_vec = chunk.data[0];
				auto true_count = VectorOperations::LessThanEquals(const_cast<Vector &>(ts_vec), bound, current_sel_ptr,
				                                                   current_count, &tmp_sel2, nullptr, nullptr);
				current_sel_ptr = &tmp_sel2;
				current_count = true_count;
			}

			// If there are additional pushed filters, fall back to per-row check using current selection
			// Build sel_matches array
			l.sel_matches.clear();
			l.sel_matches.reserve(current_count);
			if (current_count == 0) {
				l.sel_count = 0;
			} else if (g.filters && (g.filters->filters.size() > 1 || (!g.ts_min_us && !g.ts_max_us))) {
				// need per-row validation for non-timestamp filters
				for (idx_t i = 0; i < current_count; i++) {
					auto row_idx = current_sel_ptr->get_index(i);
					if (row_passes_filters(chunk, row_idx)) {
						l.sel_matches.push_back(UnsafeNumericCast<sel_t>(row_idx));
					}
				}
				l.sel_count = l.sel_matches.size();
			} else {
				// only timestamp filters were applied: take selection as-is
				for (idx_t i = 0; i < current_count; i++) {
					l.sel_matches.push_back(current_sel_ptr->get_index(i));
				}
				l.sel_count = current_count;
			}
			l.sel_pos = 0;
		}

		if (l.sel_pos >= l.sel_count) {
			if (!fetch_next_chunk())
				break;
			continue;
		}

		idx_t remaining_out = STANDARD_VECTOR_SIZE - produced;
		idx_t remaining_sel = l.sel_count - l.sel_pos;
		idx_t to_emit = MinValue<idx_t>(remaining_out, remaining_sel);
		// create a selection vector view over our matches subrange
		SelectionVector out_sel(&l.sel_matches[l.sel_pos]);
		for (idx_t out_c = 0; out_c < g.out_to_base.size(); out_c++) {
			auto base_c = g.out_to_base[out_c];
			output.data[out_c].Reference(chunk.data[base_c]);
			output.data[out_c].Slice(out_sel, to_emit);
		}
		produced += to_emit;
		l.sel_pos += to_emit;
	}

	output.SetCardinality(produced);
}

} // namespace duckdb
