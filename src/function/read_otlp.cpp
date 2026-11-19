/**
 * @file read_otlp.cpp
 * @brief DuckDB table functions using Rust otlp2records backend
 *
 * This file implements read_otlp_logs, read_otlp_traces, etc.
 * table functions that use the Rust otlp2records library for parsing.
 */

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "otlp_arrow.hpp"
#include "otlp_function_docs.hpp"
#include "otlp_ingest_limits.hpp"

// Include the Rust FFI header
#include "otlp2records.h"

namespace duckdb {

// ============================================================================
// Bind Data
// ============================================================================

// OTAP decoder handle freed via RAII. otlp_otap_decoder_free(nullptr) is a
// documented no-op, so this is safe even if construction left a null handle.
struct OtapDecoderDeleter {
	void operator()(OtlpOtapDecoder *decoder) const noexcept {
		otlp_otap_decoder_free(decoder);
	}
};
using OtapDecoderPtr = unique_ptr<OtlpOtapDecoder, OtapDecoderDeleter>;

struct ReadOTLPRustBindData : public TableFunctionData {
	vector<string> files;
	OtlpSignalType signal_type;
	OtlpInputFormat format;
	// True for the read_otap_* functions: input is the OpenTelemetry Arrow
	// Protocol (canonical BatchArrowRecords), decoded via the stateful OTAP
	// decoder FFI instead of the one-shot OTLP transform. Output schema is
	// identical (OTAP normalizes to the same flattened records).
	bool is_otap = false;

	// Column types resolved once at bind time; every decoded batch is converted with them.
	unique_ptr<OtlpArrowTable> table;
};

// ============================================================================
// Global State
// ============================================================================

struct ReadOTLPRustGlobalState : public GlobalTableFunctionState {
	mutex lock;
	atomic<idx_t> next_file {0};
	// Number of files in the glob; one file is handed to each thread via the atomic
	// next_file cursor, so the scan parallelizes across files (one file per task).
	idx_t file_count = 1;

	idx_t MaxThreads() const override {
		// One file per thread: each thread pulls a distinct file via next_file and
		// keeps fully isolated local state (file_buffer/current_batch). bind_data
		// (schema) is read-only, so multi-file globs scan in parallel. NOTE: parallelism
		// is per-file, so a glob that resolves to a SINGLE file (the common case for one
		// path argument) is processed single-threaded — there is no intra-file split.
		return MaxValue<idx_t>(1, file_count);
	}
};

// ============================================================================
// Local State
// ============================================================================

struct ReadOTLPRustLocalState : public LocalTableFunctionState {
	//! The decoded file being emitted; null between files. Output vectors may reference its buffers
	//! (zero-copy) and hold their own reference, so dropping this one never frees memory in use.
	shared_ptr<ArrowArrayWrapper> current_batch;
	idx_t batch_offset = 0;
	string file_buffer;
	vector<column_t> column_ids;
};

// ============================================================================
// Bind Function
// ============================================================================

//! What one read_ot* function reads: a signal, and whether its files are OTAP. A function that is
//! registered only to point callers elsewhere carries the error it raises instead.
struct ReadOtlpInfo : public TableFunctionInfo {
	ReadOtlpInfo(OtlpSignalType signal_type_p, bool is_otap_p, string unsupported_p = "")
	    : signal_type(signal_type_p), is_otap(is_otap_p), unsupported(std::move(unsupported_p)) {
	}
	OtlpSignalType signal_type;
	bool is_otap;
	string unsupported;
};

static unique_ptr<FunctionData> ReadOTLPRustBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &info = input.info->Cast<ReadOtlpInfo>();
	if (!info.unsupported.empty()) {
		throw NotImplementedException(info.unsupported);
	}
	auto result = make_uniq<ReadOTLPRustBindData>();
	auto file_pattern = input.inputs[0].GetValue<string>();
	auto matches = FileSystem::GetFileSystem(context).Glob(file_pattern);
	if (matches.empty()) {
		throw IOException("No files found that match the pattern \"%s\"", file_pattern);
	}
	for (auto &match : matches) {
		result->files.emplace_back(match.path);
	}
	result->signal_type = info.signal_type;
	result->format = OTLP_FORMAT_AUTO;
	result->is_otap = info.is_otap;
	result->table = OtlpArrowTable::ForSignal(context, info.signal_type);
	return_types = result->table->types;
	for (auto &name : result->table->names) {
		names.emplace_back(name);
	}
	return std::move(result);
}

// ============================================================================
// Init Functions
// ============================================================================

static unique_ptr<GlobalTableFunctionState> ReadOTLPRustInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto result = make_uniq<ReadOTLPRustGlobalState>();
	auto &bind_data = input.bind_data->Cast<ReadOTLPRustBindData>();
	result->file_count = bind_data.files.size();
	return std::move(result);
}

static unique_ptr<LocalTableFunctionState> ReadOTLPRustInitLocal(ExecutionContext &context,
                                                                 TableFunctionInitInput &input,
                                                                 GlobalTableFunctionState *global_state) {
	auto result = make_uniq<ReadOTLPRustLocalState>();
	result->column_ids = input.column_ids;
	return std::move(result);
}

// ============================================================================
// Decode helpers
// ============================================================================

// Pick the requested metric shape's array out of a fully-populated batch set:
// release the chosen shape's schema (conversion uses the bind-time column
// types) and every other present batch, returning the chosen array (a no-op
// empty array if that shape had no rows). Shared by the OTLP and OTAP metric
// paths, which both yield an OtlpMetricsArrowBatches from one parse/decode.
static ArrowArray SelectMetricShapeArray(OtlpMetricsArrowBatches &batches, OtlpSignalType signal_type) {
	auto &chosen = OtlpMetricShape(batches, signal_type);
	// No rows for this shape: a no-op empty array.
	ArrowArray array = {};
	if (chosen.present) {
		if (chosen.schema.release) {
			chosen.schema.release(&chosen.schema);
		}
		array = chosen.array;
		chosen.present = 0; // now owned by the caller, so the release below skips it
	}
	ReleaseOtlpMetricBatches(batches);
	return array;
}

// Decode one fully-buffered file into a single Arrow array for the bound signal,
// dispatching to the stateful OTAP decoder or the one-shot OTLP transform, both in
// the DuckDb Arrow profile. Any schema returned alongside the array is released
// here (conversion uses the bind-time column types); the caller owns and must
// release the returned array.
static ArrowArray DecodeFileToArray(const ReadOTLPRustBindData &bind_data, const uint8_t *input_bytes, idx_t input_len,
                                    const string &path) {
	const bool single_shape = bind_data.signal_type == OTLP_SIGNAL_LOGS || bind_data.signal_type == OTLP_SIGNAL_TRACES;
	ArrowArray array;
	ArrowSchema schema;

	if (bind_data.is_otap) {
		// OTAP is stateful, but each file is one self-contained BatchArrowRecords
		// message, so one decoder + one decode call per file. (A stream split
		// across multiple files with cross-message dictionary reuse is not
		// supported by this one-decoder-per-file model.)
		OtapDecoderPtr decoder(otlp_otap_decoder_new_with_profile(OTLP_ARROW_PROFILE_DUCKDB));
		if (!decoder) {
			throw IOException("Failed to allocate OTAP decoder for %s", path);
		}
		if (single_shape) {
			OtlpStatus status = bind_data.signal_type == OTLP_SIGNAL_LOGS
			                        ? otlp_otap_decode_logs(decoder.get(), input_bytes, input_len, &array, &schema)
			                        : otlp_otap_decode_traces(decoder.get(), input_bytes, input_len, &array, &schema);
			if (status != OTLP_OK) {
				throw IOException("OTAP decode error on %s: %s", path, otlp_status_message(status));
			}
			if (schema.release) {
				schema.release(&schema);
			}
			return array;
		}
		OtlpMetricsArrowBatches batches = {};
		OtlpStatus status = otlp_otap_decode_metrics(decoder.get(), input_bytes, input_len, &batches);
		if (status != OTLP_OK) {
			// On failure no batch is present; nothing to release.
			throw IOException("OTAP decode error on %s: %s", path, otlp_status_message(status));
		}
		return SelectMetricShapeArray(batches, bind_data.signal_type);
	}

	// OTLP: protobuf/JSON/NDJSON, auto-detected by the Rust backend.
	if (single_shape) {
		// Logs/Traces have a single shape; otlp_transform is the canonical verb for them.
		OtlpStatus status =
		    otlp_transform_with_profile(bind_data.signal_type, bind_data.format, OTLP_ARROW_PROFILE_DUCKDB, input_bytes,
		                                input_len, &array, &schema);
		if (status != OTLP_OK) {
			throw IOException("OTLP parse error on %s: %s", path, otlp_status_message(status));
		}
		// Column types were resolved at bind time; release this copy and keep only the array.
		if (schema.release) {
			schema.release(&schema);
		}
		return array;
	}
	// Metrics: one parse yields all four shapes (otlp_transform_metrics_all is the
	// single canonical metric verb). SelectMetricShapeArray keeps the requested
	// shape and releases the rest.
	OtlpMetricsArrowBatches batches = {};
	OtlpStatus status = otlp_transform_metrics_all_with_profile(bind_data.format, OTLP_ARROW_PROFILE_DUCKDB,
	                                                            input_bytes, input_len, &batches);
	if (status != OTLP_OK) {
		// On failure no batch is present; nothing to release.
		throw IOException("OTLP parse error on %s: %s", path, otlp_status_message(status));
	}
	return SelectMetricShapeArray(batches, bind_data.signal_type);
}

// ============================================================================
// Scan Function
// ============================================================================

static void ReadOTLPRustScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<ReadOTLPRustBindData>();
	auto &gstate = data.global_state->Cast<ReadOTLPRustGlobalState>();
	auto &lstate = data.local_state->Cast<ReadOTLPRustLocalState>();
	auto &fs = FileSystem::GetFileSystem(context);

	while (true) {
		if (lstate.current_batch) {
			const auto row_count = static_cast<idx_t>(lstate.current_batch->arrow_array.length);
			const auto remaining = row_count - lstate.batch_offset;
			const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, remaining);
			// Projection pushdown only prunes the Arrow->DuckDB conversion here: the Rust transform
			// already parsed the payload and built every column of the batch (the FFI has no
			// column mask), so unselected columns are materialized in Rust regardless. Skipping
			// their conversion is the smaller half of the per-column cost; a column-aware transform
			// is the lever for the larger half and waits on the streaming backend.
			OtlpArrowScan(context, *bind_data.table, lstate.current_batch, lstate.column_ids, lstate.batch_offset,
			              count, output);
			lstate.batch_offset += count;
			if (lstate.batch_offset >= row_count) {
				lstate.current_batch.reset();
				lstate.batch_offset = 0;
			}
			return;
		}

		// Get next file
		idx_t file_idx = gstate.next_file.fetch_add(1);
		if (file_idx >= bind_data.files.size()) {
			// No more files
			output.SetChildCardinality(0);
			return;
		}

		// Read file via DuckDB FS
		auto &path = bind_data.files[file_idx];
		auto handle = fs.OpenFile(path, FileOpenFlags::FILE_FLAGS_READ);

		// Whole-file materialization: the entire file is read into file_buffer and the whole
		// file is transformed into a single ArrowArray held in memory at once (input is capped
		// at otlp_limits::MAX_READ_FILE_BYTES below). This is the prototype approach; a
		// streaming/chunked reader that incrementally parses and yields batches is the intended
		// future direction.
		auto file_size = handle->GetFileSize();

		// Cap the whole-file read for safety. This bounds an entire file materialized at once and
		// is intentionally distinct from the live server's per-request body cap
		// (otlp_limits::DEFAULT_MAX_BODY_BYTES); see otlp_ingest_limits.hpp for why the two differ.
		constexpr idx_t MAX_FILE_SIZE = otlp_limits::MAX_READ_FILE_BYTES;
		if (file_size > MAX_FILE_SIZE) {
			throw IOException("File %s is too large (%llu bytes). Maximum supported size is 100MB.", path,
			                  static_cast<uint64_t>(file_size));
		}

		lstate.file_buffer.resize(file_size);
		auto bytes_read = handle->Read(lstate.file_buffer.data(), file_size);
		if (bytes_read != static_cast<idx_t>(file_size)) {
			throw IOException("Short read on file %s: expected %lld bytes, got %llu", path.c_str(),
			                  static_cast<int64_t>(file_size), static_cast<uint64_t>(bytes_read));
		}

		const auto *input_bytes = reinterpret_cast<const uint8_t *>(lstate.file_buffer.data());
		const auto input_len = lstate.file_buffer.size();
		ArrowArray array = DecodeFileToArray(bind_data, input_bytes, input_len, path);

		if (array.length < 0) {
			if (array.release) {
				array.release(&array);
			}
			throw IOException("Invalid Arrow batch: negative row count");
		}
		if (array.release && array.length > 0) {
			lstate.current_batch = OtlpOwnArrowBatch(array);
			lstate.batch_offset = 0;
			continue;
		}
		if (array.release) {
			array.release(&array);
		}
	}
}

// ============================================================================
// Registration
// ============================================================================

void RegisterReadOTLPRustFunctions(ExtensionLoader &loader) {
	//! Per signal, in OTLP_SIGNALS order: what a row is, the column count, and any detail worth naming.
	struct SignalDoc {
		const char *rows;
		int columns;
		const char *detail;
	};
	const SignalDoc docs[] = {
	    {"log records", 18, ""},
	    {"trace spans", 24, ", including events, links and a computed duration"},
	    {"gauge data points", 17, ""},
	    {"sum (counter) data points", 19, " with aggregation_temporality and is_monotonic"},
	    {"explicit-bucket histogram data points", 22, " with bucket bounds and counts"},
	    {"exponential histogram data points", 27, " with scale, zero bucket and positive/negative buckets"},
	};
	auto register_reader = [&](const string &name, shared_ptr<ReadOtlpInfo> info, string description,
	                           vector<string> examples, const char *protocol) {
		TableFunction func(name.c_str(), {LogicalType::VARCHAR}, ReadOTLPRustScan, ReadOTLPRustBind,
		                   ReadOTLPRustInitGlobal, ReadOTLPRustInitLocal);
		func.projection_pushdown = true;
		func.filter_pushdown = false;
		func.function_info = std::move(info);
		loader.RegisterFunction(
		    OtlpDocumented(std::move(func), {OtlpDoc({LogicalType::VARCHAR}, {"path"}, std::move(description),
		                                             std::move(examples), {"opentelemetry", protocol})}));
	};
	for (auto signal_type : OTLP_SIGNALS) {
		const auto &doc = docs[signal_type];
		const string table = OTLP_SIGNAL_TABLES[signal_type];
		const string otlp_name = "read_" + table;
		const string otap_name = "read_otap_" + table.substr(strlen("otlp_"));
		const bool metrics = signal_type >= OTLP_SIGNAL_METRICS_GAUGE;
		string common;
		if (metrics) {
			common += " One file can hold several metric shapes; this reader takes its own and skips the rest, so "
			          "the four metric readers all read the same file.";
		}
		common += " `path` is a single file or a glob, resolved through DuckDB's file systems (local, S3, HTTP(S), "
		          "Azure, GCS).";
		const string file = metrics ? "metrics" : table.substr(strlen("otlp_"));
		register_reader(otlp_name, make_shared_ptr<ReadOtlpInfo>(signal_type, false),
		                StringUtil::Format("Read OpenTelemetry %s from OTLP JSON, NDJSON or protobuf files into a "
		                                   "flat %d-column table%s.%s",
		                                   doc.rows, doc.columns, doc.detail, common),
		                {StringUtil::Format("SELECT * FROM %s('%s.pb');", otlp_name, file)}, "otlp");
		register_reader(otap_name, make_shared_ptr<ReadOtlpInfo>(signal_type, true),
		                StringUtil::Format("Read OpenTelemetry %s from an OTAP (OpenTelemetry Arrow Protocol) "
		                                   "BatchArrowRecords file, returning the same %d columns as %s.%s",
		                                   doc.rows, doc.columns, otlp_name, common),
		                {StringUtil::Format("SELECT * FROM %s('%s.bar');", otap_name, file)}, "otap");
	}

	// Registered only to raise, and documented so a caller who finds them in duckdb_functions() is
	// pointed at the readers that work. No examples, since every call raises.
	register_reader("read_otlp_metrics",
	                make_shared_ptr<ReadOtlpInfo>(OTLP_SIGNAL_METRICS_GAUGE, false,
	                                              "read_otlp_metrics() is not supported yet because OTLP metrics "
	                                              "have multiple shape-specific schemas. Use "
	                                              "read_otlp_metrics_gauge(), read_otlp_metrics_sum(), "
	                                              "read_otlp_metrics_histogram(), or "
	                                              "read_otlp_metrics_exp_histogram()."),
	                "Not implemented: every call raises. OTLP metrics have shape-specific schemas, so read one of "
	                "read_otlp_metrics_gauge, read_otlp_metrics_sum, read_otlp_metrics_histogram or "
	                "read_otlp_metrics_exp_histogram instead.",
	                {}, "otlp");
	register_reader("read_otlp_metrics_summary",
	                make_shared_ptr<ReadOtlpInfo>(OTLP_SIGNAL_METRICS_GAUGE, false,
	                                              "Summary metrics not yet supported. Use read_otlp_metrics_gauge() "
	                                              "or read_otlp_metrics_sum() instead."),
	                "Not implemented: every call raises. OTLP summary data points are not decoded; the other "
	                "read_otlp_metrics_* readers count them as skipped.",
	                {}, "otlp");
}

} // namespace duckdb
