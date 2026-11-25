/**
 * @file read_otlp.cpp
 * @brief DuckDB table functions using Rust otlp2records backend
 *
 * This file implements read_otlp_logs, read_otlp_traces, etc. Each is a single-file reader over the
 * Rust otlp2records library, wrapped by DuckDB's multi-file framework.
 */

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/table_function_multi_file.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

#include "otlp_arrow.hpp"
#include "otlp_function_docs.hpp"
#include "otlp_ingest_limits.hpp"

#include "otlp2records.h"

namespace duckdb {

// otlp_otap_decoder_free(nullptr) is a documented no-op.
struct OtapDecoderDeleter {
	void operator()(OtlpOtapDecoder *decoder) const noexcept {
		otlp_otap_decoder_free(decoder);
	}
};
using OtapDecoderPtr = unique_ptr<OtlpOtapDecoder, OtapDecoderDeleter>;

//! What one read_ot* function reads: a signal, and whether its files are OTAP. A function that is
//! registered only to point callers elsewhere carries the error it raises instead.
struct ReadOtlpInfo : public TableFunctionInfo {
	ReadOtlpInfo(OtlpSignalType signal_type_p, bool is_otap_p, string unsupported_p = "")
	    : signal_type(signal_type_p), is_otap(is_otap_p), unsupported(std::move(unsupported_p)) {
	}
	OtlpSignalType signal_type;
	bool is_otap;
	string unsupported;

	//! The signal's columns, resolved on first use and shared by every file of every scan: a multi-file
	//! scan binds once per file, and the schema never depends on the file.
	shared_ptr<const OtlpArrowTable> GetTable(ClientContext &context) {
		lock_guard<mutex> guard(table_lock);
		if (!table) {
			table = shared_ptr<const OtlpArrowTable>(OtlpArrowTable::ForSignal(context, signal_type));
		}
		return table;
	}

private:
	mutex table_lock;
	shared_ptr<const OtlpArrowTable> table;
};

//! Bind data of the single-file reader: one file of a multi-file scan. Globbing, file lists and the
//! multi-file options (filename, hive_partitioning, union_by_name, ...) belong to DuckDB's multi-file
//! framework, which binds this once per file.
struct ReadOtlpFileBindData : public TableFunctionData {
	OpenFileInfo file;
	OtlpSignalType signal_type = OTLP_SIGNAL_LOGS;
	// read_otap_*: decoded with the stateful OTAP decoder; same output schema as OTLP.
	bool is_otap = false;
	shared_ptr<const OtlpArrowTable> table;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<ReadOtlpFileBindData>();
		result->column_ids = column_ids;
		result->file = file;
		result->signal_type = signal_type;
		result->is_otap = is_otap;
		result->table = table;
		return std::move(result);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ReadOtlpFileBindData>();
		return file.path == other.file.path && signal_type == other.signal_type && is_otap == other.is_otap;
	}
};

//! One file's scan. The function has no local state, so the multi-file framework gives each file to
//! a single thread and parallelizes across files.
struct ReadOtlpFileGlobalState : public GlobalTableFunctionState {
	//! The signal columns to convert, and the output column each one fills.
	vector<column_t> arrow_column_ids;
	vector<idx_t> arrow_positions;
	vector<LogicalType> arrow_types;
	//! Output columns that carry file_row_number (a virtual column of the multi-file scan).
	vector<idx_t> row_number_positions;
	bool decoded = false;
	//! The decoded file; null once emitted. Output vectors may reference its buffers (zero-copy) and
	//! hold their own reference, so dropping this one never frees memory in use.
	shared_ptr<ArrowArrayWrapper> batch;
	idx_t row_count = 0;
	idx_t offset = 0;

	idx_t MaxThreads() const override {
		return 1;
	}
};

static unique_ptr<FunctionData> ReadOtlpUnsupportedBind(ClientContext &context, TableFunctionBindInput &input,
                                                        vector<LogicalType> &return_types, vector<Identifier> &names) {
	throw NotImplementedException(input.info->Cast<ReadOtlpInfo>().unsupported);
}

static void ReadOtlpUnsupportedScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	throw InternalException("an unsupported read_otlp_* reader was scanned");
}

static unique_ptr<FunctionData> ReadOtlpFileBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &info = input.info->Cast<ReadOtlpInfo>();
	auto result = make_uniq<ReadOtlpFileBindData>();
	result->file = TableFunctionFileBindInput::GetFile(input);
	result->signal_type = info.signal_type;
	result->is_otap = info.is_otap;
	result->table = info.GetTable(context);
	return_types = result->table->types;
	for (auto &name : result->table->names) {
		names.emplace_back(name);
	}
	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> ReadOtlpFileInitGlobal(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ReadOtlpFileBindData>();
	auto &file_input = TableFunctionFileInitInput::Get(input);
	auto result = make_uniq<ReadOtlpFileGlobalState>();
	for (idx_t position = 0; position < input.column_ids.size(); position++) {
		auto column_id = input.column_ids[position];
		// A multi-file scan projects a virtual column under an index of its own and says which it is.
		if (file_input.virtual_columns) {
			auto entry = file_input.virtual_columns->find(column_id);
			if (entry != file_input.virtual_columns->end()) {
				column_id = entry->second;
			}
		}
		if (column_id == MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER) {
			result->row_number_positions.push_back(position);
			continue;
		}
		if (IsVirtualColumn(column_id)) {
			throw InternalException("read_otlp: unsupported virtual column %llu", static_cast<uint64_t>(column_id));
		}
		result->arrow_column_ids.push_back(column_id);
		result->arrow_positions.push_back(position);
		result->arrow_types.push_back(bind_data.table->types[column_id]);
	}
	return std::move(result);
}

static virtual_column_map_t ReadOtlpFileVirtualColumns(ClientContext &context, optional_ptr<FunctionData> bind_data) {
	virtual_column_map_t result;
	result.insert(make_pair(MultiFileReader::COLUMN_IDENTIFIER_FILE_ROW_NUMBER,
	                        TableColumn("file_row_number", LogicalType::BIGINT)));
	return result;
}

// Takes ownership of the requested shape's array (empty if the shape had no rows) and releases everything else.
static ArrowArray SelectMetricShapeArray(OtlpMetricsArrowBatches &batches, OtlpSignalType signal_type) {
	auto &chosen = OtlpMetricShape(batches, signal_type);
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

// Returned schemas are released here (conversion uses the bind-time types); the caller owns the array.
static ArrowArray DecodeFileToArray(const ReadOtlpFileBindData &bind_data, const uint8_t *input_bytes, idx_t input_len,
                                    const string &path) {
	const bool single_shape = bind_data.signal_type == OTLP_SIGNAL_LOGS || bind_data.signal_type == OTLP_SIGNAL_TRACES;
	ArrowArray array;
	ArrowSchema schema;

	if (bind_data.is_otap) {
		// One decoder per file: a file must be a self-contained message (no cross-file dictionary reuse).
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

	if (single_shape) {
		OtlpStatus status =
		    otlp_transform_with_profile(bind_data.signal_type, OTLP_FORMAT_AUTO, OTLP_ARROW_PROFILE_DUCKDB, input_bytes,
		                                input_len, &array, &schema);
		if (status != OTLP_OK) {
			throw IOException("OTLP parse error on %s: %s", path, otlp_status_message(status));
		}
		if (schema.release) {
			schema.release(&schema);
		}
		return array;
	}
	// Metrics: one parse yields all four shapes.
	OtlpMetricsArrowBatches batches = {};
	OtlpStatus status = otlp_transform_metrics_all_with_profile(OTLP_FORMAT_AUTO, OTLP_ARROW_PROFILE_DUCKDB,
	                                                            input_bytes, input_len, &batches);
	if (status != OTLP_OK) {
		throw IOException("OTLP parse error on %s: %s", path, otlp_status_message(status));
	}
	return SelectMetricShapeArray(batches, bind_data.signal_type);
}

//! Reads and decodes the whole file into one ArrowArray (empty if the file holds no rows of this signal).
static shared_ptr<ArrowArrayWrapper> DecodeFile(ClientContext &context, const ReadOtlpFileBindData &bind_data) {
	auto &path = bind_data.file.path;
	auto &fs = FileSystem::GetFileSystem(context);
	auto handle = fs.OpenFile(bind_data.file, FileOpenFlags::FILE_FLAGS_READ);

	// The whole file is read and decoded into one in-memory ArrowArray, hence the size cap.
	auto file_size = handle->GetFileSize();
	constexpr idx_t MAX_FILE_SIZE = otlp_limits::MAX_READ_FILE_BYTES;
	if (file_size > MAX_FILE_SIZE) {
		throw IOException("File %s is too large (%llu bytes). Maximum supported size is 100MB.", path,
		                  static_cast<uint64_t>(file_size));
	}
	string buffer;
	buffer.resize(file_size);
	auto bytes_read = handle->Read(&buffer[0], file_size);
	if (bytes_read != static_cast<idx_t>(file_size)) {
		throw IOException("Short read on file %s: expected %lld bytes, got %llu", path, static_cast<int64_t>(file_size),
		                  static_cast<uint64_t>(bytes_read));
	}

	ArrowArray array =
	    DecodeFileToArray(bind_data, reinterpret_cast<const uint8_t *>(buffer.data()), buffer.size(), path);
	if (array.length < 0) {
		if (array.release) {
			array.release(&array);
		}
		throw IOException("Invalid Arrow batch: negative row count");
	}
	if (!array.release) {
		return nullptr;
	}
	if (array.length == 0) {
		array.release(&array);
		return nullptr;
	}
	return OtlpOwnArrowBatch(array);
}

static void ReadOtlpFileScan(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->Cast<ReadOtlpFileBindData>();
	auto &gstate = data.global_state->Cast<ReadOtlpFileGlobalState>();
	if (!gstate.decoded) {
		gstate.batch = DecodeFile(context, bind_data);
		gstate.row_count = gstate.batch ? static_cast<idx_t>(gstate.batch->arrow_array.length) : 0;
		gstate.decoded = true;
	}
	if (!gstate.batch) {
		output.SetChildCardinality(0);
		return;
	}
	const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, gstate.row_count - gstate.offset);
	// Projection pushdown only skips conversion: the FFI has no column mask, so Rust builds every column.
	if (gstate.row_number_positions.empty()) {
		OtlpArrowScan(context, *bind_data.table, gstate.batch, gstate.arrow_column_ids, gstate.offset, count, output);
	} else {
		DataChunk arrow_chunk;
		arrow_chunk.Initialize(Allocator::Get(context), gstate.arrow_types);
		OtlpArrowScan(context, *bind_data.table, gstate.batch, gstate.arrow_column_ids, gstate.offset, count,
		              arrow_chunk);
		for (idx_t i = 0; i < gstate.arrow_positions.size(); i++) {
			output.data[gstate.arrow_positions[i]].Reference(arrow_chunk.data[i]);
		}
		for (auto position : gstate.row_number_positions) {
			output.data[position].Sequence(static_cast<int64_t>(gstate.offset), 1, count);
		}
		output.SetChildCardinality(count);
	}
	gstate.offset += count;
	if (gstate.offset >= gstate.row_count) {
		gstate.batch.reset();
	}
}

static double ReadOtlpFileProgress(ClientContext &context, const FunctionData *bind_data,
                                   const GlobalTableFunctionState *global_state) {
	if (!global_state) {
		return 0;
	}
	auto &gstate = global_state->Cast<ReadOtlpFileGlobalState>();
	if (!gstate.decoded) {
		return 0;
	}
	if (gstate.row_count == 0) {
		return 100;
	}
	return 100.0 * static_cast<double>(gstate.offset) / static_cast<double>(gstate.row_count);
}

//! read_<signal>(path) over one file, wrapped by DuckDB's multi-file framework: `path` may be a glob,
//! a list of files, or a file struct, and every multi-file option is accepted.
static TableFunctionSet ReadOtlpFunctionSet(const string &name, shared_ptr<ReadOtlpInfo> info) {
	const bool is_otap = info->is_otap;
	TableFunction single_file(Identifier(name + "_file"),
	                          FunctionSignature().AddPositionalOnly("file", LogicalType::ANY), ReadOtlpFileScan,
	                          ReadOtlpFileBind, ReadOtlpFileInitGlobal);
	single_file.projection_pushdown = true;
	single_file.filter_pushdown = false;
	single_file.table_scan_progress = ReadOtlpFileProgress;
	single_file.get_virtual_columns = ReadOtlpFileVirtualColumns;
	single_file.function_info = std::move(info);
	TableFunctionMultiFileSettings settings;
	settings.reader_type = is_otap ? "OTAP" : "OTLP";
	return TableFunctionMultiFileWrapper::CreateFunctionSet(std::move(single_file), Identifier(name),
	                                                        std::move(settings));
}

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
	//! One description per overload of the multi-file set: a path or glob, a list of them, or a file struct.
	auto overload_docs = [](const string &description, const vector<string> &examples, const char *protocol) {
		vector<FunctionDescription> result;
		const pair<LogicalType, const char *> overloads[] = {{LogicalType::VARCHAR, "path"},
		                                                     {LogicalType::LIST(LogicalType::ANY), "paths"},
		                                                     {LogicalType::VARIANT(), "path"}};
		for (auto &overload : overloads) {
			result.push_back(
			    OtlpDoc({overload.first}, {overload.second}, description, examples, {"opentelemetry", protocol}));
		}
		return result;
	};
	auto register_reader = [&](const string &name, shared_ptr<ReadOtlpInfo> info, const string &description,
	                           const vector<string> &examples, const char *protocol) {
		loader.RegisterFunction(
		    OtlpDocumented(ReadOtlpFunctionSet(name, std::move(info)), overload_docs(description, examples, protocol)));
	};
	//! Registered only to raise and point at the working readers.
	auto register_unsupported = [&](const string &name, shared_ptr<ReadOtlpInfo> info, string description) {
		TableFunction func(Identifier(name), FunctionSignature().AddPositionalOnly("path", LogicalType::VARCHAR),
		                   ReadOtlpUnsupportedScan, ReadOtlpUnsupportedBind);
		func.function_info = std::move(info);
		loader.RegisterFunction(OtlpDocumented(
		    std::move(func),
		    {OtlpDoc({LogicalType::VARCHAR}, {"path"}, std::move(description), {}, {"opentelemetry", "otlp"})}));
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
		common += " `path` is a file, a glob, or a list of them, resolved through DuckDB's file systems (local, S3, "
		          "HTTP(S), Azure, GCS); the multi-file options (filename, hive_partitioning, union_by_name, ...) "
		          "apply.";
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

	// Registered only to raise and point at the working readers; no examples, since every call raises.
	register_unsupported("read_otlp_metrics",
	                     make_shared_ptr<ReadOtlpInfo>(OTLP_SIGNAL_METRICS_GAUGE, false,
	                                                   "read_otlp_metrics() is not supported yet because OTLP metrics "
	                                                   "have multiple shape-specific schemas. Use "
	                                                   "read_otlp_metrics_gauge(), read_otlp_metrics_sum(), "
	                                                   "read_otlp_metrics_histogram(), or "
	                                                   "read_otlp_metrics_exp_histogram()."),
	                     "Not implemented: every call raises. OTLP metrics have shape-specific schemas, so read one of "
	                     "read_otlp_metrics_gauge, read_otlp_metrics_sum, read_otlp_metrics_histogram or "
	                     "read_otlp_metrics_exp_histogram instead.");
	register_unsupported(
	    "read_otlp_metrics_summary",
	    make_shared_ptr<ReadOtlpInfo>(OTLP_SIGNAL_METRICS_GAUGE, false,
	                                  "Summary metrics not yet supported. Use read_otlp_metrics_gauge() "
	                                  "or read_otlp_metrics_sum() instead."),
	    "Not implemented: every call raises. OTLP summary data points are not decoded; the other "
	    "read_otlp_metrics_* readers count them as skipped.");
}

} // namespace duckdb
