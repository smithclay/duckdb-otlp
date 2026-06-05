#include "otlp_start_stop.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "otlp_server.hpp"
#include "otlp_storage.hpp"
#include "otlp_uri.hpp"

namespace duckdb {

static LogicalType OtlpVarcharType() {
	return LogicalType(LogicalTypeId::VARCHAR);
}

static LogicalType OtlpBooleanType() {
	return LogicalType(LogicalTypeId::BOOLEAN);
}

static LogicalType OtlpUSmallIntType() {
	return LogicalType(LogicalTypeId::USMALLINT);
}

static LogicalType OtlpUBigIntType() {
	return LogicalType(LogicalTypeId::UBIGINT);
}

static LogicalType OtlpBigIntType() {
	return LogicalType(LogicalTypeId::BIGINT);
}

struct OtlpStartStopFunctionData : public TableFunctionData {
	// These lifecycle functions are side-effecting control-plane calls that also
	// return a status row. DuckDB may rescan table functions, so `finished` makes
	// server creation/stop safe under rescan instead of repeating the side effect.
	bool finished = false;
	OtlpUri listen_uri;
	OtlpServerConfig config;
};

static unique_ptr<FunctionData> OtlpServeBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
#ifdef __EMSCRIPTEN__
	throw NotImplementedException("otlp_serve is not implemented for the wasm platform");
#endif

	auto bind_data = make_uniq<OtlpStartStopFunctionData>();
	string listen_uri = "otlp:localhost:4318";
	if (!input.inputs.empty()) {
		auto &uri_value = input.inputs[0];
		if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
			throw InvalidInputException("Invalid OTLP listen URI specified");
		}
		listen_uri = uri_value.GetValue<string>();
	}

	bind_data->listen_uri = OtlpUri(listen_uri);

	auto allow_other_hostname = input.named_parameters.find("allow_other_hostname") != input.named_parameters.end() &&
	                            input.named_parameters["allow_other_hostname"].GetValue<bool>();
	if (!allow_other_hostname && !bind_data->listen_uri.IsLocal()) {
		throw InvalidInputException(
		    "Only localhost is allowed as an OTLP hostname by default; set allow_other_hostname=true to override");
	}

	if (input.named_parameters.find("token") != input.named_parameters.end()) {
		bind_data->config.token = input.named_parameters["token"].GetValue<string>();
	} else {
		bind_data->config.token = OtlpServer::GenerateRandomToken(*context.db);
	}
	OtlpServer::ValidateToken(bind_data->config.token);

	if (input.named_parameters.find("catalog") != input.named_parameters.end()) {
		// Empty catalog = the connection's default catalog. A non-empty value targets
		// an attached writable catalog (e.g. DuckLake or Iceberg) for lakehouse ingest.
		bind_data->config.catalog_name = input.named_parameters["catalog"].GetValue<string>();
	}
	if (input.named_parameters.find("schema") != input.named_parameters.end()) {
		bind_data->config.schema_name = input.named_parameters["schema"].GetValue<string>();
		if (bind_data->config.schema_name.empty()) {
			throw InvalidInputException("schema must not be empty");
		}
	}
	if (input.named_parameters.find("parquet_export_path") != input.named_parameters.end()) {
		bind_data->config.parquet_export_path = input.named_parameters["parquet_export_path"].GetValue<string>();
		if (bind_data->config.parquet_export_path.empty()) {
			throw InvalidInputException("parquet_export_path must not be empty");
		}
		// Parquet export is a standalone durable store (the Parquet files), not a mirror of
		// a catalog table: it keeps no local table copy and is at-least-once. Combining it
		// with a catalog target would be ambiguous, so the two are mutually exclusive.
		if (!bind_data->config.catalog_name.empty()) {
			throw InvalidInputException(
			    "parquet_export_path cannot be combined with a catalog target; use a catalog "
			    "mode for transactional ingest or parquet_export_path for plain Parquet export");
		}
	}
	if (input.named_parameters.find("create_tables") != input.named_parameters.end()) {
		bind_data->config.create_tables = input.named_parameters["create_tables"].GetValue<bool>();
	}
	if (input.named_parameters.find("max_body_bytes") != input.named_parameters.end()) {
		bind_data->config.max_body_bytes = input.named_parameters["max_body_bytes"].GetValue<idx_t>();
		if (bind_data->config.max_body_bytes == 0) {
			throw InvalidInputException("max_body_bytes must be greater than zero");
		}
	}
	if (input.named_parameters.find("http_threads") != input.named_parameters.end()) {
		bind_data->config.http_threads = input.named_parameters["http_threads"].GetValue<idx_t>();
		if (bind_data->config.http_threads == 0) {
			throw InvalidInputException("http_threads must be greater than zero");
		}
	}
	if (input.named_parameters.find("max_buffered_bytes") != input.named_parameters.end()) {
		bind_data->config.max_buffered_bytes = input.named_parameters["max_buffered_bytes"].GetValue<idx_t>();
		if (bind_data->config.max_buffered_bytes == 0) {
			throw InvalidInputException("max_buffered_bytes must be greater than zero");
		}
	}

	names.emplace_back("listen_uri");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("listen_url");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("auth_token");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("schema_name");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("logs_table");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("traces_table");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("metrics_gauge_table");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("metrics_sum_table");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("metrics_histogram_table");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("metrics_exp_histogram_table");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("catalog_name");
	return_types.emplace_back(OtlpVarcharType());

	return std::move(bind_data);
}

static void OtlpServe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<OtlpStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}

	auto &state = OtlpStorageExtensionInfo::GetState(*context.db);
	state.CreateServer(context, bind_data.listen_uri, bind_data.config);
	// Mark finished as soon as the server exists: if any SetValue below throws, a
	// re-scan must not try to create the (already running) server again.
	bind_data.finished = true;

	output.SetValue(0, 0, bind_data.listen_uri.Uri());
	output.SetValue(1, 0, bind_data.listen_uri.Http());
	output.SetValue(2, 0, bind_data.config.token);
	output.SetValue(3, 0, bind_data.config.schema_name);
	output.SetValue(4, 0, "otlp_logs");
	output.SetValue(5, 0, "otlp_traces");
	output.SetValue(6, 0, "otlp_metrics_gauge");
	output.SetValue(7, 0, "otlp_metrics_sum");
	output.SetValue(8, 0, "otlp_metrics_histogram");
	output.SetValue(9, 0, "otlp_metrics_exp_histogram");
	output.SetValue(10, 0, bind_data.config.catalog_name);
	output.SetCardinality(1);
}

TableFunctionSet OtlpServeFunction::GetFunction() {
	TableFunctionSet set("otlp_serve");
	auto fun = TableFunction("otlp_serve", {OtlpVarcharType()}, OtlpServe, OtlpServeBind);
	fun.named_parameters["token"] = OtlpVarcharType();
	fun.named_parameters["catalog"] = OtlpVarcharType();
	fun.named_parameters["schema"] = OtlpVarcharType();
	fun.named_parameters["parquet_export_path"] = OtlpVarcharType();
	fun.named_parameters["create_tables"] = OtlpBooleanType();
	fun.named_parameters["allow_other_hostname"] = OtlpBooleanType();
	fun.named_parameters["max_body_bytes"] = OtlpUBigIntType();
	fun.named_parameters["http_threads"] = OtlpUBigIntType();
	fun.named_parameters["max_buffered_bytes"] = OtlpUBigIntType();
	set.AddFunction(fun);
	fun.arguments.clear();
	set.AddFunction(fun);
	return set;
}

static unique_ptr<FunctionData> OtlpStopBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<OtlpStartStopFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid OTLP listen URI specified");
	}
	bind_data->listen_uri = OtlpUri(uri_value.GetValue<string>());
	names.emplace_back("status");
	return_types.emplace_back(OtlpVarcharType());
	return std::move(bind_data);
}

static void OtlpStop(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<OtlpStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}
	auto &state = OtlpStorageExtensionInfo::GetState(*context.db);
	if (state.StopServer(context, bind_data.listen_uri)) {
		output.SetValue(0, 0, StringUtil::Format("Stopped listening on %s", bind_data.listen_uri.Uri()));
	} else {
		output.SetValue(0, 0, StringUtil::Format("No server found listening on %s", bind_data.listen_uri.Uri()));
	}
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction OtlpStopFunction::GetFunction() {
	return TableFunction("otlp_stop", {OtlpVarcharType()}, OtlpStop, OtlpStopBind);
}

struct OtlpServerListFunctionData : public TableFunctionData {
	bool initialized = false;
	idx_t offset = 0;
	vector<OtlpStorageExtensionInfo::ServerSnapshot> snapshots;
};

static unique_ptr<FunctionData> OtlpServerListBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	names.emplace_back("listen_uri");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("listen_url");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("host");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("port");
	return_types.emplace_back(OtlpUSmallIntType());
	names.emplace_back("schema_name");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("active_requests");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("total_requests");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("total_rows");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("is_listening");
	return_types.emplace_back(OtlpBooleanType());
	names.emplace_back("last_error");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("catalog_name");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("buffered_rows");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("admitted_bytes");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("oldest_buffered_age_ms");
	return_types.emplace_back(OtlpBigIntType());
	names.emplace_back("last_seal_age_ms");
	return_types.emplace_back(OtlpBigIntType());
	names.emplace_back("seals_total");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("committed_rows_total");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("seal_failures_total");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("seal_last_error");
	return_types.emplace_back(OtlpVarcharType());
	return make_uniq<OtlpServerListFunctionData>();
}

static void OtlpServerList(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<OtlpServerListFunctionData>();
	// Snapshot once on the first call, then page out STANDARD_VECTOR_SIZE rows per
	// scan so a large server count never overflows a single DataChunk.
	if (!bind_data.initialized) {
		bind_data.snapshots = OtlpStorageExtensionInfo::GetState(*context.db).ListServers();
		bind_data.initialized = true;
	}
	idx_t row = 0;
	while (bind_data.offset < bind_data.snapshots.size() && row < STANDARD_VECTOR_SIZE) {
		auto &s = bind_data.snapshots[bind_data.offset];
		output.SetValue(0, row, Value(s.listen_uri));
		output.SetValue(1, row, Value(s.listen_url));
		output.SetValue(2, row, Value(s.host));
		output.SetValue(3, row, Value::USMALLINT(s.port));
		output.SetValue(4, row, Value(s.schema_name));
		output.SetValue(5, row, Value::UBIGINT(s.active_requests));
		output.SetValue(6, row, Value::UBIGINT(s.total_requests));
		output.SetValue(7, row, Value::UBIGINT(s.total_rows));
		output.SetValue(8, row, Value::BOOLEAN(s.is_listening));
		output.SetValue(9, row, s.last_error.empty() ? Value(LogicalType::VARCHAR) : Value(s.last_error));
		output.SetValue(10, row, Value(s.catalog_name));
		output.SetValue(11, row, Value::UBIGINT(s.buffered_rows));
		output.SetValue(12, row, Value::UBIGINT(s.admitted_bytes));
		output.SetValue(13, row,
		                s.oldest_buffered_age_ms < 0 ? Value(LogicalType::BIGINT)
		                                             : Value::BIGINT(s.oldest_buffered_age_ms));
		output.SetValue(14, row,
		                s.last_seal_age_ms < 0 ? Value(LogicalType::BIGINT) : Value::BIGINT(s.last_seal_age_ms));
		output.SetValue(15, row, Value::UBIGINT(s.seals_total));
		output.SetValue(16, row, Value::UBIGINT(s.committed_rows_total));
		output.SetValue(17, row, Value::UBIGINT(s.seal_failures_total));
		output.SetValue(18, row, s.seal_last_error.empty() ? Value(LogicalType::VARCHAR) : Value(s.seal_last_error));
		row++;
		bind_data.offset++;
	}
	output.SetCardinality(row);
}

TableFunction OtlpServerListFunction::GetFunction() {
	return TableFunction("otlp_server_list", {}, OtlpServerList, OtlpServerListBind);
}

struct OtlpSealListFunctionData : public TableFunctionData {
	bool initialized = false;
	idx_t offset = 0;
	vector<OtlpStorageExtensionInfo::SealSnapshot> snapshots;
};

static unique_ptr<FunctionData> OtlpSealListBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<string> &names) {
	names = {"listen_uri",  "seal_sequence",       "started_unix_ms",      "completed_unix_ms",
	         "duration_ms", "rows_committed",      "admitted_bytes",       "success",
	         "seals_total", "seal_failures_total", "committed_rows_total", "error"};
	return_types = {OtlpVarcharType(), OtlpUBigIntType(), OtlpBigIntType(),  OtlpBigIntType(),
	                OtlpBigIntType(),  OtlpUBigIntType(), OtlpUBigIntType(), OtlpBooleanType(),
	                OtlpUBigIntType(), OtlpUBigIntType(), OtlpUBigIntType(), OtlpVarcharType()};
	return make_uniq<OtlpSealListFunctionData>();
}

static void OtlpSealList(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<OtlpSealListFunctionData>();
	if (!bind_data.initialized) {
		bind_data.snapshots = OtlpStorageExtensionInfo::GetState(*context.db).ListSeals();
		bind_data.initialized = true;
	}
	idx_t row = 0;
	while (bind_data.offset < bind_data.snapshots.size() && row < STANDARD_VECTOR_SIZE) {
		auto &snapshot = bind_data.snapshots[bind_data.offset];
		auto &event = snapshot.event;
		output.SetValue(0, row, Value(snapshot.listen_uri));
		output.SetValue(1, row, Value::UBIGINT(event.seal_sequence));
		output.SetValue(2, row, Value::BIGINT(event.started_unix_ms));
		output.SetValue(3, row, Value::BIGINT(event.completed_unix_ms));
		output.SetValue(4, row, Value::BIGINT(event.duration_ms));
		output.SetValue(5, row, Value::UBIGINT(event.rows_committed));
		output.SetValue(6, row, Value::UBIGINT(event.admitted_bytes_committed));
		output.SetValue(7, row, Value::BOOLEAN(event.success));
		output.SetValue(8, row, Value::UBIGINT(event.seals_total));
		output.SetValue(9, row, Value::UBIGINT(event.seal_failures_total));
		output.SetValue(10, row, Value::UBIGINT(event.committed_rows_total));
		output.SetValue(11, row, event.error.empty() ? Value(LogicalType::VARCHAR) : Value(event.error));
		row++;
		bind_data.offset++;
	}
	output.SetCardinality(row);
}

TableFunction OtlpSealListFunction::GetFunction() {
	return TableFunction("otlp_seal_list", {}, OtlpSealList, OtlpSealListBind);
}

struct OtlpFlushFunctionData : public TableFunctionData {
	// See OtlpStartStopFunctionData: otlp_flush is a side-effecting table function
	// that returns a row, and the guard makes DuckDB rescan safe.
	bool finished = false;
	OtlpUri listen_uri;
};

static unique_ptr<FunctionData> OtlpFlushBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
	auto bind_data = make_uniq<OtlpFlushFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid OTLP listen URI specified");
	}
	bind_data->listen_uri = OtlpUri(uri_value.GetValue<string>());
	names.emplace_back("status");
	return_types.emplace_back(OtlpVarcharType());
	names.emplace_back("sealed_rows");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("seals_total");
	return_types.emplace_back(OtlpUBigIntType());
	names.emplace_back("error");
	return_types.emplace_back(OtlpVarcharType());
	return std::move(bind_data);
}

static void OtlpFlush(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<OtlpFlushFunctionData>();
	if (bind_data.finished) {
		return;
	}
	auto &state = OtlpStorageExtensionInfo::GetState(*context.db);
	auto result = state.FlushServer(bind_data.listen_uri);
	if (!result.found) {
		output.SetValue(0, 0, StringUtil::Format("No server found listening on %s", bind_data.listen_uri.Uri()));
		output.SetValue(1, 0, Value::UBIGINT(0));
		output.SetValue(2, 0, Value::UBIGINT(0));
		output.SetValue(3, 0, Value(LogicalType::VARCHAR));
	} else {
		output.SetValue(0, 0, result.error.empty() ? Value("sealed") : Value("error"));
		output.SetValue(1, 0, Value::UBIGINT(result.sealed_rows));
		output.SetValue(2, 0, Value::UBIGINT(result.seals_total));
		output.SetValue(3, 0, result.error.empty() ? Value(LogicalType::VARCHAR) : Value(result.error));
	}
	output.SetCardinality(1);
	bind_data.finished = true;
}

TableFunction OtlpFlushFunction::GetFunction() {
	return TableFunction("otlp_flush", {OtlpVarcharType()}, OtlpFlush, OtlpFlushBind);
}

} // namespace duckdb
