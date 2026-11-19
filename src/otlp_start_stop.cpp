#include "otlp_start_stop.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "otlp_arrow.hpp"
#include "otlp_server.hpp"
#include "otlp_storage.hpp"
#include "otlp_uri.hpp"

namespace duckdb {

struct OtlpStartStopFunctionData : public TableFunctionData {
	// These lifecycle functions are side-effecting control-plane calls that also
	// return a status row. DuckDB may rescan table functions, so `finished` makes
	// server creation/stop safe under rescan instead of repeating the side effect.
	bool finished = false;
	OtlpUri listen_uri;
	vector<OtlpListenerSpec> listeners;
	OtlpServerConfig config;
};

//! Bound on listeners per server; each needs one row of the otlp_serve result chunk.
static constexpr idx_t MAX_LISTENERS_PER_SERVER = 64;

//! A VARCHAR or VARCHAR[] argument as a list of strings (a scalar is a one-element list).
static vector<string> StringOrStringList(const Value &value, const char *what) {
	if (value.IsNull()) {
		throw InvalidInputException("%s must not be NULL", what);
	}
	vector<string> result;
	if (value.type().id() == LogicalTypeId::LIST) {
		for (auto &child : ListValue::GetChildren(value)) {
			if (child.IsNull()) {
				throw InvalidInputException("%s must not contain NULL", what);
			}
			result.push_back(child.GetValue<string>());
		}
	} else {
		result.push_back(value.GetValue<string>());
	}
	return result;
}

//! otlp_serve/otap_serve options that set one OtlpServerConfig field. Numeric ones must be > 0.
struct ServeStringOption {
	const char *name;
	string OtlpServerConfig::*field;
	bool required_non_empty;
};
static const ServeStringOption STRING_OPTIONS[] = {
    {"token", &OtlpServerConfig::token, false},
    // Empty = the connection's default catalog; otherwise an attached writable catalog.
    {"catalog", &OtlpServerConfig::catalog_name, false},
    {"schema", &OtlpServerConfig::schema_name, true},
    {"parquet_export_path", &OtlpServerConfig::parquet_export_path, true},
};
struct ServeBoolOption {
	const char *name;
	bool OtlpServerConfig::*field;
};
static const ServeBoolOption BOOL_OPTIONS[] = {
    // Opt-in anonymous ingest (no bearer/x-api-key check).
    {"disable_auth", &OtlpServerConfig::disable_auth},
    // Parquet mode: create the inspection view over the exported files.
    {"parquet_views", &OtlpServerConfig::parquet_views},
    {"create_tables", &OtlpServerConfig::create_tables},
};
struct ServeNumericOption {
	const char *name;
	idx_t OtlpServerConfig::*unsigned_field;
	int64_t OtlpServerConfig::*signed_field;
};
static const ServeNumericOption NUMERIC_OPTIONS[] = {
    {"max_body_bytes", &OtlpServerConfig::max_body_bytes, nullptr},
    // Sizes the HTTP worker pool; accepted but unused by gRPC, whose runtime sizes itself.
    {"http_threads", &OtlpServerConfig::http_threads, nullptr},
    {"max_buffered_bytes", &OtlpServerConfig::max_buffered_bytes, nullptr},
    {"seal_target_bytes", &OtlpServerConfig::seal_target_bytes, nullptr},
    {"seal_max_age_ms", nullptr, &OtlpServerConfig::seal_max_age_ms},
    {"target_file_size", &OtlpServerConfig::target_file_size, nullptr},
    {"maintenance_retention_ms", nullptr, &OtlpServerConfig::maintenance_retention_ms},
    {"maintenance_interval_ms", nullptr, &OtlpServerConfig::maintenance_interval_ms},
};
//! Comma-separated resource / scope attribute keys to promote into columns.
static const std::pair<const char *, vector<string> OtlpPromoteConfig::*> PROMOTE_OPTIONS[] = {
    {"promote_resource_attributes", &OtlpPromoteConfig::resource_keys},
    {"promote_scope_attributes", &OtlpPromoteConfig::scope_keys},
};

static unique_ptr<FunctionData> OtlpServeBindImpl(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<Identifier> &names,
                                                  const string &default_uri, const string &required_scheme) {
#ifdef __EMSCRIPTEN__
	throw NotImplementedException("live OTLP ingest is not implemented for the wasm platform");
#endif

	auto bind_data = make_uniq<OtlpStartStopFunctionData>();
	const char *function_name = required_scheme == "otap" ? "otap_serve" : "otlp_serve";
	vector<string> listen_uris {default_uri};
	if (!input.inputs.empty()) {
		listen_uris = StringOrStringList(input.inputs[0], "listen URI");
	}
	if (listen_uris.empty()) {
		throw InvalidInputException("%s requires at least one listen URI", function_name);
	}
	if (listen_uris.size() > MAX_LISTENERS_PER_SERVER) {
		throw InvalidInputException("%s accepts at most %llu listen URIs", function_name,
		                            static_cast<uint64_t>(MAX_LISTENERS_PER_SERVER));
	}

	// transport is either one value applied to every URI or a list parallel to the URIs.
	vector<string> transports;
	if (input.named_parameters.find("transport") != input.named_parameters.end()) {
		transports = StringOrStringList(input.named_parameters["transport"], "transport");
		if (transports.size() != 1 && transports.size() != listen_uris.size()) {
			throw InvalidInputException("transport must be a single value or a list with one entry per listen URI "
			                            "(%llu URIs, %llu transports)",
			                            static_cast<uint64_t>(listen_uris.size()),
			                            static_cast<uint64_t>(transports.size()));
		}
	}

	for (idx_t i = 0; i < listen_uris.size(); i++) {
		if (listen_uris[i].empty()) {
			throw InvalidInputException("Invalid OTLP listen URI specified");
		}
		OtlpListenerSpec spec;
		spec.uri = OtlpUri(listen_uris[i]);
		// Strict scheme/function binding: otlp_serve serves the OTLP protocol (otlp:),
		// otap_serve serves OTAP/Arrow (otap:). A mismatched scheme is rejected so the
		// two functions never overlap and URIs are never mixed across them.
		if (spec.uri.Scheme() != required_scheme) {
			throw InvalidInputException("%s requires an '%s:' URI, but got an '%s:' URI", function_name,
			                            required_scheme, spec.uri.Scheme());
		}
		for (auto &existing : bind_data->listeners) {
			if (existing.uri.CanonicalUri() == spec.uri.CanonicalUri()) {
				throw InvalidInputException("%s listen URIs must be distinct (got %s twice)", function_name,
				                            spec.uri.Uri());
			}
		}

		// Transport + which gRPC services to register. otap: is always OTAP/Arrow over
		// gRPC. otlp: is HTTP by default; transport := 'grpc' runs OTLP/gRPC unary
		// Export on the same otlp: scheme (no otap: mixing). The two gRPC listener kinds are
		// disjoint: otlp: registers only the unary services, otap: only the Arrow ones.
		string transport_opt;
		if (!transports.empty()) {
			transport_opt = StringUtil::Lower(transports.size() == 1 ? transports[0] : transports[i]);
		}
		if (required_scheme == "otap") {
			if (!transport_opt.empty() && transport_opt != "grpc") {
				throw InvalidInputException("otap_serve is gRPC-only; transport must be 'grpc' or omitted (got '%s')",
				                            transport_opt);
			}
			spec.transport = OtlpTransport::GRPC;
			spec.grpc_service_flags = OTLP_GRPC_SERVICE_OTAP_ARROW;
		} else if (transport_opt.empty() || transport_opt == "http") {
			spec.transport = OtlpTransport::HTTP;
			spec.grpc_service_flags = 0;
		} else if (transport_opt == "grpc") {
			spec.transport = OtlpTransport::GRPC;
			spec.grpc_service_flags = OTLP_GRPC_SERVICE_OTLP_UNARY;
		} else {
			throw InvalidInputException("transport must be 'http' or 'grpc' (got '%s')", transport_opt);
		}
		bind_data->listeners.push_back(std::move(spec));
	}

	auto &config = bind_data->config;
	auto &params = input.named_parameters;
	for (auto &option : STRING_OPTIONS) {
		auto it = params.find(option.name);
		if (it != params.end()) {
			config.*option.field = it->second.GetValue<string>();
			if (option.required_non_empty && (config.*option.field).empty()) {
				throw InvalidInputException("%s must not be empty", option.name);
			}
		}
	}
	for (auto &option : BOOL_OPTIONS) {
		auto it = params.find(option.name);
		if (it != params.end()) {
			config.*option.field = it->second.GetValue<bool>();
		}
	}
	for (auto &option : NUMERIC_OPTIONS) {
		auto it = params.find(option.name);
		if (it == params.end()) {
			continue;
		}
		auto value = it->second.GetValue<int64_t>();
		if (value <= 0) {
			throw InvalidInputException("%s must be greater than zero", option.name);
		}
		if (option.unsigned_field) {
			config.*option.unsigned_field = static_cast<idx_t>(value);
		} else {
			config.*option.signed_field = value;
		}
	}
	for (auto &option : PROMOTE_OPTIONS) {
		auto it = params.find(option.first);
		if (it == params.end()) {
			continue;
		}
		for (auto key : StringUtil::Split(it->second.GetValue<string>(), ',')) {
			StringUtil::Trim(key);
			if (!key.empty()) {
				(config.promote.*option.second).push_back(key);
			}
		}
	}

	auto other_host = params.find("allow_other_hostname");
	if (other_host == params.end() || !other_host->second.GetValue<bool>()) {
		for (auto &spec : bind_data->listeners) {
			if (!spec.uri.IsLocal()) {
				throw InvalidInputException("Only localhost is allowed as an OTLP hostname by default; set "
				                            "allow_other_hostname=true to override");
			}
		}
	}
	if (!config.disable_auth) {
		// Anonymous mode needs no token; only mint one when auth is enforced and none was given.
		if (params.find("token") == params.end()) {
			config.token = OtlpServer::GenerateRandomToken(*context.db);
		}
		OtlpServer::ValidateToken(config.token);
	}
	// Parquet export is a standalone store, not a mirror of a catalog table, so the two targets
	// are mutually exclusive.
	if (!config.parquet_export_path.empty() && !config.catalog_name.empty()) {
		throw InvalidInputException("parquet_export_path cannot be combined with a catalog target; use a catalog "
		                            "mode for transactional ingest or parquet_export_path for plain Parquet export");
	}

	for (auto name : {"listen_uri", "listen_url", "auth_token", "schema_name"}) {
		names.emplace_back(name);
	}
	for (auto *table : OTLP_SIGNAL_TABLES) {
		names.emplace_back(string(table + strlen("otlp_")) + "_table");
	}
	names.emplace_back("catalog_name");
	names.emplace_back("transport");
	return_types.assign(names.size(), LogicalType::VARCHAR);

	return std::move(bind_data);
}

static unique_ptr<FunctionData> OtlpServeBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	return OtlpServeBindImpl(context, input, return_types, names, "otlp:localhost:4318", "otlp");
}

static unique_ptr<FunctionData> OtapServeBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	return OtlpServeBindImpl(context, input, return_types, names, "otap:localhost:4317", "otap");
}

static void OtlpServe(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<OtlpStartStopFunctionData>();
	if (bind_data.finished) {
		return;
	}

	auto &state = OtlpStorageExtensionInfo::GetState(*context.db);
	state.CreateServer(context, bind_data.listeners, bind_data.config);
	// Mark finished as soon as the server exists: if any SetValue below throws, a
	// re-scan must not try to create the (already running) server again.
	bind_data.finished = true;

	// One row per listener; every listener feeds the same server.
	idx_t row = 0;
	for (auto &spec : bind_data.listeners) {
		idx_t col = 0;
		for (auto &value : {spec.uri.Uri(), spec.uri.Http(), bind_data.config.token, bind_data.config.schema_name}) {
			output.SetValue(col++, row, value);
		}
		for (auto *table : OTLP_SIGNAL_TABLES) {
			output.SetValue(col++, row, table);
		}
		output.SetValue(col++, row, bind_data.config.catalog_name);
		output.SetValue(col++, row, OtlpTransportName(spec.transport));
		row++;
	}
	output.SetChildCardinality(row);
}

static TableFunctionSet BuildServeFunctionSet(const string &name, table_function_bind_t bind) {
	// DuckDB 2.0 declares optional named parameters as a typed "**kwargs" schema: an option the call
	// leaves out is not passed, so the bind keeps its own defaults exactly as with named_parameters.
	auto make_overload = [&](const vector<LogicalType> &arguments) {
		auto fun = TableFunction(Identifier(name), arguments, OtlpServe, bind);
		fun.GetSignature().WithTypedKwargs("options", [](TypedKwargs &options) {
			for (auto &option : STRING_OPTIONS) {
				options.Add(option.name, LogicalType::VARCHAR);
			}
			for (auto &option : BOOL_OPTIONS) {
				options.Add(option.name, LogicalType::BOOLEAN);
			}
			options.Add("allow_other_hostname", LogicalType::BOOLEAN);
			// 'http' (default) or 'grpc', as one value or a list parallel to the listen URIs.
			options.Add("transport", LogicalType::ANY);
			for (auto &option : NUMERIC_OPTIONS) {
				options.Add(option.name, option.unsigned_field ? LogicalType::UBIGINT : LogicalType::BIGINT);
			}
			for (auto &option : PROMOTE_OPTIONS) {
				options.Add(option.first, LogicalType::VARCHAR);
			}
		});
		return fun;
	};
	TableFunctionSet set {Identifier(name)};
	set.AddFunction(make_overload({LogicalType::VARCHAR}));
	// Several listeners (e.g. HTTP + gRPC) feeding one server: one buffer set, one sealer.
	set.AddFunction(make_overload({LogicalType::LIST(LogicalType::VARCHAR)}));
	set.AddFunction(make_overload({}));
	return set;
}

//! Everything otlp_serve and otap_serve do not share, so the descriptions themselves can.
struct ServeDocs {
	//! SQL name, e.g. "otlp_serve".
	const char *function_name;
	//! Wire protocol as a reader should think of it, e.g. "OTLP".
	const char *protocol;
	//! Listen URI used when the function is called with no argument.
	const char *default_uri;
	//! Sentence about transport selection and the scheme this function is bound to.
	const char *transport_note;
	const char *uri_example;
	const char *list_example;
};

//! One description per overload of the set: duckdb_functions() matches them on the positional
//! argument types, which is why each repeats them.
//!
//! These two are the only functions here that take named parameters, and that limits what can
//! be documented about them. duckdb_functions() renders positional and named parameters as one
//! `parameters` list, and once a description's parameter_names is non-empty every entry past
//! its end becomes "colN" -- so naming the single positional argument would replace every real
//! named-parameter name with a placeholder. Nor can the named names be appended here: they come
//! from an unordered_map that the catalog copies, so the order this code sees is not the order
//! duckdb_functions() will print. Leaving parameter_names empty keeps DuckDB's fallback, which
//! lists every named parameter by name and spells only the positional argument col0.
static vector<FunctionDescription> ServeDescriptions(const ServeDocs &docs) {
	const vector<string> categories = {"opentelemetry", "ingest"};
	// True of all three overloads, and the part a caller most needs to know before pointing an
	// exporter at this: an accepted request is not yet a committed row.
	const string durability =
	    " Rows land in the catalog named by the `catalog` parameter -- an attached database, a DuckLake or Iceberg "
	    "catalog among them -- or in the default catalog when it is unset. They are buffered in memory and "
	    "group-committed (sealed) in batches, so an accepted request is not durable yet: otlp_flush() commits on "
	    "demand and otlp_stop() commits before returning. Returns one row per listener.";
	vector<FunctionDescription> descriptions;
	descriptions.push_back(
	    OtlpDoc({}, {},
	            StringUtil::Format("Start a live %s ingest server on the default listen URI %s.%s%s", docs.protocol,
	                               docs.default_uri, docs.transport_note, durability),
	            {StringUtil::Format("SELECT * FROM %s(token := 'replace-with-a-real-token');", docs.function_name)},
	            categories));
	descriptions.push_back(OtlpDoc({LogicalType::VARCHAR}, {},
	                               StringUtil::Format("Start a live %s ingest server on one listen URI.%s%s",
	                                                  docs.protocol, docs.transport_note, durability),
	                               {docs.uri_example}, categories));
	descriptions.push_back(
	    OtlpDoc({LogicalType::LIST(LogicalType::VARCHAR)}, {},
	            StringUtil::Format("Start a live %s ingest server fed by several listeners -- one buffer set and one "
	                               "sealer behind all of them, so any one of the URIs names the whole server to "
	                               "otlp_stop() and otlp_flush().%s%s",
	                               docs.protocol, docs.transport_note, durability),
	            {docs.list_example}, categories));
	return descriptions;
}

CreateTableFunctionInfo OtlpServeFunction::GetFunction() {
	ServeDocs docs;
	docs.function_name = "otlp_serve";
	docs.protocol = "OTLP";
	docs.default_uri = "otlp:localhost:4318";
	docs.transport_note = " The transport is OTLP/HTTP by default (POST /v1/logs, /v1/traces, /v1/metrics); "
	                      "transport := 'grpc' serves OTLP/gRPC unary Export instead. Only the otlp: scheme is "
	                      "accepted.";
	docs.uri_example = "SELECT * FROM otlp_serve('otlp:localhost:4318', token := 'replace-with-a-real-token');";
	docs.list_example = "SELECT * FROM otlp_serve(['otlp:localhost:4318', 'otlp:localhost:4317'], "
	                    "transport := ['http', 'grpc'], token := 'replace-with-a-real-token');";
	return OtlpDocumented(BuildServeFunctionSet("otlp_serve", OtlpServeBind), ServeDescriptions(docs));
}

CreateTableFunctionInfo OtapServeFunction::GetFunction() {
	ServeDocs docs;
	docs.function_name = "otap_serve";
	docs.protocol = "OTAP/Arrow";
	docs.default_uri = "otap:localhost:4317";
	docs.transport_note = " The transport is always gRPC: the canonical Arrow{Logs,Traces,Metrics}Service "
	                      "bidirectional streams, which are disjoint from the services otlp_serve registers. Only "
	                      "the otap: scheme is accepted.";
	docs.uri_example = "SELECT * FROM otap_serve('otap:localhost:4317', token := 'replace-with-a-real-token');";
	docs.list_example = "SELECT * FROM otap_serve(['otap:localhost:4317', 'otap:localhost:14317'], "
	                    "token := 'replace-with-a-real-token');";
	return OtlpDocumented(BuildServeFunctionSet("otap_serve", OtapServeBind), ServeDescriptions(docs));
}

struct OtlpStopFunctionData : public TableFunctionData {
	//! True for the no-argument overload: stop every registered server rather than the one
	//! named by a URI. This is the only way to drain a server whose listen URI the caller does
	//! not know -- notably one started out-of-band over Quack, which the daemon's own
	//! otlp_stop('<its uri>') never reached, so its buffered rows were dropped unsealed at
	//! database teardown.
	bool stop_all = false;
	OtlpUri listen_uri;
	// Side-effecting control-plane call that also returns a status row. DuckDB may rescan a
	// table function, so the stop runs once (`executed`) and the rows it produced are then
	// paged out; a rescan must not stop servers a second time.
	bool executed = false;
	idx_t offset = 0;
	vector<OtlpStorageExtensionInfo::StopResult> results;
};

static unique_ptr<FunctionData> OtlpStopBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<OtlpStopFunctionData>();
	if (input.inputs.empty()) {
		bind_data->stop_all = true;
	} else {
		auto &uri_value = input.inputs[0];
		if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
			throw InvalidInputException("Invalid OTLP listen URI specified");
		}
		bind_data->listen_uri = OtlpUri(uri_value.GetValue<string>());
	}
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);
	// Rows still buffered after the final shutdown drain failed (dropped). 0 on a clean stop.
	// Lets the daemon / an orchestrator detect a data-dropping shutdown (review finding M4).
	names.emplace_back("dropped_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	return std::move(bind_data);
}

//! The status text for one stopped server.
static string StopStatusMessage(const OtlpStorageExtensionInfo::StopResult &stop) {
	auto uris = StringUtil::Join(stop.listen_uris, ", ");
	// Report the error and the row count together: a failed stop that dropped nothing and one
	// that lost a buffer are very different events, and the daemon exit code keys off the latter.
	if (!stop.error.empty()) {
		return StringUtil::Format("Failed to stop %s: %s; dropped %llu un-sealed buffered rows", uris, stop.error,
		                          static_cast<uint64_t>(stop.dropped_rows));
	}
	if (stop.dropped_rows > 0) {
		return StringUtil::Format("Stopped listening on %s; dropped %llu un-sealed buffered rows", uris,
		                          static_cast<uint64_t>(stop.dropped_rows));
	}
	return StringUtil::Format("Stopped listening on %s", uris);
}

static void OtlpStop(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<OtlpStopFunctionData>();
	auto &state = OtlpStorageExtensionInfo::GetState(*context.db);
	if (!bind_data.executed) {
		bind_data.executed = true;
		if (bind_data.stop_all) {
			bind_data.results = state.StopAllServersGraceful();
		} else {
			bind_data.results.push_back(state.StopServer(context, bind_data.listen_uri));
		}
	}
	// Nothing to stop. Answer with one informational row rather than an empty result, so both
	// overloads always say something and `SELECT * FROM otlp_stop()` is never silent.
	if (bind_data.results.empty()) {
		if (bind_data.offset > 0) {
			return;
		}
		bind_data.offset = 1;
		output.SetValue(0, 0, Value("No OTLP servers are running"));
		output.SetValue(1, 0, Value::UBIGINT(0));
		output.SetChildCardinality(1);
		return;
	}
	idx_t row = 0;
	while (bind_data.offset < bind_data.results.size() && row < STANDARD_VECTOR_SIZE) {
		auto &stop = bind_data.results[bind_data.offset];
		// Only the URI form can miss: the stop-all form reports whatever it found.
		auto status = stop.found ? StopStatusMessage(stop)
		                         : StringUtil::Format("No server found listening on %s", bind_data.listen_uri.Uri());
		output.SetValue(0, row, Value(status));
		output.SetValue(1, row, Value::UBIGINT(stop.dropped_rows));
		bind_data.offset++;
		row++;
	}
	output.SetChildCardinality(row);
}

CreateTableFunctionInfo OtlpStopFunction::GetFunction() {
	TableFunctionSet set("otlp_stop");
	// No argument: stop every server on this instance. Added alongside the URI form rather
	// than replacing it because a targeted stop is still the common case.
	set.AddFunction(TableFunction("otlp_stop", {}, OtlpStop, OtlpStopBind));
	set.AddFunction(TableFunction("otlp_stop", {LogicalType::VARCHAR}, OtlpStop, OtlpStopBind));
	const vector<string> categories = {"opentelemetry", "ingest"};
	return OtlpDocumented(
	    std::move(set),
	    {OtlpDoc({}, {},
	             "Stop every live ingest server on this database, committing each one's buffered rows before "
	             "returning. One row per stopped server. Use this before closing a database you did not start "
	             "every server on: a plain close drops buffered rows.",
	             {"SELECT * FROM otlp_stop();"}, categories),
	     OtlpDoc({LogicalType::VARCHAR}, {"listen_uri"},
	             "Stop the live ingest server that owns listener `listen_uri`, including its other listeners, "
	             "committing its buffered rows before returning.",
	             {"SELECT * FROM otlp_stop('otlp:localhost:4318');"}, categories)});
}

struct OtlpServerListFunctionData : public TableFunctionData {
	bool initialized = false;
	idx_t offset = 0;
	vector<vector<Value>> rows;
};

static unique_ptr<FunctionData> OtlpServerListBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	for (auto &column : OtlpServer::ListColumns()) {
		names.emplace_back(column.name);
		return_types.emplace_back(column.type);
	}
	return make_uniq<OtlpServerListFunctionData>();
}

static void OtlpServerList(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->CastNoConst<OtlpServerListFunctionData>();
	// Snapshot once, then page out STANDARD_VECTOR_SIZE rows per scan.
	if (!bind_data.initialized) {
		bind_data.rows = OtlpStorageExtensionInfo::GetState(*context.db).ListServers();
		bind_data.initialized = true;
	}
	idx_t row = 0;
	for (; bind_data.offset < bind_data.rows.size() && row < STANDARD_VECTOR_SIZE; bind_data.offset++, row++) {
		auto &values = bind_data.rows[bind_data.offset];
		for (idx_t col = 0; col < values.size(); col++) {
			output.SetValue(col, row, values[col]);
		}
	}
	output.SetChildCardinality(row);
}

CreateTableFunctionInfo OtlpServerListFunction::GetFunction() {
	return OtlpDocumented(
	    TableFunction("otlp_server_list", {}, OtlpServerList, OtlpServerListBind),
	    {OtlpDoc({}, {},
	             "List every live ingest listener on this database with its server's target catalog, transport and "
	             "live counters: rows received, buffered rows and bytes, seal totals and failures, last seal age "
	             "and last error.",
	             {"SELECT listen_uri, transport, buffered_rows, seals_total FROM otlp_server_list();"},
	             {"opentelemetry", "monitoring"})});
}

struct OtlpSealListFunctionData : public TableFunctionData {
	bool initialized = false;
	idx_t offset = 0;
	vector<OtlpStorageExtensionInfo::SealSnapshot> snapshots;
};

static unique_ptr<FunctionData> OtlpSealListBind(ClientContext &context, TableFunctionBindInput &input,
                                                 vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {Identifier("listen_uri"),           Identifier("seal_sequence"),
	         Identifier("started_unix_ms"),      Identifier("completed_unix_ms"),
	         Identifier("duration_ms"),          Identifier("append_duration_ms"),
	         Identifier("commit_duration_ms"),   Identifier("rows_committed"),
	         Identifier("admitted_bytes"),       Identifier("success"),
	         Identifier("seals_total"),          Identifier("seal_failures_total"),
	         Identifier("committed_rows_total"), Identifier("error")};
	return_types = {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::BIGINT,  LogicalType::BIGINT,
	                LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::BIGINT,  LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::BOOLEAN, LogicalType::UBIGINT, LogicalType::UBIGINT,
	                LogicalType::UBIGINT, LogicalType::VARCHAR};
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
		output.SetValue(5, row, Value::BIGINT(event.append_duration_ms));
		output.SetValue(6, row, Value::BIGINT(event.commit_duration_ms));
		output.SetValue(7, row, Value::UBIGINT(event.rows_committed));
		output.SetValue(8, row, Value::UBIGINT(event.admitted_bytes_committed));
		output.SetValue(9, row, Value::BOOLEAN(event.success));
		output.SetValue(10, row, Value::UBIGINT(event.seals_total));
		output.SetValue(11, row, Value::UBIGINT(event.seal_failures_total));
		output.SetValue(12, row, Value::UBIGINT(event.committed_rows_total));
		output.SetValue(13, row, event.error.empty() ? Value(LogicalType::VARCHAR) : Value(event.error));
		row++;
		bind_data.offset++;
	}
	output.SetChildCardinality(row);
}

CreateTableFunctionInfo OtlpSealListFunction::GetFunction() {
	return OtlpDocumented(
	    TableFunction("otlp_seal_list", {}, OtlpSealList, OtlpSealListBind),
	    {OtlpDoc({}, {},
	             "List the recent seal (group-commit) attempts of every live ingest server, with append and commit "
	             "timings, rows and admitted bytes committed, and the error of any that failed.",
	             {"SELECT listen_uri, seal_sequence, rows_committed, duration_ms FROM otlp_seal_list();"},
	             {"opentelemetry", "monitoring"})});
}

struct OtlpFlushFunctionData : public TableFunctionData {
	// See OtlpStartStopFunctionData: otlp_flush is a side-effecting table function
	// that returns a row, and the guard makes DuckDB rescan safe.
	bool finished = false;
	OtlpUri listen_uri;
};

static unique_ptr<FunctionData> OtlpFlushBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto bind_data = make_uniq<OtlpFlushFunctionData>();
	auto &uri_value = input.inputs[0];
	if (uri_value.IsNull() || uri_value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid OTLP listen URI specified");
	}
	bind_data->listen_uri = OtlpUri(uri_value.GetValue<string>());
	names.emplace_back("status");
	return_types.emplace_back(LogicalType::VARCHAR);
	names.emplace_back("sealed_rows");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("seals_total");
	return_types.emplace_back(LogicalType::UBIGINT);
	names.emplace_back("error");
	return_types.emplace_back(LogicalType::VARCHAR);
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
	output.SetChildCardinality(1);
	bind_data.finished = true;
}

CreateTableFunctionInfo OtlpFlushFunction::GetFunction() {
	return OtlpDocumented(
	    TableFunction("otlp_flush", {LogicalType::VARCHAR}, OtlpFlush, OtlpFlushBind),
	    {OtlpDoc({LogicalType::VARCHAR}, {"listen_uri"},
	             "Force a synchronous seal (group-commit) on the live ingest server that owns listener "
	             "`listen_uri`, so rows accepted so far become readable in the target catalog. The server keeps "
	             "running.",
	             {"SELECT * FROM otlp_flush('otlp:localhost:4318');"}, {"opentelemetry", "ingest"})});
}

} // namespace duckdb
