#include "otlp_start_stop.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

#include "otlp_arrow.hpp"
#include "otlp_registry.hpp"
#include "otlp_server.hpp"
#include "otlp_uri.hpp"

#include <algorithm>
#include <functional>
#include <optional>

namespace duckdb {

using OtlpRows = vector<vector<Value>>;

struct OtlpControlBindData : TableFunctionData {
	explicit OtlpControlBindData(std::function<OtlpRows(ClientContext &)> run_p) : run(std::move(run_p)) {
	}

	std::function<OtlpRows(ClientContext &)> run;
};

struct OtlpControlState : GlobalTableFunctionState {
	OtlpRows rows;
	idx_t offset = 0;
};

static unique_ptr<GlobalTableFunctionState> OtlpControlInit(ClientContext &context, TableFunctionInitInput &input) {
	auto state = make_uniq<OtlpControlState>();
	state->rows = input.bind_data->Cast<OtlpControlBindData>().run(context);
	return state;
}

static void OtlpControlScan(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &state = input.global_state->Cast<OtlpControlState>();
	idx_t row = 0;
	for (; state.offset < state.rows.size() && row < STANDARD_VECTOR_SIZE; state.offset++, row++) {
		const auto &values = state.rows[state.offset];
		for (idx_t col = 0; col < values.size(); col++) {
			output.data[col].Append(values[col]);
		}
	}
}

static TableFunction OtlpControlFunction(Identifier name, FunctionSignature signature, table_function_bind_t bind) {
	return TableFunction(std::move(name), std::move(signature), OtlpControlScan, bind, OtlpControlInit);
}

//! Bound on listeners per server.
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
    {"disable_auth", &OtlpServerConfig::disable_auth},
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

	vector<OtlpListenerSpec> listeners;
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
		if (spec.uri.Scheme() != required_scheme) {
			throw InvalidInputException("%s requires an '%s:' URI, but got an '%s:' URI", function_name,
			                            required_scheme, spec.uri.Scheme());
		}
		for (auto &existing : listeners) {
			if (existing.uri.CanonicalUri() == spec.uri.CanonicalUri()) {
				throw InvalidInputException("%s listen URIs must be distinct (got %s twice)", function_name,
				                            spec.uri.Uri());
			}
		}

		// The gRPC service sets are disjoint: otlp: registers only the unary services, otap: only the Arrow ones.
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
		listeners.push_back(std::move(spec));
	}

	OtlpServerConfig config;
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
		for (auto &spec : listeners) {
			if (!spec.uri.IsLocal()) {
				throw InvalidInputException("Only localhost is allowed as an OTLP hostname by default; set "
				                            "allow_other_hostname=true to override");
			}
		}
	}
	if (!config.disable_auth) {
		if (params.find("token") == params.end()) {
			config.token = OtlpServer::GenerateRandomToken(*context.db);
		}
		OtlpServer::ValidateToken(config.token);
	}
	if (!config.parquet_export_path.empty() && !config.catalog_name.empty()) {
		throw InvalidInputException("parquet_export_path cannot be combined with a catalog target; use a catalog "
		                            "mode for transactional ingest or parquet_export_path for plain Parquet export");
	}

	for (auto name : {"listen_uri", "listen_url", "auth_token", "schema_name"}) {
		names.emplace_back(name);
	}
	const bool include_signal_tables = required_scheme == "otap";
	if (include_signal_tables) {
		for (auto *table : OTLP_SIGNAL_TABLES) {
			names.emplace_back(string(table + strlen("otlp_")) + "_table");
		}
	}
	names.emplace_back("catalog_name");
	names.emplace_back("transport");
	return_types.assign(names.size(), LogicalType::VARCHAR);

	return make_uniq<OtlpControlBindData>(
	    [listeners = std::move(listeners), config = std::move(config), include_signal_tables](ClientContext &context) {
		    OtlpServerRegistry::Get(*context.db).Start(context, listeners, config);
		    OtlpRows rows;
		    for (auto &spec : listeners) {
			    vector<Value> row {spec.uri.Uri(), spec.uri.Http(), config.token, config.schema_name};
			    if (include_signal_tables) {
				    for (auto *table : OTLP_SIGNAL_TABLES) {
					    row.emplace_back(table);
				    }
			    }
			    row.emplace_back(config.catalog_name);
			    row.emplace_back(OtlpTransportName(spec.transport));
			    rows.push_back(std::move(row));
		    }
		    return rows;
	    });
}

static unique_ptr<FunctionData> OtlpServeBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	return OtlpServeBindImpl(context, input, return_types, names, "otlp:localhost:4318", "otlp");
}

static unique_ptr<FunctionData> OtapServeBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	return OtlpServeBindImpl(context, input, return_types, names, "otap:localhost:4317", "otap");
}

static TableFunctionSet BuildServeFunctionSet(const string &name, table_function_bind_t bind) {
	// Options are a typed **kwargs schema: an omitted option is not passed, so the bind keeps its defaults.
	auto make_overload = [&](FunctionSignature signature) {
		auto fun = OtlpControlFunction(Identifier(name), std::move(signature), bind);
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
	set.AddFunction(make_overload(FunctionSignature().AddPositionalOnly("listen_uri", LogicalType::VARCHAR)));
	set.AddFunction(
	    make_overload(FunctionSignature().AddPositionalOnly("listen_uris", LogicalType::LIST(LogicalType::VARCHAR))));
	set.AddFunction(make_overload({}));
	return set;
}

//! Everything otlp_serve and otap_serve do not share, so the descriptions themselves can.
struct ServeDocs {
	const char *function_name;
	const char *protocol;
	//! Listen URI used when the function is called with no argument.
	const char *default_uri;
	//! Sentence about transport selection and the scheme this function is bound to.
	const char *transport_note;
	const char *uri_example;
	const char *list_example;
};

//! One description per overload: duckdb_functions() matches them on the positional argument types.
//! parameter_names stay empty; the names come from the signature and its **kwargs schema.
static vector<FunctionDescription> ServeDescriptions(const ServeDocs &docs) {
	const vector<string> categories = {"opentelemetry", "ingest"};
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

static std::optional<OtlpUri> BindOptionalUri(const TableFunctionBindInput &input) {
	if (input.inputs.empty()) {
		return std::nullopt;
	}
	auto &value = input.inputs[0];
	if (value.IsNull() || value.GetValue<string>().empty()) {
		throw InvalidInputException("Invalid OTLP listen URI specified");
	}
	return OtlpUri(value.GetValue<string>());
}

static TableFunctionSet OptionalUriFunctions(Identifier name, table_function_bind_t bind) {
	TableFunctionSet set(name);
	set.AddFunction(OtlpControlFunction(name, {}, bind));
	set.AddFunction(
	    OtlpControlFunction(name, FunctionSignature().AddPositionalOnly("listen_uri", LogicalType::VARCHAR), bind));
	return set;
}

static constexpr const char *NO_SERVERS = "No OTLP servers are running";

//! Close an unregistered server. Stop-all reports close errors and keeps draining the other servers.
static vector<Value> StopServer(OtlpServer &server, bool catch_errors) {
	vector<string> listen_uris;
	for (auto &listener : server.Listeners()) {
		listen_uris.push_back(listener->Uri().Uri());
	}
	string error;
	try {
		server.Close();
	} catch (std::exception &ex) {
		if (!catch_errors) {
			throw;
		}
		error = ErrorData(ex).RawMessage();
	}
	auto uris = StringUtil::Join(listen_uris, ", ");
	auto dropped_rows = server.ShutdownDroppedRows();
	string status;
	if (!error.empty()) {
		status = StringUtil::Format("Failed to stop %s: %s; dropped %llu un-sealed buffered rows", uris, error,
		                            static_cast<uint64_t>(dropped_rows));
	} else if (dropped_rows > 0) {
		status = StringUtil::Format("Stopped listening on %s; dropped %llu un-sealed buffered rows", uris,
		                            static_cast<uint64_t>(dropped_rows));
	} else {
		status = StringUtil::Format("Stopped listening on %s", uris);
	}
	return {Value(status), Value::UBIGINT(dropped_rows)};
}

static unique_ptr<FunctionData> OtlpStopBind(ClientContext &, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"status", "dropped_rows"};
	return_types = {LogicalType::VARCHAR, LogicalType::UBIGINT};
	return make_uniq<OtlpControlBindData>([uri = BindOptionalUri(input)](ClientContext &context) {
		auto &registry = OtlpServerRegistry::Get(*context.db);
		auto servers = uri ? vector<shared_ptr<OtlpServer>> {registry.Remove(*uri)} : registry.RemoveAll();
		OtlpRows rows;
		for (auto &server : servers) {
			rows.push_back(StopServer(*server, /*catch_errors=*/!uri));
		}
		if (rows.empty()) {
			rows.push_back({Value(NO_SERVERS), Value::UBIGINT(0)});
		}
		return rows;
	});
}

CreateTableFunctionInfo OtlpStopFunction::GetFunction() {
	const vector<string> categories = {"opentelemetry", "ingest"};
	return OtlpDocumented(
	    OptionalUriFunctions("otlp_stop", OtlpStopBind),
	    {OtlpDoc({}, {},
	             "Stop every live ingest server on this database, committing each one's buffered rows before "
	             "returning. Returns one row per server, or one informational row if no servers are running.",
	             {"SELECT * FROM otlp_stop();"}, categories),
	     OtlpDoc({LogicalType::VARCHAR}, {"listen_uri"},
	             "Stop the live ingest server that owns listener `listen_uri`, including its other listeners, "
	             "committing its buffered rows before returning. An unknown URI raises an error.",
	             {"SELECT * FROM otlp_stop('otlp:localhost:4318');"}, categories)});
}

static unique_ptr<FunctionData> OtlpServerListBind(ClientContext &, TableFunctionBindInput &,
                                                   vector<LogicalType> &return_types, vector<Identifier> &names) {
	for (auto &column : OtlpServer::ListColumns()) {
		names.emplace_back(column.name);
		return_types.emplace_back(column.type);
	}
	return make_uniq<OtlpControlBindData>([](ClientContext &context) {
		OtlpRows rows;
		for (auto &server : OtlpServerRegistry::Get(*context.db).All()) {
			for (auto &listener : server->Listeners()) {
				vector<Value> row;
				for (auto &column : OtlpServer::ListColumns()) {
					row.push_back(column.get(*server, *listener));
				}
				rows.push_back(std::move(row));
			}
		}
		// Column 0 is listen_uri.
		std::sort(rows.begin(), rows.end(), [](const vector<Value> &a, const vector<Value> &b) {
			return StringValue::Get(a[0]) < StringValue::Get(b[0]);
		});
		return rows;
	});
}

CreateTableFunctionInfo OtlpServerListFunction::GetFunction() {
	return OtlpDocumented(
	    OtlpControlFunction("otlp_server_list", {}, OtlpServerListBind),
	    {OtlpDoc({}, {},
	             "List every live ingest listener on this database with its server's target catalog, transport and "
	             "live counters: rows received, buffered rows and bytes, seal totals and failures, last seal age "
	             "and last error.",
	             {"SELECT listen_uri, transport, buffered_rows, seals_total FROM otlp_server_list();"},
	             {"opentelemetry", "monitoring"})});
}

static unique_ptr<FunctionData> OtlpFlushBind(ClientContext &, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<Identifier> &names) {
	names = {"status", "sealed_rows", "seals_total", "error"};
	return_types = {LogicalType::VARCHAR, LogicalType::UBIGINT, LogicalType::UBIGINT, LogicalType::VARCHAR};
	return make_uniq<OtlpControlBindData>([uri = BindOptionalUri(input)](ClientContext &context) {
		auto &registry = OtlpServerRegistry::Get(*context.db);
		auto servers = uri ? vector<shared_ptr<OtlpServer>> {registry.Find(*uri)} : registry.All();
		OtlpRows rows;
		for (auto &server : servers) {
			idx_t sealed_rows = 0;
			string error;
			try {
				sealed_rows = server->FlushNow().rows;
			} catch (std::exception &ex) {
				error = ex.what();
			} catch (...) {
				error = "unknown (non-std) exception during flush";
			}
			const bool success = error.empty();
			rows.push_back({Value(success ? "sealed" : "error"), Value::UBIGINT(sealed_rows),
			                Value::UBIGINT(server->SealsTotal()),
			                success ? Value(LogicalType::VARCHAR) : Value(error)});
		}
		if (rows.empty()) {
			rows.push_back({Value(NO_SERVERS), Value::UBIGINT(0), Value::UBIGINT(0), Value(LogicalType::VARCHAR)});
		}
		return rows;
	});
}

CreateTableFunctionInfo OtlpFlushFunction::GetFunction() {
	const vector<string> categories = {"opentelemetry", "ingest"};
	return OtlpDocumented(
	    OptionalUriFunctions("otlp_flush", OtlpFlushBind),
	    {OtlpDoc({}, {},
	             "Force a synchronous seal (group-commit) on every live ingest server on this database. "
	             "Returns one row per server, or one informational row if no servers are running.",
	             {"SELECT * FROM otlp_flush();"}, categories),
	     OtlpDoc({LogicalType::VARCHAR}, {"listen_uri"},
	             "Force a synchronous seal (group-commit) on the live ingest server that owns listener "
	             "`listen_uri`, so rows accepted so far become readable in the target catalog. The server keeps "
	             "running. An unknown URI raises an error.",
	             {"SELECT * FROM otlp_flush('otlp:localhost:4318');"}, categories)});
}

} // namespace duckdb
